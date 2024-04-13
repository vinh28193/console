import random
import time
from datetime import timedelta, datetime
from pathlib import Path
from typing import Optional, Type, List, Tuple, Union

from consoles.conf import settings
from pandas import concat, DataFrame
from fasttraders.constants import (
    DATETIME_PRINT_FORMAT,
    DEFAULT_DATAFRAME_COLUMNS
)
from fasttraders.data.converter import (
    trades_list_to_df,
    trades_df_remove_duplicates, ohlcv_to_dataframe, clean_ohlcv_dataframe
)
from fasttraders.enums import CandleType
from fasttraders.log import logger
from fasttraders.ultis.timeframe import format_ms_time, dt_ts, dt_now
from fasttraders.ultis.timerange import TimeRange
from .handler import DataHandler

# MOCK
MARKET_PAIRS = [
    "ADA/BTC",
    "BAT/BTC",
    "DASH/BTC",
    "ETC/BTC",
    "ETH/BTC",
    "GBYTE/BTC",
    "LSK/BTC",
    "LTC/BTC",
    "NEO/BTC",
    "NXT/BTC",
    "TRX/BTC",
    "STORJ/BTC",
    "QTUM/BTC",
    "WAVES/BTC",
    "VTC/BTC",
    "XLM/BTC",
    "XMR/BTC",
    "XVG/BTC",
    "XRP/BTC",
    "ZEC/BTC",
    "BTC/USDT",
    "LTC/USDT",
    "ETH/USDT"
]
MOCKED_MARKET_PAIRS = dict(zip(MARKET_PAIRS, MARKET_PAIRS))


def generate_mock_trades(num_trades):
    trades = []
    for _ in range(num_trades):
        # Random timestamp within the last 1000000 milliseconds
        timestamp = int(time.time() * 1000) - random.randint(1, 1000000)
        # Random 9-digit trade ID
        trade_id = str(random.randint(100000000, 999999999))
        trade_type = random.choice(['market', 'limit'])
        side = random.choice(['buy', 'sell'])
        # Random price between 1000 and 50000
        price = round(random.uniform(1000, 50000), 2)
        # Random amount between 0.1 and 10
        amount = round(random.uniform(0.1, 10), 2)
        cost = round(price * amount, 2)
        trade = {
            'timestamp': timestamp,
            'id': trade_id,
            'type': trade_type,
            'side': side,
            'price': price,
            'amount': amount,
            'cost': cost
        }
        trades.append(trade)
    return trades


def generate_mock_ohlcv(timeframe, since, limit):
    ohlcv = []
    start_timestamp = since  # Convert since parameter to milliseconds
    for i in range(limit):
        # Convert timeframe to milliseconds
        timestamp = start_timestamp + i * (timeframe * 60000)
        # Random open price between 1000 and 50000
        open_price = round(random.uniform(1000, 50000), 2)
        # Random high price within 100 of open price
        high_price = round(open_price + random.uniform(0, 100), 2)
        # Random low price within 100 of open price
        low_price = round(open_price - random.uniform(0, 100), 2)
        # Random close price between low and high price
        close_price = round(random.uniform(low_price, high_price), 2)
        # Random volume between 1 and 1000
        volume = round(random.uniform(1, 1000), 2)

        ohlcv.append(
            [timestamp, open_price, high_price, low_price, close_price, volume]
        )
    return ohlcv


def create_datadir(datadir: Optional[str]) -> Path:
    if isinstance(datadir, Path):
        return datadir
    folder = Path(datadir)
    if not folder.is_dir():
        folder.mkdir(parents=True)
        logger.info(f'Created data directory: {datadir}')
    return folder


def get_data_handler_class(datatype: str) -> Type[DataHandler]:
    """
    Get datahandler class.
    Could be done using Resolvers, but since this may be called often and
    resolvers
    are rather expensive, doing this directly should improve performance.
    :param datatype: datatype to use.
    :return: Datahandler class
    """

    if datatype == 'json':
        from .jsondatahandler import JsonDataHandler
        return JsonDataHandler
    else:
        raise ValueError(f"No datahandler for datatype {datatype} available.")


def get_data_handler(
    datadir: Union[Path, str], data_format: Optional[str] = None,
    data_handler: Optional[DataHandler] = None
) -> DataHandler:
    """
    :param datadir: Folder to save data
    :param data_format: dataformat to use
    :param data_handler: returns this datahandler if it exists or initializes
    a new one
    """
    datadir = create_datadir(datadir)
    if not data_handler:
        HandlerClass = get_data_handler_class(data_format or 'json')
        data_handler = HandlerClass(datadir)
    return data_handler


def _load_cached_data_for_updating(
    pair: str,
    timeframe: str,
    timerange: Optional[TimeRange],
    data_handler: DataHandler,
    candle_type: CandleType,
    prepend: bool = False,
) -> Tuple[DataFrame, Optional[int], Optional[int]]:
    """
    Load cached data to download more data.
    If timerange is passed in, checks whether data from an before the stored
    data will be
    downloaded.
    If that's the case then what's available should be completely overwritten.
    Otherwise downloads always start at the end of the available data to
    avoid data gaps.
    Note: Only used by download_pair_history().
    """
    start = None
    end = None
    if timerange:
        if timerange.starttype == 'date':
            start = timerange.startdt
        if timerange.stoptype == 'date':
            end = timerange.stopdt

    # Intentionally don't pass timerange in - since we need to load the full
    # dataset.
    data = data_handler.ohlcv_load(
        pair, timeframe=timeframe,
        timerange=None, fill_missing=False,
        drop_incomplete=True, warn_no_data=False,
        candle_type=candle_type
    )
    if not data.empty:
        if not prepend and start and start < data.iloc[0]['date']:
            # Earlier data than existing data requested, redownload all
            data = DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)
        else:
            if prepend:
                end = data.iloc[0]['date']
            else:
                start = data.iloc[-1]['date']
    start_ms = int(start.timestamp() * 1000) if start else None
    end_ms = int(end.timestamp() * 1000) if end else None
    return data, start_ms, end_ms


def _download_pair_history(
    pair: str, *,
    datadir: Union[Path, str],
    timeframe: str = '5m',
    process: str = '',
    new_pairs_days: int = 30,
    data_handler: Optional[DataHandler] = None,
    timerange: Optional[TimeRange] = None,
    candle_type: CandleType,
    erase: bool = False,
    prepend: bool = False,

) -> bool:
    """
    Download latest candles from the exchange for the pair and timeframe
    passed in parameters
    The data is downloaded starting from the last correct data that
    exists in a cache. If timerange starts earlier than the data in the cache,
    the full data will be redownloaded

    :param pair: pair to download
    :param timeframe: Timeframe (e.g "5m")
    :param timerange: range of time to download
    :param candle_type: Any of the enum CandleType (must match trading mode!)
    :param erase: Erase existing data
    :return: bool with success state
    """
    datadir = create_datadir(datadir)
    data_handler = get_data_handler(datadir, data_handler=data_handler)

    try:
        if erase:
            if data_handler.ohlcv_purge(
                pair, timeframe, candle_type=candle_type
            ):
                logger.info(
                    f'Deleting existing data for pair {pair}, {timeframe}, '
                    f'{candle_type}.')

        data, since_ms, until_ms = _load_cached_data_for_updating(
            pair, timeframe, timerange,
            data_handler=data_handler,
            candle_type=candle_type,
            prepend=prepend)
        if not since_ms:
            since_ms = int(
                (datetime.now() - timedelta(days=new_pairs_days)).timestamp()
            ) * 1000
        logger.info(
            f'({process}) - Download history data for "{pair}", {timeframe}, '
            f'{candle_type} and store in {datadir}. '
            f'From {format_ms_time(since_ms)} to '
            f'{format_ms_time(until_ms) if until_ms else "now"}'
        )

        logger.debug(
            "Current Start: %s",
            f"{data.iloc[0]['date']:{DATETIME_PRINT_FORMAT}}"
            if not data.empty else 'None'
        )
        logger.debug(
            "Current End: %s",
            f"{data.iloc[-1]['date']:{DATETIME_PRINT_FORMAT}}"
            if not data.empty else 'None'
        )
        logger.info(f"Getting history of {pair}, timeframe: {timeframe}")
        # Default since_ms to 30 days if nothing is given
        new_data = generate_mock_ohlcv(0.5, since_ms, 10000)  # Todo: fetch on API
        new_dataframe = ohlcv_to_dataframe(
            new_data, timeframe, pair, fill_missing=False, drop_incomplete=True
        )
        if data.empty:
            data = new_dataframe
        else:
            # Run cleaning again to ensure there were no duplicate candles
            # Especially between existing and new data.
            data = clean_ohlcv_dataframe(
                concat([data, new_dataframe], axis=0),
                timeframe, pair,
                fill_missing=False,
                drop_incomplete=False
            )

        logger.debug(
            "New Start: %s",
            f"{data.iloc[0]['date']:{DATETIME_PRINT_FORMAT}}"
            if not data.empty else 'None'
        )
        logger.debug(
            "New End: %s",
            f"{data.iloc[-1]['date']:{DATETIME_PRINT_FORMAT}}"
            if not data.empty else 'None'
        )

        data_handler.ohlcv_store(
            pair, timeframe, data=data, candle_type=candle_type
        )
        return True

    except Exception:
        logger.exception(
            f'Failed to download history data for pair: "{pair}", timeframe: '
            f'{timeframe}.'
        )
        return False


def refresh_backtest_ohlcv_data(
    pairs: List[str], timeframes: List[str],
    datadir: Union[Path, str],
    trading_mode: str,
    timerange: Optional[TimeRange] = None,
    new_pairs_days: int = 30, erase: bool = False,
    data_format: Optional[str] = None,
    prepend: bool = False,

) -> List[str]:
    """
    Refresh stored ohlcv data
    :return: List of pairs that are not available.
    """

    datadir = create_datadir(datadir)
    data_handler = get_data_handler(datadir, data_format)
    candle_type = CandleType.get_default(trading_mode)
    pairs_not_available = []
    process = ''
    list_pairs = MOCKED_MARKET_PAIRS  # Todo: fetch form API
    for idx, pair in enumerate(pairs, start=1):
        if pair not in list_pairs:
            pairs_not_available.append(pair)
            logger.info(f"Skipping pair {pair}...")
            continue
        for timeframe in timeframes:
            logger.debug(
                f'Downloading pair {pair}, {candle_type}, interval '
                f'{timeframe}.')
            process = f'{idx}/{len(pairs)}'
            _download_pair_history(
                pair=pair, process=process,
                datadir=datadir,
                timerange=timerange,
                data_handler=data_handler,
                timeframe=str(timeframe),
                new_pairs_days=new_pairs_days,
                candle_type=candle_type,
                erase=erase, prepend=prepend
            )
    return pairs_not_available


def _download_trade_history(
    pair: str, *,
    new_pairs_days: int = 30,
    timerange: Optional[TimeRange] = None,
    data_handler: DataHandler

) -> bool:
    """
    Download trade history from the exchange.
    Appends to previously downloaded trades data.
    """
    try:

        until = None
        since = 0
        if timerange:
            if timerange.starttype == 'date':
                since = timerange.startts * 1000
            if timerange.stoptype == 'date':
                until = timerange.stopts * 1000

        trades = data_handler.trades_load(pair)

        if not trades.empty and 0 < since < trades.iloc[0]['timestamp']:
            # since is before the first trade
            logger.info(
                f"Start ({trades.iloc[0]['date']:{DATETIME_PRINT_FORMAT}}) "
                f"earlier than available data. Re-downloading trades for "
                f"{pair}..."
            )
            trades = trades_list_to_df([])

        from_id = trades.iloc[-1]['id'] if not trades.empty else None
        if not trades.empty and since < trades.iloc[-1]['timestamp']:
            # Reset since to the last available point
            # - 5 seconds (to ensure we're getting all trades)
            since = trades.iloc[-1]['timestamp'] - (5 * 1000)
            logger.info(
                f"Using last trade date -5s - Downloading trades for {pair} "
                f"since: {format_ms_time(since)}.")

        if not since:
            since = dt_ts(dt_now() - timedelta(days=new_pairs_days))

        logger.debug(
            "Current Start: %s", 'None' if trades.empty else
            f"{trades.iloc[0]['date']:{DATETIME_PRINT_FORMAT}}"
        )
        logger.debug(
            "Current End: %s", 'None' if trades.empty else
            f"{trades.iloc[-1]['date']:{DATETIME_PRINT_FORMAT}}"
        )
        logger.info(f"Current Amount of trades: {len(trades)}")

        logger.info(
            f"Getting histories for {pair}, since: {since}, until: {until}, "
            f"from: {from_id}"
        )
        # Default since_ms to 30 days if nothing is given
        new_trades = [pair, generate_mock_trades(100)]
        new_trades_df = trades_list_to_df(new_trades[1])
        trades = concat([trades, new_trades_df], axis=0)
        # Remove duplicates to make sure we're not storing data we don't need
        trades = trades_df_remove_duplicates(trades)
        data_handler.trades_store(pair, data=trades)

        logger.debug(
            "New Start: %s", 'None' if trades.empty else
            f"{trades.iloc[0]['date']:{DATETIME_PRINT_FORMAT}}"
        )
        logger.debug(
            "New End: %s", 'None' if trades.empty else
            f"{trades.iloc[-1]['date']:{DATETIME_PRINT_FORMAT}}"
        )
        logger.info(f"New Amount of trades: {len(trades)}")
        return True

    except Exception as e:
        print(e.__class__.__name__, str(e))
        logger.exception(
            f'Failed to download histories trades for pair: "{pair}". '
        )
        return False


def refresh_backtest_trades_data(
    pairs: List[str], datadir: Path, timerange: TimeRange,
    new_pairs_days: int = 30, erase: bool = False, data_format: str = 'feather'
) -> List[str]:
    """
    Refresh stored trades data
    Used by download-data subcommand.
    :return: List of pairs that are not available.
    """
    pairs_not_available = []
    data_handler = get_data_handler(datadir, data_format=data_format)
    list_pairs = MOCKED_MARKET_PAIRS
    for pair in pairs:
        if pair not in list_pairs:
            pairs_not_available.append(pair)
            logger.info(f"Skipping pair {pair}...")
            continue

        if erase:
            if data_handler.trades_purge(pair):
                logger.info(f'Deleting existing data for pair {pair}.')

        logger.info(f'Downloading trades for pair {pair}.')
        _download_trade_history(
            pair=pair,
            new_pairs_days=new_pairs_days,
            timerange=timerange,
            data_handler=data_handler
        )
    return pairs_not_available
