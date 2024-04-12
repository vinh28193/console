import numpy as np
from pathlib import Path
from typing import List, Dict, Union
from pandas import DataFrame, to_datetime, concat

from fasttraders.constants import (
    TradeList, DEFAULT_TRADES_COLUMNS,
    TRADES_DTYPES, DEFAULT_DATAFRAME_COLUMNS
)
from fasttraders.enums import CandleType
from fasttraders.log import logger
from fasttraders.ultis.timerange import TimeRange
from fasttraders.ultis.timeframe import timeframe_to_resample_freq


def ohlcv_to_dataframe(
    ohlcv: list, timeframe: str, pair: str, *, fill_missing: bool = True,
    drop_incomplete: bool = True
) -> DataFrame:
    """
    Converts a list with candle (OHLCV) data (in format returned by
    ccxt.fetch_ohlcv)
    to a Dataframe
    :param ohlcv: list with candle (OHLCV) data, as returned by
    exchange.async_get_candle_history
    :param timeframe: timeframe (e.g. 5m). Used to fill up eventual missing data
    :param pair: Pair this data is for (used to warn if fillup was necessary)
    :param fill_missing: fill up missing candles with 0 candles
                         (see ohlcv_fill_up_missing_data for details)
    :param drop_incomplete: Drop the last candle of the dataframe, assuming
    it's incomplete
    :return: DataFrame
    """
    logger.debug(
        f"Converting candle (OHLCV) data to dataframe for pair {pair}."
    )
    cols = DEFAULT_DATAFRAME_COLUMNS
    df = DataFrame(ohlcv, columns=cols)

    df['date'] = to_datetime(df['date'], unit='ms', utc=True)

    # Some exchanges return int values for Volume and even for OHLC.
    # Convert them since TA-LIB indicators used in the strategy assume floats
    # and fail with exception...
    df = df.astype(dtype={
        'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
        'volume': 'float'
    })
    return clean_ohlcv_dataframe(
        df, timeframe, pair,
        fill_missing=fill_missing,
        drop_incomplete=drop_incomplete
    )


def clean_ohlcv_dataframe(
    data: DataFrame, timeframe: str, pair: str, *,
    fill_missing: bool, drop_incomplete: bool
) -> DataFrame:
    """
    Cleanse a OHLCV dataframe by
      * Grouping it by date (removes duplicate tics)
      * dropping last candles if requested
      * Filling up missing data (if requested)
    :param data: DataFrame containing candle (OHLCV) data.
    :param timeframe: timeframe (e.g. 5m). Used to fill up eventual missing data
    :param pair: Pair this data is for (used to warn if fillup was necessary)
    :param fill_missing: fill up missing candles with 0 candles
                         (see ohlcv_fill_up_missing_data for details)
    :param drop_incomplete: Drop the last candle of the dataframe, assuming
    it's incomplete
    :return: DataFrame
    """
    # group by index and aggregate results to eliminate duplicate ticks
    data = data.groupby(by='date', as_index=False, sort=True).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'max',
    })
    # eliminate partial candle
    if drop_incomplete:
        data.drop(data.tail(1).index, inplace=True)
        logger.debug('Dropping last candle')

    if fill_missing:
        return ohlcv_fill_up_missing_data(data, timeframe, pair)
    else:
        return data


def ohlcv_fill_up_missing_data(
    dataframe: DataFrame, timeframe: str, pair: str
) -> DataFrame:
    """
    Fills up missing data with 0 volume rows,
    using the previous close as price for "open", "high" "low" and "close",
    volume is set to 0

    """

    ohlcv_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    resample_interval = timeframe_to_resample_freq(timeframe)
    # Resample to create "NAN" values
    df = dataframe.resample(resample_interval, on='date').agg(ohlcv_dict)

    # Forwardfill close for missing columns
    df['close'] = df['close'].ffill()
    # Use close for "open, high, low"
    df.loc[:, ['open', 'high', 'low']] = df[['open', 'high', 'low']].fillna(
        value={
            'open': df['close'],
            'high': df['close'],
            'low': df['close'],
        })
    df.reset_index(inplace=True)
    len_before = len(dataframe)
    len_after = len(df)
    pct_missing = (len_after - len_before) / len_before if len_before > 0 else 0
    if len_before != len_after:
        message = (
            f"Missing data fillup for {pair}, {timeframe}: "
            f"before: {len_before} - after: {len_after} - "
            f"{pct_missing:.2%}"
        )
        if pct_missing > 0.01:
            logger.info(message)
        else:
            # Don't be verbose if only a small amount is missing
            logger.debug(message)
    return df


def trim_dataframe(
    df: DataFrame, timerange, *, df_date_col: str = 'date',
    startup_candles: int = 0
) -> DataFrame:
    """
    Trim dataframe based on given timerange
    :param df: Dataframe to trim
    :param timerange: timerange (use start and end date if available)
    :param df_date_col: Column in the dataframe to use as Date column
    :param startup_candles: When not 0, is used instead the timerange start date
    :return: trimmed dataframe
    """
    if startup_candles:
        # Trim candles instead of timeframe in case of given startup_candle
        # count
        df = df.iloc[startup_candles:, :]
    else:
        if timerange.starttype == 'date':
            df = df.loc[df[df_date_col] >= timerange.startdt, :]
    if timerange.stoptype == 'date':
        df = df.loc[df[df_date_col] <= timerange.stopdt, :]
    return df


def trim_dataframes(
    preprocessed: Dict[str, DataFrame], timerange, startup_candles: int
) -> Dict[str, DataFrame]:
    """
    Trim startup period from analyzed dataframes
    :param preprocessed: Dict of pair: dataframe
    :param timerange: timerange (use start and end date if available)
    :param startup_candles: Startup-candles that should be removed
    :return: Dict of trimmed dataframes
    """
    processed: Dict[str, DataFrame] = {}

    for pair, df in preprocessed.items():
        trimed_df = trim_dataframe(
            df, timerange, startup_candles=startup_candles
        )
        if not trimed_df.empty:
            processed[pair] = trimed_df
        else:
            logger.warning(
                f'{pair} has no data left after adjusting for startup candles, '
                f'skipping.'
            )
    return processed


def order_book_to_dataframe(bids: list, asks: list) -> DataFrame:
    """
    TODO: This should get a dedicated test
    Gets order book list, returns dataframe with below format per suggested
    by creslin
    -------------------------------------------------------------------
     b_sum       b_size       bids       asks       a_size       a_sum
    -------------------------------------------------------------------
    """
    cols = ['bids', 'b_size']

    bids_frame = DataFrame(bids, columns=cols)
    # add cumulative sum column
    bids_frame['b_sum'] = bids_frame['b_size'].cumsum()
    cols2 = ['asks', 'a_size']
    asks_frame = DataFrame(asks, columns=cols2)
    # add cumulative sum column
    asks_frame['a_sum'] = asks_frame['a_size'].cumsum()

    frame = concat(
        [bids_frame['b_sum'], bids_frame['b_size'], bids_frame['bids'],
         asks_frame['asks'], asks_frame['a_size'], asks_frame['a_sum']], axis=1,
        keys=['b_sum', 'b_size', 'bids', 'asks', 'a_size', 'a_sum'])
    # logger.info('order book %s', frame )
    return frame


def convert_ohlcv_format(
    datadir: Path,
    convert_from: str,
    convert_to: str,
    erase: bool,
):
    """
    Convert OHLCV from one format to another
    :param convert_from: Source format
    :param convert_to: Target format
    :param erase: Erase source data (does not apply if source and target
    format are identical)
    """
    from .utils import get_data_handler
    src = get_data_handler(datadir, convert_from)
    trg = get_data_handler(datadir, convert_to)
    pass


def reduce_dataframe_footprint(df: DataFrame) -> DataFrame:
    """
    Ensure all values are float32 in the incoming dataframe.
    :param df: Dataframe to be converted to float/int 32s
    :return: Dataframe converted to float/int 32s
    """

    logger.debug(f"Memory usage of dataframe is "
                 f"{df.memory_usage().sum() / 1024 ** 2:.2f} MB")

    df_dtypes = df.dtypes
    for column, dtype in df_dtypes.items():
        if column in ['open', 'high', 'low', 'close', 'volume']:
            continue
        if dtype == np.float64:
            df_dtypes[column] = np.float32
        elif dtype == np.int64:
            df_dtypes[column] = np.int32
    df = df.astype(df_dtypes)

    logger.debug(f"Memory usage after optimization is: "
                 f"{df.memory_usage().sum() / 1024 ** 2:.2f} MB")

    return df


def trades_df_remove_duplicates(trades: DataFrame) -> DataFrame:
    """
    Removes duplicates from the trades DataFrame.
    Uses pandas.DataFrame.drop_duplicates to remove duplicates based on the
    'timestamp' column.
    :param trades: DataFrame with the columns constants.DEFAULT_TRADES_COLUMNS
    :return: DataFrame with duplicates removed based on the 'timestamp' column
    """
    return trades.drop_duplicates(subset=['timestamp', 'id'])


def trades_dict_to_list(trades: List[Dict]) -> TradeList:
    """
    Convert fetch_trades result into a List (to be more memory efficient).
    :param trades: List of trades, as returned by ccxt.fetch_trades.
    :return: List of Lists, with constants.DEFAULT_TRADES_COLUMNS as columns
    """
    return [[t[col] for col in DEFAULT_TRADES_COLUMNS] for t in trades]


def trades_convert_types(trades: DataFrame) -> DataFrame:
    """
    Convert Trades dtypes and add 'date' column
    """
    trades = trades.astype(TRADES_DTYPES)
    trades['date'] = to_datetime(trades['timestamp'], unit='ms', utc=True)
    return trades


def trades_list_to_df(trades: TradeList, convert: bool = True):
    """
    convert trades list to dataframe
    :param trades: List of Lists with constants.DEFAULT_TRADES_COLUMNS as
    columns
    """
    if not trades:
        df = DataFrame(columns=DEFAULT_TRADES_COLUMNS)
    else:
        df = DataFrame(trades, columns=DEFAULT_TRADES_COLUMNS)

    if convert:
        df = trades_convert_types(df)

    return df


def trades_to_ohlcv(trades: DataFrame, timeframe: str) -> DataFrame:
    """
    Converts trades list to OHLCV list
    :param trades: List of trades, as returned by ccxt.fetch_trades.
    :param timeframe: Timeframe to resample data to
    :return: OHLCV Dataframe.
    :raises: ValueError if no trades are provided
    """
    if trades.empty:
        raise ValueError('Trade-list empty.')
    df = trades.set_index('date', drop=True)
    resample_interval = timeframe_to_resample_freq(timeframe)
    df_new = df['price'].resample(resample_interval).ohlc()
    df_new['volume'] = df['amount'].resample(resample_interval).sum()
    df_new['date'] = df_new.index
    # Drop 0 volume rows
    df_new = df_new.dropna()
    return df_new.loc[:, DEFAULT_DATAFRAME_COLUMNS]


def convert_trades_to_ohlcv(
    pairs: List[str],
    timeframes: List[str],
    datadir: Union[Path, str],
    timerange: TimeRange,
    erase: bool = False,
    data_format_ohlcv: str = 'json',
    data_format_trades: str = 'json',
    candle_type: CandleType = CandleType.SPOT
) -> None:
    """
    Convert stored trades data to ohlcv data
    """
    from .utils import get_data_handler, create_datadir
    datadir = create_datadir(datadir)
    data_handler_trades = get_data_handler(
        datadir, data_format=data_format_trades
    )
    data_handler_ohlcv = get_data_handler(datadir, data_format=data_format_ohlcv)
    if not pairs:
        pairs = data_handler_trades.trades_get_pairs(datadir)

    logger.info(
        f"About to convert pairs: '{', '.join(pairs)}', "
        f"intervals: '{', '.join(timeframes)}' to {datadir}"
    )

    for pair in pairs:
        trades = data_handler_trades.trades_load(pair)
        for timeframe in timeframes:
            if erase:
                if data_handler_ohlcv.ohlcv_purge(
                    pair, timeframe, candle_type=candle_type
                ):
                    logger.info(
                        f'Deleting existing data for pair {pair}, interval '
                        f'{timeframe}.'
                    )
            try:
                ohlcv = trades_to_ohlcv(trades, timeframe)
                # Store ohlcv
                data_handler_ohlcv.ohlcv_store(
                    pair, timeframe, data=ohlcv, candle_type=candle_type
                )
            except ValueError:
                logger.exception(f'Could not convert {pair} to OHLCV.')


def convert_trades_format(
    datadir, convert_from: str, convert_to: str, erase: bool
):
    """
    Convert trades from one format to another format.
    :param convert_from: Source format
    :param convert_to: Target format
    :param erase: Erase source data (does not apply if source and target
    format are identical)
    """
    from .utils import get_data_handler
    src = get_data_handler(datadir, convert_from)
    trg = get_data_handler(datadir, convert_to)

    pairs = src.trades_get_pairs(datadir)
    logger.info(f"Converting trades for {pairs}")

    for pair in pairs:
        data = src.trades_load(pair=pair)
        logger.info(f"Converting {len(data)} trades for {pair}")
        trg.trades_store(pair, data)
        if erase and convert_from != convert_to:
            logger.info(f"Deleting source Trade data for {pair}.")
            src.trades_purge(pair=pair)
