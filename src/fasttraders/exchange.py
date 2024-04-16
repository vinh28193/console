import asyncio
from datetime import timedelta, datetime
from typing import Dict, Any, List, Optional, Tuple, Coroutine

from threading import Lock
from cachetools import TTLCache
from fasttraders.constants import (
    PairWithTimeframe, ListPairsWithTimeframes,
    OHLCVResponse
)
from fasttraders.data.converter import ohlcv_to_dataframe, clean_ohlcv_dataframe
from fasttraders.enums import TradingMode, CandleType
from fasttraders.log import logger
from fasttraders.ultis.misc import chunks
from fasttraders.ultis.timeframe import (
    timeframe_to_seconds,
    timeframe_to_prev_date, date_minus_candles, timeframe_to_msecs,
    timeframe_to_next_date, dt_humanize, dt_now, dt_ts, dt_from_ts
)
from fasttraders.ultis.wrapper import PeriodicCache
from pandas import DataFrame, concat


class Exchange:
    name = "exchange"
    timeframes = ["1m", "5m"]

    def __init__(
        self, validate: bool = True, load_leverage_tiers: bool = False
    ) -> None:
        """
        Initializes this module with the given config,
        it does basic validation whether the specified exchange and pairs are
        valid.
        :return: None
        """
        self._markets: Dict = {}
        self._trading_fees: Dict[str, Any] = {}
        self._leverage_tiers: Dict[str, List[Dict]] = {}
        # Lock event loop. This is necessary to avoid race-conditions when
        # using force* commands
        # Due to funding fee fetching.
        self._loop_lock = Lock()
        self.loop = self._init_async_loop()
        # Holds last candle refreshed time of each pair
        self._pairs_last_refresh_time: Dict[PairWithTimeframe, int] = {}
        # Timestamp of last markets refresh
        self._last_markets_refresh: int = 0

        # Cache for 10 minutes ...
        self._cache_lock = Lock()
        self._fetch_tickers_cache: TTLCache = TTLCache(maxsize=2, ttl=60 * 10)
        self._exit_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)
        self._entry_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)

        # Holds candles
        self._klines: Dict[PairWithTimeframe, DataFrame] = {}
        self._expiring_candle_cache: Dict[str, PeriodicCache] = {}

        # Holds all open sell orders for dry_run
        self._dry_run_open_orders: Dict[str, Any] = {}

        # Leverage properties
        self.trading_mode: TradingMode = TradingMode.SPOT
        self.margin_mode: None
        self.liquidation_buffer = 0.05
        self.startup_candle_count = 1

        # Assign this directly for easy access
        self.ohlcv_partial_candle = True
        self.ohlcv_require_since = True
        self.ohlcv_candle_limit_per_timeframe = {}

        self.trades_pagination = "time"  # Possible are "time" or "id"
        self._trades_pagination_arg = "since"

        self.required_candle_call_count = 1
        # Converts the interval provided in minutes in config to seconds
        self.markets_refresh_interval: int = 60 * 60 * 1000

    def _init_async_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

    def _now_is_time_to_refresh(self, pair: str, timeframe: str,
                                candle_type: CandleType) -> bool:
        # Timeframe in seconds
        interval_in_sec = timeframe_to_seconds(timeframe)
        plr = self._pairs_last_refresh_time.get((pair, timeframe, candle_type),
                                                0) + interval_in_sec
        # current,active candle open date
        now = int(timeframe_to_prev_date(timeframe).timestamp())
        return plr < now

    def _build_ohlcv_dl_jobs(
        self, pairs: ListPairsWithTimeframes, since_ms: Optional[int],
        cache: bool
    ) -> Tuple[List[Coroutine], List[Tuple[str, str, CandleType]]]:
        """
        Build Coroutines to execute as part of refresh_latest_ohlcv
        """
        input_coroutines: List[Coroutine[Any, Any, OHLCVResponse]] = []
        cached_pairs = []
        for pair, timeframe, candle_type in set(pairs):
            if (
                timeframe not in self.timeframes and
                candle_type in (CandleType.SPOT, CandleType.FUTURES)
            ):
                logger.warning(
                    f"Cannot download ({pair}, {timeframe}) combination as "
                    f"this timeframe is  not available on {self.name}. "
                    f"Available timeframes are {', '.join(self.timeframes)}.")
                continue

            if (
                not cache or
                (pair, timeframe, candle_type) not in self._klines or
                self._now_is_time_to_refresh(pair, timeframe, candle_type)
            ):

                input_coroutines.append(
                    self._build_coroutine(
                        pair, timeframe, candle_type, since_ms, cache
                    )
                )

            else:
                logger.debug(
                    f"Using cached candle (OHLCV) data for {pair}, "
                    f"{timeframe}, {candle_type} ..."
                )
                cached_pairs.append((pair, timeframe, candle_type))

        return input_coroutines, cached_pairs

    def refresh_latest_ohlcv(
        self, pairs: ListPairsWithTimeframes, *,
        since_ms: Optional[int] = None, cache: bool = True,
        drop_incomplete: Optional[bool] = None
    ) -> Dict[PairWithTimeframe, DataFrame]:
        """
        Refresh in-memory OHLCV asynchronously and set `_klines` with the result
        Loops asynchronously over pair_list and downloads all pairs async (
        semi-parallel).
        Only used in the dataprovider.refresh() method.
        :param pairs: List of 2 element tuples containing pair, interval
        to refresh
        :param since_ms: time since when to download, in milliseconds
        :param cache: Assign result to _klines. Usefull for one-off downloads
        like for pairlists
        :param drop_incomplete: Control candle dropping.
            Specifying None defaults to _ohlcv_partial_candle
        :return: Dict of [{(pair, timeframe): Dataframe}]
        """
        logger.debug("Refreshing candle (OHLCV) data for %d pairs", len(pairs))

        # Gather coroutines to run
        input_coroutines, cached_pairs = self._build_ohlcv_dl_jobs(
            pairs, since_ms, cache
        )

        results_df = {}
        # Chunk requests into batches of 100 to avoid overwelming ccxt
        # Throttling
        for input_coro in chunks(input_coroutines, 100):
            async def gather_stuff():
                return await asyncio.gather(*input_coro, return_exceptions=True)

            with self._loop_lock:
                results = self.loop.run_until_complete(gather_stuff())

            for res in results:
                if isinstance(res, Exception):
                    logger.warning(
                        f"Async code raised an exception: {repr(res)}")
                    continue
                # Deconstruct tuple (has 5 elements)
                pair, timeframe, c_type, ticks, drop_hint = res

                drop_incomplete_ = (
                    drop_hint if drop_incomplete is None else drop_incomplete
                )
                ohlcv_df = self._process_ohlcv_df(
                    pair, timeframe, c_type, ticks, cache, drop_incomplete_
                )

                results_df[(pair, timeframe, c_type)] = ohlcv_df

        # Return cached klines
        for pair, timeframe, c_type in cached_pairs:
            results_df[(pair, timeframe, c_type)] = self.klines(
                (pair, timeframe, c_type),
                copy=False
            )

        return results_df

    def klines(
        self, pair_interval: PairWithTimeframe, copy: bool = True
    ) -> DataFrame:
        if pair_interval in self._klines:
            return (
                self._klines[pair_interval].copy()
                if copy else self._klines[pair_interval]
            )
        else:
            return DataFrame()

    def ohlcv_candle_limit(
        self, timeframe: str, candle_type: CandleType,
        since_ms: Optional[int] = None
    ) -> int:
        """
        Exchange ohlcv candle limit
        Uses ohlcv_candle_limit_per_timeframe if the exchange has different
        limits
        per timeframe (e.g. bittrex), otherwise falls back to ohlcv_candle_limit
        TODO: this is most likely no longer needed since only bittrex needed
        this.
        :param timeframe: Timeframe to check
        :param candle_type: Candle-type
        :param since_ms: Starting timestamp
        :return: Candle limit as integer
        """
        return 1

    async def _async_get_historic_ohlcv(
        self, pair: str, timeframe: str,
        since_ms: int, candle_type: CandleType,
        is_new_pair: bool = False,
        raise_: bool = False,
        until_ms: Optional[int] = None
    ) -> OHLCVResponse:
        """
        Download historic ohlcv
        :param is_new_pair: used by binance subclass to allow "fast" new pair
        downloading
        :param candle_type: Any of the enum CandleType (must match trading
        mode!)
        """

        one_call = timeframe_to_msecs(timeframe) * self.ohlcv_candle_limit(
            timeframe, candle_type, since_ms
        )
        logger.debug(
            "one_call: %s msecs (%s)",
            one_call,
            dt_humanize(
                dt_now() - timedelta(milliseconds=one_call),
                only_distance=True
            )
        )
        input_coroutines = [
            self._async_get_candle_history(
                pair, timeframe, candle_type, since
            ) for since in range(since_ms, until_ms or dt_ts(), one_call)
        ]

        data: List = []
        # Chunk requests into batches of 100 to avoid overwelming ccxt
        # Throttling
        for input_coro in chunks(input_coroutines, 100):

            results = await asyncio.gather(*input_coro, return_exceptions=True)
            for res in results:
                if isinstance(res, BaseException):
                    logger.warning(
                        f"Async code raised an exception: {repr(res)}")
                    if raise_:
                        raise
                    continue
                else:
                    # Deconstruct tuple if it's not an exception
                    p, _, c, new_data, _ = res
                    if p == pair and c == candle_type:
                        data.extend(new_data)
        # Sort data again after extending the result - above calls return in
        # "async order"
        data = sorted(data, key=lambda x: x[0])
        return pair, timeframe, candle_type, data, self.ohlcv_partial_candle

    def _build_coroutine(
        self, pair: str, timeframe: str, candle_type: CandleType,
        since_ms: Optional[int], cache: bool
    ) -> Coroutine[Any, Any, OHLCVResponse]:
        not_all_data = cache and self.required_candle_call_count > 1
        if cache and (pair, timeframe, candle_type) in self._klines:
            candle_limit = self.ohlcv_candle_limit(timeframe, candle_type)
            min_date = date_minus_candles(
                timeframe, candle_limit - 5
            ).timestamp()
            # Check if 1 call can get us updated candles without hole in the
            # data.
            last_refresh_time = self._pairs_last_refresh_time.get(
                (pair, timeframe, candle_type), 0
            )
            if min_date < last_refresh_time:
                # Cache can be used - do one-off call.
                not_all_data = False
            else:
                # Time jump detected, evict cache
                logger.info(
                    f"Time jump detected. Evicting cache for {pair}, "
                    f"{timeframe}, {candle_type}")
                del self._klines[(pair, timeframe, candle_type)]

        if not since_ms and (self.ohlcv_require_since or not_all_data):
            # Multiple calls for one pair - to get more history
            one_call = timeframe_to_msecs(timeframe) * self.ohlcv_candle_limit(
                timeframe, candle_type, since_ms
            )
            move_to = one_call * self.required_candle_call_count
            now = timeframe_to_next_date(timeframe)
            since_ms = int(
                (now - timedelta(seconds=move_to // 1000)).timestamp() * 1000
            )
        if since_ms:
            print(f"Since ... {since_ms}")
            return self._async_get_historic_ohlcv(
                pair, timeframe, since_ms=since_ms, raise_=True,
                candle_type=candle_type)
        else:
            print("One call ... 'regular' refresh")
            return self._async_get_candle_history(
                pair, timeframe, since_ms=since_ms, candle_type=candle_type)

    async def _async_get_candle_history(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: Optional[int] = None,
    ) -> OHLCVResponse:
        """
        Asynchronously get candle history data using fetch_ohlcv
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        returns tuple: (pair, timeframe, ohlcv_list)
        """

        if not since_ms:
            since_ms = int(
                (datetime.now() - timedelta(days=10)).timestamp()
            ) * 1000
        try:
            # candle_limit = self.ohlcv_candle_limit(
            #     timeframe, candle_type=candle_type, since_ms=since_ms
            # )
            # Fetch OHLCV asynchronously
            s = '(' + dt_from_ts(
                since_ms
            ).isoformat() + ') ' if since_ms is not None else ''

            logger.debug(
                "Fetching pair %s, %s, interval %s, since %s %s...",
                pair, candle_type, timeframe, since_ms, s
            )

            from fasttraders.data.utils import agenerate_mock_ohlcv
            data = await agenerate_mock_ohlcv(0.5, since_ms, 10)
            print("data:", data)
            # Some exchanges sort OHLCV in ASC order and others in DESC.
            # Ex: Bittrex returns the list of OHLCV in ASC order (oldest
            # first, newest last)
            # while GDAX returns the list of OHLCV in DESC order (newest
            # first, oldest last)
            # Only sort if necessary to save computing time
            try:
                if data and data[0][0] > data[-1][0]:
                    data = sorted(data, key=lambda x: x[0])
            except IndexError:
                logger.exception("Error loading %s. Result was %s.", pair, data)
                return (
                    pair, timeframe, candle_type, [], self.ohlcv_partial_candle
                )
            logger.debug(
                "Done fetching pair %s, %s interval %s...",
                pair, candle_type, timeframe
            )
            return (
                pair, timeframe, candle_type, data, self.ohlcv_partial_candle
            )
        except Exception as e:
            raise Exception(
                f'Could not fetch historical candle (OHLCV) data '
                f'for pair {pair} due to {e.__class__.__name__}. '
                f'Message: {e}') from e

    def _process_ohlcv_df(
        self, pair: str, timeframe: str, c_type: CandleType, ticks: List[List],
        cache: bool, drop_incomplete: bool
    ) -> DataFrame:
        # keeping last candle time as last refreshed time of the pair
        if ticks and cache:
            idx = -2 if drop_incomplete and len(ticks) > 1 else -1
            self._pairs_last_refresh_time[(pair, timeframe, c_type)] = (
                ticks[idx][0] // 1000
            )
        # keeping parsed dataframe in cache
        ohlcv_df = ohlcv_to_dataframe(
            ticks, timeframe, pair=pair,
            fill_missing=True,
            drop_incomplete=drop_incomplete
        )
        if cache:
            if (pair, timeframe, c_type) in self._klines:
                old = self._klines[(pair, timeframe, c_type)]
                # Reassign so we return the updated, combined df
                ohlcv_df = clean_ohlcv_dataframe(
                    concat([old, ohlcv_df], axis=0), timeframe, pair,
                    fill_missing=True, drop_incomplete=False)
                candle_limit = self.ohlcv_candle_limit(timeframe, c_type)
                # Age out old candles
                ohlcv_df = ohlcv_df.tail(
                    candle_limit + self.startup_candle_count
                )
                ohlcv_df = ohlcv_df.reset_index(drop=True)
                self._klines[(pair, timeframe, c_type)] = ohlcv_df
            else:
                self._klines[(pair, timeframe, c_type)] = ohlcv_df
        return ohlcv_df
