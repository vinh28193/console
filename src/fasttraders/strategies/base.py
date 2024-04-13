from datetime import datetime
from typing import Tuple, List, Dict

from fasttraders.constants import ListPairsWithTimeframes
from fasttraders.data.provider import DataProvider
from fasttraders.enums import CandleType
from fasttraders.log import logger
from fasttraders.ultis.misc import remove_entry_exit_signals
from pandas import DataFrame
from .exceptions import StrategyError


class BaseStrategy:
    """
    Base of strategies
    """
    # Can this strategy go short?
    can_short: bool = False

    # associated timeframe
    timeframe: str

    # run "populate_indicators" only for new candle
    process_only_new_candles: bool = True

    # Disable checking the dataframe (converts the error into a warning message)
    disable_dataframe_checks: bool = False

    # Count of candles the strategy requires before producing valid signals
    startup_candle_count: int = 0

    # Protections
    protections: List = []

    # Class level variables (intentional) containing
    # the dataprovider (dp) (access to other candles, historic data, ...)
    # and wallets - access to the current balance.
    dp: DataProvider

    def __init__(self, bot):
        self.bot = bot
        self.process_only_new_candles: bool = True
        self.disable_dataframe_checks: bool = False
        self.timeframe = "5m"
        self._last_candle_seen_per_pair: Dict[str, datetime] = {}

    @property
    def dp(self):
        return self.bot.dp

    def bot_start(self, **kwargs) -> None:
        """
        Called only once after bot instantiation.
        :param **kwargs: Ensure to keep this here so updates to this won't
        break your strategy.
        """
        pass

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        """
        Called at the start of the bot iteration (one loop).
        Might be used to perform pair-independent tasks
        (e.g. gather some remote resource for comparison)
        :param current_time: datetime object, containing the current datetime
        :param **kwargs: Ensure to keep this here so updates to this won't
        break your strategy.
        """
        pass

    def populate_indicators(self, dataframe: DataFrame, **kwargs) -> DataFrame:
        """
        Populate indicators that will be used in the Buy, Sell, Short,
        Exit_short strategy
        :param dataframe: DataFrame with data from the exchange
        :param kwargs: Additional information, like the currently traded pair
        :return: a Dataframe with all mandatory indicators for the strategies
        """
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, **kwargs) -> DataFrame:
        """
        Based on TA indicators, populates the entry signal for the given
        dataframe
        :param dataframe: DataFrame
        :param kwargs: Additional information, like the currently traded pair
        :return: DataFrame with entry columns populated
        """
        return dataframe

    def populate_exit_trend(
        self, dataframe: DataFrame, **kwargs
    ) -> DataFrame:
        """
        Based on TA indicators, populates the exit signal for the given
        dataframe
        :param dataframe: DataFrame
        :param kwargs: Additional information, like the currently traded pair
        :return: DataFrame with exit columns populated
        """
        return dataframe

    def advise_all_indicators(
        self, data: Dict[str, DataFrame]
    ) -> Dict[str, DataFrame]:
        """
        Populates indicators for given candle (OHLCV) data (for multiple pairs)
        Does not run advise_entry or advise_exit!
        Used by optimize operations only, not during dry / live runs.
        Using .copy() to get a fresh copy of the dataframe for every strategy
        run.
        Also copy on output to avoid PerformanceWarnings pandas 1.3.0 started
        to show.
        Has positive effects on memory usage for whatever reason - also when
        using only one strategy.
        """
        return {
            pair: self.advise_indicators(pair_data.copy(), **{'pair': pair})
            for pair, pair_data in data.items()}

    def ft_advise_signals(
        self, dataframe: DataFrame, **kwargs
    ) -> DataFrame:
        """
        Call advise_entry and advise_exit and return the resulting dataframe.
        :param dataframe: Dataframe containing data from exchange, as well as
        pre-calculated indicators
        :param kwargs: Metadata dictionary with additional data (e.g. 'pair')
        :return: DataFrame of candle (OHLCV) data with indicator data and
        signals added

        """
        dataframe = self.advise_entry(dataframe, **kwargs)
        dataframe = self.advise_exit(dataframe, **kwargs)
        return dataframe

    def advise_indicators(
        self, dataframe: DataFrame, **kwargs
    ) -> DataFrame:
        """
        Populate indicators that will be used in the Buy, Sell, short,
        exit_short strategy
        This method should not be overridden.
        :param dataframe: Dataframe with data from the exchange
        :param kwargs: Additional information, like the currently traded pair
        :return: a Dataframe with all mandatory indicators for the strategies
        """
        logger.debug(f"Populating indicators for pair {kwargs.get('pair')}.")
        return self.populate_indicators(dataframe, **kwargs)

    def advise_entry(self, dataframe: DataFrame, **kwargs) -> DataFrame:
        """
        Based on TA indicators, populates the entry order signal for the
        given dataframe
        This method should not be overridden.
        :param dataframe: DataFrame
        :param kwargs: Additional information dictionary, with details like
        the currently traded pair
        :return: DataFrame with buy column
        """

        logger.debug(
            f"Populating enter signals for pair {kwargs.get('pair')}."
        )
        # Initialize column to work around Pandas bug #56503.
        dataframe.loc[:, 'enter_tag'] = ''
        df = self.populate_entry_trend(dataframe, **kwargs)
        if 'enter_long' not in df.columns:
            df = df.rename(
                {'buy': 'enter_long', 'buy_tag': 'enter_tag'},
                axis='columns'
            )

        return df

    def advise_exit(self, dataframe: DataFrame, **kwargs) -> DataFrame:
        """
        Based on TA indicators, populates the exit order signal for the given
        dataframe
        This method should not be overridden.
        :param dataframe: DataFrame
        :param kwargs: Additional information dictionary, with details like
        the currently traded pair
        :return: DataFrame with exit column
        """
        # Initialize column to work around Pandas bug #56503.
        dataframe.loc[:, 'exit_tag'] = ''
        logger.debug(
            f"Populating exit signals for pair {kwargs.get('pair')}.")
        df = self.populate_exit_trend(dataframe, **kwargs)
        if 'exit_long' not in df.columns:
            df = df.rename({'sell': 'exit_long'}, axis='columns')
        return df

    def _analyze(self, dataframe: DataFrame, **kwargs) -> DataFrame:
        """
        Parses the given candle (OHLCV) data and returns a populated DataFrame
        add several TA indicators and buy signal to it
        WARNING: Used internally only, may skip analysis if
        `process_only_new_candles` is set.
        :param dataframe: Dataframe containing data from exchange
        :param kwargs: Metadata dictionary with additional data (e.g. 'pair')
        :return: DataFrame of candle (OHLCV) data with indicator data and
        signals added
        """
        pair = str(kwargs.get('pair'))
        last_candle = self._last_candle_seen_per_pair.get(pair, None)
        new_candle = last_candle != dataframe.iloc[-1]['date']
        # Test if seen this pair and last candle before.
        # always run if process_only_new_candles is set to false
        if not self.process_only_new_candles or new_candle:

            # Defs that only make change on new candle data.
            logger.debug("TA Analysis Launched")
            dataframe = self.advise_indicators(dataframe, **kwargs)
            dataframe = self.advise_entry(dataframe, **kwargs)
            dataframe = self.advise_exit(dataframe, **kwargs)

            self._last_candle_seen_per_pair[pair] = dataframe.iloc[-1]['date']

            candle_type = kwargs.get('candle_type', CandleType.SPOT)
            timeframe = kwargs.get("timeframe") or self.timeframe
            self.dp.set_cached_df(
                pair, timeframe, dataframe, candle_type=candle_type
            )
            self.dp.emit_df(
                (pair, self.timeframe, candle_type), dataframe,
                new_candle
            )

        else:
            logger.debug("Skipping TA Analysis for already analyzed candle")
            dataframe = remove_entry_exit_signals(dataframe)

        logger.debug("Loop Analysis Launched")

        return dataframe

    def analyze(
        self, pair: str, timeframe=None, candle_type=CandleType.SPOT
    ) -> None:
        """
        Fetch data for this pair from dataprovider and analyze.
        Stores the dataframe into the dataprovider.
        The analyzed dataframe is then accessible via
        `dp.get_analyzed_dataframe()`.
        :param pair: Pair to analyze.
        :param timeframe:
        :param candle_type
        """
        timeframe = timeframe or self.timeframe

        dataframe = self.dp.ohlcv(
            pair, timeframe, candle_type=candle_type
        )
        if not isinstance(dataframe, DataFrame) or dataframe.empty:
            logger.warning('Empty candle (OHLCV) data for pair %s', pair)
            return

        try:
            df_len, df_close, df_date = self.preserve_df(dataframe)
            self._analyze(dataframe, pair=pair)
            self.assert_df(dataframe, df_len, df_close, df_date)
        except StrategyError as error:
            logger.warning(
                f"Unable to analyze candle (OHLCV) data for pair {pair}: "
                f"{error}"
            )
            return

        if dataframe.empty:
            logger.warning('Empty dataframe for pair %s', pair)
            return

    def analyzes(self, pairs: List[str]) -> None:
        """
        Analyze all pairs using analyze_pair().
        :param pairs: List of pairs to analyze
        """
        for pair in pairs:
            self.analyze(pair)

    @staticmethod
    def preserve_df(dataframe: DataFrame) -> Tuple[int, float, datetime]:
        """ keep some data for dataframes """
        return (
            len(dataframe),
            dataframe["close"].iloc[-1],
            dataframe["date"].iloc[-1]
        )

    def assert_df(
        self, dataframe: DataFrame, df_len: int, df_close: float,
        df_date: datetime
    ):
        """
        Ensure dataframe (length, last candle) was not modified, and has all
        elements we need.
        """
        message_template = "Dataframe returned from strategy has mismatching " \
                           "{}."
        message = ""
        if dataframe is None:
            message = "No dataframe returned (return statement missing?)."
        elif 'enter_long' not in dataframe:
            message = "enter_long/buy column not set."
        elif df_len != len(dataframe):
            message = message_template.format("length")
        elif df_close != dataframe["close"].iloc[-1]:
            message = message_template.format("last close price")
        elif df_date != dataframe["date"].iloc[-1]:
            message = message_template.format("last date")
        if message:
            if self.disable_dataframe_checks:
                logger.warning(message)
            else:
                raise StrategyError(message)

    def informative_pairs(self) -> ListPairsWithTimeframes:
        """
        Define additional, informative pair/interval combinations to be
        cached from the exchange.
        These pair/interval combinations are non-tradable, unless they are part
        of the whitelist as well.
        For more information, please consult the documentation
        :return: List of tuples in the format (pair, interval)
            Sample: return [("ETH/USDT", "5m"),
                            ("BTC/USDT", "15m"),
                            ]
        """
        return []

    def gather_informative_pairs(self) -> ListPairsWithTimeframes:
        """
        Internal method which gathers all informative pairs (user or
        automatically defined).
        """
        informative_pairs = self.informative_pairs()
        # Compatibility code for 2 tuple informative pairs
        informative_pairs = [
            (p[0], p[1], CandleType.from_string(p[2]) if len(
                p) > 2 and p[2] != '' else CandleType.SPOT)
            for p in informative_pairs
        ]
        return informative_pairs
