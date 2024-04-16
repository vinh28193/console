from collections import deque
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

from fasttraders.constants import PairWithTimeframe, ListPairsWithTimeframes
from fasttraders.enums import CandleType, RPCMessageType
from fasttraders.rpc.types import RPCAnalyzedDFMsg
from pandas import DataFrame


class DataProvider:

    def __init__(self, bot, **kwargs):
        self.bot = bot
        self.cached_pairs: Dict[
            PairWithTimeframe, Tuple[DataFrame, datetime]
        ] = {}
        self.slice_index: Optional[int] = None
        self.slice_date: Optional[datetime] = None

        self.producer_pairs_df: Dict[
            str, Dict[PairWithTimeframe, Tuple[DataFrame, datetime]]
        ] = {}
        self.producer_pairs: Dict[str, List[str]] = {}
        self.msg_queue: deque = deque()
        self.candle_type = kwargs.get('candle_type', CandleType.SPOT)
        self.timeframe = kwargs.get('timeframe', '1h')

    def refresh(
        self, pairs: ListPairsWithTimeframes,
        helping_pairs: Optional[ListPairsWithTimeframes] = None
    ) -> None:
        """
        Refresh data, called with each cycle
        """
        final_pairs = (pairs + helping_pairs) if helping_pairs else pairs
        self.bot.exchange.refresh_latest_ohlcv(final_pairs)

    def ohlcv(
        self,
        pair: str,
        timeframe: Optional[str] = None,
        copy: bool = True,
        candle_type: str = CandleType.SPOT
    ) -> DataFrame:
        """
        Get candle (OHLCV) data for the given pair as DataFrame
        Please use the `available_pairs` method to verify which pairs are
        currently cached.
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param copy: copy dataframe before returning if True.
                     Use False only for read-only operations (where the
                     dataframe is not modified)
        """
        _candle_type = CandleType.from_string(candle_type)
        return self.bot.exchange.klines(
            (pair, timeframe, _candle_type),
            copy=copy
        )

    def set_cached_df(
        self,
        pair: str,
        timeframe: str,
        dataframe: DataFrame,
        candle_type: CandleType
    ) -> None:
        """
        Store cached Dataframe.
        Using private method as this should never be used by a user
        (but the class is exposed via `self.dp` to the strategy)
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param dataframe: analyzed dataframe
        :param candle_type: Any of the enum CandleType (must match trading
        mode!)
        """
        pair_key = (pair, timeframe, candle_type)
        self.cached_pairs[pair_key] = (dataframe, datetime.now(timezone.utc))

    def emit_df(
        self,
        pair: PairWithTimeframe,
        dataframe: DataFrame,
        new_candle: bool
    ) -> None:
        """
        Send this dataframe as an ANALYZED_DF message to RPC

        :param pair: PairWithTimeframe tuple
        :param dataframe: Dataframe to emit
        :param new_candle: This is a new candle
        """
        msg: RPCAnalyzedDFMsg = {
            'type': RPCMessageType.ANALYZED_DF,
            'data': {
                'key': pair,
                'df': dataframe.tail(1),
                'la': datetime.now(timezone.utc)
            }
        }
        self.bot.rpc.send_msg(msg)
        if new_candle:
            self.bot.rpc.send_msg({
                'type': RPCMessageType.NEW_CANDLE,
                'data': pair,
            })
