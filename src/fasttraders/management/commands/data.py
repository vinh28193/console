from typing import List
from datetime import timedelta, datetime
from consoles.conf import settings
from consoles.management.base import BaseCommand
from fasttraders.data.converter import convert_trades_to_ohlcv
from fasttraders.log import logger
from fasttraders.ultis.pair import dynamic_expand_pairs
from fasttraders.ultis.timerange import TimeRange
from fasttraders.constants import DL_DATA_TIMEFRAMES

from fasttraders.data.utils import (
    refresh_backtest_trades_data,
    refresh_backtest_ohlcv_data, MOCKED_MARKET_PAIRS, MARKET_PAIRS
)


class Command(BaseCommand):

    def handle(self, *args, **options):

        timerange = TimeRange()
        if 'days' in options:
            time_since = (
                datetime.now() - timedelta(days=options['days'])
            ).strftime("%Y%m%d")
            timerange = TimeRange.parse_timerange(f'{time_since}-')

        if 'timerange' in options:
            timerange = timerange.parse_timerange(options['timerange'])

        pairs_not_available: List[str] = []

        # fetch from market
        available_pairs = list(MARKET_PAIRS)
        expanded_pairs = dynamic_expand_pairs(available_pairs)
        # options["download"] = True
        api = None
        # Start downloading

        try:
            trading_mode = "spot"
            datadir = options.get("datadir", settings.DATA_PATH)
            new_pairs_days = options.get("new_pairs_days", 10)
            data_format_trades = options.get("trade_format", "json")
            data_format_ohlcv = options.get("ohlcv_format", "json")
            erase = options.get("erase", False)
            timeframes = options.get("timeframes", DL_DATA_TIMEFRAMES)

            if options.get('download', False):

                pairs_not_available = refresh_backtest_trades_data(
                    pairs=expanded_pairs, datadir=datadir,
                    timerange=timerange,
                    new_pairs_days=new_pairs_days,
                    erase=erase,
                    data_format=data_format_trades
                )
                # Convert downloaded trade data to different timeframes
                convert_trades_to_ohlcv(
                    pairs=expanded_pairs, timeframes=timeframes,
                    datadir=datadir, timerange=timerange,
                    erase=erase,
                    data_format_ohlcv=data_format_ohlcv,
                    data_format_trades=data_format_trades,
                )
            else:
                pairs_not_available = refresh_backtest_ohlcv_data(
                    pairs=expanded_pairs,
                    timeframes=timeframes,
                    datadir=datadir, timerange=timerange,
                    new_pairs_days=new_pairs_days,
                    erase=erase,
                    data_format=data_format_ohlcv,
                    trading_mode=trading_mode,
                    prepend=options.get("prepend_data", False)
                )
        finally:
            if pairs_not_available:
                logger.info(
                    f"Pairs [{','.join(pairs_not_available)}] not available."
                )
