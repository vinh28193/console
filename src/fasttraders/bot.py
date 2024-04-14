from datetime import datetime, timezone
from threading import Lock
from typing import List

from consoles.conf import settings
from fasttraders.constants import ListPairsWithTimeframes, PairWithTimeframe
from fasttraders.data.provider import DataProvider
from fasttraders.enums import State, RPCMessageType, CandleType
from fasttraders.exchange import Exchange
from fasttraders.rpc import RPCManager
from fasttraders.scheduler import SafeScheduler
from fasttraders.strategies import SimpleStrategy


class Bot:
    def __init__(self) -> None:
        self.active_pair_whitelist: List[str] = []
        # Init bot state
        self.state = State.STOPPED
        # Init data
        self.exchange = Exchange(validate=True)
        # RPC
        self.rpc: RPCManager = RPCManager(self)
        self.dp = DataProvider(self)
        # Set initial bot state from config
        initial_state = getattr(settings, "BOT_INIT_STATE", State.STOPPED.name)
        self.state = State[initial_state.upper()]

        # Protect logic
        self._exit_lock = Lock()

        self._schedule = SafeScheduler()
        self.strategy = SimpleStrategy(self)
        self.strategy.bot_start()

    def notify_status(self, msg: str, msg_type=RPCMessageType.STATUS) -> None:
        """
        Public method for users of this class (worker, etc.) to send
        notifications
        via RPC about changes in the bot status.
        """
        self.rpc.send_msg({
            'type': msg_type,
            'status': msg
        })

    def startup(self) -> None:
        self.rpc.startup_messages()

    def stopped(self):
        pass

    def loop_update(self) -> None:
        self.active_pair_whitelist = [
            "BTC/USDT", "LTC/USDT", "ETH/USDT"
        ]
        # Refreshing candles
        self.dp.refresh(
            [
                (pair, "5m", CandleType.SPOT)
                for pair in self.active_pair_whitelist
            ],
            self.strategy.gather_informative_pairs())

        self.strategy.bot_loop_start(
            current_time=datetime.now(timezone.utc)
        )

        self.strategy.analyzes(self.active_pair_whitelist)

    def cleanup(self) -> None:
        print('Cleaning up modules ...')
        self.rpc.cleanup()
