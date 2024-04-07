from threading import Lock
from consoles.conf import settings
from fasttraders.enums import State, RPCMessageType
from fasttraders.rpc import RPCManager
from fasttraders.scheduler import SafeScheduler
from fasttraders.strategies import SimpleStrategy


class Bot:
    def __init__(self) -> None:
        # Init bot state
        self.state = State.STOPPED
        # Init data

        # RPC
        self.rpc: RPCManager = RPCManager(self)
        # Set initial bot state from config
        initial_state = getattr(settings, "BOT_INIT_STATE", State.STARTED.name)
        self.state = State[initial_state.upper()]

        # Protect exit-logic from forcesell and vice versa
        self._exit_lock = Lock()

        self._schedule = SafeScheduler()
        self.strategy = SimpleStrategy(self)
        self.strategy.ft_bot_start()

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

    def update(self) -> None:
        pass

    def cleanup(self) -> None:
        print('Cleaning up modules ...')
        self.rpc.cleanup()
