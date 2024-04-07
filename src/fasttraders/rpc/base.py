from abc import abstractmethod
from typing_extensions import Dict
from fasttraders.enums import State, RPCMessageType

from .types import RPCSendMsg


class RPC:

    def __init__(self, bot):
        self.bot = bot

    def rpc_start(self) -> Dict[str, str]:
        """ Handler for start """
        if self.bot.state == State.RUNNING:
            return {'status': 'already running'}

        self.bot.state = State.RUNNING
        return {'status': 'starting ...'}

    def rpc_stop(self) -> Dict[str, str]:
        """ Handler for stop """
        if self.bot.state == State.RUNNING:
            self.bot.state = State.STOPPED
            return {'status': 'stopping ...'}

        return {'status': 'already stopped'}


class RPCHandler:
    def __init__(self, rpc: RPC):
        self.rpc = rpc

    @property
    def name(self) -> str:
        """ Returns the lowercase name of the implementation """
        return self.__class__.__name__.lower()

    @abstractmethod
    def cleanup(self) -> None:
        """ Cleanup pending module resources """

    @abstractmethod
    def send_msg(self, msg: RPCSendMsg) -> None:
        """ Sends a message to all registered rpc modules """
