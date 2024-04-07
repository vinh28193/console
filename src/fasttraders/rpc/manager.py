from abc import abstractmethod
from collections import deque
from datetime import datetime
from typing import Any, List, Literal, Optional, TypedDict, Union, Dict

from fasttraders.enums import State, RPCMessageType
from fasttraders.log import logger
from .types import RPCSendMsg

from .base import RPC, RPCHandler


class RPCManager:

    def __init__(self, bot):
        self.handlers: List[RPCHandler] = []
        self.rpc = RPC(bot)
        from .telegram import TelegramHandler
        self.handlers.append(TelegramHandler(self.rpc))

    def cleanup(self) -> None:
        """ Stops all enabled rpc modules """
        logger.info('Cleaning up rpc modules ...')
        while self.handlers:
            handler = self.handlers.pop()
            logger.info('Cleaning up rpc.%s ...', handler.name)
            handler.cleanup()
            del handler

    def send_msg(self, msg: RPCSendMsg) -> None:
        """
        Send given message to all registered rpc modules.
        A message consists of one or more key value pairs of strings.
        e.g.:
        {
            'status': 'stopping bot'
        }
        """

        for handler in self.handlers:
            logger.debug('Forwarding message to rpc.%s', handler.name)
            try:
                handler.send_msg(msg)
            except NotImplementedError:
                logger.error(
                    f"Message type '{msg['type']}' not implemented by handler "
                    f"{handler.name}.")
            except Exception:
                logger.exception(
                    'Exception occurred within RPC module %s',
                    handler.name
                )

    def process_msg_queue(self, queue: deque) -> None:
        """
        Process all messages in the queue.
        """
        while queue:
            msg = queue.popleft()
            logger.info('Sending rpc msg: %s', msg)

    def startup_messages(self) -> None:
        self.send_msg({
            'type': RPCMessageType.STARTUP,
            'status': 'Test'
        })
