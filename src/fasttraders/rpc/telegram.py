import asyncio
import json
import logging
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial, wraps
from html import escape
from itertools import chain
from math import isnan
from threading import Thread
from typing import (
    Any, Callable, Coroutine, Dict, List, Literal, Optional,
    Union
)

from consoles.conf import settings
from fasttraders.log import logger
from fasttraders.enums import RPCMessageType
from tabulate import tabulate
from telegram import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton,
    ReplyKeyboardMarkup, Update
)
from telegram.constants import MessageLimit, ParseMode
from telegram.error import BadRequest, NetworkError, TelegramError
from telegram.ext import (
    Application, CallbackContext, CallbackQueryHandler,
    CommandHandler
)
from telegram.helpers import escape_markdown
from .base import RPCHandler, RPC


def authorized_only(command_handler: Callable[..., Coroutine[Any, Any, None]]):
    """
    Decorator to check if the message comes from the correct chat_id
    :param command_handler: Telegram CommandHandler
    :return: decorated function
    """

    @wraps(command_handler)
    async def wrapper(self, *args, **kwargs):
        """ Decorator logic """
        update = kwargs.get('update') or args[0]

        # Reject unauthorized messages
        if update.callback_query:
            cchat_id = int(update.callback_query.message.chat.id)
        else:
            cchat_id = int(update.message.chat_id)

        chat_id = int(settings.TELEGRAM_CHAT_ID)
        if cchat_id != chat_id:
            logger.info(
                f'Rejected unauthorized message from: {update.message.chat_id}')
            return wrapper
        # Todo: Rollback session to avoid getting data stored in a transaction.
        logger.debug(
            'Executing handler: %s for chat_id: %s',
            command_handler.__name__,
            chat_id
        )
        try:
            return await command_handler(self, *args, **kwargs)
        except Exception as e:
            await self._send_msg(str(e))
        except BaseException:
            logger.exception('Exception occurred within Telegram module')
        finally:
            # Todo: remove session
            pass

    return wrapper


class TelegramHandler(RPCHandler):
    """  This class handles all telegram communication """
    def __init__(self, rpc: RPC) -> None:
        """
        Init the Telegram call, and init the super class RPCHandler
        :return: None
        """
        super().__init__(rpc)

        self._app: Application
        self._loop: asyncio.AbstractEventLoop
        self._init_keyboard()
        self._start_thread()

    def send_msg(self, msg: RPCMessageType) -> None:
        msg_type = msg.get("type")
        notification_status = 'on'

        if notification_status == 'off':
            logger.info(f"Notification '{msg_type or 'unknown'}' not sent.")
            # Notification disabled
            return

        message = self.compose_message(deepcopy(msg))
        if message:
            disable_notification = notification_status == 'silent'
            asyncio.run_coroutine_threadsafe(
                self._send_msg(
                    msg=msg, disable_notification=disable_notification),
                self._loop
            )

    def compose_message(self, msg: RPCMessageType) -> Optional[str]:
        text = str(msg)
        return text

    async def _send_msg(
        self, msg: str, parse_mode: str = ParseMode.MARKDOWN,
        disable_notification: bool = False,
        keyboard: Optional[List[List[InlineKeyboardButton]]] = None,
        callback_path: str = "",
        reload_able: bool = False,
        query: Optional[CallbackQuery] = None
    ) -> None:
        """
        Send given markdown message
        :param msg: message
        :param parse_mode: telegram parse mode
        :return: None
        """
        reply_markup: Union[InlineKeyboardMarkup, ReplyKeyboardMarkup]
        if query:
            await self._update_msg(
                query=query, text=msg, parse_mode=parse_mode,
                callback_path=callback_path,
                reload_able=reload_able
            )
            return
        if reload_able and getattr(settings, "TELEGRAM_RELOAD", False):
            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("Refresh", callback_data=callback_path)]
            ])
        else:
            if keyboard is not None:
                reply_markup = InlineKeyboardMarkup(keyboard)
            else:
                reply_markup = ReplyKeyboardMarkup(
                    self._keyboard,
                    resize_keyboard=True
                )
        chat_id = settings.TELEGRAM_CHAT_ID
        try:
            try:
                await self._app.bot.send_message(
                    chat_id,
                    text=msg,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_notification=disable_notification,
                )
            except NetworkError as network_err:
                # Sometimes the telegram server resets the current connection,
                # if this is the case we send the message again.
                logger.warning(
                    'Telegram NetworkError: %s! Trying one more time.',
                    network_err.message
                )
                await self._app.bot.send_message(
                    chat_id,
                    text=msg,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_notification=disable_notification,
                )
        except TelegramError as telegram_err:
            logger.warning(
                'TelegramError: %s! Giving up on that message.',
                telegram_err.message
            )

    async def _update_msg(
        self, query: CallbackQuery, text: str, callback_path: str = "",
        reload_able: bool = False, parse_mode: str = ParseMode.MARKDOWN
    ) -> None:
        if reload_able:
            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("Refresh", callback_data=callback_path)],
            ])
        else:
            reply_markup = InlineKeyboardMarkup([[]])
        text += f"\nUpdated: {datetime.now().ctime()}"
        if not query.message:
            return

        try:
            await query.edit_message_text(
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
        except BadRequest as e:
            if 'not modified' in e.message.lower():
                pass
            else:
                logger.warning('TelegramError: %s', e.message)
        except TelegramError as telegram_err:
            logger.warning('TelegramError: %s! Giving up on that message.',
                           telegram_err.message)

    def _start_thread(self):
        """
        Creates and starts the polling thread
        """
        self._thread = Thread(target=self._init, name='FTTelegram')
        self._thread.start()

    def _init_keyboard(self) -> None:
        """
        Validates the keyboard configuration from telegram config
        section.
        """
        self._keyboard: List[List[Union[str, KeyboardButton]]] = [
            ['/status', '/performance'],
            ['/count', '/start', '/stop', '/help']
        ]
        # do not allow commands with mandatory arguments and critical cmds
        # TODO: DRY! - its not good to list all valid cmds here. But otherwise
        #       this needs refactoring of the whole telegram module (same
        #       problem in _help()).
        valid_keys: List[str] = [
            r'/start$', r'/stop$', r'/status$',
        ]
        # Create keys for generation
        valid_keys_print = [k.replace('$', '') for k in valid_keys]

        # custom keyboard specified in config.json
        cust_keyboard = getattr(settings, "TELEGRAM_KEYBOARD", [])
        if cust_keyboard:
            combined = "(" + ")|(".join(valid_keys) + ")"
            # check for valid shortcuts
            invalid_keys = [b for b in chain.from_iterable(cust_keyboard)
                            if not re.match(combined, b)]
            if len(invalid_keys):
                err_msg = (
                    f'Invalid commands for '
                    f'Telegram keyboard: {invalid_keys}'
                    f'\nvalid commands are: {valid_keys_print}'
                )
                raise Exception(err_msg)
            else:
                self._keyboard = cust_keyboard
                logger.info(
                    f'using custom keyboard: {self._keyboard}'
                )

    def _init_telegram_app(self):
        return Application.builder().token(
            settings.TELEGRAM_BOT_TOKEN
        ).build()

    def _init(self) -> None:
        """
        Initializes this module with the given config,
        registers all known command handlers
        and starts polling for message updates
        Runs in a separate thread.
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self._app = self._init_telegram_app()

        # Register command handler and start telegram message polling
        handles = [
            CommandHandler('start', self._start),
            CommandHandler('stop', self._stop),
        ]
        callbacks = [

        ]
        for handle in handles:
            self._app.add_handler(handle)

        for callback in callbacks:
            self._app.add_handler(callback)

        logger.info(
            'telegram is listening for following commands: %s',
            [[x for x in sorted(h.commands)] for h in handles]
        )
        self._loop.run_until_complete(self._startup_telegram())

    async def _startup_telegram(self) -> None:
        await self._app.initialize()
        await self._app.start()
        if self._app.updater:
            await self._app.updater.start_polling(
                bootstrap_retries=-1,
                timeout=20,
                # read_latency=60,  # Assumed transmission latency
                drop_pending_updates=True,
                # stop_signals=[],  # Necessary as we don't run on the main
                # thread
            )
            while True:
                await asyncio.sleep(10)
                if not self._app.updater.running:
                    break

    async def _cleanup_telegram(self) -> None:
        if self._app.updater:
            await self._app.updater.stop()
        await self._app.stop()
        await self._app.shutdown()

    def cleanup(self) -> None:
        """
        Stops all running telegram threads.
        :return: None
        """
        # This can take up to `timeout` from the call to `start_polling`.
        asyncio.run_coroutine_threadsafe(self._cleanup_telegram(), self._loop)
        self._thread.join()

    @authorized_only
    async def _start(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /start.
        Starts Thread
        :param update: message update
        :return: None
        """
        msg = self.rpc.rpc_start()
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _stop(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /stop.
        Stops Thread
        :param update: message update
        :return: None
        """
        msg = self.rpc.rpc_stop()
        await self._send_msg(f"Status: `{msg['status']}`")
