import logging
import time
import traceback
from datetime import datetime, timezone, timedelta
from os import getpid
from typing import Optional, Callable, Any

from consoles.conf import settings
from fasttraders.log import logger
from fasttraders.bot import Bot
from fasttraders.enums import State
from fasttraders.ultis.timeframe import timeframe_to_next_date, format_date


class Worker:
    """
    Bot worker class
    """

    def __init__(self, **options) -> None:
        """
        Init all variables and objects the bot needs to work
        """
        logger.info(f"Starting worker")

        self.options = options
        self._init()

        self._heartbeat_msg: float = 0

        # Tell systemd that we completed initialization phase
        self._notify("READY=1")

    def _init(self) -> None:
        # Init the instance of the bot
        self.bot = Bot()
        self._throttle_secs = getattr(settings, 'PROCESS_THROTTLE_SECS', 5)
        self._heartbeat_interval = getattr(settings, 'HEARTBEAT_INTERVAL', 60)

        # Todo: sdnotify
        self._sd_notify = None

    def _notify(self, message: str) -> None:
        """
        Removes the need to verify in all occurrences if sd_notify is enabled
        :param message: Message to send to systemd if it's enabled.
        """
        if self._sd_notify:
            logger.debug(f"sd_notify: {message}")
            self._sd_notify.notify(message)

    def run(self) -> None:
        state = None
        while True:
            state = self._worker(old_state=state)

    def _worker(self, old_state: Optional[State]) -> State:
        """
        The main routine that runs each throttling iteration and handles the
        states.
        :param old_state: the previous service state from the previous call
        :return: current service state
        """

        state = self.bot.state
        # Log state transition
        if state != old_state:
            logger.info(
                f"Changing state"
                f"{f'from {old_state.name}' if old_state else ''} to: "
                f"{state.name}"
            )
            if state == State.RUNNING:
                self.bot.startup()

            if state == State.STOPPED:
                self.bot.cleanup()

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0

        if state == State.STOPPED:
            # Ping systemd watchdog before sleeping in the stopped state
            self._notify("WATCHDOG=1\nSTATUS=State: STOPPED.")

            self._throttle(func=self.stopped, throttle_secs=self._throttle_secs)

        elif state == State.RUNNING:
            # Ping systemd watchdog before throttling
            self._notify("WATCHDOG=1\nSTATUS=State: RUNNING.")

            # Use an offset of 1s to ensure a new candle has been issued
            self._throttle(func=self.running, throttle_secs=self._throttle_secs)

        if self._heartbeat_interval:
            now = time.time()
            if (now - self._heartbeat_msg) > self._heartbeat_interval:
                logger.info(
                    f"Bot heartbeat. PID={getpid()}, "
                    f" state='{state.name}'"
                )
                self._heartbeat_msg = now

        return state

    def _throttle(
        self, func: Callable[..., Any], throttle_secs: float,
        timeframe: Optional[str] = None, timeframe_offset: float = 1.0,
        *args, **kwargs
    ) -> Any:
        """
        Throttles the given callable that it
        takes at least `min_secs` to finish execution.
        :param func: Any callable
        :kwargs throttle_secs: throttling interation execution time limit in
        seconds
        :return: Any (result of execution of func)
        """
        last_throttle_start_time = time.time()
        logger.debug("========================================")
        result = func(*args, **kwargs)
        time_passed = time.time() - last_throttle_start_time
        sleep_duration = throttle_secs - time_passed
        if timeframe:
            next_tf = timeframe_to_next_date(timeframe)
            next_tft = next_tf.timestamp() - time.time()
            next_tf_with_offset = next_tft + timeframe_offset
            if next_tft < sleep_duration < next_tf_with_offset:
                sleep_duration = next_tf_with_offset
            sleep_duration = min(sleep_duration, next_tf_with_offset)
        sleep_duration = max(sleep_duration, 0.0)
        next_iter = (
            datetime.now(timezone.utc) +
            timedelta(seconds=sleep_duration)
        )

        logger.info(
            f"Throttling with '{func.__name__}()': sleep for "
            f"{sleep_duration:.2f}s, "
            f"last iteration took {time_passed:.2f}s. "
            f"next: {format_date(next_iter)}"
        )

        self._sleep(sleep_duration)
        return result

    @staticmethod
    def _sleep(sleep_duration: float) -> None:
        """Local sleep method - to improve testability"""
        time.sleep(sleep_duration)

    def stopped(self) -> None:
        self.bot.stopped()

    def running(self) -> None:
        try:
            self.bot.loop_update()
        except Exception:  # noqa
            tb = traceback.format_exc()
            hint = 'Issue `/start` if you think it is safe to restart.'

            self.bot.notify_status(
                f'*Exception:*\n```\n{tb}```\n {hint}',
                msg_type='Exception'
            )

            logger.exception('Stopping bot ...')
            self.bot.state = State.STOPPED

    def exit(self) -> None:
        self._notify("STOPPING=1")
        self.bot.notify_status('process died')
        self.bot.cleanup()
