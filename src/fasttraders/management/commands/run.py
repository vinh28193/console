import logging
import signal
from typing import Any, Dict
from consoles.management.base import BaseCommand
from fasttraders.log import logger


class Command(BaseCommand):

    def handle(self, *args, **options):
        """
            Main entry point for trading mode
            """
        # Import here to avoid loading worker module when it's not used
        from fasttraders.worker import Worker

        def term_handler(signum, frame):
            # Raise KeyboardInterrupt - so we can handle it in the same way
            # as Ctrl-C
            raise KeyboardInterrupt()

        # Create and run worker
        worker = None
        try:
            signal.signal(signal.SIGTERM, term_handler)
            worker = Worker(**options)
            worker.run()
        except Exception as e:
            logger.error(str(e))
            print(e)
            logger.exception("Fatal exception!")
        except KeyboardInterrupt:
            logger.info('SIGINT received, aborting ...')
        finally:
            if worker:
                logger.info("worker found ... calling exit")
                worker.exit()
        return 0
