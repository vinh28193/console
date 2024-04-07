from enum import Enum


class State(Enum):
    """
    Bot application states
    """
    STARTED = 0
    RUNNING = 1
    STOPPED = 2

    def __str__(self):
        return f"{self.name.lower()}"
