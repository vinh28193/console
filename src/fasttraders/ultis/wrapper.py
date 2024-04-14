from datetime import datetime, timezone
from functools import wraps
from typing import TypeVar, Callable, Any, cast

from cachetools import TTLCache

F = TypeVar('F', bound=Callable[..., Any])


def safe_wrapper(f: F, default_value=None, raise_error=False) -> F:
    """
    Wrapper around user-provided methods and functions.
    Caches all exceptions and returns either the default_retval (if it's not
    None) or raises
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as error:
            if default_value is None and not raise_error:
                raise Exception(str(error)) from error
            return default_value
        except Exception as error:
            if default_value is None and not raise_error:
                raise Exception(str(error)) from error
            return raise_error

    return cast(F, wrapper)


class PeriodicCache(TTLCache):
    """
    Special cache that expires at "straight" times
    A timer with ttl of 3600 (1h) will expire at every full hour (:00).
    """

    def __init__(self, maxsize, ttl, getsizeof=None):
        def local_timer():
            ts = datetime.now(timezone.utc).timestamp()
            offset = (ts % ttl)
            return ts - offset

        # Init with smlight offset
        super().__init__(
            maxsize=maxsize, ttl=ttl - 1e-5, timer=local_timer,
            getsizeof=getsizeof
        )
