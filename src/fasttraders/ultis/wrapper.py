from functools import wraps
from typing import TypeVar, Callable, Any, cast

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
