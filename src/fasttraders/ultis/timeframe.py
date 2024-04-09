import re
import arrow
from datetime import datetime, timezone
from typing import Optional
from .decimal_to_precision import ROUND_DOWN, ROUND_UP


def dt_now() -> datetime:
    """Return the current datetime in UTC."""
    return datetime.now(timezone.utc)


def dt_utc(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0,
    second: int = 0, microsecond: int = 0
) -> datetime:
    """Return a datetime in UTC."""
    return datetime(
        year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc
    )


def dt_ts(dt: Optional[datetime] = None) -> int:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the current datetime in UTC.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return int(dt_now().timestamp() * 1000)


def dt_ts_def(dt: Optional[datetime], default: int = 0) -> int:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the given default.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return default


def dt_ts_none(dt: Optional[datetime]) -> Optional[int]:
    """
    Return dt in ms as a timestamp in UTC.
    If dt is None, return the given default.
    """
    if dt:
        return int(dt.timestamp() * 1000)
    return None


def dt_floor_day(dt: datetime) -> datetime:
    """Return the floor of the day for the given datetime."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def dt_from_ts(timestamp: float) -> datetime:
    """
    Return a datetime from a timestamp.
    :param timestamp: timestamp in seconds or milliseconds
    """
    if timestamp > 1e10:
        # Timezone in ms - convert to seconds
        timestamp /= 1000
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def shorten_date(_date: str) -> str:
    """
    Trim the date so it fits on small screens
    """
    new_date = re.sub('seconds?', 'sec', _date)
    new_date = re.sub('minutes?', 'min', new_date)
    new_date = re.sub('hours?', 'h', new_date)
    new_date = re.sub('days?', 'd', new_date)
    new_date = re.sub('^an?', '1', new_date)
    return new_date


def dt_humanize(dt: datetime, **kwargs) -> str:
    """
    Return a humanized string for the given datetime.
    :param dt: datetime to humanize
    :param kwargs: kwargs to pass to arrow's humanize()
    """
    return arrow.get(dt).humanize(**kwargs)


def format_date(date: Optional[datetime]) -> str:
    """
    Return a formatted date string.
    Returns an empty string if date is None.
    :param date: datetime to format
    """
    if date:
        return date.strftime('%Y-%m-%d %H:%M:%S')
    return ''


def format_ms_time(date: int) -> str:
    """
    convert MS date to readable format.
    : epoch-string in ms
    """
    return datetime.fromtimestamp(date / 1000.0).strftime('%Y-%m-%dT%H:%M:%S')


def parse_timeframe(timeframe):
    amount = int(timeframe[0:-1])
    unit = timeframe[-1]
    if 'y' == unit:
        scale = 60 * 60 * 24 * 365
    elif 'M' == unit:
        scale = 60 * 60 * 24 * 30
    elif 'w' == unit:
        scale = 60 * 60 * 24 * 7
    elif 'd' == unit:
        scale = 60 * 60 * 24
    elif 'h' == unit:
        scale = 60 * 60
    elif 'm' == unit:
        scale = 60
    elif 's' == unit:
        scale = 1
    else:
        raise ValueError('timeframe unit {} is not supported'.format(unit))
    return amount * scale


def round_timeframe(timeframe, timestamp, direction=ROUND_DOWN):
    ms = parse_timeframe(timeframe) * 1000
    # Get offset based on timeframe in milliseconds
    offset = timestamp % ms
    return timestamp - offset + (ms if direction == ROUND_UP else 0)


def timeframe_to_next_date(timeframe: str,
                           date: Optional[datetime] = None) -> datetime:
    """
    Use Timeframe and determine next candle.
    :param timeframe: timeframe in string format (e.g. "5m")
    :param date: date to use. Defaults to now(utc)
    :returns: date of next candle (with utc timezone)
    """
    if not date:
        date = datetime.now(timezone.utc)
    new_timestamp = round_timeframe(timeframe, dt_ts(date), ROUND_UP) // 1000
    return dt_from_ts(new_timestamp)
