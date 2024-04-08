from .decimal_to_precision import ROUND_DOWN, ROUND_UP


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