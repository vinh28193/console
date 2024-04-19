from datetime import datetime, timedelta, timezone
from typing import Optional, Union

import numpy as np
import pandas as pd
from fasttraders.constants import DEFAULT_TRADES_COLUMNS
from fasttraders.ultis.timeframe import (
    timeframe_to_minutes, timeframe_to_seconds, dt_from_ts, dt_now
)


def generate_trades_history(
    n_rows, start_date: Optional[datetime] = None, days=5
):
    np.random.seed(42)
    if not start_date:
        start_date = dt_now()

        # Generate random data
    end_date = start_date + timedelta(days=days)
    _start_timestamp = start_date.timestamp()
    _end_timestamp = pd.to_datetime(end_date).timestamp()

    random_timestamps_in_seconds = np.random.uniform(
        _start_timestamp, _end_timestamp, n_rows
    )
    timestamp = pd.to_datetime(random_timestamps_in_seconds, unit='s')

    _id = [
        f'a{np.random.randint(1e6, 1e7 - 1)}cd{np.random.randint(100, 999)}'
        for _ in range(n_rows)
    ]

    side = np.random.choice(['buy', 'sell'], n_rows)

    # Initial price and subsequent changes
    initial_price = 0.019626
    price_changes = np.random.normal(0, initial_price * 0.05, n_rows)
    price = np.cumsum(np.concatenate(([initial_price], price_changes)))[:n_rows]

    amount = np.random.uniform(0.011, 20, n_rows)
    cost = price * amount

    # Create DataFrame
    df = pd.DataFrame({
        'timestamp': timestamp, 'id': _id, 'type': None,
        'side': side,
        'price': price, 'amount': amount, 'cost': cost
    })
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.sort_values('timestamp').reset_index(drop=True)
    assert list(df.columns) == DEFAULT_TRADES_COLUMNS + ['date']
    return df


def generate_test_data(
    timeframe: str, size: int,
    start: Optional[Union[datetime, str, int, float]] = None
):
    np.random.seed(42)

    if not start:
        start = dt_now()
    elif isinstance(start, (int, float)):
        start = dt_from_ts(start)
    base = np.random.normal(20, 2, size=size)
    if timeframe == '1y':
        date = pd.date_range(start, periods=size, freq='1YS', tz='UTC')
    elif timeframe == '1M':
        date = pd.date_range(start, periods=size, freq='1MS', tz='UTC')
    elif timeframe == '3M':
        date = pd.date_range(start, periods=size, freq='3MS', tz='UTC')
    elif timeframe == '1w' or timeframe == '7d':
        date = pd.date_range(start, periods=size, freq='1W-MON', tz='UTC')
    else:
        tf_mins = timeframe_to_minutes(timeframe)
        if tf_mins >= 1:
            date = pd.date_range(
                start, periods=size, freq=f'{tf_mins}min', tz='UTC'
            )
        else:
            tf_secs = timeframe_to_seconds(timeframe)
            date = pd.date_range(
                start, periods=size, freq=f'{tf_secs}s', tz='UTC'
            )
    df = pd.DataFrame({
        'date': date,
        'open': base,
        'high': base + np.random.normal(2, 1, size=size),
        'low': base - np.random.normal(2, 1, size=size),
        'close': base + np.random.normal(0, 1, size=size),
        'volume': np.random.normal(200, size=size)
    })
    df = df.dropna()
    return df


def generate_test_data_raw(
    timeframe: str, size: int, start: Optional[Union[datetime, str, int, float]] = None
):
    """ Generates data in the ohlcv format used by ccxt """
    df = generate_test_data(timeframe, size, start)
    df['date'] = df.loc[:, 'date'].view(np.int64) // 1000 // 1000
    return list(
        list(x) for x in zip(*(df[x].values.tolist() for x in df.columns))
    )


async def async_generate_test_data_raw(
    timeframe: str, size: int, start: Optional[Union[datetime, str, int, float]] = None
):
    return generate_test_data_raw(timeframe, size, start)
