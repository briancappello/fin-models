from __future__ import annotations

import pandas as pd
import talib as ta


def is_crossed(
    df: pd.DataFrame,
    *,
    column: str = None,
    value: int | float = None,
):
    """
    Returns true if there was an overnight or intraday cross of the given value
    """
    if column is None and value is None:
        raise TypeError('One of `column` or `value` is required to be passed.')
    elif column and column not in df.columns:
        raise ValueError(f'The column {column!r} is missing from `df`.')

    value = df[column].iloc[-1] if column else value
    intraday_cross = df.iloc[-1].Open < value < df.iloc[-1].Close
    overnight_cross = df.iloc[-2].Close < value < df.iloc[-1].Open
    return intraday_cross or overnight_cross


def num_bars_since(df: pd.DataFrame, ts: pd.Timestamp):
    return len(df) - df.index.get_loc(ts) - 1
