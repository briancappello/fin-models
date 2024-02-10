import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import talib as ta

from scipy.signal import argrelextrema


"""
s = df.some_bool_col
s[s].index ==> indexes (timestamps) where True
"""


def volume_30ma(df: pd.DataFrame):
    return ta.SMA(df.Volume, timeperiod=30).iloc[-1]


def days_above_percent_change(df: pd.DataFrame, pct_change: float) -> pd.DataFrame:
    """
    Filter days where percent change was greater than `pct_change`.
    """
    percent_changes = pct_changes_df(df)
    if pct_change > 0:
        s = percent_changes >= pct_change
    else:
        s = percent_changes <= pct_change
    return percent_changes[s[s].index]


def days_with_above_avg_volume(
    df: pd.DataFrame,
    vol_ma: int = 50,
    multiple: float = 3,
) -> pd.DataFrame:
    """
    Filter days where volume as at least `multiple` greater than trailing moving average.
    """
    vol_ma_df = ta.SMA(df.Volume, timeperiod=vol_ma)
    s = (df.Volume / vol_ma_df) > multiple
    return vol_ma_df[s[s].index]


def bars_since_previous_high(df: pd.DataFrame) -> int:
    """
    Count the number of days since the price was higher than the current close.

    Given EOD data for a date
    0 == The latest bar is the highest bar
    1 == Yesterday was higher
    50 == 50 days ago was the most recent bar higher than the latest bar
    """

    bar = df.iloc[-1]
    priors = df.iloc[:-1]

    higher_bars_filter = priors.Close > bar.Close
    higher_bars = priors[higher_bars_filter]
    if higher_bars.empty:
        return 0

    most_recent_higher_ts = higher_bars.index[-1]
    return len(df) - df.index.get_loc(most_recent_higher_ts) - 1


def macd_divergence(df: pd.DataFrame):
    """
    macd, macd_signal, histogram = ta.MACD(df.Close)

    right side, macd should be above the signal
    left side, both values should be lower than signal on the right
    """


def pct_changes_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the percent changes from one bar to the next.
    """
    prev_closes = df.Close.shift()
    return ((df.Close - prev_closes) / prev_closes) * 100


def pct_changes_bodies_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the percent change of bodies.
    """
    return ((df.Close - df.Open) / df.Open) * 100


def is_expanding_volume(df: pd.DataFrame, num_bars: int = 3) -> bool:
    if len(df) < num_bars:
        return False
    return (df.index[-num_bars:] == df.Volume[-num_bars:].sort_values().index).all()


def is_expanding_bodies(
    df: pd.DataFrame, num_bars: int = 3, bullish: bool = True
) -> bool:
    bars = df[-num_bars:]
    bodies = bars.Close - bars.Open

    # check all bars are in the same direction
    # allow the first bar to be a doji w/ same open & close
    all_up_days = (bodies >= 0).all()
    all_down_days = (bodies <= 0).all()
    if (bullish and not all_up_days) or (not bullish and not all_down_days):
        return False

    is_advancing = (bars.index == bars.Close.sort_values(ascending=bullish).index).all()
    bodies_expanding = (bars.index == bodies.sort_values(ascending=bullish).index).all()
    return is_advancing and bodies_expanding


def median_volume(df: pd.DataFrame, num_bars: int = 50) -> float:
    if len(df) <= num_bars:
        return float(np.median(df.Volume))
    return float(np.median(df.Volume[-num_bars:]))


def median_body(df: pd.DataFrame, num_bars: int = 50) -> float:
    bodies = (df.Close[-num_bars:] - df.Open[-num_bars:]).abs()
    return float(np.median(bodies))


def volume_multiple_of_median(df: pd.DataFrame, num_bars: int = 50) -> float:
    median_vol = median_volume(df, num_bars)
    if median_vol:
        return df.Volume.iloc[-1] / median_vol
    return 0


def mean_volume(df: pd.DataFrame, num_bars: int = 50) -> float:
    if len(df) <= num_bars:
        return float(np.mean(df.Volume))
    return float(np.mean(df.Volume[-num_bars:]))


def volume_multiple_of_mean(df: pd.DataFrame, num_bars: int = 50) -> float:
    mean_vol = mean_volume(df, num_bars)
    if mean_vol:
        return df.Volume.iloc[-1] / mean_vol
    return 0


def volume_sum_of_prior_days(df: pd.DataFrame) -> int:
    if len(df) < 2 or df.Volume.iloc[-2] > df.Volume.iloc[-1]:
        return 0

    sum = 0
    prior_days = 0
    while len(df) >= (prior_days + 2) and sum < df.Volume.iloc[-1]:
        sum += df.Volume.iloc[-(prior_days + 2)]
        prior_days += 1
    return prior_days


def is_trading_safe(df):
    return len(df) > 3 and median_volume(df) > 200_000


def crossed_ma(df: pd.DataFrame, ma: int = 200, within_bars: int = 1):
    if len(df) < ma:
        return False

    sma = ta.SMA(df.Close, timeperiod=ma)
    for i in range(1, within_bars + 1):
        # include gaps
        if df.Close.iloc[-(i + 1)] < sma.iloc[-i] < df.Close.iloc[-i]:
            return True
    return False


def gapped_ma(df: pd.DataFrame, ma: int = 200):
    if len(df) < ma:
        return False
    yesterday, today = df.iloc[-2], df.iloc[-1]
    return yesterday.Close < ta.SMA(df.Close, timeperiod=ma) < today.Open


def true_false_counts(series: pd.Series):
    """
    input: a boolean series
    returns: two-tuple (num_true, num_false)
    """
    return series.value_counts().sort_index(ascending=False).tolist()


def local_min_max(df, num_periods=5):
    mins = df.Close.iloc[argrelextrema(df.Close.values, np.less_equal, order=num_periods)]
    maxs = df.Close.iloc[
        argrelextrema(df.Close.values, np.greater_equal, order=num_periods)
    ]
    return mins, maxs


def plot_local_min_max(df, nump=5):
    mins, maxs = local_min_max(df, nump)
    fig, ax = plt.subplots()
    ax.scatter(maxs.index, maxs, c="g")
    ax.scatter(mins.index, mins, c="r")
    ax.plot(df.index, df.Close)
    plt.show()
