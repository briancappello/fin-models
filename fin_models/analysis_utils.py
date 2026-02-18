from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import numpy as np
import pandas as pd
import talib as ta
import talib.abstract as ta_abstract

from scipy.signal import argrelextrema
from scipy.stats import linregress

from fin_models.data_classes import Bar
from fin_models.enums import Freq
from fin_models.services import nyse, store


"""
s = df.some_bool_col
s[s].index ==> indexes (timestamps) where True
"""


def volume_30ma(df: pd.DataFrame):
    return ta.SMA(df["Volume"], timeperiod=30).iloc[-1]


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
    vol_ma_df = ta.SMA(df["Volume"], timeperiod=vol_ma)
    s = (df["Volume"] / vol_ma_df) > multiple
    return vol_ma_df[s[s].index]


def bars_since_previous_high(df: pd.DataFrame) -> int:
    """
    Count the number of days since open/close price was higher than the current close.

    Given EOD data for a date
    0 == The latest bar is the highest bar
    1 == Yesterday was higher
    50 == 50 days ago was the most recent bar higher than the latest bar
    """

    bar = df.iloc[-1]
    priors = df.iloc[:-1]

    higher_closes_filter = priors.Close > bar.Close
    higher_opens_filter = priors.Open > bar.Close
    higher_bars = priors[higher_opens_filter | higher_closes_filter]
    if higher_bars.empty:
        return 0

    most_recent_higher_ts = higher_bars.index[-1]
    return num_bars_since_ts(df, most_recent_higher_ts)


def macd_divergence(df: pd.DataFrame):
    """
    macd, macd_signal, histogram = ta.MACD(df.Close)

    right side, macd should be above the signal
    left side, both values should be lower than signal on the right
    """


def pct_change(close: float, prior_close: float) -> float:
    return ((close - prior_close) / prior_close) * 100


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
    return ((df.Close - df["Open"]) / df["Open"]) * 100


def is_expanding_volume(df: pd.DataFrame, num_bars: int = 3) -> bool:
    if len(df) < num_bars:
        return False
    return (df.index[-num_bars:] == df["Volume"][-num_bars:].sort_values().index).all()


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
        raise TypeError("One of `column` or `value` is required to be passed.")
    elif column and column not in df.columns:
        raise ValueError(f"The column {column!r} is missing from `df`.")

    value = df[column].iloc[-1] if column else value
    intraday_cross = df.iloc[-1].Open < value < df.iloc[-1].Close
    overnight_cross = df.iloc[-2].Close < value < df.iloc[-1].Open
    return intraday_cross or overnight_cross


def num_bars_since_ts(df: pd.DataFrame, ts: pd.Timestamp):
    return len(df) - df.index.get_loc(ts) - 1


def median_volume(df: pd.DataFrame, num_bars: int = 50) -> float:
    if len(df) <= num_bars:
        return float(np.median(df["Volume"]))
    return float(np.median(df["Volume"][-num_bars:]))


def rolling_median_volume(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return df["Volume"].rolling(num_bars).median()


def median_body(df: pd.DataFrame, num_bars: int = 50) -> float:
    bodies = (df["Close"][-num_bars:] - df["Open"][-num_bars:]).abs()
    return float(np.median(bodies))


def rolling_median_body(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return (df["Close"] - df["Open"]).abs().rolling(num_bars).median()


def median_body_pct(df: pd.DataFrame, num_bars: int = 50) -> float:
    bodies = (
        (df["Close"][-num_bars:] - df["Open"][-num_bars:]) / df["Open"][-num_bars:]
    ).abs()
    return float(np.median(bodies)) * 100


def rolling_median_body_pct(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return ((df["Close"] - df["Open"]) / df["Open"]).abs().rolling(num_bars).median()


def body_multiple_of_median(df: pd.DataFrame, num_bars: int = 50) -> float:
    this_body = abs(df["Close"].iloc[-1] - df["Open"].iloc[-1])
    median = median_body(df, num_bars)
    return this_body / median


def body_multiple_of_median_pct(df: pd.DataFrame, num_bars: int = 50) -> float:
    this_body_pct = (
        abs((df["Close"].iloc[-1] - df["Open"].iloc[-1]) / df["Open"].iloc[-1]) * 100
    )
    median_pct = median_body_pct(df, num_bars)
    return this_body_pct / median_pct


def volume_multiple_of_median(df: pd.DataFrame, num_bars: int = 50) -> float:
    median_vol = median_volume(df, num_bars)
    if median_vol:
        return df["Volume"].iloc[-1] / median_vol
    return 0


def rolling_volume_multiple_of_median(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return df["Volume"] / rolling_median_volume(df, num_bars)


def intraday_volume_multiple_of_median(
    df: pd.DataFrame,
    from_time: str = "04:00",
    to_time: str = "09:30",
    num_bars: int = 50,
) -> pd.DataFrame:
    between_time = df.between_time(from_time, to_time)
    day_agg = between_time["Volume"].resample("D").sum()
    median_v = day_agg.rolling(num_bars).median()

    latest_median_v = median_v.iloc[-1]
    if np.isnan(latest_median_v) or latest_median_v == 0:
        median_v = day_agg.rolling(num_bars).mean()

    rv = pd.DataFrame(
        {
            "premarket_volume": day_agg,
            "premarket_volume_median": median_v,
            "premarket_volume_multiple_of_median": day_agg / median_v,
        }
    )
    return rv


def mean_volume(df: pd.DataFrame, num_bars: int = 50) -> float:
    if len(df) <= num_bars:
        return float(np.mean(df["Volume"]))
    return float(np.mean(df["Volume"][-num_bars:]))


def rolling_mean_volume(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return df["Volume"].rolling(num_bars).mean()


def volume_multiple_of_mean(df: pd.DataFrame, num_bars: int = 50) -> float:
    mean_vol = mean_volume(df, num_bars)
    if mean_vol:
        return df["Volume"].iloc[-1] / mean_vol
    return 0


def rolling_volume_multiple_of_mean(df: pd.DataFrame, num_bars: int = 50) -> pd.Series:
    return df["Volume"] / rolling_mean_volume(df, num_bars)


def volume_sum_of_prior_days(df: pd.DataFrame) -> int:
    if len(df) < 2 or df["Volume"].iloc[-2] > df["Volume"].iloc[-1]:
        return 0

    sum = 0
    prior_days = 0
    while len(df) >= (prior_days + 2) and sum < df["Volume"].iloc[-1]:
        sum += df["Volume"].iloc[-(prior_days + 2)]
        prior_days += 1
    return prior_days


def calculate_outlier_score(data, data_point):
    """
    Calculates an outlier score for a given data point based on its distance from the population mean and standard deviation.
    Calculates the mean and standard deviation from the input data.

    Args:
        data: A NumPy array representing the entire population data.
        data_point: The data point for which to calculate the outlier score.

    Returns:
        A float representing the outlier score. Higher scores indicate more outlier-ness.
        Returns np.nan if the standard deviation is zero to avoid division by zero.
    """

    data_mean = np.mean(data)
    data_std = np.std(data)

    if data_std == 0:
        return np.nan

    distance_from_mean = abs(data_point - data_mean)
    outlier_score = distance_from_mean / data_std

    return outlier_score


def is_trading_safe(df):
    return len(df) > 3 and median_volume(df) > 200_000


def slope(series: pd.Series) -> float:
    xs = [0, 1]
    ys = [series.iloc[-2], series.iloc[-1]]
    slope_val, y_intercept = np.polyfit(xs, ys, deg=1)
    return slope_val


def is_sloping_up(series: pd.Series) -> bool:
    return slope(series) > 0


def is_sloping_down(series: pd.Series) -> bool:
    return slope(series) < 0


def crossed_ma(df: pd.DataFrame, ma: int = 200, within_bars: int = 1):
    """upwards cross only"""
    if len(df) < ma:
        return False

    sma = ta.SMA(df["Close"], timeperiod=ma)
    for i in range(1, within_bars + 1):
        # include gaps
        if df["Close"].iloc[-(i + 1)] < sma.iloc[-i] < df["Close"].iloc[-i]:
            return True
    return False


def gapped_ma(df: pd.DataFrame, ma: int = 200):
    if len(df) < ma:
        return False
    yesterday, today = df.iloc[-2], df.iloc[-1]
    return yesterday.Close < ta.SMA(df["Close"], timeperiod=ma) < today.Open


def true_false_counts(series: pd.Series):
    """
    input: a boolean series
    returns: two-tuple (num_true, num_false)
    """
    return series.value_counts().sort_index(ascending=False).tolist()


def local_min_max(df, num_periods=5):
    mins = df["Close"].iloc[
        argrelextrema(df["Close"].values, np.less_equal, order=num_periods)
    ]
    maxs = df["Close"].iloc[
        argrelextrema(df["Close"].values, np.greater_equal, order=num_periods)
    ]
    return mins, maxs


def is_expanding_bbands(df: pd.DataFrame, last_n_bars: int = 3) -> bool:
    bbands_timeperiod = 20
    upper, _, lower = ta.BBANDS(df["Close"], timeperiod=bbands_timeperiod)
    lower[lower < 0.1] = 0.1
    log_diff = np.log10(upper.iloc[-bbands_timeperiod:]) - np.log10(
        lower.iloc[-bbands_timeperiod:]
    )
    median_bbands_spread = np.median(log_diff)
    linreg = linregress(range(1, last_n_bars + 1), log_diff.iloc[-last_n_bars:])
    return median_bbands_spread > log_diff.iloc[-last_n_bars] and linreg.slope > 0


def line_segments_crossed(a1_y: float, a2_y: float, b1_y: float, b2_y: float) -> bool:
    """
    Determine if two line segments crossed each other.

    First line segment a given by endpoints a1, a2 (only y coordinates)
    Second line segment b given by endpoints b1, b2 (only y coordinates)
    """
    # https://stackoverflow.com/questions/3252194/numpy-and-line-intersections#answer-3252222

    # transform y coords into (x, y) coordinates
    a1 = np.array([-1, a1_y])
    a2 = np.array([0, a2_y])
    b1 = np.array([-1, b1_y])
    b2 = np.array([0, b2_y])

    def perp(a):
        b = np.empty_like(a)
        b[0] = -a[1]
        b[1] = a[0]
        return b

    diff_a = a2 - a1
    diff_b = b2 - b1
    diff_left = a1 - b1
    dap = perp(diff_a)

    denominator = np.dot(dap, diff_b)
    numerator = np.dot(dap, diff_left)
    if np.isnan(denominator) or not denominator:
        return False

    intersection_point = (numerator / denominator) * diff_b + b1
    x_coord, y_coord = intersection_point
    # FIXME should we return the intersection point? x = 2 for example allows anticipation of
    # a likely cross (in 2 bars)
    return -1 < x_coord < 0


def count_continuous_values(a: np.ndarray):
    """
    Count the number of continuous values in an array.

    For example:

        input => np.array([1, 2, 2, 3, 3, 3]) ==> np.array([1, 2, 3])
        input => np.array([True, True, False False False]) ==> np.array([2, 3])
        input => np.array([True, False, True True True]) ==> np.array([1, 1, 3])
    """
    return np.diff(np.flatnonzero(np.concatenate(([True], a[1:] != a[:-1], [True]))))


def lines_crossed(s1: pd.Series, s2: pd.Series, timeperiod: int = 200):
    """
    Determine if two lines crossed each other (without whipsaws over `timeperiod` bars).
    Returns
        'up' if `s1` crossed upwards over `s2`, or
        'down' if `s1` crossed downwards under `s2`, or
        False otherwise

    """
    a1: np.ndarray = s1.values[-timeperiod:]
    a2: np.ndarray = s2.values[-timeperiod:]

    bool_diff = (a1 - a2) > 0  # a1 > a2
    bool_counts = count_continuous_values(bool_diff)
    if (no_cross := len(bool_counts) == 1) or (whipsaws := (len(bool_counts) > 2)):
        return False

    cross_up = bool_diff[-1] == True
    return "up" if cross_up else "down"


def signal(df: pd.DataFrame, freq: Freq) -> dict:
    result: dict[str, Any] = dict()

    if len(df) < 2:
        return result

    bar = df.iloc[-1]
    prev_bar = df.iloc[-2]
    ts = bar.name.date().isoformat() if freq >= Freq.day else bar.name.isoformat()

    result.update(
        dict(
            ts=ts,
            bar_open=bar.Open,
            bar_high=bar.High,
            bar_low=bar.Low,
            bar_close=bar.Close,
            bar_volume=bar.Volume,
            prev_open=prev_bar.Open,
            prev_high=prev_bar.High,
            prev_low=prev_bar.Low,
            prev_close=prev_bar.Close,
            prev_volume=prev_bar.Volume,
            percent_change=pct_change(bar.Close, prev_bar.Close),
            body_percent_change=pct_change(bar.Close, bar.Open),
        )
    )

    if len(df) > 3:
        result.update(
            dict(
                is_expanding_volume=is_expanding_volume(df, num_bars=3),
            )
        )

    if len(df) > 50:
        result.update(
            dict(
                median_body=median_body(df, num_bars=50),
                body_multiple_of_median=body_multiple_of_median(df, num_bars=50),
                median_volume=median_volume(df, num_bars=50),
                volume_multiple_of_median=volume_multiple_of_median(df, num_bars=50),
                crossed_sma_100=crossed_ma(df, ma=100),
                crossed_sma_200=crossed_ma(df, ma=200),
                bars_since_prev_high=bars_since_previous_high(df),
                is_expanding_bbands=is_expanding_bbands(df),
            )
        )

    return result


def get_watchlist_data(symbol: str, day_df: pd.DataFrame, min_df: pd.DataFrame):
    current_date = min_df.iloc[-1].name.date()
    prior_date = nyse.get_prior_trading_date(current_date)
    premarket_volume_multiple_of_median = intraday_volume_multiple_of_median(min_df)[
        "premarket_volume_multiple_of_median"
    ].iloc[-1]
    return dict(
        ticker=symbol,
        premarket_volume_multiple_of_median=premarket_volume_multiple_of_median,
    )


"""
every indicator:
================

initialization inputs:
----------------------
symbol
freq
df (historical, up to current day)

realtime inputs:
----------------
bar (real-time) [injected per call]
-> keep track of all intraday bars [should be injected per call] 

outputs:
--------
sentiment
raw_values

"""


class Line:
    def __init__(
        self,
        line: pd.Series | np.ndarray,
        name: str | None = None,
        bar: Bar | None = None,
    ):
        self.series = line
        self.name = name
        self.bar = None

    @property
    def prior_value(self) -> float:
        return self.series.iloc[-2]

    @property
    def value(self) -> float:
        return self.series.iloc[-1]

    @property
    def slope(self) -> float:
        xs = [0, 1]
        ys = [self.prior_value, self.value]
        slope_val, y_intercept = np.polyfit(xs, ys, deg=1)
        return slope_val

    @property
    def is_sloping_up(self) -> bool:
        return self.slope > 0

    @property
    def is_sloping_down(self) -> bool:
        return self.slope < 0

    def bar_percent_above_line(self, attr: str = "Close") -> float:
        barval = getattr(self.bar, attr)
        return ((barval - self.value) / self.value) * 100


class Calculatable:
    def __init__(
        self,
        fn: Callable[[pd.DataFrame], Any] = None,
    ):
        self.fn = fn
        self.df: pd.DataFrame | None = None
        self.result: Any = None
        self.bar: Bar | None = None

    def calculate(
        self,
        df: pd.DataFrame,
        bar: Bar,
    ) -> None:
        self.result = self.fn(df)
        self.bar = bar


class LineCross:
    first_line_name = "SMA100"
    second_line_name = "SMA200"

    def __init__(self, first_line: Line, second_line: Line):
        self.first_line = first_line
        self.second_line = second_line

    @property
    def sentiment(self) -> float:
        # positive if faster line crosses over slower line
        # negative if faster line crosses under slower line
        return some_indicator_of_bullishness  # negative number for bearish


class PriceLineCross:
    on_high_volume = "NameOfSomeMeasureForHighVolume"
    over_under_line_name = "NameOfSomeLine"


class Candle:
    def get_candles(self, df: pd.DataFrame) -> dict[str, dict[str, str | int]]:
        candle_names = {
            name: ta_abstract.Function(name).info["display_name"]
            for name in ta.get_function_groups()["Pattern Recognition"]
        }

        def is_candle(fn_name):
            fn = getattr(ta, fn_name)
            return fn(df.Open, df.High, df.Low, df.Close).iloc[-1]

        rv = {}
        for candle_name in candle_names:
            if r := is_candle(df):
                rv[candle_name] = {
                    "name": candle_names[candle_name],
                    "value": r,
                }
        return rv


class BarsSincePreviousHigh:
    name: str

    def __init__(self):
        pass

    def calculate(self, df: pd.DataFrame) -> int:
        """
        Count the number of days since the price was higher than the current close.

        Given EOD data for a date
        0 == The latest bar is the highest bar
        1 == Yesterday was higher
        50 == 50 days ago was the most recent bar higher than the latest bar
        """

        bar = df.iloc[-1]
        priors = df.iloc[:-1]

        higher_closes_filter = priors.Close > bar.Close
        higher_opens_filter = priors.Open > bar.Close
        higher_bars = priors[higher_opens_filter | higher_closes_filter]
        if higher_bars.empty:
            return 0

        most_recent_higher_ts = higher_bars.index[-1]
        return len(df) - df.index.get_loc(most_recent_higher_ts) - 1


class BBANDS(Calculatable):
    name = "BBANDS"

    def __init__(
        self,
        timeperiod: int = 20,
        num_dev_up: int = 2,
        num_dev_down: int = 2,
    ):
        super().__init__(
            fn=lambda df: ta.BBANDS(
                df.Close,
                timeperiod=timeperiod,
                nbdevup=num_dev_up,
                nbdevdn=num_dev_down,
            )
        )
        # args
        self.timeperiod: int = timeperiod
        self.num_dev_up: int = num_dev_up
        self.num_dev_down: int = num_dev_down
        # results
        self.upper: Line | None = None
        self.sma: Line | None = None
        self.lower: Line | None = None
        self.is_expanding: bool | None = None

    def calculate(self, df: pd.DataFrame, bar: Bar):
        super().calculate(df, bar)
        upper, sma, lower = self.result
        self.upper = Line(upper)
        self.sma = Line(sma)
        self.lower = Line(lower)
        self.calc_is_expanding()

    def calc_is_expanding(self, last_n_bars: int = 3) -> bool:
        lower = self.lower.series.copy()
        lower[lower < 0.1] = 0.1
        log_diff = np.log10(self.upper.series.iloc[-self.timeperiod :]) - np.log10(
            lower.iloc[-self.timeperiod :]
        )
        median_bbands_spread = np.median(log_diff)
        linreg = linregress(range(1, last_n_bars + 1), log_diff.iloc[-last_n_bars:])
        self.is_expanding = (
            median_bbands_spread > log_diff.iloc[-last_n_bars] and linreg.slope > 0
        )
        return self.is_expanding

    @property
    def sentiment(self):
        if not self.is_expanding:
            return None

        return 25 if self.is_expanding else None


class RSI(Calculatable):
    name = "RSI"

    def __init__(self, timeperiod: int = 14):
        super().__init__(fn=lambda df: ta.RSI(df.Close, timeperiod=timeperiod))
        self.timeperiod = timeperiod
        self.rsi: Line | None = None

    def calculate(
        self,
        df: pd.DataFrame,
        bar: Bar,
    ) -> None:
        super().calculate(df, bar)
        self.rsi = Line(self.result)

    def sentiment(self):
        if value := self.rsi.value <= 50:
            return 50 - value
        return -(value - 50)
