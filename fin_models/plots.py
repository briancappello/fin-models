from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import fin_models.analysis_utils as au


def plot_linear_regression(df: pd.DataFrame, num_bars: int = 50, deg: int = 1):
    xs = list(range(num_bars))
    ys = df.iloc[-num_bars:]
    trend = np.polyfit(xs, ys, deg)
    plt.scatter(xs, ys)
    plt.plot(xs, np.polyval(trend, xs))
    plt.show()

    if deg == 1:
        return trend[0]  # trend == (slope, y_intercept); return slope


def plot_local_min_max(df, nump=5):
    mins, maxs = au.local_min_max(df, nump)
    fig, ax = plt.subplots()
    ax.scatter(maxs.index, maxs, c="g")
    ax.scatter(mins.index, mins, c="r")
    ax.plot(df.index, df.Close)
    plt.show()
