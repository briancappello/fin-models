from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import fin_models.analysis_utils as au


def plot_linear_regression(df: pd.DataFrame, num_bars: int = 50):
    xs = list(range(num_bars))
    ys = df.iloc[-num_bars:]
    slope, y_intercept = np.polyfit(xs, ys, deg=1)
    plt.scatter(xs, ys)
    plt.plot(xs, np.polyval((slope, y_intercept), xs))
    plt.show()
    return slope


def plot_local_min_max(df, nump=5):
    mins, maxs = au.local_min_max(df, nump)
    fig, ax = plt.subplots()
    ax.scatter(maxs.index, maxs, c="g")
    ax.scatter(mins.index, mins, c="r")
    ax.plot(df.index, df.Close)
    plt.show()
