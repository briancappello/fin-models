import pandas as pd


def days_since_previous_high(df: pd.DataFrame):
    """
    Count the number of days since the price was higher than the current close.

    Given EOD data for a date, 1 == Yesterday
    """
    bar = df.iloc[-1]
    priors = df.iloc[:-1]

    higher_bars = priors.Close > bar.Close
    most_recent_higher_ts = higher_bars[higher_bars].index[-1]

    return len(df) - df.index.get_loc(most_recent_higher_ts) - 1


def macd_divergence(df: pd.DataFrame):
    """
    macd, macd_signal, histogram = ta.MACD(df.Close)

    right side, macd should be above the signal
    left side, both values should be lower than signal on the right
    """


