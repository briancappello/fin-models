from __future__ import annotations

import argparse
import json
import multiprocessing
import os

from datetime import date

import numpy as np
import pandas as pd

from joblib import Parallel, delayed

from fin_models import analysis_utils as au
from fin_models.enums import Freq
from fin_models.services import store


results_dir = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "analysis-results",
)
os.makedirs(results_dir, exist_ok=True)
# end_date = "2023-05-12"


def json_default(o):
    if isinstance(o, (np.int8, np.int16, np.int32, np.int64)):
        return int(o)
    elif isinstance(o, (np.float32, np.float64)):
        return float(o)
    elif isinstance(o, np.bool_):
        return bool(o)
    elif isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError(f"Unable to convert {o!r} ({type(o)} to JSON.")


def signal(symbol: str, end_date: str):
    no_result = dict(symbol=symbol)

    df = store.get(symbol)
    if df is None or df.empty:
        return no_result

    df = df.loc[:end_date]
    if len(df) < 100:
        return no_result

    return dict(
        symbol=symbol,
        volume=df.Volume.iloc[-1],
        median_volume=au.median_volume(df, num_bars=50),
        volume_multiple_of_median=au.volume_multiple_of_median(df, num_bars=50),
        is_expanding_volume=au.is_expanding_volume(df, num_bars=3),
        close=df.Close.iloc[-1],
        body_percent_change=au.pct_changes_bodies_df(df).iloc[-1],
        crossed_sma_100=au.crossed_ma(df, ma=100),
        crossed_sma_200=au.crossed_ma(df, ma=200),
        bars_since_prior_high=au.bars_since_previous_high(df),
    )


def cached_results(
    cache_filename: str,
    results: list[dict] | None = None,
    fresh: bool = False,
) -> pd.DataFrame:
    results = results or []

    if not fresh and os.path.exists(cache_filename) and not results:
        with open(cache_filename) as f:
            try:
                results = json.load(f)
            except json.JSONDecodeError:
                print(f"corrupt json file: {cache_filename}")
                results = []
    elif results:
        with open(cache_filename, "w") as f:
            json.dump(results, f, default=json_default)

    return pd.DataFrame.from_records(results)


def calculate_for_date(end_date: str | None = None, fresh: bool = False) -> pd.DataFrame:
    end_date = end_date or date.today().isoformat()
    results_filename = os.path.join(results_dir, f"{end_date}_results.json")

    df = cached_results(cache_filename=results_filename, fresh=fresh)
    if df.empty:
        fn_calls = [
            delayed(signal)(symbol=symbol, end_date=end_date)
            for symbol in store.symbols(freq=Freq.day)
        ]

        r = Parallel(
            n_jobs=multiprocessing.cpu_count(),
            backend="multiprocessing",
        )(fn_calls)

        df = cached_results(results_filename, r)
    df.set_index("symbol", inplace=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.isoformat)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    df = calculate_for_date(args.date, fresh=args.fresh)
    filter1 = df["crossed_sma_100"] & (df["bars_since_prior_high"] > 20)
    filter2 = df["volume_multiple_of_median"] > 3

    print(df[filter2])
