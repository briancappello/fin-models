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
from fin_models.config import Config
from fin_models.enums import Freq
from fin_models.services import nyse, store


results_dir = os.path.join(
    Config.DATA_DIR,
    "analysis-results",
)
os.makedirs(results_dir, exist_ok=True)


def json_default(o):
    if isinstance(o, (np.int8, np.int16, np.int32, np.int64)):
        return int(o)
    elif isinstance(o, (np.float32, np.float64)):
        return float(o)
    elif isinstance(o, np.bool_):
        return bool(o)
    elif isinstance(o, (np.ndarray, pd.Index)):
        return o.tolist()
    raise TypeError(f"Unable to convert {o!r} ({type(o)} to JSON.")


def dump(obj, filepath):
    with open(filepath, "w") as f:
        json.dump(obj, f, default=json_default)


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
        dump(results, cache_filename)

    return pd.DataFrame.from_records(results)


def calculate_for_date(dt: date | str, fresh: bool = False) -> pd.DataFrame:
    dt = pd.Timestamp(dt).date().isoformat()
    results_filename = os.path.join(results_dir, f"{dt}_results.json")

    df = cached_results(cache_filename=results_filename, fresh=fresh)
    if df.empty:
        fn_calls = [
            delayed(au.signal)(symbol=symbol, dt=dt)
            for symbol in store.get_symbols(freq=Freq.day)
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
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    dt = args.date or nyse.get_latest_trading_date()
    print(f"Calculating signals for {dt!r}")

    df = calculate_for_date(dt, fresh=args.fresh)

    crossed_sma100_filter = df["crossed_sma_100"] & (df["bars_since_prior_high"] > 20)

    vol_multiple_of_median_filter = (df["volume_multiple_of_median"] > 3) & (
        df["median_volume"] > 1_000
    )
    vol_multiple_of_median = df[vol_multiple_of_median_filter][
        "volume_multiple_of_median"
    ].sort_values(ascending=False)

    max_gainers_filter = df["pct_change"] > 10
    max_gainers = df[max_gainers_filter]["pct_change"].sort_values(ascending=False)

    dump(
        dict(
            max_gainers={
                "label": "Max Gainers",
                "symbols": max_gainers.index,
            },
            vol_multiple_of_median={
                "label": "Vol Multiple Of Median",
                "symbols": vol_multiple_of_median.index,
            },
        ),
        Config.JSON_WATCHLISTS_PATH,
    )
    print(max_gainers)
