from __future__ import annotations

import argparse
import itertools
import multiprocessing

from datetime import date

import pandas as pd

from joblib import Parallel, delayed

from fin_models import Bar, Freq
from fin_models.bar_handlers import BarHandler, History, construct_bar_handlers
from fin_models.services import nyse, store
from fin_models.watchlists import MaxMovers, MaxVolumeMultiplesOfMedian, Watchlist


class AllWatchlists(BarHandler):
    max_movers: MaxMovers
    max_volume_multiples_of_median: MaxVolumeMultiplesOfMedian


def do_it_sequential(
    freq: Freq,
    symbols: list[str],
    dt: pd.Timestamp,
    save: bool = False,
) -> dict[type[Watchlist], pd.DataFrame]:
    # preload daily symbol history
    history = History(store)
    bars = []
    for symbol in symbols:
        df = store.get(symbol, freq).loc[:dt]
        history.symbol_data[symbol][freq] = df
        bar = Bar.from_series(df.iloc[-1], symbol=symbol, freq=freq)
        bars.append(bar)

    bar_handlers = construct_bar_handlers(AllWatchlists, [history])
    for bar in bars:
        for bar_handler in bar_handlers:
            bar_handler.handle_bar(bar, refresh=False)

    rv: dict[type[Watchlist], pd.DataFrame] = {}
    wl: Watchlist
    for wl in [
        bh for bh in bar_handlers if isinstance(bh, tuple(Watchlist.__subclasses__()))
    ]:
        wl_df = wl.get_watchlist_dataframe()
        rv[wl.__class__] = wl_df
        if save:
            wl.save(wl_df)
    return rv


def do_it_parallel(
    freq: Freq,
    dt: pd.Timestamp,
) -> dict[type[Watchlist], pd.DataFrame]:
    symbols = store.get_symbols(Freq.min_1, dt=dt)

    symbol_partitions = list(
        itertools.batched(symbols, len(symbols) // multiprocessing.cpu_count())
    )

    unaggregated_results: list[dict[type[Watchlist], pd.DataFrame]] = Parallel(
        n_jobs=len(symbol_partitions),
        backend="multiprocessing",
    )(
        [
            delayed(do_it_sequential)(
                freq=freq, symbols=symbol_partition, dt=dt, save=False
            )
            for symbol_partition in symbol_partitions
        ]
    )

    rv = {}
    wl_class: type[Watchlist]
    for wl_class in unaggregated_results[0]:
        wl_df: pd.DataFrame = wl_class.sort(
            wl_df=pd.concat([d[wl_class] for d in unaggregated_results]).reset_index(
                drop=True
            )
        )
        wl_class.save(wl_df)
        rv[wl_class] = wl_df
    return rv


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args()

    ts: pd.Timestamp = pd.Timestamp(
        args.date or nyse.get_latest_trading_date()
    ).tz_localize("America/New_York")

    print(f"Calculating watchlists for {ts!r}")
    do_it_parallel(freq=Freq.day, dt=ts)
    print("Done.")
