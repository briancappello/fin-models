from __future__ import annotations

import json
import multiprocessing

from collections import defaultdict
from datetime import date, timedelta

import click
import pandas as pd

from joblib import Parallel, delayed

from fin_models.bulk_downloader import bulk_download
from fin_models.config import Config
from fin_models.date_utils import DateType, to_ts
from fin_models.enums import Freq
from fin_models.services import nyse, store
from fin_models.utils import chunk
from fin_models.vendors import polygon

from .groups import main


@main.command("sync")
@click.option(
    "--symbols",
    type=str,
    default=None,
    help="Optional list of symbols to initialize or update",
)
@click.option(
    "--types",
    type=click.Choice(
        [t.name for t in polygon.TickerType] + ["common", "preferred"],
        case_sensitive=False,
    ),
    multiple=True,
    default=None,
    help="Types of share classes to initialize or update",
)
@click.option(
    "--start",
    type=str,
    default=None,
    help=f"isoformat date to start from"
    f" (default {Config.POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE} years ago)",
)
@click.option(
    "--end",
    type=str,
    default=None,
    help="isoformat date to end on (default latest trading date)",
)
@click.option(
    "--freq",
    type=click.Choice(["minute", "day"]),
    default="day",
    help="Frequency to initialize or update (minute or day, default day)",
)
def sync_command(
    symbols: str | None = None,
    types: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    freq: str = "day",
):
    """
    Initialize or update historical data from Polygon

    FIXME:
        - how to handle delisted symbols?
        - on splits, handle refetching all stored frequencies
    """
    freq = {"minute": Freq.min_1, "day": Freq.day}[freq]

    if symbols:
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]
    else:
        types = polygon.normalize_ticker_types(types)
        symbols = polygon.get_symbols(types)
        # symbols = []
        # for symbol in store.get_symbols():
        #     hm = store.get_historical_metadata(symbol, freq=freq)
        #     if hm is None or hm.latest_bar_utc.date() != nyse.get_latest_trading_date():
        #         symbols.append(symbol)

    init_or_update(
        symbols=symbols,
        freq=freq,
        start=start,
        end=end,
    )


def init_or_update(
    symbols: list[str],
    freq: Freq,
    start: DateType | None = None,
    end: DateType | None = None,
):
    if freq not in {Freq.min_1, Freq.day}:
        raise NotImplementedError(
            "Data fetching is currently only implemented for Freq.min_1 and Freq.day"
        )

    end_ts = to_ts(
        end,
        default=nyse.get_latest_trading_date_schedule(include_extended=True)["post"],
    )
    start_ts = to_ts(
        start,
        default=end_ts
        - pd.Timedelta(days=365 * Config.POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE),
    )

    for split in polygon.get_splits(dt=end_ts):
        # FIXME handle re-fetching all stored frequencies?
        store._delete_all(symbol=split["ticker"])

    symbol_start_dates = {}
    for symbol in symbols:
        historical_metadata = store.get_historical_metadata(symbol, freq)
        if start or historical_metadata is None:
            symbol_start_dates[symbol] = start_ts
        else:
            symbol_start_dates[symbol] = historical_metadata.latest_bar_utc

    count = 0
    if freq == Freq.day:
        urls = [
            polygon.make_history_url(
                symbol, freq=Freq.day, start=symbol_start_dates[symbol], end=end
            )
            for symbol in symbols
        ]
        _bulk_download_and_store(urls, freq, progress=count, total=len(symbols))

    elif freq == Freq.min_1:
        parallel_urls = {}
        for symbol in symbols:
            urls = polygon.make_minutely_urls(
                symbol, freq=freq, start=symbol_start_dates[symbol], end=end
            )
            parallel_urls[symbol] = urls

        errored_symbols = {k: 0 for k in parallel_urls}
        while errored_symbols:
            errored = [
                symbol
                for symbol in Parallel(
                    n_jobs=multiprocessing.cpu_count() * 4, verbose=10
                )(
                    delayed(_multicpu_download_and_parse)(urls)
                    for symbol, urls in parallel_urls.items()
                    if errored_symbols.get(symbol, 3) < 3
                )
                if symbol
            ]
            for errored_symbol in errored:
                errored_symbols[errored_symbol] += 1
            for not_errored in set(parallel_urls) - set(errored):
                errored_symbols.pop(not_errored, None)


def _bulk_download_and_store(
    urls: list[str],
    freq: Freq,
    progress: int,
    total: int,
):
    for url_batch in chunk(urls, 2000):
        successes, errors, exceptions = bulk_download(url_batch)
        for resp in successes:
            progress += 1
            df = polygon.json_to_df(resp.json)
            m = polygon.HISTORY_URL_REGEX.match(resp.url)
            symbol = m.groupdict()["symbol"]
            store.write(symbol, freq, df)
            print(f"{symbol} ({progress} / {total}): Added {len(df)} bars")


def _multicpu_download_and_parse(urls: list[str]):
    m = polygon.HISTORY_URL_REGEX.match(urls[0]).groupdict()
    symbol = m["symbol"]
    timeframe = m["timeframe"]
    freq = {"minute": Freq.min_1, "day": Freq.day}[timeframe]

    print(f"Downloading {timeframe} data for {symbol}")
    successes, errors, exceptions = bulk_download(urls)

    if errors or exceptions:
        print(f"Encountered errors downloading {symbol}")
        wtf = errors or exceptions
        print(wtf[0].json)
        return symbol

    dataframes = []
    for resp in successes:
        df = polygon.json_to_df(resp.json)
        dataframes.append(df)

    df = pd.concat(dataframes).sort_index()
    try:
        print(f"Saving {timeframe} data for {symbol} ({len(df)} bars)")
        store.write(symbol, freq, df)
    except:  # noqa
        store._delete_freq(symbol, freq)
        return symbol

    return None


def _flex_bulk_download_and_store(
    urls_by_symbol: dict[str, list[str]],
    freq: Freq,
):
    batches = []
    batch = []
    for symbol, urls in urls_by_symbol.items():
        if (len(batch) + len(urls)) < 2000:
            batch.extend(urls)
        else:
            batches.append(batch)
            batch = urls
    if batch:
        batches.append(batch)

    progress = 0
    for url_batch in batches:
        print(f"Downloading a batch of {len(url_batch)} URLs")
        successes, errors, exceptions = bulk_download(url_batch)

        if errors or exceptions:
            raise RuntimeError(errors + exceptions)

        # FIXME: this is by far the most time consuming part
        print("Parsing the responses")
        dataframes_by_symbol = defaultdict(list)
        for resp in successes:
            df = polygon.json_to_df(resp.json)
            m = polygon.HISTORY_URL_REGEX.match(resp.url)
            symbol = m.groupdict()["symbol"]
            dataframes_by_symbol[symbol].append(df)

        for symbol, dataframes in dataframes_by_symbol.items():
            progress += 1
            df = pd.concat(dataframes).sort_index()
            print(
                f"{progress}/{len(urls_by_symbol)}: writing {symbol} (added {len(df)} bars)"
            )
            store.write(symbol, freq, df)
