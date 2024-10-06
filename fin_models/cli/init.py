from __future__ import annotations

import json

from datetime import date, timedelta

import click
import pandas as pd

from fin_models.bulk_downloader import bulk_download
from fin_models.date_utils import to_ts
from fin_models.enums import Freq
from fin_models.services import store
from fin_models.utils import chunk
from fin_models.vendors import polygon

from .groups import main


@main.command("init")
@click.option(
    "--symbols",
    type=str,
    default=None,
    help="Optional list of symbols to initialize",
)
@click.option(
    "--types",
    type=click.Choice(["commonn", "CS", "preferred", "PFD", "ETF"], case_sensitive=False),
    multiple=True,
    default=None,
    help="types of share classes to initialize",
)
@click.option(
    "--start",
    type=str,
    default=None,
    help="isoformat date to start from (default 10 years ago)",
)
@click.option(
    "--end",
    type=str,
    default=None,
    help="isoformat date to end on (default today)",
)
@click.option(
    "--timeframe",
    type=click.Choice(["minute", "day"]),
    default="day",
    help="the timeframe to initialize",
)
def init_command(
    symbols: str | None = None,
    types: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    timeframe: str = "day",
):
    """Initialize historical data from Polygon"""
    freq = {"minute": Freq.min_1, "day": Freq.day}[timeframe]
    types = polygon.normalize_ticker_types(types)
    end = to_ts(end, default=date.today())
    start = to_ts(start, default=end - timedelta(days=365 * 5))

    if symbols:
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]
    else:
        symbols = polygon.get_symbols(types)

    # filter out already-initialized symbols
    symbols = [symbol for symbol in symbols if not store.has(symbol, freq)]

    init(symbols, start, end, freq)


def init(symbols_: list[str], start, end, freq: Freq):
    if freq == Freq.day:
        urls = [
            polygon.make_history_url(symbol, Freq.day, start, end) for symbol in symbols_
        ]
        for url_batch in chunk(urls, 200):
            successes, errors, exceptions = bulk_download(url_batch)
            for resp in successes:
                df = polygon.json_to_df(resp.json)
                m = polygon.HISTORY_URL_REGEX.match(resp.url)
                symbol = m.groupdict()["symbol"]
                store.write(symbol, freq, df)
                print(f"{symbol}: Added {len(df)} bars")

    elif freq == Freq.min_1:
        for symbol in symbols_:
            urls = polygon.make_minutely_urls(symbol, start, end)
            successes, errors, exceptions = bulk_download(urls)

            dataframes = []
            for r in successes:
                try:
                    dataframes.append(polygon.json_to_df(r.json))
                except json.JSONDecodeError:
                    break

            if dataframes and len(dataframes) == len(successes):
                df = pd.concat(dataframes).sort_index()
                store.write(symbol, freq, df)
                print(f"{symbol}: Added {len(df)} bars")
