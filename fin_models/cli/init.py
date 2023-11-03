from __future__ import annotations

from datetime import date, timedelta

import click
import pandas as pd

from fin_models.bulk_downloader import bulk_download
from fin_models.date_utils import to_ts
from fin_models.services import timeframe_stores
from fin_models.utils import chunk
from fin_models.vendors import polygon, yahoo

from .main import main


@main.command("init")
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
    types: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    timeframe: str = "day",
):
    types = polygon.normalize_ticker_types(types)
    end = to_ts(end, default=date.today())
    start = to_ts(start, default=end - timedelta(days=365 * 5))

    all_symbols_data = polygon.get_tickers(types)
    tickers = [
        d["ticker"]
        for d in all_symbols_data
        if not timeframe_stores[timeframe].has(d["ticker"])
    ]

    init(tickers, start, end, timeframe)


def init(symbols_: list[str], start, end, timeframe: str = "day"):
    store = timeframe_stores[timeframe]
    if timeframe == "day":
        urls = [
            polygon.make_history_url(symbol, timeframe, start, end) for symbol in symbols_
        ]
        for batch in chunk(urls, 200):
            successes, errors, exceptions = bulk_download(batch)
            for resp in successes:
                df = polygon.json_to_df(resp.json)
                m = polygon.HISTORY_URL_REGEX.match(resp.url)
                symbol = m.groupdict()["symbol"]
                store.write(symbol, df)
                print(f"{symbol}: Added {len(df)} bars")

    elif timeframe == "minute":
        for symbol in symbols_:
            urls = polygon.make_minutely_urls(symbol, start, end)
            successes, errors, exceptions = bulk_download(urls)
            # print(
            #     f"successes: {len(successes)}, errors: {len(errors)}, exceptions:"
            #     f" {len(exceptions)}"
            # )

            dataframes = []
            for r in successes:
                dataframes.append(polygon.json_to_df(r.json))

            if dataframes and len(dataframes) == len(successes):
                df = pd.concat(dataframes).sort_index()
                store.write(symbol, df)
                print(f"{symbol}: Added {len(df)} bars")
