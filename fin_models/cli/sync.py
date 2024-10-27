from __future__ import annotations

import json

from datetime import timedelta

import click
import pandas as pd

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
    types = polygon.normalize_ticker_types(types)
    end = to_ts(end, default=nyse.get_latest_trading_date_schedule()["market_close"])
    start = to_ts(
        start,
        default=end - timedelta(days=365 * Config.POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE),
    )

    if symbols:
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]
    else:
        symbols = polygon.get_symbols(types)

    init_or_update(
        symbols=symbols,
        start=start,
        end=end,
        freq={"minute": Freq.min_1, "day": Freq.day}[freq],
    )


def init_or_update(
    symbols: list[str],
    start: DateType,
    end: DateType,
    freq: Freq,
):
    if freq not in {Freq.min_1, Freq.day}:
        raise NotImplementedError(
            "Data fetching is currently only implemented for Freq.min_1 and Freq.day"
        )

    for split in polygon.get_splits(dt=end):
        # FIXME handle re-fetching all stored frequencies?
        store._delete_all(symbol=split["ticker"])

    symbol_start_dates = {}
    for symbol in symbols:
        historical_metadata = store.get_historical_metadata(symbol, freq)
        if historical_metadata is None:
            symbol_start_dates[symbol] = start
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
        for url_batch in chunk(urls, 200):
            successes, errors, exceptions = bulk_download(url_batch)
            for resp in successes:
                count += 1
                df = polygon.json_to_df(resp.json)
                m = polygon.HISTORY_URL_REGEX.match(resp.url)
                symbol = m.groupdict()["symbol"]
                store.write(symbol, freq, df)
                print(f"{symbol} ({count} / {len(symbols)}): Added {len(df)} bars")

    elif freq == Freq.min_1:
        for symbol in symbols:
            count += 1
            urls = polygon.make_minutely_urls(
                symbol, freq=freq, start=symbol_start_dates[symbol], end=end
            )
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
                print(f"{symbol} ({count} / {len(symbols)}): Added {len(df)} bars")
