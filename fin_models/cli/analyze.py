from __future__ import annotations

import json
import os

from datetime import date

import click
import numpy as np
import pandas as pd

from fin_models import analysis_utils as au
from fin_models.config import Config
from fin_models.db import Session
from fin_models.enums import Freq
from fin_models.models import Stats
from fin_models.services import nyse, store

from .groups import main


@main.command("analyze")
@click.argument(
    "symbols",
    type=str,
    default=None,
    required=False,
    # help="Optional list of symbols to initialize or update",
)
@click.option(
    "--day",
    type=str,
    default=None,
    help="isoformat of date to analyze (defaults to last trading day)",
)
@click.option(
    "--freq",
    type=click.Choice(["minute", "day"]),
    default="day",
    help="Frequency to initialize or update (minute or day, default day)",
)
def analyze_command(
    symbols: str | None = None,
    day: str | None = None,
    freq: str = "day",
) -> None:
    freq = {"minute": Freq.min_1, "day": Freq.day}[freq]
    day = date.fromisoformat(day) if day else nyse.get_latest_trading_date()

    print(f"Analyzing statistics for {freq=} on {day.isoformat()}")

    if symbols:
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]
    else:
        symbols = store.get_symbols(freq)

    session = Session()
    print(session.connection().engine)

    watchlists = []

    for symbol in symbols:
        df = store.get(symbol, end_dt=day, freq=freq)
        # df_min = store.get(symbol, end_dt=day, freq=Freq.min_1)
        if day.isoformat() not in df.index:
            print(f"data is not up to date for {symbol}")
            continue
        df = df.loc[: day.isoformat()]

        signals: dict = au.signal(df, freq)
        if not signals:
            continue

        # if len(df) > 50:
        #     try:
        #         watchlists.append(au.get_watchlist_data(symbol, df, df_min))
        #     except IndexError:
        #         pass

        instance = (
            session.query(Stats)
            .filter_by(symbol=symbol, day=day, freq=freq)
            .one_or_none()
        )
        if not instance:
            instance = Stats(symbol=symbol, day=day, freq=freq)
        instance.stats = signals

        session.add(instance)
        session.commit()

    # df = pd.DataFrame(watchlists).set_index('ticker').replace([np.inf, -np.inf], np.nan)
    #
    # for key in (set(df.keys()) - {'ticker'}):
    #     sorted = df.dropna(subset=[key]).sort_values(key, ascending=False)
    #     print(sorted.head(50))
    #     with open(os.path.join(Config.DATA_DIR, 'watchlists', f'{key}.json'), 'w') as f:
    #         json.dump(dict(key=key, label=key, tickers=sorted.index[:50].to_list()), f, indent=2)
