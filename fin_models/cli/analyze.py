from __future__ import annotations

from pprint import pprint as print

import click

from fin_models import analysis_utils as au
from fin_models.db import Session
from fin_models.enums import Freq
from fin_models.models import Stats
from fin_models.services import store

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
    "--freq",
    type=click.Choice(["minute", "day"]),
    default="day",
    help="Frequency to initialize or update (minute or day, default day)",
)
def analyze_command(
    symbols: str | None = None,
    freq: str = "day",
) -> None:
    freq = {"minute": Freq.min_1, "day": Freq.day}[freq]

    if symbols:
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]
    else:
        symbols = store.get_symbols(freq)

    session = Session()

    for symbol in symbols:
        signals: dict = au.signal(symbol=symbol, freq=freq)
        signals.pop("symbol")
        day = signals.pop("day", None)
        if not day:
            continue

        stats = Stats(symbol=symbol, day=day, stats=signals)
        session.add(stats)

    session.commit()
