from __future__ import annotations

import json
import os

import click

from fin_models.config import Config
from fin_models.vendors import polygon

from .groups import main


@main.command("symbols")
@click.option(
    "--types",
    type=click.Choice(["commonn", "CS", "preferred", "PFD", "ETF"], case_sensitive=False),
    multiple=True,
    default=None,
    help="types of share classes to fetch and store",
)
def symbols_command(types: list[str] | str | None = None):
    """Fetch and store ticker symbols data supported by Polygon."""
    types = polygon.normalize_ticker_types(types)
    data = get_and_save_symbols_data(types)

    for t in types:
        print(f'{t}: {len([d for d in data if d["type"] == t])}')


def get_and_save_symbols_data(types: list[str] | str | None = None) -> list[dict]:
    data = polygon.get_tickers(types)

    os.makedirs(os.path.dirname(Config.SYMBOLS_DATA_FILEPATH), exist_ok=True)
    with open(Config.SYMBOLS_DATA_FILEPATH, "w") as f:
        json.dump(data, f, indent=2)

    return data
