from __future__ import annotations

import json
import os

import click

from fin_models.config import Config
from fin_models.vendors import polygon

from .main import main


FILEPATH = os.path.join(Config.DATA_DIR, "symbols.json")


@main.command("symbols")
@click.option(
    "--types",
    type=click.Choice(["commonn", "CS", "preferred", "PFD", "ETF"], case_sensitive=False),
    multiple=True,
    default=None,
    help="types of share classes to fetch and store",
)
def symbols_command(types: list[str] | str | None = None):
    types = polygon.normalize_ticker_types(types)
    data = get_symbols(types)

    for t in types:
        print(f'{t}: {len([d for d in data if d["type"] == t])}')


def get_symbols(types: list[str] | str | None = None) -> list[dict]:
    data = polygon.get_tickers(types)

    os.makedirs(Config.DATA_DIR, exist_ok=True)
    with open(FILEPATH, "w") as f:
        json.dump(data, f, indent=2)

    return data
