from __future__ import annotations

from fin_models.cli.groups import yahoo
from fin_models.vendors import yahoo as vendor


@yahoo.command()
def most_actives():
    print(vendor.get_most_actives().to_json(orient="records", indent=2))


@yahoo.command()
def trending():
    print(vendor.get_trending_tickers().to_json(orient="records", indent=2))


@yahoo.command()
def gainers():
    print(vendor.get_gainers_tickers().to_json(orient="records", indent=2))


@yahoo.command()
def losers():
    print(vendor.get_losers_tickers().to_json(orient="records", indent=2))
