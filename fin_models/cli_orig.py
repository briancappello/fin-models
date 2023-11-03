import importlib

from datetime import timedelta

import aiohttp
import click
import pandas as pd

from aiohttp.client_exceptions import ContentTypeError


@main.command()
def agg():
    amd = store.get("AMD")

    # weekly
    df = amd.resample("W").apply(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )
    df.index = df.index - pd.Timedelta(days=6)

    # monthly
    df = amd.resample("MS").apply(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )
    print(df)

    # yearly
    df = amd.resample("YS").apply(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )


# all you really care about is the tickers
# one file per day, updated every 5 mins? 15 mins?
# append a timestamped line on every update


@main.command()
def trending():
    yahoo.get_trending_tickers().to_json("trending.json", orient="index", indent=2)


@main.command()
def gainers():
    yahoo.get_gainers_tickers().to_json("gainers.json", orient="index", indent=2)


@main.command()
def losers():
    yahoo.get_losers_tickers().to_json("losers.json", orient="index", indent=2)


@main.command("most-actives")
def most_actives():
    yahoo.get_most_actives().to_json("most-actives.json", orient="index", indent=2)


if __name__ == "__main__":
    cli()


"""
when does a ticker get onto the trending list
how long does it stay on the list
how does its price behave before/during/after
"""
