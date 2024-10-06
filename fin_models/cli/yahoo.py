from __future__ import annotations

import asyncio
import random

import aiohttp
import click

from fin_models.cli.groups import yahoo
from fin_models.enums import Freq
from fin_models.services import store
from fin_models.utils import chunk
from fin_models.vendors import polygon
from fin_models.vendors import yahoo as vendor


@yahoo.command("init")
@click.option("--symbols", type=str, default=None)
def _init_daily_with_yahoo(symbols: str | None = None):
    """Initialize daily historical data from Yahoo! Finance"""
    if not symbols:
        symbols = polygon.get_symbols()
    elif isinstance(symbols, str):
        symbols = [symbol.strip().upper() for symbol in symbols.split(",")]

    # filter out already-existing symbols
    symbols = [symbol for symbol in symbols if not store.has(symbol, freq=Freq.day)]

    async def dl(session, symbol):
        await asyncio.sleep(random.random() * 2)
        url, cookies = yahoo.get_yfi_url_and_cookies(symbol)
        try:
            print(f"Fetching {symbol}")
            async with session.get(
                url,
                cookies=cookies,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101"
                        " Firefox/102.0"
                    ),
                    "Host": "query1.finance.yahoo.com",
                    "Origin": "https://finance.yahoo.com",
                    "Referer": f"https://finance.yahoo.com/chart/{symbol}",
                },
            ) as r:
                data = await r.json()
                if r.status != 200:
                    return symbol, data["chart"]["error"]["code"]
        except Exception as e:
            return symbol, e
        else:
            df = yahoo.yfi_json_to_df(data, Freq.day)
            if df is None:
                return symbol, "Invalid Data"
            click.echo(f"Writing {symbol}")
            store.write(symbol, Freq.day, df)

    async def dl_all(symbols):
        errors = []
        async with aiohttp.ClientSession() as session:
            for batch in chunk(symbols, 5):
                tasks = [dl(session, symbol) for symbol in batch]
                results = await asyncio.gather(*tasks, return_exceptions=False)
                errors.extend([error for error in results if error])
        return errors

    loop = asyncio.get_event_loop()
    errors = loop.run_until_complete(dl_all(symbols))

    if errors:
        click.echo("!!! Handling Errors !!!")
        for error in errors:
            if isinstance(error, tuple):
                symbol, msg = error
                print(f"{symbol}: {msg}")
            else:
                print(error)  # FIXME: properly handle exceptions...

    click.echo("Done")


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


"""
# all you really care about is the tickers
# one file per day, updated every 5 mins? 15 mins?
# append a timestamped line on every update

when does a ticker get onto the trending list
how long does it stay on the list
how does its price behave before/during/after
"""
