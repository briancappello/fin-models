import asyncio

import aiohttp
import click
import pandas as pd

from pandas.tseries.frequencies import to_offset

from fin_models.store import Store
from fin_models.vendors import polygon, yahoo


store = Store()


def chunk(string, size):
    for i in range(0, len(string), size):
        yield string[i:i+size]


@click.group()
def cli():
    """CLI Commands"""


@cli.command()
@click.argument('symbol')
@click.option('--timeframe', default='1d')
def df(symbol, timeframe):
    df = yahoo.get_df(symbol, timeframe=timeframe)
    print(df[:-1])


@cli.command()
def agg():
    amd = store.read('AMD')

    # weekly
    df = amd.resample('W').apply({'Open': 'first',
                                  'High': 'max',
                                  'Low': 'min',
                                  'Close': 'last',
                                  'Volume': 'sum'})
    df.index = df.index - pd.Timedelta(days=6)

    # monthly
    df = amd.resample('MS').apply({'Open': 'first',
                                  'High': 'max',
                                  'Low': 'min',
                                  'Close': 'last',
                                  'Volume': 'sum'})
    print(df)

    # yearly
    df = amd.resample('YS').apply({'Open': 'first',
                                  'High': 'max',
                                  'Low': 'min',
                                  'Close': 'last',
                                  'Volume': 'sum'})


@cli.command()
def init():
    latest_bars = polygon.daily_bars()
    symbols = [symbol for symbol in latest_bars.keys()
               if not store.has(symbol)]

    async def dl(session, symbol):
        url, cookies = yahoo.get_yfi_url_and_cookies(symbol)
        try:
            print(f'Fetching {symbol}')
            async with session.get(url, cookies=cookies, headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:89.0) '
                              'Gecko/20100101 Firefox/89.0',
            }) as r:
                data = await r.json()
                if r.status != 200:
                    return symbol, data['chart']['error']['code']
        except Exception as e:
            return symbol, e
        else:
            df = yahoo.yfi_json_to_df(data, '1d')
            if df is None:
                return symbol, "Invalid Data"
            click.echo(f'Writing {symbol}')
            store.write(symbol, df)

    async def dl_all(symbols):
        errors = []
        async with aiohttp.ClientSession() as session:
            for batch in chunk(symbols, 8):
                tasks = [dl(session, symbol) for symbol in batch]
                results = await asyncio.gather(*tasks, return_exceptions=False)
                errors.extend([error for error in results if error])
        return errors

    loop = asyncio.get_event_loop()
    errors = loop.run_until_complete(
        dl_all(symbols)
    )

    if errors:
        click.echo('!!! Handling Errors !!!')
        for error in errors:
            if isinstance(error, tuple):
                symbol, msg = error
                print(f'{symbol}: {msg}')
            else:
                print(error)  # FIXME: properly handle exceptions...

    click.echo('Done')




# all you really care about is the tickers
# one file per day, updated every 5 mins? 15 mins?
# append a timestamped line on every update


@cli.command()
def trending():
    yahoo.get_trending_tickers().to_json('trending.json', orient='index', indent=2)


@cli.command()
def gainers():
    yahoo.get_gainers_tickers().to_json('gainers.json', orient='index', indent=2)


@cli.command()
def losers():
    yahoo.get_losers_tickers().to_json('losers.json', orient='index', indent=2)


@cli.command('most-actives')
def most_actives():
    yahoo.get_most_actives().to_json('most-actives.json', orient='index', indent=2)


if __name__ == '__main__':
    cli()


"""
when does a ticker get onto the trending list
how long does it stay on the list
how does its price behave before/during/after
"""
