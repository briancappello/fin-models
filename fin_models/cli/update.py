import click

from fin_models.bulk_downloader import bulk_download
from fin_models.cli.main import main
from fin_models.services import timeframe_stores
from fin_models.utils import chunk
from fin_models.vendors import polygon


@main.command("update")
@click.option(
    "--timeframe",
    type=click.Choice(["minute", "day"]),
    default="day",
    help="the timeframe to update",
)
def update_command(timeframe: str = "day"):
    store = timeframe_stores[timeframe]
    urls = [
        polygon.make_history_url(
            symbol,
            timeframe,
            start=store.get(symbol).iloc[-1].name,
        )
        for symbol in store.symbols()
    ]

    for batch in chunk(urls, 200):
        successes, errors, exceptions = bulk_download(batch)
        for resp in successes:
            m = polygon.HISTORY_URL_REGEX.match(resp.url)
            symbol = m.groupdict()["symbol"]
            print(f"Updating {symbol}")
            df = polygon.json_to_df(resp.json)
            store.append(symbol, df)
