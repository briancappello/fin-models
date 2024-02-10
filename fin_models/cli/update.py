import click

from fin_models.bulk_downloader import bulk_download
from fin_models.cli.main import main
from fin_models.enums import Freq
from fin_models.services import store
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
    freq = {"minute": Freq.min_1, "day": Freq.day}[timeframe]

    symbols = store.symbols(freq)
    count = 0
    for batch in chunk(symbols, 100):
        urls = [
            polygon.make_history_url(
                symbol,
                freq,
                start=store.get(symbol, freq).iloc[-1].name,
            )
            for symbol in batch
        ]
        successes, errors, exceptions = bulk_download(urls)
        for resp in successes:
            count += 1
            m = polygon.HISTORY_URL_REGEX.match(resp.url)
            symbol = m.groupdict()["symbol"]
            print(f"Updating {symbol} ({count} / {len(symbols)})")
            df = polygon.json_to_df(resp.json)
            store.append(symbol, df, freq)
