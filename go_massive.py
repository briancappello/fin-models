from __future__ import annotations

import argparse

from datetime import date, datetime

import pandas as pd

from fin_models import store
from fin_models.bar_handlers import (
    BarHandler,
    BarPrinter,
    History,
    IntradayVolume,
    PerSymbolAttribute,
)
from fin_models.bar_handlers.recursive_constructor import construct_bar_handlers
from fin_models.data_classes import Bar, Freq
from fin_models.vendors.massive import MassiveLiveClient
from refresh_watchlists import AllWatchlists


class Multistrategy(BarHandler):
    history: History = History(store, freqs={Freq.min_1: 40_000, Freq.day: 200})
    intraday_volume: IntradayVolume
    printer: BarPrinter

    premarket_signal: Bar = PerSymbolAttribute()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.premarket_signals: dict[str, Bar] = {}
        # self.intraday_signals: dict[str, Bar] = {}
        # self.aftermarket_signals: dict[str, Bar] = {}

    def handle_bar(self, bar: Bar, **kwargs) -> None:
        super().handle_bar(bar)

        if bar.is_premarket:
            if (
                self.intraday_volume.premarket_volume > 1_000_000
                and self.intraday_volume.volume_multiple_of_median > 10
                and not self.premarket_signal
            ):
                print("*" * 80)
                print(bar)
                print(self.intraday_volume.premarket_volume)
                print(self.intraday_volume.volume_multiple_of_median)
                self.premarket_signal = bar


if __name__ == "__main__":
    # ms = Multistrategy()
    # amd = store.get("INBS", Freq.min_1)
    # bar = Bar.from_series(amd.loc["2025-12-31"].iloc[30], symbol="INBS", freq=Freq.min_1)
    # ms.handle_bar(bar)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--strategy", type=str, action="append")
    parser.add_argument("--symbols", type=str, default="*")

    args = parser.parse_args()

    client = MassiveLiveClient(
        bar_handlers=construct_bar_handlers(Multistrategy),
        symbols=args.symbols.split(","),
        start_dt=args.date,
        server_url="ws://localhost:8765",
    )
    client.run()
    print("Done consuming.")
