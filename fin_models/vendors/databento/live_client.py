from __future__ import annotations

import os
import sys
import traceback

from pprint import pprint
from zoneinfo import ZoneInfo

import databento
import pandas as pd

from fin_models import analysis_utils as au
from fin_models.data_classes import Bar
from fin_models.enums import Freq
from fin_models.services import nyse, store


EST = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

PRICE_DENOMINATOR = 1e9


class Scanner:
    def __init__(self):
        self.symbol_dir = {}
        self.min_history = {}
        self._prior_date = nyse.get_prior_trading_date(
            nyse.get_latest_trading_date(include_extended=True)
        ).isoformat()
        print(f"{self._prior_date=}")
        self._date_51_days_ago = nyse.get_date_n_daily_bars_ago(51)
        self.signal_lit = {}
        self.prior_days = {}
        self.hwm = {}
        self.scale = {}

    def scan(self, event: databento.OHLCVMsg | databento.SymbolMappingMsg) -> None:
        if isinstance(event, databento.SymbolMappingMsg):
            symbol = self.symbol_dir[event.instrument_id] = event.stype_out_symbol
            self.min_history[symbol] = store.get(
                symbol,
                freq=Freq.min_1,
                start_dt=self._date_51_days_ago,
                end_dt=self._prior_date,
            )
            daily = store.get(symbol)
            if daily is not None and self._prior_date in daily.index:
                self.prior_days[symbol] = daily.loc[self._prior_date]
            return

        elif not isinstance(event, databento.OHLCVMsg):
            return

        bar = Bar(
            Epoch=pd.Timestamp(event.ts_event, unit="ns", tz="UTC").tz_convert(
                "America/New_York"
            ),
            freq=Freq.min_1,
            symbol=self.symbol_dir[event.instrument_id],
            Open=event.open / PRICE_DENOMINATOR,
            High=event.high / PRICE_DENOMINATOR,
            Low=event.low / PRICE_DENOMINATOR,
            Close=event.close / PRICE_DENOMINATOR,
            Volume=event.volume,
        )
        print(bar)
        df = self.min_history[bar.symbol]
        if df is None or df.empty or bar.symbol not in self.prior_days:
            return
        df = self.min_history[bar.symbol] = df._append(bar.to_series())
        assert isinstance(df.index, pd.DatetimeIndex)
        if bar.symbol == "RDI":
            store.write("RDI", Freq.min_1, df)

        if bar.symbol in self.signal_lit:
            self.hwm[bar.symbol] = max(bar.Close, self.hwm.get(bar.symbol, bar.Close))
            return

        stats = au.intraday_volume_multiple_of_median(df)
        premarket_volume = stats["premarket_volume"].iloc[-1]
        premarket_volume_multiple_of_median = stats[
            "premarket_volume_multiple_of_median"
        ].iloc[-1]

        prior_close = self.prior_days[bar.symbol].Close

        if (
            premarket_volume > 750_000
            and premarket_volume_multiple_of_median > 10
            and 1 < prior_close < bar.Close < 10
            and bar.Close > bar.Open
            and bar.symbol not in self.signal_lit
        ):
            self.signal_lit[bar.symbol] = bar
            self.scale[bar.symbol] = premarket_volume_multiple_of_median
            print(
                f"{bar.symbol=}: {bar.Epoch=}, {bar.Open=}, {bar.High=}, {bar.Low=}, {bar.Close=}, {bar.Volume=}"
            )


def main():
    client = databento.Live(key=os.getenv("DATABENTO_API_KEY"))
    client.subscribe(
        dataset="EQUS.MINI",
        schema=databento.Schema.OHLCV_1M,
        symbols="ALL_SYMBOLS",
        # symbols="PALI,BDRX,APVO,ENVB,BEAT,SNTI,NXDR,MNTS,SCNX,ADVB,IRBT,BBGI,CGTL,KITT",
        start="2025-12-11T04:00:00-0500",
    )

    def wtf(e):
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)

    scanner = Scanner()
    client.add_callback(scanner.scan, exception_callback=wtf)
    client.start()
    client.block_for_close()
    for symbol in scanner.signal_lit:
        entry = scanner.signal_lit[symbol].Close
        exit = scanner.hwm[symbol]
        print(f"{symbol}: {entry} -> {exit} {exit > entry} {scanner.scale[symbol]}")


if __name__ == "__main__":
    main()

    hist = databento.Historical()
