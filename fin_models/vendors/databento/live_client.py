from __future__ import annotations

import os
import sys
import traceback

from datetime import date, datetime

import databento
import pandas as pd

from databento import Schema

from fin_models.bar_handlers import BarHandler
from fin_models.data_classes import Bar
from fin_models.date_utils import EST, UTC
from fin_models.enums import Freq


PRICE_DENOMINATOR = 1e9


SCHEMA_MAP: dict[Freq, Schema] = {  # type: ignore
    Freq.second: Schema.OHLCV_1S,
    Freq.min_1: Schema.OHLCV_1M,
    Freq.hour: Schema.OHLCV_1H,
    Freq.day: Schema.OHLCV_1D,
}


class DatabentoLiveClient:
    def __init__(
        self,
        bar_handlers: list[BarHandler],
        freq: Freq = Freq.min_1,
        dataset: str = "EQUS.MINI",
        symbols: list[str] | str = "ALL_SYMBOLS",
        start_dt: pd.Timestamp | datetime | date | str | int | None = None,
    ):
        self.client = databento.Live(key=os.getenv("DATABENTO_API_KEY"))
        self.bar_handlers = bar_handlers
        self.freq = freq
        self.dataset = dataset
        self.symbols = "ALL_SYMBOLS" if symbols == "*" else symbols
        self.start_dt = start_dt
        self.symbol_dir = {}

    def run(self):
        self.client.subscribe(
            dataset=self.dataset,
            schema=SCHEMA_MAP[self.freq],
            symbols=self.symbols,
            start=self.start_dt,
        )
        self.client.add_callback(
            self._handle_event,
            exception_callback=self._handle_exception,
        )

        self.client.start()
        self.client.block_for_close()

    def _handle_event(
        self,
        event: databento.OHLCVMsg | databento.SymbolMappingMsg,
    ) -> None:
        if isinstance(event, databento.SymbolMappingMsg):
            symbol = self.symbol_dir[event.instrument_id] = event.stype_out_symbol
            return

        elif not isinstance(event, databento.OHLCVMsg):
            return

        bar = Bar(
            Epoch=pd.Timestamp(event.ts_event, unit="ns", tz=UTC).tz_convert(EST),
            freq=self.freq,
            symbol=self.symbol_dir[event.instrument_id],
            Open=event.open / PRICE_DENOMINATOR,
            High=event.high / PRICE_DENOMINATOR,
            Low=event.low / PRICE_DENOMINATOR,
            Close=event.close / PRICE_DENOMINATOR,
            Volume=event.volume,
        )
        for bar_handler in self.bar_handlers:
            bar_handler.handle_bar(bar)

    def _handle_exception(self, e: Exception):
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)


__all__ = [
    "DatabentoLiveClient",
]
