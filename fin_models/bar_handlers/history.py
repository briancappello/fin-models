from __future__ import annotations

import pandas as pd

from fin_models.data_classes import Bar, Freq
from fin_models.services import store as default_store
from fin_models.store import DataframeGetter

from .bar_handler import BarHandler, PerSymbolAttribute


class History(BarHandler):
    _current_index = PerSymbolAttribute()

    def __init__(
        self,
        store: DataframeGetter = None,
        freqs: dict[Freq, int | None] = None,
    ):
        super().__init__()
        self.store = store or default_store
        self.freq_requests = freqs or {}

    def handle_bar(self, bar: Bar, **kwargs) -> None:
        super().handle_bar(bar)
        self._current_index = bar.Epoch

        for freq, num_bars in self.freq_requests.items():
            self._load(freq)
            # if freq not in self:
            #     df = self.store.get(bar.symbol, freq)
            #     if df is None or df.empty:
            #         self[freq] = None
            #         continue
            #
            #     df = df.loc[: bar.Epoch.date().isoformat()]
            #     self[freq] = df if not num_bars else df.iloc[-(num_bars + 1) : -1]

        df = self.get(bar.freq)
        if df is None or df.empty:
            df = pd.DataFrame([bar.to_series()])
            df.index.name = "Epoch"
        elif bar.Epoch not in self[bar.freq].index:
            df = self[bar.freq]._append(bar.to_series())
        self[bar.freq] = df

    def get(self, freq: Freq, num_bars: int | None = None) -> pd.DataFrame:
        self._load(freq)

        if freq < Freq.day and self._current_index.strftime("%H:%M:%S") == "00:00:00":
            df = self[freq].loc[: self._current_index.date().isoformat()]
        else:
            df = self[freq].loc[: self._current_index]

        if num_bars:
            return df.iloc[-num_bars:]
        return df

    def _load(self, freq: Freq):
        if freq in self:
            return

        df = self.store.get(self.symbol, freq)
        if df is None or df.empty:
            df = None
        self[freq] = df
