from __future__ import annotations

import json
import os

from typing import Any

import pandas as pd

from fin_models import Bar, Freq
from fin_models.bar_handlers import (
    BarHandler,
    History,
    PerSymbolAttribute,
)
from fin_models.config import Config


class NameDescriptor:
    def __get__(self, instance, owner):
        return owner.__name__


class Watchlist(BarHandler):
    name: str = NameDescriptor()
    key: str | None = None

    history: History
    history_reqs = {Freq.day: None}

    row: dict[str, Any] = PerSymbolAttribute()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_ts = None

    def handle_bar(self, bar: Bar, refresh: bool = False, **kwargs) -> None:
        super().handle_bar(bar)

        if self._last_ts is None:
            self._last_ts = bar.Epoch

        df = self.history[Freq.day].loc[: bar.Epoch.date().isoformat()]
        prior_day_bar = None
        if len(df) > 1:
            prior_day_bar = Bar.from_series(df.iloc[-2], symbol=bar.symbol, freq=bar.freq)

        self.row = self.calc_row(bar, prior_day_bar)

        should_refresh = bar.Epoch > self._last_ts
        if should_refresh:
            self._last_ts = bar.Epoch
        if refresh or should_refresh:
            wl_df = self.get_watchlist_dataframe()
            self.save(wl_df)

    def get_watchlist_dataframe(self) -> pd.DataFrame:
        return self.sort(
            pd.DataFrame.from_records(
                [
                    dict(symbol=symbol) | data["row"]
                    for symbol, data in self.symbol_data.items()
                ]
            )
        )

    def calc_row(
        self,
        bar: Bar,
        prior_day_bar: Bar | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def sort(cls, wl_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    @classmethod
    def filter_top_symbols(cls, df: pd.DataFrame) -> list[str]:
        if df.empty:
            return []
        return df.symbol[:100].to_list()

    @classmethod
    def save(cls, wl_df: pd.DataFrame) -> None:
        key = cls.key or cls.name.lower().replace(" ", "-")
        with open(os.path.join(Config.WATCHLISTS_DIR, f"{key}.json"), "w") as f:
            f.write(
                json.dumps(
                    dict(
                        key=key,
                        name=cls.name,
                        tickers=cls.filter_top_symbols(wl_df),
                    ),
                )
            )
