from __future__ import annotations

from fin_models import analysis_utils as au
from fin_models.bar_handlers import BarHandler, History, PerSymbolAttribute
from fin_models.data_classes import Bar, Freq
from fin_models.services import nyse
from fin_models.store import (
    between_time_aftermarket,
    between_time_intraday,
    between_time_premarket,
)


class IntradayVolume(BarHandler):
    history: History
    history_reqs = {Freq.min_1: None}

    premarket_volume: int = PerSymbolAttribute()
    intraday_volume: int = PerSymbolAttribute()
    aftermarket_volume: int = PerSymbolAttribute()

    volume: int = PerSymbolAttribute()  # total volume pre + reg + post
    median_volume: int = PerSymbolAttribute()
    volume_multiple_of_median: float = PerSymbolAttribute()

    def handle_bar(self, bar: Bar, **kwargs) -> None:
        super().handle_bar(bar)
        if bar.freq < Freq.day:
            self.handle_minutely_bar(bar)
        else:
            self.handle_daily_bar(bar)

    def handle_minutely_bar(self, bar: Bar):
        df = self.get_history(Freq.min_1)

        # set volume, median_volume, and volume_multiple_of_median
        for k, v in au.intraday_volume_multiple_of_median(
            df,
            from_time="04:00",
            to_time=bar.Epoch.strftime("%H:%M"),
        ).items():
            self[k] = v

        if nyse.is_premarket(bar):
            self.premarket_volume = self.get("premarket_volume", 0) + bar.Volume

        elif nyse.is_intraday(bar):
            self.intraday_volume = self.get("intraday_volume", 0) + bar.Volume

        elif nyse.is_aftermarket(bar):
            self.aftermarket_volume = self.get("aftermarket_volume", 0) + bar.Volume

    def handle_daily_bar(self, bar: Bar):
        df = self.get_history(Freq.min_1)

        # set volume, median_volume, and volume_multiple_of_median
        for k, v in au.intraday_volume_multiple_of_median(
            df,
            from_time="04:00",
            to_time="20:00",
        ).items():
            self[k] = v

        df_today = df.loc[bar.Epoch.date().isoformat()]
        self.premarket_volume = between_time_premarket(df_today).Volume.sum()
        self.intraday_volume = between_time_intraday(df_today).Volume.sum()
        self.aftermarket_volume = between_time_aftermarket(df_today).Volume.sum()
