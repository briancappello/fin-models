from __future__ import annotations

from typing import Any

import pandas as pd

from fin_models import Bar, Freq
from fin_models import analysis_utils as au
from fin_models.bar_handlers import History
from fin_models.services import nyse

from ..bar_handlers.intraday_volume import IntradayVolume
from .watchlist import Watchlist


class MaxVolumeMultiplesOfMedian(Watchlist):
    history: History
    history_reqs = {Freq.min_1: None, Freq.day: None}
    intraday_volume: IntradayVolume

    def calc_row(
        self,
        bar: Bar,
        prior_day_bar: Bar | None,
    ) -> dict[str, Any]:
        days_min_df = (
            self.history[Freq.min_1]
            .loc[bar.Epoch.date().isoformat()]
            .loc[: (bar.Epoch if bar.freq < Freq.day else bar.Epoch.date().isoformat())]
        )
        if bar.freq >= Freq.day:
            day_open = bar.Open
        elif nyse.is_premarket(bar):
            day_open = days_min_df.iloc[0].Open
        else:
            day_open = days_min_df.between_time("09:30", "16:00").iloc[0].Open

        return dict(
            pct_change=au.pct_change(
                close=days_min_df.High.max(),
                prior_close=prior_day_bar.Close if prior_day_bar else day_open,
            ),
            volume=self.intraday_volume.volume,
            median_volume=self.intraday_volume.median_volume,
            volume_multiple_of_median=self.intraday_volume.volume_multiple_of_median,
            premarket_volume=self.intraday_volume.premarket_volume,
            is_bull=bar.Close > prior_day_bar.Close if prior_day_bar else day_open,
        )

    @classmethod
    def sort(cls, wl_df: pd.DataFrame) -> pd.DataFrame:
        if wl_df.empty:
            return wl_df

        return (
            wl_df.where(
                (wl_df.is_bull == True)
                & (
                    (wl_df.median_volume > 500_000)
                    | (wl_df.premarket_volume > 250_000)
                    | (wl_df.volume > 500_000)
                    | (wl_df.volume_multiple_of_median > 30)
                )
            )
            .sort_values(
                "volume_multiple_of_median",
                ascending=False,
            )
            .dropna()
        )
