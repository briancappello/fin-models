from __future__ import annotations

from fin_models.bar_handlers import construct_bar_handlers
from fin_models.data_classes import Bar, Freq
from fin_models.watchlists import MaxVolumeMultiplesOfMedian

from tests.data import load_data


def test_max_volume_multiples_of_median():
    df = load_data("VERO", Freq.min_1)

    bar_handlers = construct_bar_handlers(MaxVolumeMultiplesOfMedian)
    wl = [x for x in bar_handlers if isinstance(x, MaxVolumeMultiplesOfMedian)][0]

    for i in df.loc["2026-01-15"].index:
        bar = Bar.from_series(df.loc[i], "VERO", Freq.min_1)

        for bh in bar_handlers:
            bh.handle_bar(bar)

        wl_df = wl.get_watchlist_dataframe()
