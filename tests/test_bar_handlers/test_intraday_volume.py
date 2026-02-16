from __future__ import annotations

import pandas as pd

from fin_models.bar_handlers import History, IntradayVolume, construct_bar_handlers
from fin_models.data_classes import Bar, Freq
from fin_models.store import (
    DataframeGetter,
    between_time_aftermarket,
    between_time_intraday,
    between_time_premarket,
)

from tests.data import load_data


def test_intraday_volume():
    bar_handlers = construct_bar_handlers(IntradayVolume)
    iv = [x for x in bar_handlers if isinstance(x, IntradayVolume)][0]

    df = load_data("AMD", Freq.min_1).loc["2023-01-03"]
    premarket_bars = df.between_time("04:00", "09:30", inclusive="left")
    intraday_bars = df.between_time("09:30", "16:00", inclusive="left")
    aftermarket_bars = df.between_time("16:00", "20:00", inclusive="left")

    premarket_volume = premarket_bars.Volume.sum()
    intraday_volume = intraday_bars.Volume.sum()
    aftermarket_volume = aftermarket_bars.Volume.sum()

    for i in df.index:
        bar = Bar.from_series(df.loc[i], "AMD", Freq.min_1)

        for bar_handler in bar_handlers:
            bar_handler.handle_bar(bar)

    assert iv.premarket_volume == premarket_volume
    assert iv.intraday_volume == intraday_volume
    assert iv.aftermarket_volume == aftermarket_volume


def test_intraday_volume_with_daily_bar():
    class CustomStore(DataframeGetter):
        def get(self, symbol: str, freq: Freq, **kwargs) -> pd.DataFrame:
            return load_data(symbol, freq)

    history = History(store=CustomStore())
    bar_handlers = construct_bar_handlers(IntradayVolume, [history])
    iv = [x for x in bar_handlers if isinstance(x, IntradayVolume)][0]

    daily_df = load_data("AMD", Freq.day)
    minutely_df = load_data("AMD", Freq.min_1)

    for i in daily_df.index:
        bar = Bar.from_series(daily_df.loc[i], "AMD", Freq.day)
        for bar_handler in bar_handlers:
            bar_handler.handle_bar(bar)

        df = minutely_df.loc[bar.Epoch.date().isoformat()]
        premarket_bars = between_time_premarket(df)
        intraday_bars = between_time_intraday(df)
        aftermarket_bars = between_time_aftermarket(df)

        premarket_volume = premarket_bars.Volume.sum()
        intraday_volume = intraday_bars.Volume.sum()
        aftermarket_volume = aftermarket_bars.Volume.sum()

        assert iv.premarket_volume == premarket_volume
        assert iv.intraday_volume == intraday_volume
        assert iv.aftermarket_volume == aftermarket_volume
