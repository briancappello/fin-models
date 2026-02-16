from __future__ import annotations

import pandas as pd

from fin_models.bar_handlers import BarHandler, History, construct_bar_handlers
from fin_models.data_classes import Bar, Freq
from fin_models.store import DataframeGetter, Store

from tests.data import load_data


class CustomStore(DataframeGetter):
    def get(self, symbol: str, freq: Freq, **kwargs) -> pd.DataFrame:
        return load_data(symbol, freq)


def test_history_default_store():
    class WantsHistory(BarHandler):
        history: History

    history, wants_history = construct_bar_handlers(WantsHistory)
    assert isinstance(history, History)
    assert isinstance(history.store, Store)
    assert isinstance(wants_history, WantsHistory)


def test_history_custom_store_in_constructor():
    class WantsHistory(BarHandler):
        history: History

    history_instance = History(store=CustomStore())
    history, wants_history = construct_bar_handlers(WantsHistory, [history_instance])
    assert history == history_instance
    assert isinstance(history, History)
    assert isinstance(history.store, CustomStore)
    assert isinstance(wants_history, WantsHistory)


def test_history_custom_store_on_bar_handler():
    class WantsHistory(BarHandler):
        history: History = History(store=CustomStore())

    history, wants_history = construct_bar_handlers(WantsHistory)
    assert isinstance(history, History)
    assert isinstance(history.store, CustomStore)
    assert isinstance(wants_history, WantsHistory)


def test_history_custom_store_on_nested_bar_handler():
    class WantsHistory(BarHandler):
        history: History = History(store=CustomStore())

    class Another(BarHandler):
        history: History

    class Root(BarHandler):
        another: Another
        wants_history: WantsHistory

    history, another, wants_history, root = construct_bar_handlers(
        Root, _custom_scope=locals()
    )
    assert isinstance(history, History)
    assert isinstance(history.store, CustomStore)
    assert isinstance(another, Another)
    assert isinstance(another.history.store, CustomStore)
    assert isinstance(wants_history, WantsHistory)
    assert isinstance(wants_history.history.store, CustomStore)
    assert isinstance(root, Root)


def test_get_daily_history():
    class WantsHistory(BarHandler):
        history: History

    history, wants_history = bar_handlers = construct_bar_handlers(WantsHistory)

    amd = load_data("AMD", Freq.day)
    for i in amd.index:
        bar = Bar.from_series(amd.loc[i], "AMD", Freq.day)
        for bh in bar_handlers:
            bh.handle_bar(bar)
            df = wants_history.get_history(Freq.day)
            assert df.iloc[-1].name == bar.Epoch


def test_get_intraday_history():
    history = History(store=CustomStore())
    history.symbol_data["AMD"][Freq.min_1] = load_data("AMD", Freq.min_1)

    class WantsHistory(BarHandler):
        history: History

    history, wants_history = bar_handlers = construct_bar_handlers(
        WantsHistory, [history]
    )

    amd = load_data("AMD", Freq.min_1).loc["2023-01-03"]
    for i in amd.index:
        bar = Bar.from_series(amd.loc[i], "AMD", Freq.min_1)
        for bh in bar_handlers:
            bh.handle_bar(bar)
            df = wants_history.get_history(Freq.min_1)
            assert df.iloc[-1].name == bar.Epoch
            df = history.get(Freq.min_1)
            assert df.iloc[-1].name == bar.Epoch
