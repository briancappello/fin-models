from __future__ import annotations

import pandas as pd

from fin_models import Bar, Freq, store
from fin_models.bar_handlers import BarHandler, History, PerSymbolAttribute
from fin_models.bar_handlers.recursive_constructor import construct_bar_handlers


class One(BarHandler):
    num_bars: int = PerSymbolAttribute()

    def handle_bar(self, bar: Bar, **kwargs) -> None:
        self.num_bars = (self.num_bars or 0) + 1


class Two(BarHandler):
    one: One


class Three(BarHandler):
    one: One
    two: Two


class WithHistory(BarHandler):
    history_reqs = {Freq.day: 200}
    history: History


class RootStrategy(BarHandler):
    history_reqs = {Freq.day: 500}
    history: History
    with_history: WithHistory
    three: Three
    two: Two
    one: One = One()


def test_recursive_constructor():
    execution_order = construct_bar_handlers(RootStrategy, [History(store)])
    assert [i.__class__ for i in execution_order] == [
        One,
        History,
        Two,
        WithHistory,
        Three,
        RootStrategy,
    ]
    # one = execution_order[0]
    root = execution_order[-1]
    assert root.one == root.two.one == root.three.one
    assert root.two == root.three.two

    bar = Bar(
        symbol="TEST",
        freq=Freq.min_1,
        Epoch=pd.Timestamp("2026-01-01 09:30:00-0500"),
        Open=5,
        High=5.5,
        Low=4.5,
        Close=5,
        Volume=100,
    )
    for handler in execution_order:
        handler.handle_bar(bar)
    assert root.one.num_bars == 1

    assert root.history.freq_requests == {Freq.day: 500}
