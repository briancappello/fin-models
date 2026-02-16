from __future__ import annotations

from collections import defaultdict
from typing import Any, Union

import pandas as pd

from fin_models.data_classes import Bar, Freq
from fin_models.utils import get_class_from_module


class ClassLocationDescriptor:
    def __get__(self, instance, cls):
        return f"{cls.__module__}:{cls.__name__}"


class PerSymbolAttribute:
    def __set_name__(self, klass, name):
        self.name = name

    def __get__(self, instance, klass):
        return instance.symbol_data[instance.symbol].get(self.name)

    def __set__(self, instance, value):
        instance.symbol_data[instance.symbol][self.name] = value


class BarHandler:
    """
    Base class for all bar handlers.

    class DoSomething(BarHandler):
        # other bar handlers to be injected, specified as type annotations on the class
        history: History

        def handle_bar(bar: Bar):
            # every subclass must call super here to make per-symbol magic work
            super().handle_bar(bar)
    """

    # public
    history_reqs: dict[Freq, int]

    # internal
    import_path = ClassLocationDescriptor()

    def __init__(self, **injected_bar_handlers):
        # public
        for k, v in injected_bar_handlers.items():
            setattr(self, k, v)

        self.symbol = None
        self.symbol_data: dict[str, dict[str, Any]] = defaultdict(dict)

    def handle_bar(self, bar: Bar, **kwargs) -> None:
        self.symbol = bar.symbol

    def get_history(self, freq, num_bars: int | None = None) -> pd.DataFrame:
        if num_bars is None:
            num_bars = getattr(self, "history_reqs", {}).get(freq)
        df = self.history.get(freq)
        return df if num_bars is None else df.iloc[-num_bars:]

    # per-symbol dict-like interface on self
    def __setitem__(self, key, value):
        self.symbol_data[self.symbol][key] = value

    def __getitem__(self, key):
        return self.symbol_data[self.symbol][key]

    def __contains__(self, key):
        return key in self.symbol_data[self.symbol]

    def get(self, key, default=None):
        return self.symbol_data[self.symbol].get(key, default)

    @classmethod
    def get_class_bar_handlers(
        cls, _custom_scope: dict | None = None
    ) -> dict[str, Union[type["BarHandler"], "BarHandler"]]:
        custom_scope = _custom_scope or {}
        cls_annotations = {}
        for k, v in cls.__annotations__.items():
            if isinstance(v, str):
                try:
                    cls_annotations[k] = get_class_from_module(f"{cls.__module__}:{v}")
                except AttributeError:
                    cls_annotations[k] = custom_scope.get(v, v)
            else:
                cls_annotations[k] = v

        return {k: v for k, v in vars(cls).items() if isinstance(v, BarHandler)} | {
            k: getattr(cls, k, v)
            for k, v in cls_annotations.items()
            if isinstance(v, type) and issubclass(v, BarHandler)
        }


__all__ = [
    "BarHandler",
    "PerSymbolAttribute",
]
