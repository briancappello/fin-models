from __future__ import annotations

import functools
import importlib
import typing as t

import pandas as pd

from .calendar import Calendar
from .date_utils import DateType
from .store import Store


class StrategyRunner:
    def __init__(
        self,
        strategies: dict[str, dict],
        store: Store | None = None,
        calendar: Calendar | str = "NYSE",
        results_path: str | None = None,
        symbols: list[str] | None = None,
    ):
        self.store = store or Store()
        self.calendar = (
            Calendar(exchange=calendar) if isinstance(calendar, str) else calendar
        )
        self.results_path = results_path
        self.symbols = symbols
        self.strategies = self.load_strategies(strategies)

    @staticmethod
    def load_strategies(strategies: dict[str, dict[str, t.Any]]) -> dict[str, callable]:
        r = {}
        for strategy_path, strategy_kwargs in strategies.items():
            module_path, strategy_name = strategy_path.rsplit(".", maxsplit=1)
            try:
                module = importlib.import_module(module_path)
            except (ImportError, ModuleNotFoundError) as e:
                raise ImportError(
                    f"Could not import the {module_path!r} module while "
                    f"attempting to load {strategy_name}!"
                ) from e

            strategy = getattr(module, strategy_name)
            r[strategy_name] = (
                strategy(**strategy_kwargs)
                if isinstance(strategy, type)
                else functools.partial(strategy, **strategy_kwargs)
            )
        return r

    def run(
        self,
        symbols: list[str] | None = None,
        date: DateType | None = None,
    ) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
        symbols = symbols or self.symbols or self.store.symbols()
        date = (
            date or self.calendar.get_latest_trading_date_schedule().market_close
        ).isoformat()[:10]
        results = pd.DataFrame(index=symbols, columns=list(self.strategies.keys()))
        errors = []
        data = {symbol: self.store.get(symbol) for symbol in symbols}
        for symbol, df in data.items():
            for strategy_name, strategy_callable in self.strategies.items():
                try:
                    result = strategy_callable(df[:date])
                except Exception as e:
                    errors.append((repr(e), symbol))
                else:
                    results[strategy_name][symbol] = result
        return results, errors
