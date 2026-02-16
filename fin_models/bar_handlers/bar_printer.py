from __future__ import annotations

import pandas as pd

from fin_models import Bar
from fin_models.bar_handlers.bar_handler import BarHandler


class BarPrinter(BarHandler):
    def handle_bar(self, bar: Bar, **kwargs) -> None:
        super().handle_bar(bar)
        print(bar)
