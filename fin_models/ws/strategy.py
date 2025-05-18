from __future__ import annotations

from datetime import date

from fin_models.trading.client import TradingClient
from fin_models.ws.data_classes import Bar


class Strategy:
    def __init__(self, dt: date, symbols: list[str], trading_client: TradingClient):
        self.dt = dt
        self.symbols = symbols
        self.trading_client = trading_client

    def handle_message(self, msg: dict) -> Bar:
        return Bar.from_ws_msg(msg)
