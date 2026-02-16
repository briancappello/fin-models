from __future__ import annotations

import json
import os
import sys
import traceback

from datetime import date, datetime
from typing import Literal

import pandas as pd

from massive.websocket import WebSocketClient as BaseWebSocketClient
from massive.websocket import env_key as MASSIVE_API_KEY_ENVVAR
from massive.websocket.models import Feed, Market

from fin_models.bar_handlers import BarHandler
from fin_models.data_classes import Bar, Freq


SUPPORTED_FREQS = {
    Freq.second: "A",
    Freq.min_1: "AM",
}
SUPPORTED_PREFIXES = set(SUPPORTED_FREQS.values())


class MassiveLiveClient:
    def __init__(
        self,
        bar_handlers: list[BarHandler],
        freq: Freq = Freq.min_1,
        dataset: Literal[
            "stocks",
            "options",
            "forex",
            "crypto",
            "indices",
            "futures",
            "futures/cme",
            "futures/cbot",
            "futures/nymex",
            "futures/comex",
        ] = "stocks",
        symbols: list[str] | str = "*",
        start_dt: pd.Timestamp | datetime | date | str | int | None = None,
        server_url: str | None = None,
    ):
        self.client = WebsocketClient(
            market=Market(dataset),
            start_dt=start_dt,
            server_url=server_url,
        )
        self.bar_handlers = bar_handlers
        self.freq = freq
        self.symbols = symbols

    def run(self):
        try:
            self._subscribe()
            self.client.run(self._handle_event, process_exception=self._handle_exception)
        except KeyboardInterrupt:
            print("\nDisconnecting from Massive...")

    def _handle_event(self, raw_msg: str | bytes):
        for msg in json.loads(raw_msg):
            if msg.get("ev") in SUPPORTED_PREFIXES:
                bar = Bar.from_ws_msg(msg)
                for bar_handler in self.bar_handlers:
                    bar_handler.handle_bar(bar)

    def _handle_exception(self, e: Exception):
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)

    def _subscribe(self):
        prefix = SUPPORTED_FREQS[self.freq]

        if isinstance(self.symbols, list):
            subscriptions = [f"{prefix}.{symbol}" for symbol in self.symbols]
        elif self.symbols == "*":
            subscriptions = [f"{prefix}.*"]
        elif "," in self.symbols:
            subscriptions = [
                f"{prefix}.{symbol.strip()}" for symbol in self.symbols.split(",")
            ]
        else:
            subscriptions = [f"{prefix}.{self.symbols}"]

        self.client.subscribe(*subscriptions)


class WebsocketClient(BaseWebSocketClient):
    def __init__(
        self,
        market: Market = Market.Stocks,
        start_dt: date | None = None,
        server_url: str | None = None,
        **kwargs,
    ):
        super().__init__(
            api_key=os.getenv(MASSIVE_API_KEY_ENVVAR),
            feed=Feed.RealTime,
            market=market,
            raw=True,
            verbose=False,
            **kwargs,
        )
        self.start_dt = start_dt
        if server_url:
            self.url = server_url

    async def _subscribe(self, topics: list[str] | set[str]):
        if self.websocket is None or len(topics) == 0:
            return

        start_dt = self.start_dt
        if start_dt is not None and not hasattr(start_dt, "isoformat"):
            start_dt = pd.Timestamp(start_dt)

        subs = ",".join(topics)
        await self.websocket.send(
            self.json.dumps(
                {
                    "action": "subscribe",
                    "params": subs,
                    "date": start_dt.isoformat() if start_dt else None,
                }
            )
        )


__all__ = [
    "MassiveLiveClient",
]
