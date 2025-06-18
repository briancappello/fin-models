from __future__ import annotations

import argparse
import asyncio
import json

from collections import defaultdict
from datetime import date, time

import pandas as pd

from websockets.asyncio.client import connect

from fin_models import analysis_utils as au
from fin_models.config import Config
from fin_models.enums import Freq
from fin_models.order_book import OrderRequest
from fin_models.trading.client import TradingClient
from fin_models.trading.private_alpaca import TradingClient as PrivateTradingClient
from fin_models.ws.strategy import Strategy


URL = "wss://delayed.polygon.io/stocks"
URL = "ws://localhost:8765"
LOCAL_BROKER = "ws://localhost:8777"


class CustomStrategy(Strategy):
    def __init__(self, dt: date, symbols: list[str], trading_client: TradingClient):
        super().__init__(dt, symbols, trading_client)
        self.stats = {}
        for symbol in symbols:
            self.stats[symbol] = au.signal(symbol, freq=Freq.day, dt=dt.isoformat())
        self.signals = []
        self.premarket_vol = defaultdict(float)

        """
        client handles keeping track of entry/exit prices
        client handles staying under account limits
        server just says "you were filled"
        """

    def handle_message(self, msg: dict):
        bar = super().handle_message(msg)
        if bar.Epoch.time() < time(9, 30):
            self.premarket_vol[bar.symbol] += bar.Volume

        daily_median_vol = self.stats[bar.symbol]["median_volume"]

        # FIXME
        # check bullish
        # check greater than prior close ?
        if self.premarket_vol[bar.symbol] > daily_median_vol:
            print(">>>", bar.Epoch)

        if bar.Volume > daily_median_vol:
            # time since last signal
            # context relative to SMAs
            # trading base in prior price history?

            multiple = bar.Volume / daily_median_vol
            # print(bar, multiple)
            if multiple > 5:
                order_request = OrderRequest.limit_order(
                    side="buy",
                    qty=1,
                    symbol=bar.symbol,
                    limit_price=bar.Close,
                    extended_hours=True,
                    ts=bar.Epoch,
                )
                order = self.trading_client.submit_order(order_request)
                print(order_request.ts)


class WebsocketClient:
    def __init__(self, strategy: Strategy, url: str = URL):
        self.strategy = strategy
        self._url = url
        self._connection = None

    async def _send(self, msg: dict):
        return await self._connection.send(json.dumps(msg))

    async def _recv(self) -> dict:
        return json.loads(await self._connection.recv())

    async def connect(self):
        self._connection = await connect(self._url)

        msg = await self._recv()
        print("connect", msg)
        if len(msg) != 1 or msg[0].get("status") != "connected":
            raise Exception(f"Connection error: {msg[0].get('message')}")

    async def auth(self):
        await self._send(dict(action="auth", params=Config.POLYGON_API_KEY))
        msg = await self._recv()
        print("auth", msg)
        if len(msg) != 1 or msg[0].get("status") != "auth_success":
            raise Exception(f"Authentication error: {msg[0].get('message')}")

    async def subscribe(self):
        await self._send(
            dict(
                action="subscribe",
                params=",".join([f"AM.{symbol}" for symbol in self.strategy.symbols]),
            )
        )
        msg = await self._recv()
        print("subscribe", msg)
        if len(msg) != 1 or msg[0].get("status") != "success":
            raise Exception(f"Subscription error: {msg[0].get('message')}")

    async def run(self):
        await asyncio.wait_for(self.connect(), timeout=1)
        await asyncio.wait_for(self.auth(), timeout=1)
        await asyncio.wait_for(self.subscribe(), timeout=1)

        # FIXME can you tee together two iterators? (ie ws connections)
        async for msg in self._connection:
            self.strategy.handle_message(json.loads(msg))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument(
        "--symbols",
        type=lambda symbols: [symbol.strip().upper() for symbol in symbols.split(",")],
    )
    args = parser.parse_args()

    # FIXME
    # check symbols are tradeable on Alpaca
    # positions bookkeeping

    trading_client = TradingClient(PrivateTradingClient, paper=True)
    client = WebsocketClient(
        strategy=CustomStrategy(
            dt=args.date, symbols=args.symbols, trading_client=trading_client
        )
    )
    asyncio.run(client.run())
