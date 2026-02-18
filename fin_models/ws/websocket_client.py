from __future__ import annotations

import argparse
import asyncio
import json
import math

from collections import defaultdict
from datetime import date, time
from typing import ItemsView

import pandas as pd

from alpaca.trading.client import TradingClient as AlpacaTradingClient
from alpaca.trading.enums import (
    OrderSide,
    TimeInForce,
)
from websockets.asyncio.client import connect

from fin_models import analysis_utils as au
from fin_models.config import Config
from fin_models.data_classes import Bar
from fin_models.date_utils import EST
from fin_models.db import Session
from fin_models.enums import Freq
from fin_models.models import Position
from fin_models.order_book import OrderRequest
from fin_models.services import nyse, store
from fin_models.trading.local_trading_client import TradingClient as LocalTradingClient
from fin_models.trading.trading_client import TradingClient
from fin_models.ws.strategy import Strategy


URL = "wss://delayed.polygon.io/stocks"
# URL = "ws://localhost:8765"
LOCAL_BROKER = "ws://localhost:8777"


class CustomStrategy(Strategy):
    def __init__(
        self,
        dt: date,
        symbols: list[str],
        trading_client: TradingClient,
    ):
        super().__init__(dt, symbols, trading_client)
        self.stats = {}
        for symbol in symbols:
            df = store.get(symbol, freq=Freq.day)
            self.stats[symbol] = au.signal(df, freq=Freq.day)
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


class ManualStrategy(Strategy):
    def __init__(
        self,
        dt: date,
        symbols: list[str],
        trading_client: TradingClient,
        breakout_entry_prices: dict[str, float] = None,
        limit_prices: dict[str, float] = None,
        quantities: dict[str, int] = None,
    ):
        super().__init__(dt, symbols, trading_client)
        self.breakout_entry_prices = breakout_entry_prices or {}
        self.limit_prices = limit_prices or {}
        self.quantities = quantities or {}
        self.intraday_median_volumes = {}

        self.dataframes = {}
        for symbol in symbols:
            self.dataframes[symbol] = store.get(symbol, freq=Freq.min_1, end_dt=self.dt)

        self.session = Session()
        self.positions: dict[str, Position] = {}
        self.high_water_marks: dict[str, float] = {}  # highest close of bar since entry

    def pct_below_price(
        self,
        price: float,
        pct_below_entry: float = 0.15,
    ) -> float:
        return price - (price * pct_below_entry)

    def notional_qty(
        self,
        price: float,
        dollar_amount: int = 100,
    ):
        return math.floor(dollar_amount / price)

    def handle_message(self, msg: dict):
        bar: Bar = super().handle_message(msg)

        # add bar to df cache
        df = self.dataframes[bar.symbol]
        df = df._append(bar.to_series())
        self.dataframes[bar.symbol] = df

        # check for position
        position: Position | None = self.positions.get(
            bar.symbol,
            self.session.query(Position)
            .filter_by(
                ticker=bar.symbol,
                status="open",
            )
            .one_or_none(),
        )
        if bar.symbol in self.positions:
            self.session.refresh(position)

        # signal to initiate position
        limit_price = self.limit_prices.get(bar.symbol, bar.Open)
        if not position and limit_price < 25:
            premarket_volume_multiple_of_median = au.intraday_volume_multiple_of_median(
                df
            )["premarket_volume_multiple_of_median"].iloc[-1]

            price_above_entry_breakout = (
                bar.symbol not in self.breakout_entry_prices
                or bar.Open > self.breakout_entry_prices[bar.symbol]
            )

            if premarket_volume_multiple_of_median > 5 and price_above_entry_breakout:
                # initiate position
                position = Position(ticker=bar.symbol, side="long", qty=0)
                self.positions[bar.symbol] = position
                self.session.add(position)
                self.session.commit()

                # submit order
                order = OrderRequest.limit_order(
                    symbol=bar.symbol,
                    side=OrderSide.BUY,
                    qty=self.quantities.get(
                        bar.symbol,
                        self.notional_qty(limit_price, dollar_amount=50),
                    ),
                    limit_price=limit_price,
                    extended_hours=True,
                    time_in_force=TimeInForce.DAY,
                    ts=pd.Timestamp.now(EST),
                )
                print(f"Placing BUY order {order}")
                self.trading_client.submit_order(order)
                self.high_water_marks[bar.symbol] = bar.Close

                # trade stream websocket client handles updating the position in DB
                return

        # one trade per symbol for the day
        if not position or position.status == "closed":
            return

        hwm = self.high_water_marks.get(bar.symbol, bar.Close)
        if bar.Close > hwm:
            hwm = self.high_water_marks[bar.symbol] = bar.Close

        # if stop/exit logic
        if bar.Close < self.pct_below_price(hwm, 0.15):
            # market order if possible, otherwise limit 15% below this bar's close for extended hours
            order = None
            kwargs = dict(
                symbol=bar.symbol,
                side=OrderSide.SELL,
                qty=position.qty,
                time_in_force=TimeInForce.DAY,
            )

            if nyse.is_extended_hours():
                order = OrderRequest.limit_order(
                    limit_price=self.pct_below_price(bar.Close, 0.15),
                    extended_hours=True,
                    **kwargs,
                )
            elif nyse.is_market_open():
                order = OrderRequest.market_order(**kwargs)

            if order:
                print(f"Placing SELL order {order}")
                self.trading_client.submit_order(order)

        # FIXME: if missed market, cancel orders
        # elif (
        #     position.open_orders and position.qty == 0
        #     # and some timeout since entry
        # ):
        #     pass


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
        for d in msg:
            if d.get("ev") == "status" and d.get("status") != "success":
                print(f"Error subscribing: {d.get('message')}")
            else:
                print(d)

    async def run(self):
        await asyncio.wait_for(self.connect(), timeout=1)
        await asyncio.wait_for(self.auth(), timeout=1)
        await asyncio.wait_for(self.subscribe(), timeout=1)

        # FIXME can you tee together two iterators? (ie ws connections)
        async for msg in self._connection:
            for bar in json.loads(msg):
                self.strategy.handle_message(bar)


def parse_arg_pairs(s) -> ItemsView[str, str]:
    """
    key: value, key2: value2, ...
    """
    d = {}
    for pair in s.split(","):
        first, second = pair.split(":")
        d[first.strip()] = second.strip()
    return d.items()


def float_pairs(s: str, key_to_upper: bool = True) -> dict[str, float]:
    return {
        key.upper() if key_to_upper else key: float(value)
        for key, value in parse_arg_pairs(s)
    }


def int_pairs(s: str, key_to_upper: bool = True) -> dict[str, int]:
    return {
        key.upper() if key_to_upper else key: int(value)
        for key, value in parse_arg_pairs(s)
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
    )
    parser.add_argument(
        "--symbols",
        type=lambda symbols: [symbol.strip().upper() for symbol in symbols.split(",")],
    )
    parser.add_argument(
        "--limit-prices",
        type=float_pairs,
    )
    parser.add_argument(
        "--quantities",
        type=int_pairs,
    )
    parser.add_argument(
        "--breakout-prices",
        type=float_pairs,
    )

    args = parser.parse_args()

    # FIXME
    # check symbols are tradeable on Alpaca
    # positions bookkeeping

    trading_client = TradingClient(AlpacaTradingClient, paper=False)
    client = WebsocketClient(
        strategy=ManualStrategy(
            dt=args.date,
            symbols=args.symbols,
            trading_client=trading_client,
            breakout_entry_prices=args.breakout_prices,
            limit_prices=args.limit_prices,
            quantities=args.quantities,
        )
    )
    asyncio.run(client.run())
