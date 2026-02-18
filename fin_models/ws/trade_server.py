#!/usr/bin/env python
from __future__ import annotations

import asyncio
import json

from argparse import ArgumentParser
from datetime import date

import pandas as pd

from websockets.asyncio.server import serve as ws_serve

from fin_models.enums import Freq
from fin_models.services import nyse, store


WS_HOST = "localhost"
WS_PORT = 8766


class TradeServer:
    def __init__(self, start_date: date | None = None):
        self.start_date = start_date if start_date else nyse.get_latest_trading_date()
        self._authed = False
        self._subscriptions = set()

    async def __call__(self, ws):
        async for raw_msg in ws:
            msg = json.loads(raw_msg)
            if msg["action"] in {"auth", "authenticate"}:
                msg = {
                    "stream": "authorization",
                    "data": {
                        "action": "authenticate",
                        "status": "authorized",
                    },
                }
                await ws.send(json.dumps(msg))

            elif msg["action"] == "listen":
                self._subscriptions = set(msg["data"]["streams"])
                msg = {
                    "stream": "listening",
                    "data": {
                        "streams": list(self._subscriptions),
                    },
                }
                await ws.send(json.dumps(msg))

                print(f"subscriptions={self._subscriptions}")
                example_trade_event = {
                    "stream": "trade_updates",
                    "data": {
                        "event": "fill",
                        "execution_id": "2f63ea93-423d-4169-b3f6-3fdafc10c418",
                        "order": {
                            "asset_class": "us_equity",
                            "asset_id": "1cf35270-99ee-44e2-a77f-6fab902c7f80",
                            "cancel_requested_at": None,
                            "canceled_at": None,
                            "client_order_id": "4642fd68-d59a-47d7-a9ac-e22f536828d1",
                            "created_at": "2022-04-19T13:45:04.981350886-04:00",
                            "expired_at": None,
                            "extended_hours": False,
                            "failed_at": None,
                            "filled_at": "2022-04-19T17:45:05.024916716Z",
                            "filled_avg_price": "105.8988475",
                            "filled_qty": "100",
                            "hwm": None,
                            "id": "a5be8f5e-fdfa-41f5-a644-7a74fe947a8f",
                            "legs": None,
                            "limit_price": None,
                            "notional": None,
                            "order_class": "",
                            "order_type": "market",
                            "qty": "100",
                            "replaced_at": None,
                            "replaced_by": None,
                            "replaces": None,
                            "side": "buy",
                            "status": "filled",
                            "stop_price": None,
                            "submitted_at": "2022-04-19T13:45:04.980944666-04:00",
                            "symbol": "AMD",
                            "time_in_force": "gtc",
                            "trail_percent": None,
                            "trail_price": None,
                            "type": "market",
                            "updated_at": "2022-04-19T13:45:05.027690731-04:00",
                        },
                        "position_qty": "0",
                        "price": "105.8988475",
                        "qty": "100",
                        "timestamp": "2022-04-19T17:45:05.024916716Z",
                    },
                }
                await ws.send(json.dumps(example_trade_event))


async def main():
    parser = ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args()

    server = TradeServer(start_date=args.date)
    async with ws_serve(server, host=WS_HOST, port=WS_PORT) as ws_server:
        print(f"Serving forever on ws://{WS_HOST}:{WS_PORT}")
        await ws_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
