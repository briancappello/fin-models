#!/usr/bin/env python
from __future__ import annotations

import asyncio
import json

from argparse import ArgumentParser
from datetime import date

import pandas as pd

from websockets import ServerConnection
from websockets.asyncio.server import serve as ws_serve

from fin_models.enums import Freq
from fin_models.services import nyse, store


WS_HOST = "localhost"
WS_PORT = 8765


class DataServer:
    def __init__(self, start_date: date | None = None):
        self.start_date = start_date if start_date else nyse.get_latest_trading_date()
        self._authed = False
        self._subscriptions = set()
        self._symbols = set()
        self.dataframes: dict[str, pd.DataFrame] = {}

    async def __call__(self, ws: ServerConnection):
        async def send(m):
            await ws.send(json.dumps([m]))

        def cache_data(ticker: str, freq: Freq, start_date: date | None = None):
            if ticker in self.dataframes:
                return

            try:
                self.dataframes[ticker] = store.get(ticker, freq=freq).loc[
                    (start_date or self.start_date).isoformat()
                ]
            except KeyError:
                pass

        await send(
            dict(ev="status", status="connected", message="Connected Successfully")
        )

        async for raw_msg in ws:
            msg = json.loads(raw_msg)
            if msg["action"] == "auth":
                await send(
                    {
                        "ev": "status",
                        "status": "auth_success",
                        "message": "authenticated",
                    }
                )

            elif msg["action"] == "subscribe":
                self._subscriptions = set(msg["params"].split(","))
                if start_date := msg.get("date"):
                    self.start_date = date.fromisoformat(start_date)

                await send(
                    {
                        "ev": "status",
                        "status": "success",
                        "message": f"subscribed to: {','.join(self._subscriptions)}",
                    }
                )

                print(f"Loading data for {self._subscriptions}...\n")

                for sub in self._subscriptions:
                    polygon_tf, symbol = sub.split(".")
                    if polygon_tf != "AM":
                        continue

                    if symbol == "*":
                        self._symbols |= set(store.get_symbols(freq=Freq.min_1))
                    else:
                        self._symbols.add(symbol)

                print("Finished loading data. Serving bars...")

                for ts in pd.date_range(
                    start=f"{self.start_date.isoformat()} 04:00",
                    end=f"{self.start_date.isoformat()} 20:00",
                    tz="America/New_York",
                    inclusive="both",
                    freq="min",
                ):
                    for symbol in self._symbols:
                        if symbol not in self.dataframes:
                            cache_data(symbol, Freq.min_1)

                        df = self.dataframes.get(symbol)
                        if df is None or df.empty:
                            continue

                        if ts in df.index:
                            bar = df.loc[ts]
                            await send(
                                dict(
                                    ev="AM",
                                    sym=symbol,
                                    v=int(bar.Volume),
                                    av=int(
                                        df.loc[self.start_date.isoformat()]
                                        .loc[:ts]
                                        .Volume.sum()
                                    ),
                                    op=None,  # today's open
                                    vw=None,  # bar's volume weighted avg price
                                    o=bar.Open,
                                    h=bar.High,
                                    l=bar.Low,
                                    c=bar.Close,
                                    a=None,  # today's volume weighted avg price
                                    z=None,  # avg trade size for this bar
                                    s=int(
                                        ts.timestamp() * 1000
                                    ),  # start of bar in unix milliseconds
                                    e=int(
                                        (ts.timestamp() + 60) * 1000
                                    ),  # end of bar in unix milliseconds
                                )
                            )

                    # give the client time to process messages
                    if len(self._symbols) > 2_000:
                        await asyncio.sleep(len(self._symbols) / 10_000)

                print("Done.")
                break
        await ws.close()


async def main():
    parser = ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    args = parser.parse_args()

    server = DataServer(start_date=args.date)
    async with ws_serve(server, host=WS_HOST, port=WS_PORT) as ws_server:
        print(f"Serving forever on ws://{WS_HOST}:{WS_PORT}")
        await ws_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
