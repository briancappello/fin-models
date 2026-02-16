#!/usr/bin/env python
from __future__ import annotations

import asyncio
import json

from argparse import ArgumentParser
from datetime import date

import pandas as pd

from websockets import ServerConnection
from websockets.asyncio.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed

from fin_models.enums import Freq
from fin_models.services import nyse, store


WS_HOST = "localhost"
WS_PORT = 8765


class DataServer:
    def __init__(self, start_date: date | None = None):
        self.default_start_date = start_date if start_date else nyse.get_latest_trading_date()
        self.dataframes: dict[str, pd.DataFrame] = {}

    async def __call__(self, ws: ServerConnection):
        client_subscriptions = set()
        client_symbols = set()
        client_start_date = self.default_start_date

        async def send(m):
            await ws.send(json.dumps([m]))

        def cache_data(ticker: str, freq: Freq):
            if ticker in self.dataframes:
                return

            try:
                self.dataframes[ticker] = store.get(ticker, freq=freq)
            except KeyError:
                pass

        try:
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
                    client_subscriptions = set(msg["params"].split(","))
                    if start_date_str := msg.get("date"):
                        client_start_date = date.fromisoformat(start_date_str)

                    await send(
                        {
                            "ev": "status",
                            "status": "success",
                            "message": f"subscribed to: {','.join(client_subscriptions)}",
                        }
                    )

                    print(f"Loading data for {client_subscriptions}...\n")

                    for sub in client_subscriptions:
                        parts = sub.split(".")
                        if len(parts) != 2:
                            continue
                        polygon_tf, symbol = parts
                        if polygon_tf != "AM":
                            continue

                        if symbol == "*":
                            client_symbols |= set(store.get_symbols(freq=Freq.min_1))
                        else:
                            client_symbols.add(symbol)

                    print("Finished loading data. Serving bars...")

                    for ts in pd.date_range(
                        start=f"{client_start_date.isoformat()} 04:00",
                        end=f"{client_start_date.isoformat()} 20:00",
                        tz="America/New_York",
                        inclusive="both",
                        freq="min",
                    ):
                        for symbol in client_symbols:
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
                                            df.loc[client_start_date.isoformat()]
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
                        if len(client_symbols) > 2_000:
                            await asyncio.sleep(len(client_symbols) / 10_000)

                        await asyncio.sleep(0.5)

                    print("Done.")
                    break
        except ConnectionClosed:
            print(f"Client {ws.remote_address} disconnected")
        finally:
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
