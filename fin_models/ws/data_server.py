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
WS_PORT = 8765


class DataServer:
    def __init__(self, start_date: date | None = None):
        self.start_date = start_date if start_date else nyse.get_latest_trading_date()
        self._authed = False
        self._subscriptions = set()

    async def __call__(self, ws):
        msg = [dict(ev="status", status="connected", message="Connected Successfully")]
        await ws.send(json.dumps(msg))

        async for raw_msg in ws:
            msg = json.loads(raw_msg)
            if msg["action"] == "auth":
                msg = [
                    {"ev": "status", "status": "auth_success", "message": "authenticated"}
                ]
                await ws.send(json.dumps(msg))

            elif msg["action"] == "subscribe":
                self._subscriptions = set(msg["params"].split(","))
                msg = [
                    {
                        "ev": "status",
                        "status": "success",
                        "message": f'subscribed to: {",".join(self._subscriptions)}',
                    }
                ]
                await ws.send(json.dumps(msg))

                print(self._subscriptions)

                dataframes = {}
                for sub in self._subscriptions:
                    polygon_tf, symbol = sub.split(".")
                    if polygon_tf != "AM":
                        continue

                    dataframes[symbol] = store.get(symbol, freq=Freq.min_1).loc[
                        self.start_date.isoformat()
                    ]

                for ts in pd.date_range(
                    start=f"{self.start_date.isoformat()} 04:00",
                    end=f"{self.start_date.isoformat()} 20:00",
                    tz="America/New_York",
                    inclusive="both",
                    freq="min",
                ):
                    for symbol, df in dataframes.items():
                        # FIXME
                        # await asyncio.sleep(1)

                        if ts in df.index:
                            bar = df.loc[ts]
                            msg = dict(
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
                            await ws.send(json.dumps([msg]))


async def main():
    parser = ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args()

    server = DataServer(start_date=args.date)
    async with ws_serve(server, host=WS_HOST, port=WS_PORT) as ws_server:
        print(f"Serving forever on ws://{WS_HOST}:{WS_PORT}")
        await ws_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
