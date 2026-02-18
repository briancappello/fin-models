from __future__ import annotations

import argparse
import json

from alpaca.trading.enums import OrderStatus
from alpaca.trading.models import TradeUpdate
from alpaca.trading.stream import TradingStream as BaseTradingStream

from fin_models.config import Config
from fin_models.db import Session
from fin_models.models import Order, Position


HOST = "localhost"
PORT = 8766


class TradingStream(BaseTradingStream):
    async def _auth(self):
        await self._ws.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self._api_key,
                    "secret": self._secret_key,
                }
            )
        )
        r = await self._ws.recv()
        msg = json.loads(r)
        if msg.get("data").get("status") != "authorized":
            raise ValueError("failed to authenticate")
        print(f"Authorized {msg}")


async def handler(msg: TradeUpdate):
    print(type(msg), msg)
    session = Session()

    position: Position
    if not (
        position := session.query(Position)
        .filter_by(ticker=msg.order.symbol, status="open")
        .one_or_none()
    ):
        position = Position(
            ticker=msg.order.symbol,
            status="open",
            side="long" if msg.order.side == "buy" else "short",
        )

    position.qty = msg.position_qty
    if position.qty == 0.0 and msg.order.status == OrderStatus.FILLED:
        position.status = "closed"

    session.add(position)

    order: Order
    if not (
        order := session.query(Order)
        .filter_by(client_order_id=msg.order.client_order_id)
        .one_or_none()
    ):
        order = Order(client_order_id=msg.order.client_order_id)

    order_kwargs = dict(
        vendor_order_id=msg.order.id,
        position=position,
        ticker=msg.order.symbol,
        vendor_asset_id=msg.order.asset_id,
        side=msg.order.side,
        qty=msg.order.qty,
        type=msg.order.type,
        time_in_force=msg.order.time_in_force,
        extended_hours=msg.order.extended_hours or False,
        limit_price=msg.order.limit_price,
        stop_price=msg.order.stop_price,
        status=msg.order.status,
        filled_at=msg.order.filled_at,
        filled_qty=msg.order.filled_qty,
        filled_avg_price=msg.order.filled_avg_price,
        submitted_at=msg.order.submitted_at,
        created_at=msg.order.created_at,
        updated_at=msg.order.updated_at,
    )

    order.update(**order_kwargs)
    session.add(order)
    session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="local")

    args = parser.parse_args()
    assert args.host in {"live", "paper", "local"}

    api_key = {
        "live": Config.ALPACA_API_KEY_LIVE,
        "paper": Config.ALPACA_API_KEY_PAPER,
        "local": "",
    }[args.host]

    secret_key = {
        "live": Config.ALPACA_API_SECRET_LIVE,
        "paper": Config.ALPACA_API_SECRET_PAPER,
        "local": "",
    }[args.host]

    kwargs = dict(
        api_key=api_key,
        secret_key=secret_key,
        paper=True if args.host == "paper" else False,
    )
    if args.host == "local":
        kwargs["url_override"] = f"ws://{HOST}:{PORT}"

    client = TradingStream(**kwargs)
    client.subscribe_trade_updates(handler)
    client.run()
