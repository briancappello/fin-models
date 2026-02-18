from __future__ import annotations

import functools

from datetime import datetime
from typing import Callable, Literal
from zoneinfo import ZoneInfo

import click

from alpaca.trading.client import TradingClient as AlpacaTradingClient
from alpaca.trading.enums import (
    OrderSide,
    TimeInForce,
)
from alpaca.trading.models import Order
from tabulate import tabulate

from fin_models.order_book import OrderRequest
from fin_models.trading.trading_client import TradingClient

from .groups import trade


get_trading_client: Callable[[], TradingClient] = functools.partial(
    TradingClient, client_class=AlpacaTradingClient
)


def print_orders(orders: list[Order]):
    table = [
        (order.side.name, order.type.value, order.symbol, order.qty, order.filled_qty)
        for order in orders
    ]
    click.echo(tabulate(table, headers=["Side", "Type", "Symbol", "Qty", "Filled Qty"]))


@trade.command()
@click.argument("quantity", type=int)
@click.argument("symbol", type=str)
@click.option("--limit", type=float, default=None)
@click.option(
    "--tif",
    type=click.Choice(["day", "gtc", "open", "close", "ioc", "fok"]),
    default="day",
)
@click.option("--extended", is_flag=True, default=False)
@click.option("--paper", is_flag=True, default=False)
def buy(
    quantity: int,
    symbol: str,
    limit: float | None = None,
    tif: str = "day",
    extended: bool = False,
    paper: bool = False,
):
    """Place a buy order"""
    client: TradingClient = get_trading_client(paper=paper)

    order_kwargs = dict(
        side=OrderSide.BUY,
        qty=quantity,
        symbol=symbol,
        time_in_force=TimeInForce({"open": "opg", "close": "cls"}.get(tif, tif)),
    )
    if limit:
        order_request = OrderRequest.limit_order(
            limit_price=limit,
            extended_hours=extended,
            **order_kwargs,
        )
    else:
        order_request = OrderRequest.market_order(**order_kwargs)

    order: Order = client.submit_order(order_request)
    click.echo(order.client_order_id)


@trade.command()
@click.argument("quantity", type=int)
@click.argument("symbol", type=str)
@click.option("--limit", type=float, default=None)
@click.option(
    "--tif",
    type=click.Choice(["day", "gtc", "open", "close", "ioc", "fok"]),
    default="day",
)
@click.option("--extended", is_flag=True, default=False)
@click.option("--paper", is_flag=True, default=False)
def sell(
    quantity: int,
    symbol: str,
    limit: float | None = None,
    tif: str = "day",
    extended: bool = False,
    paper: bool = False,
):
    """Place a sell order"""
    client: TradingClient = get_trading_client(paper=paper)

    order_kwargs = dict(
        side=OrderSide.SELL,
        qty=quantity,
        symbol=symbol,
        time_in_force=TimeInForce({"open": "opg", "close": "cls"}.get(tif, tif)),
    )
    if limit:
        order_request = OrderRequest.limit_order(
            limit_price=limit,
            extended_hours=extended,
            **order_kwargs,
        )
    else:
        order_request = OrderRequest.market_order(**order_kwargs)

    order: Order = client.submit_order(order_request)
    click.echo(order.client_order_id)


@trade.command()
@click.argument("symbol", default=None)
@click.option("--status", type=click.Choice(["open", "closed", "all"]), default="all")
@click.option("--id", type=str, default=None)
@click.option("--client-id", type=str, default=None)
@click.option("--paper", is_flag=True, default=False)
def status(
    symbol: str | None = None,
    status: str = "all",
    id: str | None = None,
    client_id: str | None = None,
    paper: bool = False,
):
    """Check order status"""
    client: TradingClient = get_trading_client(paper=paper)

    if id or client_id:
        orders: list[Order] = [client.get_order(id=id, client_id=client_id)]
    else:
        orders: list[Order] = client.get_orders(
            status=status, symbols=[symbol] if symbol else None
        )
    print_orders(orders)


@trade.command()
@click.argument("symbol", default=None)
@click.option("--paper", is_flag=True, default=False)
def positions(symbol: str | None = None, paper: bool = False):
    client: TradingClient = get_trading_client(paper=paper)
    positions = client.get_positions(symbol)
    print(type(positions[0]))
