from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pandas as pd

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient as AlpacaTradingClient
from alpaca.trading.enums import (
    AssetClass,
    AssetExchange,
    AssetStatus,
    OrderSide,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.models import Asset, Order, Position
from alpaca.trading.requests import (
    GetAssetsRequest,
    GetOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
)

from fin_models.config import Config
from fin_models.order_book import OrderRequest
from fin_models.trading.private_alpaca import TradingClient as PrivateTradingClient


class TradingClient:
    """
    Public interface for abstracting away specific broker implementations.
    """

    def __init__(
        self,
        client_class: type[AlpacaTradingClient | PrivateTradingClient],
        paper: bool = True,
    ):
        kwargs = dict(
            api_key=Config[f"ALPACA_API_KEY_{'PAPER' if paper else 'LIVE'}"],
            secret_key=Config[f"ALPACA_API_SECRET_{'PAPER' if paper else 'LIVE'}"],
            paper=paper,
        )
        self.client = client_class(**kwargs)

    def get_asset(
        self,
        symbol: str | None = None,
        *,
        id: UUID | str | None = None,
    ) -> Asset:
        if not (symbol or id):
            raise TypeError("One of `symbol` or `id` must be provided.")
        return self.client.get_asset(id or symbol.upper())

    def get_assets(
        self,
        *,
        status: AssetStatus | None = None,
        asset_class: AssetClass | None = None,
        exchange: AssetExchange | None = None,
        attributes: str | None = None,
    ) -> list[Asset]:
        return self.client.get_all_assets(
            GetAssetsRequest(
                status=status,
                asset_class=asset_class,
                exchange=exchange,
                attributes=attributes,
            )
        )

    def submit_order(self, order_request: OrderRequest) -> Order:
        return self.client.submit_order(request=order_request.order_request)

    def get_order(
        self,
        id: UUID | str | None = None,
        *,
        client_id: str | None = None,
    ) -> Order:
        if id:
            return self.client.get_order_by_id(id)
        elif client_id:
            return self.client.get_order_by_client_id(client_id)

        raise TypeError("One of `id` or `client_id` must be provided.")

    def get_orders(
        self,
        *,
        status: QueryOrderStatus = QueryOrderStatus.OPEN,
        after: datetime | None = None,
        side: OrderSide | None = None,
        symbols: list[str] | None = None,
        limit: int = 500,
    ) -> list[Order]:
        return self.client.get_orders(
            GetOrdersRequest(
                status=status,
                limit=limit,
                after=after,
                direction=side,
                symbols=symbols,
            )
        )

    def cancel_order(self, id: UUID | str) -> Order:
        self.client.cancel_order_by_id(id)
        return self.get_order(id)

    def get_position(self, symbol: str) -> Position | None:
        try:
            return self.client.get_open_position(symbol)
        except APIError:
            return None

    def get_positions(self) -> list[Position]:
        return self.client.get_all_positions()

    def close_position(self, symbol: str) -> Order:
        return self.client.close_position(symbol)
