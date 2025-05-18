from __future__ import annotations

import functools
import pprint
import uuid

from datetime import datetime, timezone

import pandas as pd

from alpaca.trading import (
    AssetClass,
    AssetExchange,
    AssetStatus,
    LimitOrderRequest,
    MarketOrderRequest,
    OrderStatus,
    StopLimitOrderRequest,
    StopLossRequest,
    StopOrderRequest,
    TakeProfitRequest,
    TrailingStopOrderRequest,
)
from alpaca.trading.enums import (
    OrderClass,
    OrderSide,
    OrderType,
    PositionIntent,
    TimeInForce,
)
from alpaca.trading.models import Order
from alpaca.trading.requests import (
    OrderRequest as AlpacaOrderRequest,
)
from pydantic import BaseModel

from fin_models.order_book import OrderBook, OrderRequest


def unwrap_pydantic(fn=None):
    if fn is None:
        return functools.partial(unwrap_pydantic)

    def wrapper(self: object, pydantic_model: BaseModel):
        return fn(self, **pydantic_model.model_dump())

    return wrapper


class TradingClient:
    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
        paper: bool = True,
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper

    def get_asset(self, symbol: str):
        pass

    @unwrap_pydantic
    def get_all_assets(
        self,
        *,
        asset_class: AssetClass | None = None,
        exchange: AssetExchange | None = None,
        status: AssetStatus | None = None,
        attributes: str | None = None,
    ):
        pass

    def submit_order(self, request: AlpacaOrderRequest) -> Order:
        order_request = OrderRequest[request.client_order_id]
        match request:
            case MarketOrderRequest():
                pass
            case LimitOrderRequest():
                pass
            case StopOrderRequest():
                pass
            case StopLimitOrderRequest():
                pass
            case TrailingStopOrderRequest():
                pass

        now = datetime.now(tz=timezone.utc)
        return Order(
            id=str(uuid.uuid4()),
            asset_id=str(uuid.uuid4()),
            client_order_id=request.client_order_id,
            created_at=now,
            updated_at=now,
            submitted_at=now,
            type=request.type,
            side=request.side,
            symbol=request.symbol,
            qty=request.qty,
            asset_class=AssetClass.US_EQUITY,
            order_class=OrderClass.SIMPLE,
            time_in_force=request.time_in_force,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            extended_hours=request.extended_hours,
            status=OrderStatus.ACCEPTED,
            filled_at=now,  # FIXME
            filled_qty=request.qty,
            filled_avg_price=1.0,  # FIXME
            # deprecated
            order_type=request.type,
        )
