from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca.trading import (
    LimitOrderRequest,
    MarketOrderRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
    TrailingStopOrderRequest,
)
from alpaca.trading.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from alpaca.trading.requests import (
    OrderRequest as AlpacaOrderRequest,
)

from fin_models.date_utils import EST, to_ts


def to_enum(enum_class, value):
    try:
        return enum_class[value]
    except KeyError:
        return enum_class(value)


class _OrderRequestMetaclass(type):
    def __getitem__(cls, client_id: str):
        return cls._order_requests[client_id]


class OrderRequest(metaclass=_OrderRequestMetaclass):
    _order_requests = {}

    @property
    def side(self) -> OrderSide:
        return self.order_request.side

    @property
    def qty(self) -> float:
        return self.order_request.qty

    @property
    def symbol(self) -> str:
        return self.order_request.symbol

    @property
    def type(self) -> OrderType:
        return self.order_request.type

    @property
    def time_in_force(self) -> TimeInForce:
        return self.order_request.time_in_force

    @property
    def extended_hours(self) -> bool:
        return self.order_request.extended_hours

    @property
    def limit_price(self) -> float | None:
        if self.type not in {OrderType.LIMIT, OrderType.STOP_LIMIT}:
            return None
        return self.order_request.limit_price

    @property
    def stop_price(self) -> float | None:
        if self.type not in {OrderType.STOP, OrderType.STOP_LIMIT}:
            return None
        return self.order_request.stop_price

    @property
    def client_order_id(self):
        return "~".join(
            [
                self.order_request.side.value,
                str(self.order_request.qty),
                self.order_request.symbol,
                self.order_request.type.value,
                self.ts.isoformat(),
            ]
        )

    # PUBLIC CONSTRUCTORS
    # -------------------

    @classmethod
    def market_order(
        cls,
        side: OrderSide | str,
        qty: float,
        symbol: str,
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        ts: pd.Timestamp | datetime | None = None,
    ) -> OrderRequest:
        request = MarketOrderRequest(
            side=to_enum(OrderSide, side),
            qty=qty,
            symbol=symbol,
            extended_hours=False,
            time_in_force=to_enum(TimeInForce, time_in_force),
        )
        return cls._add(request, ts)

    @classmethod
    def limit_order(
        cls,
        side: OrderSide | str,
        qty: float,
        symbol: str,
        limit_price: float,
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        extended_hours: bool = False,
        ts: pd.Timestamp | datetime | None = None,
    ) -> OrderRequest:
        request = LimitOrderRequest(
            side=to_enum(OrderSide, side),
            qty=qty,
            symbol=symbol,
            limit_price=limit_price,
            time_in_force=to_enum(TimeInForce, time_in_force),
            extended_hours=extended_hours,
        )
        return cls._add(request, ts)

    @classmethod
    def stop_order(
        cls,
        side: OrderSide | str,
        qty: float,
        symbol: str,
        stop_price: float,
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        ts: pd.Timestamp | datetime | None = None,
    ) -> OrderRequest:
        request = StopOrderRequest(
            side=to_enum(OrderSide, side),
            qty=qty,
            symbol=symbol,
            stop_price=stop_price,
            time_in_force=to_enum(TimeInForce, time_in_force),
            extended_hours=False,
        )
        return cls._add(request, ts)

    @classmethod
    def stop_limit_order(
        cls,
        side: OrderSide | str,
        qty: float,
        symbol: str,
        stop_price: float,
        limit_price: float,
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        extended_hours: bool = False,
        ts: pd.Timestamp | datetime | None = None,
    ) -> OrderRequest:
        request = StopLimitOrderRequest(
            side=to_enum(OrderSide, side),
            qty=qty,
            symbol=symbol,
            stop_price=stop_price,
            limit_price=limit_price,
            time_in_force=to_enum(TimeInForce, time_in_force),
            extended_hours=extended_hours,
        )
        return cls._add(request, ts)

    @classmethod
    def trailing_stop_order(
        cls,
        side: OrderSide | str,
        qty: float,
        symbol: str,
        trail_price: float | None = None,
        trail_percent: float | None = None,
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        ts: pd.Timestamp | datetime | None = None,
    ) -> OrderRequest:
        request = TrailingStopOrderRequest(
            side=to_enum(OrderSide, side),
            qty=qty,
            symbol=symbol,
            trail_price=trail_price,
            trail_percent=trail_percent,
            time_in_force=to_enum(TimeInForce, time_in_force),
            extended_hours=False,
        )
        return cls._add(request, ts)

    # PRIVATE CONSTRUCTORS
    # --------------------

    @classmethod
    def _add(
        cls,
        order_request: AlpacaOrderRequest,
        ts: pd.Timestamp | datetime | str | None = None,
    ) -> OrderRequest:
        instance = cls(order_request, ts)
        order_request.client_order_id = instance.client_order_id
        return instance

    def __init__(
        self,
        order_request: AlpacaOrderRequest,
        ts: pd.Timestamp | datetime | str | None = None,
    ):
        """
        Private constructor; use one of the following for the public interface:
            - OrderRequest.market_order
            - OrderRequest.limit_order
            - OrderRequest.stop_limit_order
        """
        self.order_request = order_request
        self.ts = to_ts(ts).astimezone(EST)
        OrderRequest._order_requests[self.client_order_id] = self

    def __repr__(self):
        return f"OrderRequest(client_id={self.client_order_id!r})"


class OrderBook:
    def __init__(self):
        self.orders = {}
