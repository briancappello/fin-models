from __future__ import annotations

from .. import db


class Order(db.Model):
    class Meta:
        repr = ("id", "ticker", "type", "side", "qty")
        created_at = None
        updated_at = None

    client_order_id = db.Column(db.String, unique=True, index=True)
    vendor_order_id = db.Column(db.String, unique=True, index=True)

    ticker = db.Column(db.String(16))
    vendor_asset_id = db.Column(db.String(64))

    side = db.Column(db.String(8))  # buy or sell
    qty = db.Column(db.Float)
    type = db.Column(db.String(16))  # market, limit, stop, stop_limit, trailing_stop
    time_in_force = db.Column(db.String(8))  # day, gtc, opg, cls, ioc, fok
    extended_hours = db.Column(db.Boolean)
    limit_price = db.Column(db.Float, nullable=True)
    stop_price = db.Column(db.Float, nullable=True)

    # https://alpaca.markets/docs/api-references/broker-api/trading/orders/#order-status
    status = db.Column(db.String(32))  # new, filled, partially_filled, expired, canceled

    filled_at = db.Column(db.DateTime, nullable=True)
    filled_qty = db.Column(db.Float, nullable=True)
    filled_avg_price = db.Column(db.Float, nullable=True)

    created_at = db.Column(db.DateTime)
    submitted_at = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime)

    position_id = db.foreign_key("Position", nullable=True)
    position = db.relationship("Position", back_populates="orders")
