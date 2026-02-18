from __future__ import annotations

from sqlalchemy.ext.hybrid import hybrid_property

from .. import db


class Position(db.Model):
    class Meta:
        repr = ("id", "ticker", "side", "qty")

    ticker = db.Column(db.String(16))
    side = db.Column(db.String(8))  # long/short
    qty = db.Column(db.Float)
    # avg_entry_price = db.Column(db.Float, nullable=True)  # FIXME property from all orders
    # cost_basis = db.Column(db.Float)  # FIXME property from all orders
    status = db.Column(db.String(8), default="open")  # open/closed

    # FIXME profit/loss tracking

    orders = db.relationship("Order", back_populates="position")

    @hybrid_property
    def cost_basis(self):
        cost_basis = 0
        for order in self.orders:
            if order.status in {"filled", "partially_filled"}:
                cost_basis += order.filled_qty * order.filled_avg_price
        return cost_basis

    @hybrid_property
    def avg_entry_price(self):
        return self.cost_basis / self.qty

    @hybrid_property
    def open_orders(self):
        return [
            order
            for order in self.orders
            if order.status
            in {
                "new",
                "accepted",
                "pending_new",
                "accepted_for_bidding",
            }
        ]
