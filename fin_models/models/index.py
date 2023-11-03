from .. import db
from .equity_index import EquityIndex


class Index(db.Model):
    class Meta:
        repr = ("id", "ticker", "name")

    ticker = db.Column(db.String(16), index=True, unique=True)
    name = db.Column(db.String(64), index=True, unique=True)

    index_data_vendors = db.relationship("IndexDataVendor", back_populates="index")

    index_equities = db.relationship(
        "EquityIndex", back_populates="index", cascade="all, delete-orphan"
    )
    equities = db.association_proxy(
        "index_equities", "equity", creator=lambda equity: EquityIndex(equity=equity)
    )
