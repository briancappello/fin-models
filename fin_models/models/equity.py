from .. import db
from ..enums import AssetType
from .asset import Asset
from .equity_index import EquityIndex


class Equity(Asset):
    class Meta:
        repr = ("id", "ticker")

    __mapper_args__ = {
        "polymorphic_identity": AssetType.Equity,
    }

    id = db.foreign_key("Asset", primary_key=True)
    company_name = db.Column(db.String, index=True)
    company_description = db.Column(db.Text, nullable=True)

    equity_indexes = db.relationship(
        "EquityIndex", back_populates="equity", cascade="all, delete-orphan"
    )
    indexes = db.association_proxy(
        "equity_indexes", "index", creator=lambda equity: EquityIndex(equity=equity)
    )

    sector_id = db.foreign_key("Sector", nullable=True)
    sector = db.relationship("Sector", back_populates="equities")

    industry_id = db.foreign_key("Industry", nullable=True)
    industry = db.relationship("Industry", back_populates="equities")

    # active = db.Column(Boolean(name='active'), default=True)  # active == listed & trading
