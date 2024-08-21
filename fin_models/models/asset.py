from __future__ import annotations

from .. import db
from ..enums import AssetType
from .asset_data_vendor import AssetDataVendor


class Asset(db.Model):
    """
    Base class for tradable assets. Should not be used directly.
    """

    class Meta:
        repr = ("id", "type", "ticker")

    type = db.Column(db.Enum(AssetType))  # polymorphic discriminator column
    __mapper_args__ = {
        "polymorphic_on": type,
        "polymorphic_identity": AssetType.Asset,
    }

    # canonical ticker
    ticker = db.Column(db.String(16), index=True, unique=True)

    asset_data_vendors = db.relationship(
        "AssetDataVendor", back_populates="asset", cascade="all, delete-orphan"
    )
    data_vendors = db.association_proxy(
        "asset_data_vendors",
        "data_vendor",
        creator=lambda data_vendor: AssetDataVendor(data_vendor=data_vendor),
    )

    asset_watchlists = db.relationship("WatchlistAsset", back_populates="asset")

    market_id = db.foreign_key("Market")
    market = db.relationship("Market", back_populates="assets")

    country = db.association_proxy("market", "country")
    currency = db.association_proxy("market", "currency")
    exchange = db.association_proxy("market", "exchange")
