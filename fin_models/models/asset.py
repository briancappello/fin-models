from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from .asset_data_vendor import AssetDataVendor


if TYPE_CHECKING:
    from .country import Country
    from .currency import Currency
    from .data_vendor import DataVendor
    from .exchange import Exchange
    from .market import Market
    from .watchlist_asset import WatchlistAsset


class Asset(Base):
    """
    Base class for tradable assets. Should not be used directly.
    """

    class Meta:
        repr = ("id", "type", "ticker")

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "Asset",
    }

    id: Mapped[pk]
    type: Mapped[str]  # polymorphic discriminator column

    # canonical ticker
    ticker: Mapped[str] = mapped_column(index=True, unique=True)

    asset_data_vendors: Mapped[list["AssetDataVendor"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
    )
    data_vendors: Mapped[list["DataVendor"]] = association_proxy(
        "asset_data_vendors",
        "data_vendor",
        creator=lambda data_vendor: AssetDataVendor(data_vendor=data_vendor),
    )

    asset_watchlists: Mapped[list["WatchlistAsset"]] = relationship(
        back_populates="asset"
    )

    market_id: Mapped[int] = mapped_column(ForeignKey("market.id"))
    market: Mapped["Market"] = relationship(back_populates="assets")

    country: Mapped["Country"] = association_proxy("market", "country")
    currency: Mapped["Currency"] = association_proxy("market", "currency")
    exchange: Mapped["Exchange"] = association_proxy("market", "exchange")
