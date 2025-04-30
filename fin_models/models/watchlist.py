from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from .watchlist_asset import WatchlistAsset


if TYPE_CHECKING:
    from .asset import Asset


class Watchlist(Base):
    class Meta:
        repr = ("id", "name")

    id: Mapped[pk]
    name: Mapped[str] = mapped_column(unique=True, index=True)

    # user_id = db.foreign_key('User')
    # user = relationship('User', back_populates='watchlists')

    watchlist_assets: Mapped[list["WatchlistAsset"]] = relationship(
        back_populates="watchlist",
        cascade="all, delete-orphan",
    )
    assets: Mapped[list["Asset"]] = association_proxy(
        "watchlist_assets",
        "asset",
        creator=lambda asset: WatchlistAsset(asset=asset),
    )
