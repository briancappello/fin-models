from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base


if TYPE_CHECKING:
    from .asset import Asset
    from .watchlist import Watchlist


class WatchlistAsset(Base):
    asset_id: Mapped[int] = mapped_column(
        ForeignKey("asset.id"),
        primary_key=True,
    )
    asset: Mapped["Asset"] = relationship(back_populates="asset_watchlists")

    watchlist_id: Mapped[int] = mapped_column(
        ForeignKey("watchlist.id"),
        primary_key=True,
    )
    watchlist: Mapped["Watchlist"] = relationship(back_populates="watchlist_assets")
