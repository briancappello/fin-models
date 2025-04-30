from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Text
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .asset import Asset
from .equity_index import EquityIndex


if TYPE_CHECKING:
    from .index import Index
    from .industry import Industry
    from .sector import Sector


class Equity(Asset):
    class Meta:
        repr = ("id", "ticker")

    __mapper_args__ = {
        "polymorphic_identity": "Equity",
    }

    id: Mapped[int] = mapped_column(ForeignKey("asset.id"), primary_key=True)
    company_name: Mapped[str] = mapped_column(index=True)
    company_description: Mapped[str | None] = mapped_column(Text)

    equity_indexes: Mapped[list["EquityIndex"]] = relationship(
        back_populates="equity",
        cascade="all, delete-orphan",
    )
    indexes: Mapped[list["Index"]] = association_proxy(
        "equity_indexes",
        "index",
        creator=lambda equity: EquityIndex(equity=equity),
    )

    sector_id: Mapped[int | None] = mapped_column(ForeignKey("sector.id"))
    sector: Mapped["Sector"] = relationship("Sector", back_populates="equities")

    industry_id: Mapped[int | None] = mapped_column(ForeignKey("industry.id"))
    industry: Mapped["Industry"] = relationship(back_populates="equities")

    # active = mapped_column(Boolean(name='active'), default=True)  # active == listed & trading
