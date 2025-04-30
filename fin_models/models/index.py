from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from .equity_index import EquityIndex


if TYPE_CHECKING:
    from .equity import Equity
    from .index_data_vendor import IndexDataVendor


class Index(Base):
    class Meta:
        repr = ("id", "ticker", "name")

    id: Mapped[pk]
    ticker: Mapped[int] = mapped_column(String(16), index=True, unique=True)
    name: Mapped[str] = mapped_column(String(64), index=True, unique=True)

    index_data_vendors: Mapped[list["IndexDataVendor"]] = relationship(
        back_populates="index",
    )

    index_equities: Mapped[list["EquityIndex"]] = relationship(
        back_populates="index",
        cascade="all, delete-orphan",
    )
    equities: Mapped[list["Equity"]] = association_proxy(
        "index_equities",
        "equity",
        creator=lambda equity: EquityIndex(equity=equity),
    )
