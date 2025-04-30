from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base


if TYPE_CHECKING:
    from .equity import Equity
    from .index import Index


class EquityIndex(Base):
    """Join table between Equity and Index"""

    class Meta:
        repr = ("equity", "index")

    equity_id: Mapped[int] = mapped_column(ForeignKey("equity.id"), primary_key=True)
    equity: Mapped["Equity"] = relationship(back_populates="equity_indexes")

    index_id: Mapped[int] = mapped_column(ForeignKey("index.id"), primary_key=True)
    index: Mapped["Index"] = relationship(back_populates="index_equities")
