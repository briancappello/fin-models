from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .equity import Equity
    from .sector import Sector


class Industry(Base):
    class Meta:
        repr = ("id", "name", "sector")

    id: Mapped[pk]
    name: Mapped[str] = mapped_column(String(64), index=True, unique=True)

    equities: Mapped[list["Equity"]] = relationship(back_populates="industry")

    sector_id: Mapped[int] = mapped_column(ForeignKey("sector.id"), nullable=True)
    sector: Mapped["Sector"] = relationship("Sector", back_populates="industries")
