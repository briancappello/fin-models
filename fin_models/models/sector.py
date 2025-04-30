from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .equity import Equity
    from .industry import Industry


class Sector(Base):
    class Meta:
        repr = ("id", "name")

    id: Mapped[pk]
    name: Mapped[str] = mapped_column(String(32), index=True, unique=True)

    equities: Mapped[list["Equity"]] = relationship(back_populates="sector")

    industries: Mapped[list["Industry"]] = relationship(back_populates="sector")
