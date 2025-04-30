from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .market import Market


class Exchange(Base):
    class Meta:
        repr = ("id", "abbrev", "name")

    id: Mapped[pk]
    abbrev: Mapped[str] = mapped_column(String(16), unique=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)

    markets: Mapped[list["Market"]] = relationship(back_populates="exchange")
