from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .asset import Asset
    from .country import Country
    from .currency import Currency
    from .exchange import Exchange


class Market(Base):
    class Meta:
        repr = ("id", "abbrev", "name")

    id: Mapped[pk]
    abbrev: Mapped[str] = mapped_column(String(16))
    name: Mapped[str] = mapped_column(String(64))

    assets: Mapped[list["Asset"]] = relationship("Asset", back_populates="market")

    country_id: Mapped[int] = mapped_column(ForeignKey("country.id"))
    country: Mapped["Country"] = relationship(back_populates="markets")

    currency: Mapped["Currency"] = association_proxy("country", "currency")

    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchange.id"))
    exchange: Mapped["Exchange"] = relationship(back_populates="markets")


# FIXME
"""
timezone

premarket open
market open
market close
aftermarket close

trading calendar from zipline?
"""
