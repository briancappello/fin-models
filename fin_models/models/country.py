from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String, or_
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .currency import Currency
    from .market import Market


class CountryCodeComparator(Comparator):
    def operate(self, op, other):
        return or_(
            op(self.expression.iso_code, other), op(self.expression.iso_code3, other)
        )


class CountryNameComparator(Comparator):
    def operate(self, op, other):
        return or_(
            op(self.expression._name, other),
            op(self.expression.iso_name, other),
            op(self.expression.native_name, other),
        )


class Country(Base):
    class Meta:
        repr = ("id", "code", "name")

    id: Mapped[pk]

    iso_code: Mapped[str] = mapped_column(
        String(2),
        index=True,
        unique=True,
    )  # ISO 3166-1 alpha-2
    iso_code3: Mapped[str] = mapped_column(
        String(3),
        index=True,
        unique=True,
    )  # ISO 3166-1 alpha-3
    iso_name: Mapped[str] = mapped_column(
        String(64),
        index=True,
        unique=True,
    )  # official english short name (ISO 3166/MA)
    _name: Mapped[str] = mapped_column(
        "name",
        String(64),
        index=True,
        nullable=True,
        unique=True,
    )  # common english name
    _native_name: Mapped[str] = mapped_column(
        "native_name",
        String(64),
        nullable=True,
        unique=True,
    )

    currency_id: Mapped[int] = mapped_column(ForeignKey("currency.id"))
    currency: Mapped["Currency"] = relationship(back_populates="countries")

    markets: Mapped[list["Market"]] = relationship(back_populates="country")

    @hybrid_property
    def code(self):
        return self.iso_code

    @code.comparator
    def code(cls):
        return CountryCodeComparator(cls)

    @hybrid_property
    def name(self):
        return self._name or self.iso_name

    @name.setter
    def name(self, name):
        self._name = name

    @name.comparator
    def name(cls):
        return CountryNameComparator(cls)

    @hybrid_property
    def native_name(self):
        return self._native_name or self.name

    @native_name.setter
    def native_name(self, native_name):
        self._native_name = native_name
