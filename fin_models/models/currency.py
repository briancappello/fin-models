from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String, or_
from sqlalchemy.ext.hybrid import Comparator, hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import literal
from sqlalchemy.types import Text

from fin_models.db import Base, pk


if TYPE_CHECKING:
    from .country import Country


class CurrencyNameComparator(Comparator):
    def operate(self, op, other):
        return or_(
            op(self.expression._name, other),
            op(self.expression.iso_name, other),
            op(self.expression.plural, other),
        )


class PluralComparator(Comparator):
    def operate(self, op, other):
        return or_(
            op(self.expression._plural, other),
            op(self.expression._name + literal("s", Text), other),
            op(self.expression.iso_name + literal("s", Text), other),
        )


class Currency(Base):
    class Meta:
        repr = ("id", "code", "name")

    id: Mapped[pk]

    iso_code: Mapped[str] = mapped_column(String(3), index=True, unique=True)  # ISO 4217
    iso_name: Mapped[str] = mapped_column(String(32), index=True, unique=True)  # ISO 4217
    _name: Mapped[str] = mapped_column(
        "name",
        String(32),
        index=True,
        nullable=True,
        unique=True,
    )  # common english name
    _plural: Mapped[str] = mapped_column("plural", String(32), nullable=True, unique=True)
    symbol: Mapped[str] = mapped_column(String(8), nullable=True)

    countries: Mapped[list["Country"]] = relationship(back_populates="currency")

    @hybrid_property
    def code(self):
        return self.iso_code

    @hybrid_property
    def name(self):
        return self._name or self.iso_name

    @name.setter
    def name(self, name):
        self._name = name

    @name.comparator
    def name(cls):
        return CurrencyNameComparator(cls)

    @hybrid_property
    def plural(self):
        return self._plural or "{}s".format(self.name)

    @plural.setter
    def plural(self, plural):
        self._plural = plural

    @plural.comparator
    def plural(cls):
        return PluralComparator(cls)
