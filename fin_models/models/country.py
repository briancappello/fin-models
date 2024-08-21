from __future__ import annotations

from sqlalchemy import or_
from sqlalchemy.ext.hybrid import Comparator

from .. import db


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


class Country(db.Model):
    class Meta:
        repr = ("id", "code", "name")

    iso_code = db.Column(db.String(2), index=True, unique=True)  # ISO 3166-1 alpha-2
    iso_code3 = db.Column(db.String(3), index=True, unique=True)  # ISO 3166-1 alpha-3
    iso_name = db.Column(
        db.String(64), index=True, unique=True
    )  # official english short name (ISO 3166/MA)
    _name = db.Column(
        "name", db.String(64), index=True, nullable=True, unique=True
    )  # common english name
    _native_name = db.Column("native_name", db.String(64), nullable=True, unique=True)

    currency_id = db.foreign_key("Currency")
    currency = db.relationship("Currency", back_populates="countries")

    markets = db.relationship("Market", back_populates="country")

    @db.hybrid_property
    def code(self):
        return self.iso_code

    @code.comparator
    def code(cls):
        return CountryCodeComparator(cls)

    @db.hybrid_property
    def name(self):
        return self._name or self.iso_name

    @name.setter
    def name(self, name):
        self._name = name

    @name.comparator
    def name(cls):
        return CountryNameComparator(cls)

    @db.hybrid_property
    def native_name(self):
        return self._native_name or self.name

    @native_name.setter
    def native_name(self, native_name):
        self._native_name = native_name
