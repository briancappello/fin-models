from .. import db
from sqlalchemy import or_
from sqlalchemy.sql import literal
from sqlalchemy.types import Text
from sqlalchemy.ext.hybrid import Comparator


class CurrencyNameComparator(Comparator):
    def operate(self, op, other):
        return or_(op(self.expression._name, other),
                   op(self.expression.iso_name, other),
                   op(self.expression.plural, other))


class PluralComparator(Comparator):
    def operate(self, op, other):
        return or_(op(self.expression._plural, other),
                   op(self.expression._name + literal('s', Text), other),
                   op(self.expression.iso_name + literal('s', Text), other))


class Currency(db.Model):
    class Meta:
        repr = ('id', 'code', 'name')

    iso_code = db.Column(db.String(3), index=True, unique=True)    # ISO 4217
    iso_name = db.Column(db.String(32), index=True, unique=True)   # ISO 4217
    _name = db.Column('name', db.String(32), index=True, nullable=True, unique=True)  # common english name
    _plural = db.Column('plural', db.String(32), nullable=True, unique=True)
    symbol = db.Column(db.String(8), nullable=True)

    countries = db.relationship('Country', back_populates='currency')

    @db.hybrid_property
    def code(self):
        return self.iso_code

    @db.hybrid_property
    def name(self):
        return self._name or self.iso_name

    @name.setter
    def name(self, name):
        self._name = name

    @name.comparator
    def name(cls):
        return CurrencyNameComparator(cls)

    @db.hybrid_property
    def plural(self):
        return self._plural or '{}s'.format(self.name)

    @plural.setter
    def plural(self, plural):
        self._plural = plural

    @plural.comparator
    def plural(cls):
        return PluralComparator(cls)
