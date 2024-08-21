from __future__ import annotations

from .. import db


class Market(db.Model):
    class Meta:
        repr = ("id", "abbrev", "name")

    abbrev = db.Column(db.String(16))
    name = db.Column(db.String(64))

    assets = db.relationship("Asset", back_populates="market")

    country_id = db.foreign_key("Country")
    country = db.relationship("Country", back_populates="markets")

    currency = db.association_proxy("country", "currency")

    exchange_id = db.foreign_key("Exchange")
    exchange = db.relationship("Exchange", back_populates="markets")


# FIXME
"""
timezone

premarket open
market open
market close
aftermarket close

trading calendar from zipline?
"""
