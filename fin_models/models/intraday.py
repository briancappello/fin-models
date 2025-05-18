from __future__ import annotations

from .. import db


class Intraday(db.Model):
    symbol = db.Column(db.String)
    timestamp = db.Column(db.DateTime)
    open = db.Column(db.Float)
    high = db.Column(db.Float)
    low = db.Column(db.Float)
    close = db.Column(db.Float)
    volume = db.Column(db.BigInteger)
