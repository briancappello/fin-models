from __future__ import annotations

from .. import db


class Exchange(db.Model):
    class Meta:
        repr = ("id", "abbrev", "name")

    abbrev = db.Column(db.String(16), unique=True)
    name = db.Column(db.String(64), unique=True)

    markets = db.relationship("Market", back_populates="exchange")
