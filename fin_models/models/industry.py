from .. import db


class Industry(db.Model):
    class Meta:
        repr = ("id", "name", "sector")

    name = db.Column(db.String(64), index=True, unique=True)

    equities = db.relationship("Equity", back_populates="industry")

    sector_id = db.foreign_key("Sector", nullable=True)
    sector = db.relationship("Sector", back_populates="industries")
