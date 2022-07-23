from .. import db


class Sector(db.Model):
    class Meta:
        repr = ('id', 'name')

    name = db.Column(db.String(32), index=True, unique=True)

    equities = db.relationship('Equity', back_populates='sector')

    industries = db.relationship('Industry', back_populates='sector')
