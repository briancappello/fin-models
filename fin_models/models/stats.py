from __future__ import annotations

from sqlalchemy.dialects.postgresql import JSONB

from fin_models import db
from fin_models.enums import Freq


class Stats(db.Model):
    class Meta:
        repr = ("id", "symbol", "day")
        unique_together = ("symbol", "day", "freq")
        created_at = None
        updated_at = None

    symbol = db.Column(db.String, index=True, nullable=False)
    day = db.Column(db.Date, index=True, nullable=False)
    freq = db.Column(db.Enum(Freq), index=True, nullable=False)
    stats = db.Column(JSONB)
