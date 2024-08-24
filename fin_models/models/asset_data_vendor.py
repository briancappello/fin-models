from __future__ import annotations

from .. import db
from ..date_utils import utcnow
from ..enums import Freq


class AssetDataVendor(db.Model):
    """Join table between Asset and DataVendor"""

    class Meta:
        repr = ("asset_id", "data_vendor_id", "ticker")

    asset_id = db.foreign_key("Asset", primary_key=True)
    asset = db.relationship("Asset", back_populates="asset_data_vendors")

    data_vendor_id = db.foreign_key("DataVendor", primary_key=True)
    data_vendor = db.relationship("DataVendor", back_populates="data_vendor_assets")

    # vendor-specific ticker (if different from canonical ticker)
    _ticker = db.Column("ticker", db.String(16), nullable=True)

    minutely_last_updated = db.Column(db.DateTime(), nullable=True)
    daily_last_updated = db.Column(db.DateTime(), nullable=True)
    weekly_last_updated = db.Column(db.DateTime(), nullable=True)
    monthly_last_updated = db.Column(db.DateTime(), nullable=True)

    def __init__(self, asset=None, data_vendor=None, **kwargs):
        super(AssetDataVendor, self).__init__(**kwargs)
        if asset:
            self.asset = asset
        if data_vendor:
            self.data_vendor = data_vendor

    @db.hybrid_property
    def ticker(self):
        return self._ticker or self.asset.ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker

    def last_updated(self, frequency: Freq):
        if frequency == Freq.min_1:
            return self.minutely_last_updated
        elif frequency == Freq.day:
            return self.daily_last_updated
        elif frequency == Freq.week:
            return self.weekly_last_updated
        if frequency == Freq.month:
            return self.monthly_last_updated
        raise NotImplementedError

    def set_last_updated(self, frequency: Freq, time=None):
        time = time or utcnow()
        if frequency == Freq.min_1:
            self.minutely_last_updated = time
        elif frequency == Freq.day:
            self.daily_last_updated = time
        elif frequency == Freq.week:
            self.weekly_last_updated = time
        elif frequency == Freq.month:
            self.monthly_last_updated = time
        raise NotImplementedError
