from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from ..date_utils import utcnow
from ..enums import Freq


if TYPE_CHECKING:
    from .asset import Asset
    from .data_vendor import DataVendor


class AssetDataVendor(Base):
    """Join table between Asset and DataVendor"""

    class Meta:
        repr = ("asset_id", "data_vendor_id", "ticker")

    id: Mapped[pk]

    asset_id: Mapped[int] = mapped_column(ForeignKey("asset.id"), primary_key=True)
    asset: Mapped["Asset"] = relationship(back_populates="asset_data_vendors")

    data_vendor_id: Mapped[int] = mapped_column(
        ForeignKey("data_vendor.id"),
        primary_key=True,
    )
    data_vendor: Mapped["DataVendor"] = relationship(back_populates="data_vendor_assets")

    # vendor-specific ticker (if different from canonical ticker)
    _ticker: Mapped[str | None] = mapped_column("ticker")

    minutely_last_updated: Mapped[datetime]
    daily_last_updated: Mapped[datetime]
    weekly_last_updated: Mapped[datetime]
    monthly_last_updated: Mapped[datetime]

    @hybrid_property
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
