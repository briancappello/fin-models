from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base


if TYPE_CHECKING:
    from .data_vendor import DataVendor
    from .index import Index


class IndexDataVendor(Base):
    """Join table between Index and DataVendor"""

    class Meta:
        repr = ("index_id", "data_vendor_id", "ticker")

    index_id: Mapped[int] = mapped_column(ForeignKey("index.id"), primary_key=True)
    index: Mapped["Index"] = relationship(back_populates="index_data_vendors")

    data_vendor_id: Mapped[int] = mapped_column(
        ForeignKey("data_vendor.id"), primary_key=True
    )
    data_vendor: Mapped["DataVendor"] = relationship(back_populates="data_vendor_indexes")

    # vendor-specific index ticker (if different from canonical index ticker)
    _ticker: Mapped[str | None] = mapped_column("ticker", String(16))

    @hybrid_property
    def ticker(self):
        return self._ticker or self.index.ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker
