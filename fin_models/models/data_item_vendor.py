from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base


if TYPE_CHECKING:
    from .data_item import DataItem
    from .data_vendor import DataVendor


class DataItemVendor(Base):
    """
    join table between DataItem and DataVendor
    """

    data_item_id: Mapped[int] = mapped_column(
        ForeignKey("data_item.id"),
        primary_key=True,
    )
    data_item: Mapped["DataItem"] = relationship(back_populates="data_item_vendors")

    data_vendor_id: Mapped[int] = mapped_column(
        ForeignKey("data_vendor.id"),
        primary_key=True,
    )
    data_vendor: Mapped["DataVendor"] = relationship(back_populates="data_vendor_items")

    priority: Mapped[int | None]
