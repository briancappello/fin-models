from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from .data_item_vendor import DataItemVendor


if TYPE_CHECKING:
    from .data_vendor import DataVendor


class DataItem(Base):
    id: Mapped[pk]

    key: Mapped[str] = mapped_column(String(32))
    update_frequency: Mapped[str] = mapped_column(String(32))
    update_at: Mapped[str] = mapped_column(String(32))

    data_item_vendors: Mapped[list["DataItemVendor"]] = relationship(
        back_populates="data_item",
    )
    data_vendors: Mapped[list["DataVendor"]] = association_proxy(
        "data_item_vendors",
        "data_vendor",
        creator=lambda item: DataItemVendor(data_item=item),
    )
