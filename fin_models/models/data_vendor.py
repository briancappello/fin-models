from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fin_models.db import Base, pk

from .asset_data_vendor import AssetDataVendor
from .index_data_vendor import IndexDataVendor


if TYPE_CHECKING:
    from .asset import Asset
    from .data_item import DataItem
    from .data_item_vendor import DataItemVendor
    from .index import Index


class DataVendor(Base):
    class Meta:
        repr = ("id", "key", "name")

    id: Mapped[pk]

    key: Mapped[str] = mapped_column(String(16), index=True, unique=True)
    name: Mapped[str] = mapped_column(String(64), index=True, unique=True)
    priority: Mapped[int]

    data_vendor_assets: Mapped[list["AssetDataVendor"]] = relationship(
        back_populates="data_vendor",
    )
    assets: Mapped[list["Asset"]] = association_proxy(
        "data_vendor_assets",
        "asset",
        creator=lambda asset: AssetDataVendor(asset=asset),
    )

    data_vendor_indexes: Mapped[list["IndexDataVendor"]] = relationship(
        back_populates="data_vendor",
    )
    indexes: Mapped[list["Index"]] = association_proxy(
        "data_vendor_indexes",
        "index",
        creator=lambda index: IndexDataVendor(index=index),
    )

    data_vendor_items: Mapped[list["DataItemVendor"]] = relationship(
        back_populates="data_vendor",
    )
    data_items: Mapped[list["DataItem"]] = association_proxy(
        "data_vendor_items",
        "data_item",
    )
