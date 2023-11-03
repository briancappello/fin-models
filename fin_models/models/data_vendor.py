from .. import db
from .asset_data_vendor import AssetDataVendor
from .index_data_vendor import IndexDataVendor


class DataVendor(db.Model):
    class Meta:
        repr = ("id", "key", "name")

    key = db.Column(db.String(16), index=True, unique=True)
    name = db.Column(db.String(64), index=True, unique=True)
    priority = db.Column(db.Integer)

    data_vendor_assets = db.relationship("AssetDataVendor", back_populates="data_vendor")
    assets = db.association_proxy(
        "data_vendor_assets", "asset", creator=lambda asset: AssetDataVendor(asset=asset)
    )

    data_vendor_indexes = db.relationship("IndexDataVendor", back_populates="data_vendor")
    indexes = db.association_proxy(
        "data_vendor_indexes", "index", creator=lambda index: IndexDataVendor(index=index)
    )

    data_vendor_items = db.relationship("DataItemVendor", back_populates="data_vendor")
    data_items = db.association_proxy("data_vendor_items", "data_item")
