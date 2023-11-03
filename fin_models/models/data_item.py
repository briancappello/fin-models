from .. import db
from .data_item_vendor import DataItemVendor


class DataItem(db.Model):
    key = db.Column(db.String(32))
    update_frequency = db.Column(db.String(32))
    update_at = db.Column(db.String(32))

    data_item_vendors = db.relationship("DataItemVendor", back_populates="data_item")
    data_vendors = db.association_proxy(
        "data_item_vendors",
        "data_vendor",
        creator=lambda item: DataItemVendor(data_item=item),
    )
