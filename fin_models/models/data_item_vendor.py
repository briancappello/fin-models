from __future__ import annotations

from .. import db


class DataItemVendor(db.Model):
    """
    join table between DataItem and DataVendor
    """

    class Meta:
        pk = None

    data_item_id = db.foreign_key("DataItem", primary_key=True)
    data_item = db.relationship("DataItem", back_populates="data_item_vendors")

    data_vendor_id = db.foreign_key("DataVendor", primary_key=True)
    data_vendor = db.relationship("DataVendor", back_populates="data_vendor_items")

    priority = db.Column(db.Integer, nullable=True)

    def __init__(self, data_item=None, data_vendor=None, priority=None, **kwargs):
        super().__init__(priority=priority, **kwargs)
        if data_item:
            self.data_item = data_item
        if data_vendor:
            self.data_vendor = data_vendor
