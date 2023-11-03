from .. import db


class IndexDataVendor(db.Model):
    """Join table between Index and DataVendor"""

    class Meta:
        repr = ("index_id", "data_vendor_id", "ticker")

    index_id = db.foreign_key("Index", primary_key=True)
    index = db.relationship("Index", back_populates="index_data_vendors")

    data_vendor_id = db.foreign_key("DataVendor", primary_key=True)
    data_vendor = db.relationship("DataVendor", back_populates="data_vendor_indexes")

    # vendor-specific index ticker (if different from canonical index ticker)
    _ticker = db.Column("ticker", db.String(16), nullable=True)

    def __init__(self, index=None, data_vendor=None, **kwargs):
        super(IndexDataVendor, self).__init__(**kwargs)
        if index:
            self.index = index
        if data_vendor:
            self.data_vendor = data_vendor

    @db.hybrid_property
    def ticker(self):
        return self._ticker or self.index.ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker
