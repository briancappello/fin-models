from .. import db


class WatchlistAsset(db.Model):
    asset_id = db.foreign_key("Asset", primary_key=True)
    asset = db.relationship("Asset", back_populates="asset_watchlists")

    watchlist_id = db.foreign_key("Watchlist", primary_key=True)
    watchlist = db.relationship("Watchlist", back_populates="watchlist_assets")

    def __init__(self, asset=None, watchlist=None, **kwargs):
        super().__init__(**kwargs)
        if asset is not None:
            self.asset = asset
        if watchlist is not None:
            self.watchlist = watchlist
