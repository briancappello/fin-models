from .. import db
from .watchlist_asset import WatchlistAsset


class Watchlist(db.Model):
    class Meta:
        repr = ("id", "name")

    name = db.Column(db.String, unique=True, index=True)

    # user_id = db.foreign_key('User')
    # user = db.relationship('User', back_populates='watchlists')

    watchlist_assets = db.relationship(
        "WatchlistAsset", back_populates="watchlist", cascade="all, delete-orphan"
    )
    assets = db.association_proxy(
        "watchlist_assets", "asset", creator=lambda asset: WatchlistAsset(asset=asset)
    )
