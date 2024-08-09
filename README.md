# Fin Models

A work-in-progress library for handling financial data.

Current features:

* A Pandas DataFrame `Store` for reading and writing historical timeseries data.
    * Historical data can be fetched from or Yahoo! Finance or polygon.io
* SQLAlchemy Models
    * Asset
    * Equity
    * EquityIndex (join table)
    * Country
    * Currency
    * Exchange
    * Index
    * Industry
    * Market
    * Sector
    * Watchlist
    * WatchlistAsset (join table)
* Fetching lists of equities is supported for
    * AMEX
    * NASDAQ (GS, GM, CM, 100)
    * NYSE
    * Dow Jones (DJI, DJT, DJU)
    * SP500
