from __future__ import annotations

import re

from datetime import date, timedelta
from typing import TypeAlias
from urllib.parse import urlencode

import pandas as pd
import requests

from fin_models.config import Config
from fin_models.dataclasses import CompanyDetails
from fin_models.date_utils import DateType, isodate, to_ts
from fin_models.enums import Enum, Freq
from fin_models.serializers import CompanyDetailsSerializer


_default = object()


HOST = "https://api.polygon.io"
VALID_TIMEFRAMES = {
    Freq.min_1: "minute",
    Freq.min_5: "minute",
    Freq.min_10: "minute",
    Freq.min_15: "minute",
    Freq.min_30: "minute",
    Freq.hour: "hour",
    Freq.day: "day",
    Freq.week: "week",
    Freq.month: "month",
    Freq.quarter: "quarter",
    Freq.year: "year",
}
TIMEFRAME_AGGS = {
    Freq.min_5: 5,
    Freq.min_10: 10,
    Freq.min_15: 15,
    Freq.min_30: 30,
}


class AssetClass(Enum):
    stocks = "Stocks"
    options = "Options"
    crypto = "Crypto"
    fx = "FX"
    indices = "Indices"


class TickerType(Enum):
    ADRC = "American Depository Receipt Common"
    ADRP = "American Depository Receipt Preferred"
    ADRR = "American Depository Receipt Rights"
    ADRW = "American Depository Receipt Warrants"
    AGEN = "Agency Bond"
    BASKET = "Basket"
    BOND = "Corporate Bond"
    CS = "Common Stock"
    EQLK = "Equity Linked Bond"
    ETF = "Exchange Traded Fund"
    ETN = "Exchange Traded Note"
    ETS = "Single-security ETF"
    ETV = "Exchange Traded Vehicle"
    FUND = "Fund"
    GDR = "Global Depository Receipts"
    LT = "Liquidating Trust"
    NYRS = "New York Registry Shares"
    OS = "Ordinary Shares"
    OTHER = "Other Security Type"
    PFD = "Preferred Stock"
    RIGHT = "Rights"
    SP = "Structured Product"
    UNIT = "Unit"
    WARRANT = "Warrant"


TickerTypes: TypeAlias = list[TickerType] | list[str] | TickerType | str


HISTORY_URL_REGEX = re.compile(
    r".*/v2/aggs/ticker/(?P<symbol>.+)/range/1/(?P<timeframe>.+)/(?P<start>.+)/(?P<end>.+?)/?\?.+"
)

# polygon has a limit of 50_000 bars
# assuming extended hours trading, 7 days a week, 1 bar/minute ==> 52 days
MAX_MINUTE_DAYS = pd.Timedelta(days=50)


def datefmt(dt: DateType | str | None) -> str:
    """
    Return a URL-formatted date/time. If `dt` is None, returns the current UTC timestamp.
    """
    ts = to_ts(dt)
    if ts == pd.Timestamp(ts.date(), tz=ts.tz):
        return isodate(ts)
    return str(int(ts.timestamp() * 1000))


def make_url(uri: str, query_params: dict | None = None) -> str:
    query = {**(query_params or {}), **dict(apiKey=Config.POLYGON_API_KEY)}
    if "://" in uri:
        if "apiKey" in uri:
            return uri
        return f"{uri}{'&' if '?' in uri else '?'}{urlencode(query)}"
    return f"{HOST}/{uri.strip('/')}?{urlencode(query)}"


def _get(uri: str, query_params: dict | None = None) -> dict | list:
    r = requests.get(make_url(uri, query_params))
    r.raise_for_status()
    return r.json()


def _to_bar(d: dict) -> dict:
    return dict(
        Epoch=pd.Timestamp(d["t"] * 1000 * 1000, tz="UTC").tz_convert("America/New_York"),
        Open=d["o"],
        High=d["h"],
        Low=d["l"],
        Close=d["c"],
        Volume=int(d["v"]),
        VWAP=d.get("vw"),
    )


def json_to_df(data: dict) -> pd.DataFrame:
    if data["resultsCount"] == 0:
        return pd.DataFrame()
    return pd.DataFrame.from_records([_to_bar(d) for d in data["results"]], index="Epoch")


def get_daily_bars_for_date(
    date_: DateType | str | None = None,
    adjusted: bool = True,
    include_otc: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Return OHLCV bars for all active symbols on the given date
    """
    date_ = to_ts(date_, default=date.today())
    data = _get(
        f"/v2/aggs/grouped/locale/us/market/stocks/{isodate(date_)}",
        dict(adjusted=adjusted, include_otc=include_otc),
    )
    return {
        d["T"]: pd.DataFrame.from_records([_to_bar(d)], index="Epoch")
        for d in data["results"]
        if d["T"].isupper() and "." not in d["T"]
    }


def make_minutely_urls(
    symbol: str,
    freq: Freq,
    start: DateType | str,
    end: DateType | str,
) -> list[str]:
    start = to_ts(start)
    end = to_ts(end)

    intervals = []
    while True:
        interval_end = start + MAX_MINUTE_DAYS
        intervals.append((start, min(end, interval_end)))
        if interval_end >= end:
            break
        start = interval_end + pd.Timedelta(days=1)

    return [make_history_url(symbol, freq, s, e) for s, e in intervals]


def make_history_url(
    symbol: str,
    freq: Freq,
    start: DateType | str,
    end: DateType | str | None = None,
) -> str:
    s = datefmt(start)
    e = datefmt(end)
    timeframe = VALID_TIMEFRAMES[freq]
    agg = TIMEFRAME_AGGS.get(freq, 1)
    return make_url(
        f"/v2/aggs/ticker/{symbol.upper()}/range/{agg}/{timeframe}/{s}/{e}",
        dict(adjusted="true", sort="asc", limit=50_000),
    )


def get_df(
    symbol: str,
    freq: Freq = Freq.day,
    start: DateType | str | None = None,
    end: DateType | str | None = None,
) -> pd.DataFrame:
    end = to_ts(end, default=date.today())
    start = to_ts(
        start,
        default=end - timedelta(days=365 * Config.POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE),
    )

    if freq < Freq.hour and (end - start) > MAX_MINUTE_DAYS:
        dataframes = []
        for url in make_minutely_urls(symbol, freq, start, end):
            dataframes.append(json_to_df(_get(url)))
        return pd.concat(dataframes)

    url = make_history_url(symbol, freq, start, end)
    return json_to_df(_get(url))


def get_exchanges(asset_class: str = "stocks") -> list[dict]:
    """
    Get a list of exchanges for the given `asset_class`.

    Example return value::

        [{
          "acronym": "AMEX",
          "asset_class": "stocks",
          "id": 1,
          "locale": "us",
          "mic": "XASE",
          "name": "NYSE American, LLC",
          "operating_mic": "XNYS",
          "participant_id": "A",
          "type": "exchange",
          "url": "https://www.nyse.com/markets/nyse-american"
        }]
    """
    data = _get("/v3/reference/exchanges", dict(asset_class=asset_class))
    return data["results"]


def normalize_ticker_types(types: TickerTypes | None = None) -> list[str]:
    """
    Normalize types into strings supported by the Polygon API.

    Defaults to ADRC, ADRP, CS, PFD and ETF.
    """
    if not types:
        return [
            t.name
            for t in (
                TickerType.ADRC,
                TickerType.ADRP,
                TickerType.CS,
                TickerType.PFD,
                TickerType.ETF,
            )
        ]
    elif isinstance(types, TickerType):
        return [types.name]
    elif isinstance(types, str):
        aliases = {
            "common": "CS",  # common stock
            "preferred": "PFD",  # preferred stock
            "etf": "ETF",  # exchange traded fund
        }
        types = [
            aliases.get(t.strip().lower(), t.strip().upper()) for t in types.split(",")
        ]

    return [TickerType[t].name for t in types]


def get_tickers(types: TickerTypes | None = None) -> list[dict]:
    """
    Get a list of all tickers data supported by Polygon by share class type.

    Example return value::

        [{
            "ticker": "A",
            "name": "Agilent Technologies Inc.",
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": true,
            "currency_name": "usd",
            "cik": "0001090872",
            "composite_figi": "BBG000C2V3D6",
            "share_class_figi": "BBG001SCTQY4",
            "last_updated_utc": "2023-06-21T00:00:00Z"
        }]
    """
    types = normalize_ticker_types(types)
    tickers = []

    data = dict(next_url="/v3/reference/tickers")
    while True:
        data = _get(data["next_url"], dict(market="stocks", active="true", limit=1000))
        tickers.extend(
            [
                d
                for d in data["results"]
                if d.get("type") in types and d["ticker"].isupper()
            ]
        )
        if not data.get("next_url"):
            break
    return tickers


def get_symbols(types: TickerTypes | None = None) -> list[str]:
    """
    Get a list of all symbols supported by Polygon by share class type.
    """
    return [d["ticker"] for d in get_tickers(types)]


def get_company_details(
    symbol: str,
    on_date: DateType | str | None = None,
) -> CompanyDetails:
    """
    Get company details for a ticker symbol on a given date.

    Example return value::

        {
            "active": true,
            "address": {
                "address1": "One Apple Park Way",
                "city": "Cupertino",
                "postal_code": "95014",
                "state": "CA",
            },
            "cik": "0000320193",
            "composite_figi": "BBG000B9XRY4",
            "currency_name": "usd",
            "description": "...",
            "homepage_url": "https://www.apple.com",
            "list_date": "1980-12-12",
            "locale": "us",
            "market": "stocks",
            "market_cap": 2771126040150,
            "name": "Apple Inc.",
            "phone_number": "(408) 996-1010",
            "primary_exchange": "XNAS",
            "round_lot": 100,
            "share_class_figi": "BBG001S5N8V8",
            "share_class_shares_outstanding": 16406400000,
            "sic_code": "3571",
            "sic_description": "ELECTRONIC COMPUTERS",
            "ticker": "AAPL",
            "ticker_root": "AAPL",
            "total_employees": 154000,
            "type": "CS",
            "weighted_shares_outstanding": 16334371000,
        }
    """
    data = _get(
        f"/v3/reference/tickers/{symbol.upper()}",
        query_params=dict(date=isodate(on_date)) if on_date else None,
    )["results"]
    data.pop("branding", None)
    return CompanyDetailsSerializer().load(data)


def get_splits(
    dt: DateType | str | None = _default,
    ticker: str | None = None,
) -> list[dict]:
    """
    Get a list of splits on a given date. Defaults to the current date.

    Example response::

        {
            "execution_date": "2024-10-10",
            "id": "<UID>",
            "split_from": 20,
            "split_to": 1,
            "ticker": "SYMBOL",
        },
    """
    query_params = dict(limit=1000)
    if dt is not None:
        query_params["execution_date"] = isodate(None if dt is _default else dt)
    if ticker:
        query_params["ticker"] = ticker

    data = _get("/v3/reference/splits", query_params=query_params)
    return data["results"]
