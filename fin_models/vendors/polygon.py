from __future__ import annotations

import re

from datetime import date, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests

from fin_models.config import Config
from fin_models.date_utils import DateType, isodate, to_ts
from fin_models.enums import Freq


HOST = "https://api.polygon.io"
VALID_TIMEFRAMES = {
    Freq.min_1: "minute",
    Freq.hour: "hour",
    Freq.day: "day",
    Freq.week: "week",
    Freq.month: "month",
    Freq.quarter: "quarter",
    Freq.year: "year",
}
TICKER_TYPES = {
    "common": "CS",  # common stock
    "preferred": "PFD",  # preferred stock
    "etf": "ETF",  # exchange traded fund
}
HISTORY_URL_REGEX = re.compile(
    r".*/v2/aggs/ticker/(?P<symbol>.+)/range/1/(?P<timeframe>.+)/(?P<start>.+)/(?P<end>.+?)/?\?.+"
)

# polygon has a limit of 50_000 bars
# assuming extended hours trading, 7 days a week, 1 bar/minute ==> 52 days
MAX_MINUTE_DAYS = pd.Timedelta(days=50)


def datefmt(dt: DateType | str | None) -> str:
    ts = to_ts(dt)
    if ts == pd.Timestamp(ts.date()):
        return isodate(ts)
    return str(int(ts.timestamp() * 1000))


def make_url(uri: str, query_params: dict | None = None) -> str:
    query = {**(query_params or {}), **dict(apiKey=Config.POLYGON_API_KEY)}
    if "://" in uri:
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
    )


def json_to_df(data: dict) -> pd.DataFrame:
    if data["resultsCount"] == 0:
        return pd.DataFrame()
    return pd.DataFrame.from_records([_to_bar(d) for d in data["results"]], index="Epoch")


def daily_bars(
    date_: DateType | str | None = None,
    adjusted: bool = True,
    include_otc: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Return OHLCV bars for all active symbols on the given date
    """
    date_ = to_ts(date_, date.today())
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
    start: DateType | str,
    end: DateType | str,
    agg: int = 1,
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

    return [make_history_url(symbol, Freq.min_1, s, e, agg) for s, e in intervals]


def make_history_url(
    symbol: str,
    freq: Freq,
    start: DateType | str,
    end: DateType | str | None = None,
    agg: int = 1,
) -> str:
    s = datefmt(start)
    e = datefmt(end)
    timeframe = VALID_TIMEFRAMES[freq]
    return make_url(
        f"/v2/aggs/ticker/{symbol.upper()}/range/{agg}/{timeframe}/{s}/{e}",
        dict(adjusted="true", sort="asc", limit=50_000),
    )


def get_df(
    symbol: str,
    freq: Freq = Freq.day,
    start: DateType | str | None = None,
    end: DateType | str | None = None,
    agg: int = 1,
) -> pd.DataFrame:
    end = to_ts(end, default=date.today())
    start = to_ts(start, default=end - timedelta(days=365 * 10))

    if freq == Freq.min_1 and (end - start) > MAX_MINUTE_DAYS:
        dataframes = []
        for url in make_minutely_urls(symbol, start, end, agg):
            dataframes.append(json_to_df(requests.get(url).json()))
        return pd.concat(dataframes)

    url = make_history_url(symbol, freq, start, end, agg)
    return json_to_df(_get(url))


def get_exchanges(asset_class: str = "stocks") -> list[dict]:
    """
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


def normalize_ticker_types(types: list[str] | str | None = None) -> list[str]:
    if isinstance(types, str):
        types = types.split(",")

    return (
        [TICKER_TYPES.get(t.lower().strip(), t.upper()) for t in types]
        if types
        else list(TICKER_TYPES.values())
    )


def get_tickers(types: list[str] | str | None = None) -> list[dict]:
    """
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
            [d for d in data["results"] if d["type"] in types and d["ticker"].isupper()]
        )
        if not data.get("next_url"):
            break
    return tickers
