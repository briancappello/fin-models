from __future__ import annotations

import functools
import json

from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import requests

from dateutil.parser import parse as _parse_dt
from dateutil.tz import gettz
from requests.cookies import RequestsCookieJar

from fin_models.enums import Freq
from fin_models.utils import get_soup, kmbt_to_int, table_to_df, to_float, to_percent


BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{query}"
VALID_TIMEFRAMES = {
    Freq.min_1: "1m",
    Freq.min_5: "5m",
    Freq.min_10: "10m",
    Freq.min_15: "15m",
    Freq.min_30: "30m",
    Freq.hour: "1h",  # seems to be the same as 60m
    Freq.day: "1d",
    Freq.week: "1wk",  # not the same as 5d; 1wk is calendar-aligned
    Freq.month: "1mo",
    Freq.quarter: "3mo",
}
EST = gettz("America/New_York")
BST = gettz("Europe/London")
CEST = gettz("Europe/Berlin")
IST = gettz("Asia/Kolkata")

# alias dateutil.parser.parse here to more sensible name/defaults
parse_datetime = functools.partial(
    _parse_dt,
    default=datetime.combine(datetime.now(), time(0, tzinfo=timezone.utc)),
    tzinfos={"EST": EST, "EDT": EST, "BST": BST, "CEST": CEST, "IST": IST},
)


def to_datetime(dt: date | datetime | pd.Timestamp | int | str) -> datetime:
    if isinstance(dt, int):
        return datetime.fromtimestamp(dt)
    elif isinstance(dt, pd.Timestamp):
        return dt.to_pydatetime()
    elif isinstance(dt, datetime):
        return dt
    elif isinstance(dt, date):
        return datetime(*dt.timetuple()[:6])
    return parse_datetime(dt)


def to_est(dt: datetime) -> datetime:
    return dt.astimezone(EST)


def sanitize_dates(
    start: date | datetime | pd.Timestamp | int | str | None = None,
    end: date | datetime | pd.Timestamp | int | str | None = None,
    timeframe: Freq = Freq.day,
):
    if end is None:
        end = date.today() + timedelta(days=1)
    end = to_datetime(end)

    if start is None:
        days = (
            7
            if timeframe == Freq.min_1
            else (50 if timeframe < Freq.day else (365 * 100 - 1))  # 100 years(ish)
        )
        start = end - timedelta(days=days)
    start = to_datetime(start)

    return to_est(start), to_est(end)


class CookieCrumbCache:
    def __init__(
        self,
        url: str = "https://finance.yahoo.com/chart/INTC",
        max_age: timedelta | None = timedelta(hours=6),
    ):
        self._url = url
        self._max_age = max_age
        self._cookies = None
        self._crumb = None
        self._time_fetched = None

    @property
    def cookies(self):
        self._maybe_refresh()
        return self._cookies

    @property
    def crumb(self):
        self._maybe_refresh()
        return self._crumb

    def _maybe_refresh(self):
        is_expired = not self._time_fetched or (
            datetime.now() - self._time_fetched > self._max_age
        )
        if is_expired:
            self._time_fetched = datetime.now()
            self._crumb, self._cookies = self.get_yfi_crumb_and_cookies(self._url)

    @staticmethod
    def get_yfi_crumb_and_cookies(url):
        r = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
                ),
            },
        )
        html = r.content.decode("unicode-escape")
        start_search = '"crumb": "'
        start_idx = html.find(start_search) + len(start_search)
        crumb = html[start_idx : html.find('"', start_idx)]
        return crumb.strip(), r.cookies


cookie_crumb_cache = CookieCrumbCache()


def get_yfi_url_and_cookies(
    symbol: str,
    freq: Freq = Freq.day,
    start: datetime | None = None,
    end: datetime | None = None,
    include_extended: bool = False,
) -> tuple[str, RequestsCookieJar]:
    if freq not in VALID_TIMEFRAMES:
        raise NotImplementedError(
            f"Yahoo does not support freq={freq}. "
            f"Must be one of {list(VALID_TIMEFRAMES.keys())}."
        )

    start, end = sanitize_dates(start, end, freq)
    q = {
        "symbol": symbol,
        "period1": int(start.timestamp()),
        "period2": int(end.timestamp()),
        "interval": VALID_TIMEFRAMES[freq],
        "useYfid": "true",
        "includePrePost": ("true" if include_extended and freq < Freq.day else "false"),
        "events": "div|split|earn",
        "crumb": cookie_crumb_cache.crumb,
        "corsDomain": "finance.yahoo.com",
        "lang": "en-US",
        "region": "US",
    }
    return BASE_URL.format(symbol=symbol, query=urlencode(q)), cookie_crumb_cache.cookies


def yfi_json_to_df(data: dict, freq: Freq = Freq.day) -> pd.DataFrame | None:
    result = data["chart"]
    if result["error"]:
        print(result["error"])
        return None

    data = result["result"][0]
    try:
        quotes = data["indicators"]["quote"][0]
        index = (
            pd.DatetimeIndex(pd.to_datetime(data["timestamp"], unit="s"), name="Epoch")
            .tz_localize("UTC")
            .tz_convert("America/New_York")
        )
        if freq == Freq.day:
            index = index.normalize()

        df = pd.DataFrame(quotes, index=index)
        df.volume = df.volume.fillna(0).astype("int64")
        df = df.ffill().sort_index()
        df.rename(columns=lambda name: name.title(), inplace=True)
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        if not len(df) or freq != Freq.day:
            return df

        # check if 2 or more rows for the latest date
        tail = df[df.iloc[-1].name.round("D"):]  # fmt: skip
        if len(tail) == 1:
            return df

        # and if so, pick the best daily bar by greatest volume
        latest = tail.sort_values("Volume").iloc[-1]
        return pd.concat([df.iloc[:-len(tail)], pd.DataFrame([latest])])  # fmt: skip
    except Exception as e:
        print(str(e), "\n", data)
        return None


def get_df(
    symbol: str,
    freq: Freq = Freq.day,
    start: datetime | None = None,
    end: datetime | None = None,
    include_extended: bool = False,
) -> pd.DataFrame | None:
    """
    Fetch historical data from Yahoo! Finance.
    """
    url, cookies = get_yfi_url_and_cookies(
        symbol,
        freq=freq,
        start=start,
        end=end,
        include_extended=include_extended,
    )
    r = requests.get(
        url,
        cookies=cookies,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
            ),
        },
    )

    if r.status_code != 200:
        print(url, r.status_code, r.headers, r.content)
        return None

    return yfi_json_to_df(r.json(), freq)


def get_most_actives(
    region: str = "us",
    min_intraday_vol: int = 250_000,
    min_intraday_price: float = 1.0,
    num_results: int = 100,
) -> pd.DataFrame:
    return _get_predefined_screener_results("most_actives")

    # old working (now broken) code for custom screeners
    # error message is "Unauthorized / Invalid Crumb" - but the crumb is correct. Perhaps cookies?
    url = "https://query2.finance.yahoo.com/v1/finance/screener?" + urlencode(
        {
            "crumb": cookie_crumb_cache.crumb,
            "lang": "en-US",
            "region": region.upper(),
            "formatted": "true",
            "corsDomain": "finance.yahoo.com",
        }
    )

    r = requests.post(
        url,
        json={
            "offset": 0,
            "size": num_results,
            "sortField": "dayvolume",
            "sortType": "DESC",
            "quoteType": "EQUITY",
            "query": {
                "operator": "AND",
                "operands": [
                    {"operator": "eq", "operands": ["region", region.lower()]},
                    {
                        "operator": "gt",
                        "operands": ["dayvolume", int(min_intraday_vol)],
                    },
                    {
                        "operator": "gt",
                        "operands": ["intradayprice", float(min_intraday_price)],
                    },
                ],
            },
            "userId": "",
            "userIdType": "guid",
        },
        headers={
            "Accept": "*/*",
            "Connection": "keep-alive",
            # "access-control-request-method": "POST",
            # "access-control-request-headers ": "content-type",
            "priority": "u=4",
            "Host": "query2.finance.yahoo.com",
            "Origin": "https://finance.yahoo.com",
            "Referer": "https://finance.yahoo.com/screener/predefined/most_actives/",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
            ),
            "Content-Type": "application/json",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "sec-gpc": "1",
            "te": "trailers",
        },
        cookies=cookie_crumb_cache.cookies,
    )

    try:
        data = r.json()["finance"]
    except (KeyError, json.JSONDecodeError):
        raise Exception(str(r.text))

    if "error" in data and data["error"]:
        raise Exception(str(data["error"]))

    df = pd.DataFrame.from_records(
        [
            {k: v["raw"] if isinstance(v, dict) else v for k, v in quote.items()}
            for quote in data["result"][0]["quotes"]
        ],
        index="symbol",
    )

    # drop junk data before returning (some kind of yahoo-specific (testing?) symbol)
    return df.drop("YTESTQFTACHYON")


def get_trending_tickers() -> pd.DataFrame:
    url = "https://finance.yahoo.com/trending-tickers"
    soup = get_soup(url)
    table = soup.find(attrs={"id": "list-res-table"}).find("table")
    df = table_to_df(table, index_col="symbol")

    df = (
        df.drop(["day_chart", "week_range", "intraday_high_low"], axis=1)
        .rename(columns={"%_change": "pct_change"})
        .replace("-", np.nan)
        .replace("N/A", np.nan)
        .dropna()
    )
    df = _convert_column_types(df)

    # FIXME: yahoo only returns the current time on business days, what does it
    # return on the weekends? do we need to set the correct date to the last
    # trading day, or does yahoo also return the date string when queried on
    # the weekends?
    df["market_time"] = pd.to_datetime(
        [parse_datetime(dt) for dt in df["market_time"]], utc=True
    )
    return df


def get_gainers_tickers() -> pd.DataFrame:
    return _get_predefined_screener_results("day_gainers")


def get_losers_tickers() -> pd.DataFrame:
    return _get_predefined_screener_results("day_losers")


def _get_predefined_screener_results(
    predefined_screener: str, count: int = 50
) -> pd.DataFrame:
    soup = get_soup(
        url=f"https://finance.yahoo.com/screener/predefined/{predefined_screener}/?offset=0&count={count}",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
            ),
        },
    )
    table = soup.find(attrs={"id": "scr-res-table"}).find("table")
    df = table_to_df(table, index_col="symbol")

    df = (
        df.drop(["pe_ratio__ttm", "week_range", "avg_vol____month"], axis=1)
        .rename(columns={"%_change": "pct_change", "price__intraday": "last_price"})
        .replace("-", np.nan)
        .replace("N/A", np.nan)
        .dropna()
    )
    return _convert_column_types(df)


def _convert_column_types(df: pd.DataFrame) -> pd.DataFrame:
    df["last_price"] = df["last_price"].apply(to_float)
    df["change"] = df["change"].apply(to_float)
    df["pct_change"] = df["pct_change"].apply(to_percent)
    df["volume"] = df["volume"].apply(kmbt_to_int)
    df["market_cap"] = df["market_cap"].apply(kmbt_to_int)
    return df
