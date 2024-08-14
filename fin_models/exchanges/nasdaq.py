import io

import pandas as pd
import requests

from ..utils import html_unescape, str_strip


NASDAQ_GS_URL = "https://api.nasdaq.com/api/screener/stocks?exchange=NASDAQ&exsubcategory=NGS&download=true"
NASDAQ_GM_URL = "https://api.nasdaq.com/api/screener/stocks?exchange=NASDAQ&exsubcategory=NGM&download=true"
NASDAQ_CM_URL = "https://api.nasdaq.com/api/screener/stocks?exchange=NASDAQ&exsubcategory=NCM&download=true"


def get_exchange_df(url):
    """A DataFrame of AMEX listed equities

    Indexed by ticker with columns: company_name, last_sale, market_cap, sector, industry
    """
    r = requests.get(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"
            ),
        },
    )
    d = r.json()["data"]
    df = (
        pd.DataFrame.from_records(d["rows"], columns=d["headers"])
        .drop(columns=["country", "ipoyear", "netchange", "pctchange", "url"])
        .rename(
            columns={
                "symbol": "ticker",
                "name": "company_name",
                "lastsale": "last_sale",
                "marketCap": "market_cap",
            }
        )
    )
    df = df[~df["ticker"].str.contains(r"\^")].set_index("ticker")
    df["last_sale"] = df["last_sale"].apply(
        lambda p: float(p.replace(",", "").strip("$"))
    )
    df["market_cap"] = df["market_cap"].apply(
        lambda mc: int(mc.replace(",", "").split(".")[0] or "0")
    )
    df["volume"] = df["volume"].apply(int)
    return df


def get_nasdaq_gs_df():
    """A DataFrame of NASDAQ Global Select Market listed equities (large cap)

    Indexed by ticker with columns: company_name, last_sale, market_cap, sector, industry
    """
    return get_exchange_df(NASDAQ_GS_URL)


def get_nasdaq_gm_df():
    """A DataFrame of NASDAQ Global Market listed equities (mid cap)

    Indexed by ticker with columns: company_name, last_sale, market_cap, sector, industry
    """
    return get_exchange_df(NASDAQ_GM_URL)


def get_nasdaq_cm_df():
    """A DataFrame of NASDAQ Common Market listed equities (small cap)

    Indexed by ticker with columns: company_name, last_sale, market_cap, sector, industry
    """
    return get_exchange_df(NASDAQ_CM_URL)
