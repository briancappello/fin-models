import pandas as pd
import requests


NDX_URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"


def get_nasdaq_100_df():
    """
    A DataFrame of NASDAQ-100 components

    Indexed by ticker with columns: company_name, market_cap, last_sale
    """
    r = requests.get(
        NDX_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"
            ),
        },
    )
    d = r.json()["data"]["data"]
    df = (
        pd.DataFrame.from_records(d["rows"])
        .drop(columns=["netChange", "percentageChange", "deltaIndicator"])
        .rename(
            columns={
                "symbol": "ticker",
                "companyName": "company_name",
                "marketCap": "market_cap",
                "lastSalePrice": "last_sale",
            }
        )
    )
    df["last_sale"] = df["last_sale"].apply(lambda p: float(p.strip("$")))
    df["market_cap"] = df["market_cap"].apply(lambda mc: int(mc.replace(",", "")))
    return df.set_index("ticker")
