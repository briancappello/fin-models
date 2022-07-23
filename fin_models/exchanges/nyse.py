from .nasdaq import get_exchange_df


NYSE_URL = "https://api.nasdaq.com/api/screener/stocks?exchange=NYSE&download=true"


def get_nyse_df():
    """A DataFrame of NYSE listed equities

    Indexed by ticker with columns: company_name, last_sale, market_cap, sector, industry
    """
    return get_exchange_df(NYSE_URL)
