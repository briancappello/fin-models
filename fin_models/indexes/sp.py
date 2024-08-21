from __future__ import annotations

from ..utils import get_wiki_table_df


SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def get_sp_500_df():
    """
    A DataFrame of S&P 500 components

    Indexed by ticker with columns: company_name, sector, industry, date_added, headquarters
    """
    cols = {
        "symbol": "ticker",
        "gics_sector": "sector",
        "gics_sub_industry": "industry",
        "date_first_added": "date_added",
        "headquarters_location": "headquarters",
        "security": "company_name",
    }
    df = get_wiki_table_df(SP500_URL)
    return (
        df.drop(set(df.columns) - set(cols.keys()), axis=1)
        .rename(columns=cols)
        .set_index("ticker")
    )
