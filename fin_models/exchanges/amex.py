from __future__ import annotations

from .nasdaq import get_exchange_df


AMEX_URL = "https://api.nasdaq.com/api/screener/stocks?exchange=AMEX&download=true"


def get_amex_df():
    return get_exchange_df(AMEX_URL)
