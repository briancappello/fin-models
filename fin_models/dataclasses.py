from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from fin_models.enums import Freq


@dataclass
class Address:
    address1: str
    city: str
    state: str
    postal_code: str
    address2: str | None = None


@dataclass
class CompanyDetails:
    active: bool
    currency_name: str
    locale: str  # us | global
    market: str  # stocks | crypto | fx | otc | indices
    name: str
    ticker: str

    address: Address | None = None
    cik: str | None = None  # https://en.wikipedia.org/wiki/Central_Index_Key
    composite_figi: str | None = None
    delisted_utc: date | None = None
    description: str | None = None
    homepage_url: str | None = None
    list_date: date | None = None
    market_cap: int | None = None
    phone_number: str | None = None
    primary_exchange: str | None = None
    round_lot: int | None = None
    share_class_figi: str | None = None
    share_class_shares_outstanding: int | None = None
    sic_code: str | None = None  # https://www.sec.gov/info/edgar/siccodes.htm
    sic_description: str | None = None
    ticker_root: str | None = None
    ticker_suffix: str | None = None
    total_employees: int | None = None
    type: str | None = (
        None  # https://polygon.io/docs/stocks/get_v3_reference_tickers_types
    )
    weighted_shares_outstanding: int | None = None


@dataclass
class HistoricalMetadata:
    freq: Freq
    first_bar_utc: datetime | pd.Timestamp
    latest_bar_utc: datetime | pd.Timestamp
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    timezone: str = "America/New_York"

    @property
    def first_bar_dt(self) -> datetime:
        return self.first_bar_utc.astimezone(ZoneInfo(self.timezone))

    @property
    def latest_bar_dt(self) -> datetime:
        return self.latest_bar_utc.astimezone(ZoneInfo(self.timezone))

    @property
    def latest_bar(self) -> pd.Series:
        index = ["Open", "High", "Low", "Close", "Volume"]
        return pd.Series(
            data=[getattr(self, col) for col in index],
            index=index,
            name=pd.Timestamp(self.latest_bar_dt),
        )
