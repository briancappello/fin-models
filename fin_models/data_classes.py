from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from fin_models.enums import Freq


@dataclass(kw_only=True)
class Address:
    address1: str
    city: str
    state: str
    postal_code: str
    address2: str | None = None


@dataclass(kw_only=True)
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


@dataclass(kw_only=True)
class HistoricalMetadata:
    freq: Freq
    first_bar_utc: pd.Timestamp
    latest_bar_utc: pd.Timestamp
    Open: float
    High: float
    Low: float
    Close: float
    Volume: float
    timezone: str = "America/New_York"

    @property
    def first_bar_utc(self) -> pd.Timestamp:
        return self._first_bar_utc

    @first_bar_utc.setter
    def first_bar_utc(self, value: pd.Timestamp | datetime | str) -> None:
        self._first_bar_utc = pd.Timestamp(value).astimezone("UTC")

    @property
    def first_bar_dt(self) -> pd.Timestamp:
        return self.first_bar_utc.astimezone(self.timezone)

    @property
    def latest_bar_utc(self) -> pd.Timestamp:
        return self._latest_bar_utc

    @latest_bar_utc.setter
    def latest_bar_utc(self, value: pd.Timestamp | datetime | str) -> None:
        self._latest_bar_utc = pd.Timestamp(value).astimezone("UTC")

    @property
    def latest_bar_dt(self) -> pd.Timestamp:
        return self.latest_bar_utc.astimezone(self.timezone)

    @property
    def latest_bar(self) -> pd.Series:
        index = ["Open", "High", "Low", "Close", "Volume"]
        return pd.Series(
            data=[getattr(self, col) for col in index],
            index=index,
            name=self.latest_bar_dt,
        )

    @property
    def latest_bar_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(
            data=[
                dict(
                    Epoch=self.latest_bar_dt,
                    Open=self.Open,
                    High=self.High,
                    Low=self.Low,
                    Close=self.Close,
                    Volume=self.Volume,
                )
            ],
            index="Epoch",
        )
