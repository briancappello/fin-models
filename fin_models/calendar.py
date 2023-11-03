from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional, Union

import pandas as pd
import pandas_market_calendars as mcal

from fin_models.date_utils import DateType, to_ts, utc_now


TIME_BUFFER = pd.Timedelta(days=7)


class Calendar:
    def __init__(self, exchange: str = "NYSE"):
        self.calendar = mcal.get_calendar(exchange.upper())
        self.tz = self.calendar.tz

    def schedule(
        self,
        start: DateType | str,
        end: Optional[DateType | str] = None,
        include_extended: bool = False,
    ):
        return self.calendar.schedule(
            start_date=to_ts(start),
            end_date=to_ts(end),
            start="pre" if include_extended else "market_open",
            end="post" if include_extended else "market_close",
            tz=self.tz,
        )

    def get_latest_trading_date_schedule(
        self,
        include_extended: bool = False,
    ) -> pd.Series:
        """
        Returns the schedule for the current (or most recent) valid trading day. It's a series
        with keys "pre", "market_open", "market_close", and "post".

        For example::

            On Friday at 8:30AM EST, returns Thursday's schedule
            On Friday at 8:30AM EST with `include_extended=True`, returns Friday's schedule
            On Friday after 9:30AM EST, returns Friday's schedule
            On Saturday, returns Friday's schedule
        """
        end = utc_now()
        start = end - TIME_BUFFER
        schedule = self.schedule(start, end, include_extended=include_extended)
        return schedule[
            schedule["pre" if include_extended else "market_open"] <= end
        ].iloc[-1]

    def get_latest_trading_date(
        self,
        include_extended: bool = False,
    ) -> date:
        return self.get_latest_trading_date_schedule(
            include_extended=include_extended,
        ).name.date()

    def get_valid_dates(
        self,
        start: DateType | str,
        end: Optional[DateType | str] = None,
    ) -> List[date]:
        """
        Returns a list of dates the market was open between start and end (inclusive).
        """
        start = to_ts(start)
        end = to_ts(end)
        closes = self.schedule(start, end).market_close
        return closes[closes < end].map(lambda ts: ts.date()).to_list()

    def is_market_open(
        self,
        at_ts: Optional[DateType | str] = None,
        include_extended: bool = False,
    ) -> bool:
        """
        Check whether the market is open at a given timestamp
        """
        at_ts = to_ts(at_ts, _naive_assumed_tz=self.tz)
        schedule = self.schedule(
            start=at_ts - TIME_BUFFER,
            end=at_ts + TIME_BUFFER,
            include_extended=include_extended,
        )
        return self.calendar.open_at_time(schedule, at_ts)
