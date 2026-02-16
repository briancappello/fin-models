from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

import pandas as pd
import pandas_market_calendars as mcal

from fin_models.data_classes import Bar, Freq
from fin_models.date_utils import DateType, to_ts, ts_utcnow


TIME_BUFFER = pd.Timedelta(days=7)
_default = object()


class Calendar:
    def __init__(self, exchange: str = "NYSE"):
        self.calendar = mcal.get_calendar(exchange.upper())
        self.tz = self.calendar.tz

    def schedule(
        self,
        start: DateType | str,
        end: DateType | str | None = None,
        include_extended: bool = False,
    ):
        return self.calendar.schedule(
            start_date=to_ts(start),
            end_date=to_ts(end)
            if end
            else self.get_latest_trading_date(include_extended=include_extended),
            start="pre" if include_extended else "market_open",
            end="post" if include_extended else "market_close",
            tz=self.tz,
        )

    def get_date_n_daily_bars_ago(
        self,
        num_bars_ago: int,
        from_date: date | str | None = None,
    ) -> date:
        if isinstance(from_date, str):
            from_date = date.fromisoformat(from_date)
        end = from_date or self.get_latest_trading_date()

        schedule = self.schedule(
            start=end - timedelta(days=max(10, int(num_bars_ago / 0.65))),
            end=end,
        )
        return schedule.iloc[-num_bars_ago].name.date()

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
        end = ts_utcnow()
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

    def get_prior_trading_date(self, current_date=None):
        end = to_ts(current_date) if current_date else self.get_latest_trading_date()
        schedule = self.schedule(start=end - TIME_BUFFER, end=end, include_extended=True)
        return schedule.index[-2].date()

    def get_valid_dates(
        self,
        start: DateType | str,
        end: DateType | str | None = None,
    ) -> list[date]:
        """
        Returns a list of dates the market was open between start and end (inclusive).
        """
        start = to_ts(start)
        end = to_ts(end)
        closes = self.schedule(start, end).market_close
        return closes[closes < end].map(lambda ts: ts.date()).to_list()

    def is_market_open(
        self,
        at_ts: DateType | str | None = None,
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

    def is_extended_hours(self, at_ts: DateType | str | None = None) -> bool:
        return self.is_market_open(
            at_ts, include_extended=True
        ) and not self.is_market_open(at_ts, include_extended=False)

    @lru_cache
    def schedule_for_date(self, day: date, tz: str = _default) -> pd.Series:
        return self.calendar.schedule(
            start_date=day,
            end_date=day,
            start="pre",
            end="post",
            tz=self.tz if tz == _default else tz,
        ).iloc[0]

    def is_premarket(self, bar: Bar):
        if bar.freq >= Freq.day:
            return False
        schedule = self.schedule_for_date(bar.Epoch.date())
        return schedule.pre <= bar.Epoch < schedule.market_open

    def is_intraday(self, bar: Bar):
        if bar.freq >= Freq.day:
            return True
        schedule = self.schedule_for_date(bar.Epoch.date())
        return schedule.market_open <= bar.Epoch < schedule.market_close

    def is_aftermarket(self, bar: Bar):
        if bar.freq >= Freq.day:
            return False
        schedule = self.schedule_for_date(bar.Epoch.date())
        return schedule.market_close <= bar.Epoch <= schedule.post
