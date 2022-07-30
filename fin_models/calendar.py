from datetime import date, datetime
from typing import List, Optional, Union


import pandas as pd
import pandas_market_calendars as mcal

TIME_BUFFER = pd.Timedelta(days=5)


class Calendar:
    def __init__(self, exchange: str = 'NYSE'):
        self.calendar = mcal.get_calendar(exchange)
        self.tz = self.calendar.tz

    def get_latest_trading_date(self) -> date:
        end = self._now()
        start = end - TIME_BUFFER
        opens = self.calendar.schedule(start, end, tz=self.tz).market_open
        return opens[opens < end].iloc[-1].date()

    def get_valid_dates(
            self,
            start: Union[date, pd.Timestamp],
            end: Optional[Union[date, pd.Timestamp]] = None,
    ) -> List[date]:
        start = self._as_ts(start)
        end = self._as_ts(end, default_now=True)
        closes = self.calendar.schedule(start, end, tz=self.tz).market_close
        return closes[closes < end].map(lambda ts: ts.date()).to_list()

    def is_market_open(self, ts: Optional[Union[datetime, pd.Timestamp]] = None) -> bool:
        ts = self._as_ts(ts, default_now=True)
        schedule = self.calendar.schedule(ts - TIME_BUFFER, ts + TIME_BUFFER, tz=self.tz)
        return self.calendar.open_at_time(schedule, ts)

    def _as_ts(
            self,
            ts: Union[None, str, date, datetime, pd.Timestamp],
            *,
            default_now: bool = False,
    ) -> pd.Timestamp:
        if ts is None and default_now:
            return self._now()
        elif ts is None:
            raise ValueError

        if not isinstance(ts, pd.Timestamp):
            ts = pd.Timestamp(ts)  # type: ignore
        return ts.tz_convert(self.tz)

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now(tz=self.tz)
