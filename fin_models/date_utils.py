from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Union
from zoneinfo import ZoneInfo

import pandas as pd


DateType = Union[date, datetime, pd.Timestamp]

UTC = timezone.utc
EASTERN_TZ = ZoneInfo("America/New_York")


def utc_now() -> pd.Timestamp:
    return pd.Timestamp("now", tz=UTC)


def to_ts(
    dt: DateType | str | None,
    default: DateType | str = "now",
    _naive_assumed_tz=EASTERN_TZ,
) -> pd.Timestamp:
    """
    Returns input converted to pd.Timestamp with timezone UTC.

    If timezone is missing from input, assume America/New_York.
    """
    if dt:
        ts = pd.Timestamp(dt)
    else:
        if default == "now":
            return utc_now()
        ts = pd.Timestamp(default)

    is_date = False
    if ts == pd.Timestamp(ts.date()):
        is_date = True

    if not ts.tz:
        ts = ts.tz_localize(_naive_assumed_tz)
    ts = ts.astimezone(UTC)

    if is_date:
        return ts.replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    return ts


def isodate(dt: DateType | str | None) -> str:
    return to_ts(dt).strftime("%Y-%m-%d")
