from __future__ import annotations

import os

import databento
import pandas as pd

from fin_models import nyse


def go(symbol, start_dt):
    client = databento.Historical(os.getenv("DATABENTO_API_KEY"))
    df = client.timeseries.get_range(
        "EQUS.MINI",
        start=start_dt,
        # end=pd.Timestamp("now", tz="America/New_York") - pd.Timedelta(minutes=4),
        symbols=symbol,
        schema=databento.Schema.OHLCV_1M,
    ).to_df()

    print(df)


if __name__ == "__main__":
    go("amd", start_dt="2025-12-11T04:00:00-0500")
