from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fin_models.dataclasses import HistoricalMetadata
from fin_models.enums import Freq


class TestHistoricalMetadata:
    def test_date_properties(self):
        data = dict(
            freq=Freq.day,
            first_bar_utc=datetime.now(timezone.utc),
            latest_bar_utc=datetime.now(timezone.utc),
            Open=1,
            High=2,
            Low=3,
            Close=4,
            Volume=3.1,
        )
        historical_metadata = HistoricalMetadata(**data)
        assert isinstance(historical_metadata.first_bar_utc, pd.Timestamp)
        assert isinstance(historical_metadata.first_bar_dt, pd.Timestamp)
        assert isinstance(historical_metadata.latest_bar_utc, pd.Timestamp)
        assert isinstance(historical_metadata.latest_bar_dt, pd.Timestamp)
