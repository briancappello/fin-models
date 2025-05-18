from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fin_models.enums import Freq


@dataclass(kw_only=True)
class Bar:
    freq: Freq
    symbol: str
    Epoch: pd.Timestamp  # start of bar
    EpochClose: pd.Timestamp  # end of bar
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int
    VWAP: float

    @classmethod
    def from_ws_msg(cls, msg: dict):
        return cls(
            freq=dict(AM=Freq.min_1)[msg["ev"]],
            symbol=msg["sym"],
            Epoch=pd.Timestamp(msg["s"] * 1_000_000, tz="UTC").tz_convert(
                "America/New_York"
            ),
            EpochClose=pd.Timestamp(msg["e"] * 1_000_000, tz="UTC").tz_convert(
                "America/New_York"
            ),
            Open=msg["o"],
            High=msg["h"],
            Low=msg["l"],
            Close=msg["c"],
            Volume=int(msg["v"]),
            VWAP=msg["vw"],
        )
