import json
import os

from dataclasses import asdict, dataclass
from datetime import date
from typing import List, Optional

import pandas as pd

from fin_models.config import Config


RESAMPLE_COLUMNS = {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                    "Volume": "sum",
                }


@dataclass
class Metadata:
    latest: Optional[date] = None


class Store:
    def __init__(self, root_dir=None, timeframe: str = "day"):
        self.root_dir = os.path.join(root_dir or Config.DATA_DIR, timeframe)
        self.timeframe = timeframe
        os.makedirs(self.root_dir, exist_ok=True)
        self._metadata_path = os.path.join(self.root_dir, ".metadata.json")

    def symbols(self) -> List[str]:
        ext_len = len(".pickle")
        return [
            filename[:-ext_len]
            for filename in os.listdir(self.root_dir)
            if filename.endswith(".pickle")
        ]

    def has(self, symbol: str) -> bool:
        return os.path.exists(self._path(symbol))

    def get(self, symbol: str) -> Optional[pd.DataFrame]:
        if not self.has(symbol):
            return None
        return pd.read_pickle(self._path(symbol))

    def agg(self, df: pd.DataFrame, to_timeframe):
        if self.timeframe == "minute":
            # df.resample('5Min').apply(RESAMPLE_COLUMNS).ffill()
            raise NotImplementedError
        elif self.timeframe == "day":
            if to_timeframe in {"D", "1D"}:
                return df
            elif to_timeframe in {"W", "1W"}:
                new = df.resample("W").apply(RESAMPLE_COLUMNS)
                new.index = new.index - pd.Timedelta(days=6)
                return new
            elif to_timeframe in {"M", "1M"}:
                return df.resample("MS").apply(RESAMPLE_COLUMNS)
        raise NotImplementedError

    def write(self, symbol: str, df: pd.DataFrame) -> None:
        df.to_pickle(self._path(symbol))

    def append(self, symbol: str, bars: pd.DataFrame) -> pd.DataFrame:
        df = self.get(symbol)
        try:
            df = df.iloc[: df.index.get_loc(bars.iloc[0].name)]
        except KeyError:
            pass
        new = pd.concat([df, bars])
        self.write(symbol, new)
        return new

    def __getitem__(self, symbol):
        if not self.has(symbol):
            raise KeyError
        return self.get(symbol)

    def __setitem__(self, symbol, value):
        self.write(symbol, value)

    @property
    def metadata(self) -> Metadata:
        return Metadata(**self._read_metadata())

    def _read_metadata(self) -> dict:
        if not os.path.exists(self._metadata_path):
            return {}
        with open(self._metadata_path) as f:
            return json.load(f)

    def _write_metadata(self, metadata: Metadata) -> None:
        with open(self._metadata_path, "w") as f:
            json.dump(asdict(metadata), f, indent=2)

    def _path(self, symbol: str) -> str:
        return f"{self.root_dir}/{symbol.upper()}.pickle"
