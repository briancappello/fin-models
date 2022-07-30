from dataclasses import dataclass, asdict
import json
import os

from datetime import date
from typing import List, Optional

import pandas as pd

from fin_models.config import Config


@dataclass
class Metadata:
    latest: Optional[date] = None


class Store:
    def __init__(self):
        if not os.path.exists(Config.DATA_DIR):
            os.mkdir(Config.DATA_DIR)
        self._metadata_path = os.path.join(Config.DATA_DIR, '.metadata.json')

    @staticmethod
    def symbols() -> List[str]:
        ext_len = len('.pickle')
        return [filename[:-ext_len]
                for filename in os.listdir(Config.DATA_DIR)
                if filename[0] != '.']

    def has(self, symbol: str) -> bool:
        return os.path.exists(self._path(symbol))

    def read(self, symbol: str) -> Optional[pd.DataFrame]:
        if not self.has(symbol):
            return None
        return pd.read_pickle(self._path(symbol))

    def write(self, symbol: str, df: pd.DataFrame) -> None:
        df.to_pickle(self._path(symbol))

    def append(self, symbol: str, bars: pd.DataFrame) -> pd.DataFrame:
        df = self.read(symbol)
        new = pd.concat([df, bars])
        # self.write(symbol, new)
        return new

    @property
    def metadata(self) -> Metadata:
        return Metadata(**self._read_metadata())

    def _read_metadata(self) -> dict:
        if not os.path.exists(self._metadata_path):
            return {}
        with open(self._metadata_path) as f:
            return json.load(f)

    def _write_metadata(self, metadata: Metadata) -> None:
        with open(self._metadata_path, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)

    @staticmethod
    def _path(symbol: str) -> str:
        return f'{Config.DATA_DIR}/{symbol.upper()}.pickle'
