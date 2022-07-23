import os

from typing import Optional

import pandas as pd

from fin_models.config import Config


class Store:
    def __init__(self):
        if not os.path.exists(Config.DATA_DIR):
            os.mkdir(Config.DATA_DIR)

    def has(self, symbol: str):
        return os.path.exists(self._path(symbol))

    def read(self, symbol: str) -> Optional[pd.DataFrame]:
        if not self.has(symbol):
            return None
        return pd.read_pickle(self._path(symbol))

    def write(self, symbol: str, df: pd.DataFrame):
        df.to_pickle(self._path(symbol))

    @staticmethod
    def _path(symbol: str):
        return f'{Config.DATA_DIR}/{symbol.upper()}.pickle'
