from __future__ import annotations

import os
import shutil

from datetime import datetime

import pandas as pd

from fin_models.config import Config
from fin_models.dataclasses import CompanyDetails, HistoricalMetadata
from fin_models.enums import Freq
from fin_models.serializers import (
    CompanyDetailsSerializer,
    HistoricalMetadataSerializer,
)


RESAMPLE_COLUMNS = {
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last",
    "Volume": "sum",
}


class Store:
    # FIXME: take parametrized data vendor and trading calendar?
    def __init__(self, _root_dir: str | None = None):
        self._root_dir = _root_dir or os.path.join(Config.DATA_DIR, "symbol-data")
        os.makedirs(self._root_dir, exist_ok=True)

    def get(
        self,
        symbol: str,
        freq: Freq = Freq.day,
        columns=("Open", "High", "Low", "Close", "Volume"),
    ) -> pd.DataFrame | None:
        """
        Return all historical data for the given symbol at the requested frequency.
        """
        source_freq = self._get_source_freq(symbol, freq)
        if not source_freq:
            return None

        df = pd.read_pickle(self._path(symbol, source_freq))[list(columns)]
        if source_freq == freq or df.empty:
            return df
        return self.agg(df, freq)

    def get_company_details(self, symbol: str) -> CompanyDetails | None:
        filepath = self._company_details_path(symbol)
        if not os.path.exists(filepath):
            return None

        with open(filepath) as f:
            return CompanyDetailsSerializer().loads(f.read())

    def get_historical_metadata(
        self, symbol: str, freq: Freq
    ) -> HistoricalMetadata | None:
        filepath = self._historical_metadata_path(symbol, freq)
        if not os.path.exists(filepath):
            return self._write_historical_metadata(symbol, freq, self.get(symbol, freq))

        with open(filepath) as f:
            return HistoricalMetadataSerializer().loads(f.read())

    def get_latest_dt(self, symbol: str, freq: Freq) -> datetime | None:
        data = self.get_historical_metadata(symbol, freq)
        if not data:
            return None
        return data.latest_bar_dt

    def has(self, symbol: str, freq: Freq = Freq.day) -> bool:
        """
        Returns true if the store has data for the given symbol and frequency.
        """
        return bool(self._get_source_freq(symbol, freq))

    def has_freq(self, symbol: str, freq: Freq) -> bool:
        """
        Returns true if we have data for the given symbol and exact frequency.
        """
        return os.path.exists(self._path(symbol, freq))

    def symbols(self, freq: Freq | None = None) -> list[str]:
        """
        Get a list of all ticker symbols in the store.
        """
        return list(
            sorted(
                {
                    dir_entry.name
                    for dir_entry in os.scandir(self._root_dir)
                    if dir_entry.is_dir()
                    and not dir_entry.name.startswith(".")
                    and (
                        self.has_freq(dir_entry.name, freq) if freq is not None else True
                    )
                }
            )
        )

    def append(self, symbol: str, freq: Freq, bars: pd.DataFrame) -> pd.DataFrame:
        if bars.empty:
            return self.get(symbol, freq)

        if not self.has_freq(symbol, freq):
            # FIXME should we just call self.write() here?
            raise NotImplementedError(
                f"{freq} does not exist on-disk for {symbol}, cannot append"
            )

        df = self.get(symbol, freq)
        try:
            # slice off overlapping timestamps (prefer new values over existing)
            df = df.iloc[: df.index.get_loc(bars.iloc[0].name)]
        except KeyError:
            pass

        new_df = pd.concat([df, bars])
        self.write(symbol, freq, new_df)
        return new_df

    @staticmethod
    def agg(df: pd.DataFrame, to_freq: Freq) -> pd.DataFrame:
        if df.empty:
            return df

        resample_freq = {Freq.month: "MS", Freq.year: "YS"}.get(to_freq, to_freq.value)
        agg_df = df.resample(resample_freq).apply(RESAMPLE_COLUMNS).ffill()
        if to_freq == Freq.week:
            agg_df.index = agg_df.index - pd.Timedelta(days=6)
        return agg_df

    def write(self, symbol: str, freq: Freq, df: pd.DataFrame) -> None:
        self._write_historical_metadata(symbol, freq, df)
        df.to_pickle(self._path(symbol, freq))

    def write_company_details(self, symbol: str, data: CompanyDetails) -> None:
        with open(self._company_details_path(symbol), "w") as f:
            f.write(CompanyDetailsSerializer().dumps(data))

    def _write_historical_metadata(
        self, symbol: str, freq: Freq, df: pd.DataFrame
    ) -> HistoricalMetadata | None:
        if df is None or df.empty:
            return

        bar = df.iloc[-1]
        data = HistoricalMetadata(
            freq=freq,
            first_bar_utc=df.iloc[0].name,  # type: ignore
            latest_bar_utc=bar.name,  # type: ignore
            Open=bar.Open,
            High=bar.High,
            Low=bar.Low,
            Close=bar.Close,
            Volume=bar.Volume,
        )
        with open(self._historical_metadata_path(symbol, freq), "w") as f:
            f.write(HistoricalMetadataSerializer().dumps(data))
        return data

    def _company_details_path(self, symbol: str):
        filepath = os.path.join(
            self._root_dir,
            symbol.upper(),
            "company-details.json",
        )
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath

    def _historical_metadata_path(self, symbol: str, freq: Freq):
        filepath = os.path.join(
            self._root_dir,
            symbol.upper(),
            f"{self._freq_filename(freq)}.json",
        )
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath

    def _get_source_freq(self, symbol: str, freq: Freq) -> Freq | None:
        if freq == Freq[0]:
            return freq if self.has_freq(symbol, freq) else None

        possible_frequencies = (
            reversed(Freq) if freq == Freq[-1] else reversed(Freq[: freq + 1])
        )
        for source_freq in possible_frequencies:
            if self.has_freq(symbol, source_freq):
                return source_freq
        return None

    def _freq_filename(self, freq: Freq) -> str:
        return freq.value if freq < Freq.day else freq.name

    def _path(self, symbol: str, freq: Freq) -> str:
        filepath = os.path.join(
            self._root_dir,
            symbol.upper(),
            f"{self._freq_filename(freq)}.pickle",
        )
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath

    def _delete_freq(self, symbol: str, freq: Freq):
        filepath = self._path(symbol, freq)
        if os.path.exists(filepath):
            os.remove(filepath)

    def _delete_all(self, symbol):
        shutil.rmtree(os.path.join(self._root_dir, symbol.upper()))
