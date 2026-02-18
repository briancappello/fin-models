from __future__ import annotations

import os

import pandas as pd

from fin_models.data_classes import Freq


DATA_DIR = os.path.dirname(os.path.abspath(__file__))


def _get_filepath(symbol, freq: Freq) -> str:
    filename = f"{symbol.upper()}.{freq.value if Freq.second < freq < Freq.day else freq.name}.json"
    filepath = os.path.join(DATA_DIR, filename)
    return filepath


def save_data(symbol, freq: Freq, df: pd.DataFrame):
    df.to_json(_get_filepath(symbol, freq), orient="split")


def load_data(symbol, freq: Freq) -> pd.DataFrame:
    df = pd.read_json(_get_filepath(symbol, freq), orient="split")
    df.index = df.index.tz_localize("UTC").tz_convert("America/New_York")  # type: ignore
    df.index.name = "Epoch"
    return df[["Open", "High", "Low", "Close", "Volume"]]
