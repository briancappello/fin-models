from __future__ import annotations

import os.path
import tempfile
import typing as t

import pandas as pd
import pytest

from pandas.testing import assert_frame_equal, assert_series_equal

from fin_models.enums import Freq
from fin_models.store import Store


DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture()
def store() -> t.Generator[Store, None, None]:
    with tempfile.TemporaryDirectory() as tempdir:
        yield Store(tempdir)


@pytest.fixture()
def full_store() -> t.Generator[Store, None, None]:
    with tempfile.TemporaryDirectory() as tempdir:
        store = Store(tempdir)
        store.write("AMD", Freq.min_1, load_data("AMD", Freq.min_1))
        store.write("INTC", Freq.min_1, load_data("INTC", Freq.min_1))
        store.write("NVDA", Freq.min_1, load_data("NVDA", Freq.min_1))
        store.write("AMD", Freq.day, load_data("AMD", Freq.day))
        store.write("INTC", Freq.day, load_data("INTC", Freq.day))
        store.write("NVDA", Freq.day, load_data("NVDA", Freq.day))
        yield store


def _get_filepath(symbol, freq: Freq) -> str:
    filename = f"{symbol}.{freq.value if freq < Freq.day else freq.name}.json"
    filepath = os.path.join(DATA_DIR, filename)
    return filepath


def save_data(symbol, freq: Freq, df: pd.DataFrame):
    df.to_json(_get_filepath(symbol, freq), orient="split")


def load_data(symbol, freq: Freq) -> pd.DataFrame:
    df = pd.read_json(_get_filepath(symbol, freq), orient="split")
    df.index = df.index.tz_localize("UTC").tz_convert("America/New_York")  # type: ignore
    df.index.name = "Epoch"
    return df[["Open", "High", "Low", "Close", "Volume"]]


class TestEmptyStore:
    def test_get_company_details_returns_none(self, store):
        assert store.get_company_details("AMD") is None


@pytest.mark.parametrize("freq", list(Freq))
class TestEmptyStoreWithFreq:
    def test_no_symbols(self, store, freq):
        assert store.symbols() == []
        assert store.symbols(freq) == []

    def test_has_returns_false(self, store, freq):
        assert store.has("AMD", freq) is False

    def test_has_freq_returns_false(self, store, freq):
        assert store.has_freq("AMD", freq) is False

    def test_get_returns_none(self, store, freq):
        assert store.get("AMD", freq) is None

    def test_get_latest_dt_returns_none(self, store, freq):
        assert store.get_latest_dt("AMD", freq) is None

    def test_get_historical_metadata(self, store, freq):
        assert store.get_historical_metadata("AMD", freq) is None


class TestStoreWithData:
    def test_symbols(self, store):
        symbols = ["AMD", "INTC", "NVDA"]
        for symbol in symbols:
            os.makedirs(os.path.join(store._root_dir, symbol))
        assert store.symbols() == symbols

        for freq in Freq:
            assert store.symbols(freq) == []

    def test_symbols_with_freq(self, full_store):
        symbols = ["AMD", "INTC", "NVDA"]
        frequencies_with_data = [Freq.min_1, Freq.day]
        for freq in frequencies_with_data:
            assert full_store.symbols(freq) == symbols

        for freq in [f for f in Freq if f not in frequencies_with_data]:
            assert full_store.symbols(freq) == []

    def test_has_freq(self, full_store):
        frequencies_with_data = [Freq.min_1, Freq.day]
        for freq in frequencies_with_data:
            for symbol in full_store.symbols(freq):
                assert full_store.has_freq(symbol, freq)

        for freq in [f for f in Freq if f not in frequencies_with_data]:
            for symbol in full_store.symbols(freq):
                assert not full_store.has_freq(symbol, freq)

    def test_get_source_freq(self, full_store):
        min_1_source_frequencies = Freq[: Freq.day]
        day_source_frequencies = Freq[Freq.day :]

        for freq in min_1_source_frequencies:
            assert full_store._get_source_freq("AMD", freq) == Freq.min_1
        for freq in day_source_frequencies:
            assert full_store._get_source_freq("AMD", freq) == Freq.day

        full_store._delete_freq("AMD", Freq.day)
        for freq in Freq:
            assert full_store._get_source_freq("AMD", freq) == Freq.min_1

    def test_get(self, full_store):
        for freq in [Freq.min_1, Freq.day]:
            for symbol in full_store.symbols(freq):
                expected = load_data(symbol, freq)
                df = full_store.get(symbol, freq)
                assert_frame_equal(df, expected)

    def test_agg(self):
        pass

    def test_write(self, store):
        expected = load_data("AMD", Freq.day)
        latest_bar = expected.iloc[-1]
        store.write("AMD", Freq.day, expected)

        assert_frame_equal(store.get("AMD", Freq.day), expected)
        historical_metadata = store.get_historical_metadata("AMD", Freq.day)
        assert historical_metadata.freq == Freq.day
        assert historical_metadata.timezone == "America/New_York"
        assert historical_metadata.first_bar_dt == expected.iloc[0].name
        assert historical_metadata.first_bar_utc == expected.iloc[0].name.astimezone(
            "UTC"
        )
        assert historical_metadata.latest_bar_dt == latest_bar.name
        assert historical_metadata.latest_bar_utc == latest_bar.name.astimezone("UTC")
        assert historical_metadata.Open == latest_bar.Open
        assert historical_metadata.High == latest_bar.High
        assert historical_metadata.Low == latest_bar.Low
        assert historical_metadata.Close == latest_bar.Close
        assert historical_metadata.Volume == latest_bar.Volume
        assert_series_equal(historical_metadata.latest_bar, latest_bar)
        assert_frame_equal(
            historical_metadata.latest_bar_df,
            pd.DataFrame.from_records(
                [
                    dict(
                        Epoch=latest_bar.name,
                        Open=latest_bar.Open,
                        High=latest_bar.High,
                        Low=latest_bar.Low,
                        Close=latest_bar.Close,
                        Volume=int(latest_bar.Volume),
                    )
                ],
                index="Epoch",
            ),
        )
