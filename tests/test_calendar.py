from __future__ import annotations

import pytest

from fin_models.calendar import Calendar


@pytest.fixture(scope="session")
def nyse():
    yield Calendar(exchange="NYSE")


friday_is_open = "2023-07-07"
saturday_is_closed = "2023-07-08"


class TestCalendar:
    def test_open_regular_hours(self, nyse):
        assert not nyse.is_market_open(f"{friday_is_open} 09:29:59")
        assert nyse.is_market_open(f"{friday_is_open} 09:30:00")
        assert nyse.is_market_open(f"{friday_is_open} 15:59:59")
        assert not nyse.is_market_open(f"{friday_is_open} 16:00:00")

    def test_open_extended_hours(self, nyse):
        assert not nyse.is_market_open(
            f"{friday_is_open} 03:59:59",
            include_extended=True,
        )
        assert nyse.is_market_open(
            f"{friday_is_open} 04:00:00",
            include_extended=True,
        )
        assert nyse.is_market_open(
            f"{friday_is_open} 19:59:59",
            include_extended=True,
        )
        assert not nyse.is_market_open(
            f"{friday_is_open} 20:00:00",
            include_extended=True,
        )

    def test_closed_weekends(self, nyse):
        assert not nyse.is_market_open(saturday_is_closed)
