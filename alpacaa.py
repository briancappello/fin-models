from __future__ import annotations

from alpaca.trading.client import TradingClient as AlpacaTradingClient

from fin_models.trading.client import TradingClient
from fin_models.trading.private_alpaca import TradingClient as PrivateTradingClient


if __name__ == "__main__":
    client = TradingClient(PrivateTradingClient, paper=False)

    print(client.get_asset("nvda"))
