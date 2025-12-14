from __future__ import annotations

from alpaca.trading.client import TradingClient as AlpacaTradingClient

from fin_models.trading.local_trading_client import TradingClient as LocalTradingClient
from fin_models.trading.trading_client import TradingClient


if __name__ == "__main__":
    client = TradingClient(LocalTradingClient, paper=False)

    print(client.get_asset("nvda"))
