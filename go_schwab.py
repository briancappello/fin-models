from __future__ import annotations

import os

from pprint import pprint

from schwab.auth import easy_client


SCHWAB_API_KEY = os.getenv("SCHWAB_API_KEY")
SCHWAB_APP_SECRET = os.getenv("SCHWAB_API_SECRET")
CALLBACK_URL = "https://127.0.0.1:8182"
TOKEN_PATH = os.path.expanduser("~/.schwab/token.json")

if __name__ == "__main__":
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)

    schwab = easy_client(
        api_key=SCHWAB_API_KEY,
        app_secret=SCHWAB_APP_SECRET,
        callback_url=CALLBACK_URL,
        token_path=TOKEN_PATH,
    )

    accounts = schwab.get_account_numbers()
    pprint(accounts.json())
