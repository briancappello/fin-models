from __future__ import annotations

import os


class DictClass(type):
    def __getitem__(self, item):
        return getattr(self, item)


class Config(metaclass=DictClass):
    DATA_DIR: str = os.path.expanduser("~/.fin-models-data")
    SYMBOLS_DATA_FILEPATH = os.path.join(DATA_DIR, "symbols.json")
    JSON_WATCHLISTS_PATH = os.path.join(DATA_DIR, "watchlists.json")

    DATABASE_URI: str = "{engine}://{user}:{pw}@{host}:{port}/{db}".format(
        engine=os.getenv("SQLALCHEMY_DATABASE_ENGINE", "postgresql+psycopg2"),
        user=os.getenv("SQLALCHEMY_DATABASE_USER", "fin_models"),
        pw=os.getenv("SQLALCHEMY_DATABASE_PASSWORD", "fin-models"),
        host=os.getenv("SQLALCHEMY_DATABASE_HOST", "127.0.0.1"),
        port=os.getenv("SQLALCHEMY_DATABASE_PORT", 5432),
        db=os.getenv("SQLALCHEMY_DATABASE_NAME", "fin_models"),
    )

    DATABASE_URI: str = "{engine}://{user}:{pw}@{host}:{port}/{db}".format(
        engine=os.getenv("SQLALCHEMY_DATABASE_ENGINE", "postgresql+psycopg2"),
        user=os.getenv("SQLALCHEMY_DATABASE_USER", "fun_techan"),
        pw=os.getenv("SQLALCHEMY_DATABASE_PASSWORD", "fun_techan"),
        host=os.getenv("SQLALCHEMY_DATABASE_HOST", "127.0.0.1"),
        port=os.getenv("SQLALCHEMY_DATABASE_PORT", 5432),
        db=os.getenv("SQLALCHEMY_DATABASE_NAME", "fun_techan"),
    )

    POLYGON_API_KEY: str = os.getenv("POLYGON_API_KEY")
    POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE: int = 5

    ALPACA_API_KEY_LIVE: str = os.getenv("ALPACA_API_KEY_LIVE")
    ALPACA_API_SECRET_LIVE: str = os.getenv("ALPACA_API_SECRET_LIVE")

    ALPACA_API_KEY_PAPER: str = os.getenv("ALPACA_API_KEY_PAPER")
    ALPACA_API_SECRET_PAPER: str = os.getenv("ALPACA_API_SECRET_PAPER")
