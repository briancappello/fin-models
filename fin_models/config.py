from __future__ import annotations

import os


class Config:
    DATA_DIR: str = os.path.expanduser("~/.fin-models-data")
    SYMBOLS_DATA_FILEPATH = os.path.join(DATA_DIR, "symbols.json")

    DATABASE_URI: str = "{engine}://{user}:{pw}@{host}:{port}/{db}".format(
        engine=os.getenv("SQLALCHEMY_DATABASE_ENGINE", "postgresql+psycopg2"),
        user=os.getenv("SQLALCHEMY_DATABASE_USER", "fin_models"),
        pw=os.getenv("SQLALCHEMY_DATABASE_PASSWORD", "fin-models"),
        host=os.getenv("SQLALCHEMY_DATABASE_HOST", "127.0.0.1"),
        port=os.getenv("SQLALCHEMY_DATABASE_PORT", 5432),
        db=os.getenv("SQLALCHEMY_DATABASE_NAME", "fin_models"),
    )

    POLYGON_API_KEY: str = os.getenv("POLYGON_API_KEY")
    POLYGON_NUM_HISTORICAL_YEARS_AVAILABLE: int = 5
