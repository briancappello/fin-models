from __future__ import annotations

import re

from datetime import datetime
from typing import Annotated

from sqlalchemy import TIMESTAMP, BigInteger, MetaData, create_engine, func
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    declared_attr,
    mapped_column,
    sessionmaker,
)
from sqlalchemy.orm import (
    registry as SQLAlchemyRegistry,
)

from .config import Config


registry = SQLAlchemyRegistry(
    metadata=MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        },
    ),
    type_annotation_map={
        int: BigInteger,
        datetime: TIMESTAMP(timezone=True),
    },
)

# types
pk = Annotated[int, mapped_column(primary_key=True)]


class Base(DeclarativeBase):
    __abstract__ = True

    registry = registry

    created_at: Mapped[datetime] = mapped_column(default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())

    @declared_attr
    def __tablename__(cls):
        # convert ClassName to snake_case
        s = re.sub(r"([a-z0-9])([A-Z])", "\\1_\\2", cls.__name__)
        return re.sub(r"([A-Z])([A-Z][a-z])", "\\1_\\2", s).lower()


engine = create_engine(Config.DATABASE_URI)
session_factory = sessionmaker(bind=engine)
