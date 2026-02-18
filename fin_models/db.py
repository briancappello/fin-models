from __future__ import annotations

import json

from datetime import date, datetime

import numpy as np

from sqlalchemy_unchained import *
from sqlalchemy_unchained import _relationship, _wrap_with_default_query_class

from .config import Config


def json_dumps(*a, **kw):
    def default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        elif isinstance(o, np.bool_):
            return bool(o)
        elif isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)

        raise TypeError(
            f"Unable to dump to json type={type(o)}, please implement a default encoder for this type"
        )

    return json.dumps(*a, default=default, **kw)


engine_kwargs = dict(
    isolation_level=(
        "REPEATABLE READ" if Config.DATABASE_URI.startswith("postgresql") else None
    ),
    json_serializer=json_dumps,
)

engine = create_engine(Config.DATABASE_URI, **engine_kwargs)

Session = scoped_session_factory(
    bind=engine,
    query_cls=Query,
)
SessionManager.set_session_factory(Session)
Model = declarative_base(bind=engine)
relationship = _wrap_with_default_query_class(_relationship, Query)
