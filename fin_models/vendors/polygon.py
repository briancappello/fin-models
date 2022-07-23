from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

import requests

from fin_models.config import Config


HOST = 'https://api.polygon.io'


def get(url, query_params):
    query_params.update(dict(apiKey=Config.POLYGON_API_KEY))
    r = requests.get(f'{HOST}/{url.strip("/")}?{urlencode(query_params)}')
    r.raise_for_status()
    return r.json()


def daily_bars(date: Optional[datetime] = None,
               adjusted: bool = True,
               include_otc: bool = False) -> dict:
    date_str = (date or datetime.now()).strftime('%Y-%m-%d')
    date_str = '2022-07-08'
    data = get(f'/v2/aggs/grouped/locale/us/market/stocks/{date_str}', dict(
        adjusted=adjusted,
        include_otc=include_otc,
    ))
    return {
        d['T']: dict(
            Timestamp=d['t'],  # FIXME convert to proper datetime (unix timestamp for start of the aggregate window)
            Open=d['o'],
            High=d['h'],
            Low=d['l'],
            Close=d['c'],
            Volume=int(d['v']),
        )
        for d in data['results']
        if d['T'].isupper()
        and '.' not in d['T']
    }
