from datetime import date, datetime
from typing import Dict, Union
from urllib.parse import urlencode

import pandas as pd
import requests

from fin_models.config import Config


HOST = 'https://api.polygon.io'


def get(url: str, query_params: dict) -> dict:
    query_params.update(dict(apiKey=Config.POLYGON_API_KEY))
    r = requests.get(f'{HOST}/{url.strip("/")}?{urlencode(query_params)}')
    r.raise_for_status()
    return r.json()


def daily_bars(
        date_: Union[str, date, datetime, pd.Timestamp],
        adjusted: bool = True,
        include_otc: bool = False,
) -> Dict[str, pd.DataFrame]:
    if not isinstance(date_, pd.Timestamp):
        date_ = pd.Timestamp(date_)  # type: ignore
    date_str = (date_ or pd.Timestamp.now()).strftime('%Y-%m-%d')
    data = get(f'/v2/aggs/grouped/locale/us/market/stocks/{date_str}', dict(
        adjusted=adjusted,
        include_otc=include_otc,
    ))
    return {
        d['T']: pd.DataFrame.from_records([
            dict(
                Epoch=pd.Timestamp(d['t'] * 1000 * 1000, tz='UTC').tz_convert('America/New_York'),
                Open=d['o'],
                High=d['h'],
                Low=d['l'],
                Close=d['c'],
                Volume=int(d['v']),
            ),
        ], index='Epoch')
        for d in data['results']
        if d['T'].isupper()
        and '.' not in d['T']
    }
