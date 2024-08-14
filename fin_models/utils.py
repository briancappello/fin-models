import html
import re

from datetime import datetime, timezone

import pandas as pd
import requests

from bs4 import BeautifulSoup


TICKER_IN_PARENTHESIS_RE = re.compile(r"(?P<company_name>.+) \((?P<ticker>[A-Z]+)\)")


def get_soup(url, **kwargs) -> BeautifulSoup:
    """Returns an instance of BeautifulSoup for the given URL"""
    return BeautifulSoup(requests.get(url, **kwargs).content, "lxml")


def table_to_df(table, index_col=None, columns=None) -> pd.DataFrame:
    """Converts an HTML table to a DataFrame

    Uses the first row as the column labels, converted to snakecase
    """
    header, *rows = table.find_all("tr")
    cols = columns or [
        re.sub(r"[^a-z%]", " ", th.text.strip().lower()).strip().replace(" ", "_")
        for th in header.find_all(["td", "th"])
    ]
    rows = [
        list(td.text.strip() for td in tr.find_all(["td", "th"]))[: len(cols)]
        for tr in rows
    ]
    df = pd.DataFrame(rows, columns=cols)
    if index_col:
        df.set_index(index_col, inplace=True)
    return df


def get_wiki_table_df(url, index_col=None, columns=None):
    """Returns the first table of a Wikipedia page as a DataFrame"""
    soup = get_soup(url)
    table = soup.find("table", attrs={"class": "wikitable"})
    return table_to_df(table, index_col, columns)


def wiki_components_list_to_df(list_tag):
    d = {"ticker": [], "company_name": []}
    for li in list_tag.find_all("li"):
        match = TICKER_IN_PARENTHESIS_RE.search(li.text)
        d["ticker"].append(match.group("ticker"))
        d["company_name"].append(match.group("company_name"))

    return pd.DataFrame(d).set_index("ticker")


def chunk(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def utcnow() -> datetime:
    """
    Returns a current timezone-aware ``datetime.datetime`` in UTC.
    """
    return datetime.now(timezone.utc)


def str_strip(s):
    try:
        return s.strip()
    except AttributeError:
        return s


def to_float(f):
    try:
        return float(f)
    except ValueError:
        return f


def to_int(i):
    try:
        return int(i)
    except ValueError:
        return i


def kmbt_to_int(s):
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
    for suffix, multiplier in multipliers.items():
        if s.endswith(suffix):
            return int(float(s.replace(",", "").replace(suffix, "")) * multiplier)
    return int(s.replace(",", ""))


def to_percent(s):
    try:
        return float(s.rstrip("%"))
    except ValueError:
        return s


def html_unescape(s):
    try:
        return html.unescape(s)
    except TypeError:
        return s
