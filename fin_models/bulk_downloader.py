from __future__ import annotations

import asyncio
import json

from typing import List, Tuple, Union

import aiohttp


class AsyncResponse:
    def __init__(
        self, url: str, content: bytes, content_type: str, http_status_code: int
    ):
        self.url: str = url
        self.content: bytes = content
        self.content_type: str = content_type
        self.http_status_code: int = http_status_code

    @property
    def json(self) -> dict | list:
        if self.content_type == "application/json":
            return json.loads(self.content)

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")

    @property
    def is_success(self) -> bool:
        return self.http_status_code == 200

    @property
    def is_exception(self) -> bool:
        return False


class AsyncException:
    def __init__(self, url, e: Exception):
        self.url = url
        self.exception = e

    @property
    def is_success(self) -> bool:
        return False

    @property
    def is_exception(self) -> bool:
        return True


async def dl(
    session: aiohttp.ClientSession,
    url: str,
) -> Union[AsyncResponse, AsyncException]:
    try:
        async with session.get(url) as r:
            content = await r.read()
            content_type = r.headers["content-type"]
            return AsyncResponse(url, content, content_type, r.status)
    except Exception as e:
        return AsyncException(url, e)


async def dl_all(
    all_urls: List[str],
    batch_size: int = 20,
) -> Tuple[List[AsyncResponse], List[AsyncResponse], List[AsyncException]]:
    successes = []
    errors = []
    exceptions = []
    conn = aiohttp.TCPConnector(limit=0, limit_per_host=batch_size)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [dl(session, url) for url in all_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes.extend([r for r in results if r.is_success])
        errors.extend([r for r in results if not r.is_success and not r.is_exception])
        exceptions.extend([r for r in results if r.is_exception])
    return successes, errors, exceptions


def bulk_download(
    urls: list[str],
) -> Tuple[List[AsyncResponse], List[AsyncResponse], List[AsyncException]]:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(dl_all(urls))
