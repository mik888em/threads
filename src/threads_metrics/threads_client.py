"""Клиент Threads API для сбора постов."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_random_exponential


class ThreadsAPIError(RuntimeError):
    """Ошибка работы с Threads API."""


@dataclass(slots=True)
class ThreadsPost:
    """Описание поста Threads."""

    id: str
    permalink: str
    data: Dict[str, Any]


@dataclass(slots=True)
class ThreadsFetchResult:
    """Результат получения постов."""

    posts: List[ThreadsPost]
    next_cursor: Optional[str]


class ThreadsClient:
    """Клиент для обращения к Threads Graph API."""

    def __init__(
        self,
        base_url: str,
        timeout: float,
        concurrency_limit: int = 5,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        """Создаёт новый экземпляр клиента.

        Args:
            base_url: Базовый URL Graph API.
            timeout: Таймаут запросов.
            concurrency_limit: Ограничение числа одновременных запросов.
            transport: Необязательный кастомный транспорт для httpx.
        """

        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout, transport=transport)
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._concurrency_limit = concurrency_limit

    async def close(self) -> None:
        """Закрывает клиент."""

        await self._client.aclose()

    @property
    def concurrency_limit(self) -> int:
        """Возвращает установленное ограничение параллелизма."""

        return self._concurrency_limit

    async def fetch_posts(self, access_token: str, after: Optional[str] = None) -> ThreadsFetchResult:
        """Загружает все посты для указанного токена доступа.

        Args:
            access_token: Токен доступа Threads.
            after: Курсор пагинации, с которого начинать выборку.

        Returns:
            Результат с постами и курсором продолжения.
        """

        posts: List[ThreadsPost] = []
        cursor = after
        params: Dict[str, Any] = {"fields": "id,permalink,text,media_type,media_url,like_count,repost_count,reply_count"}
        if after:
            params["after"] = after

        while True:
            async with self._semaphore:
                response_data = await self._request("/me/threads", access_token=access_token, params=params)
            data = response_data.get("data", [])
            for item in data:
                permalink = self._sanitize_permalink(item.get("permalink", ""))
                posts.append(ThreadsPost(id=str(item.get("id")), permalink=permalink, data=item))

            paging = response_data.get("paging", {})
            cursors = paging.get("cursors", {})
            after_cursor = cursors.get("after")
            next_url = paging.get("next")
            if not after_cursor and next_url:
                after_cursor = self._extract_after_from_url(next_url)

            if not after_cursor:
                break

            cursor = after_cursor
            params["after"] = after_cursor

            if not next_url:
                break

        if cursor is None and posts:
            cursor = posts[-1].id
        return ThreadsFetchResult(posts=posts, next_cursor=cursor)

    async def _request(self, path: str, *, access_token: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {access_token}"}

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_random_exponential(multiplier=1, max=30),
            retry=retry_if_exception_type(httpx.HTTPError),
            reraise=True,
        ):
            with attempt:
                response = await self._client.get(path, params=params, headers=headers)
                response.raise_for_status()
                return response.json()
        raise ThreadsAPIError("Не удалось получить ответ от Threads API")

    @staticmethod
    def _sanitize_permalink(permalink: str) -> str:
        prefixes = (
            "https://www.threads.com/",
            "https://www.threads.net/",
        )

        for prefix in prefixes:
            if permalink.startswith(prefix):
                permalink = permalink[len(prefix) :]
                if not permalink.startswith("/"):
                    permalink = f"/{permalink}"
                break

        if "?" in permalink:
            permalink = permalink.split("?", maxsplit=1)[0]

        return permalink

    @staticmethod
    def _extract_after_from_url(url: str) -> Optional[str]:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        after_values = query.get("after")
        if after_values:
            return after_values[0]
        return None


__all__ = ["ThreadsClient", "ThreadsPost", "ThreadsAPIError", "ThreadsFetchResult"]
