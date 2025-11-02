"""Клиент Threads API для сбора постов."""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_random_exponential

logger = logging.getLogger(__name__)


INSIGHTS_METRICS: tuple[str, ...] = (
    "views",
    "likes",
    "replies",
    "reposts",
    "quotes",
    "shares",
)


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
        api_version: str = "v1.0",
        posts_url_override: Optional[str] = None,
    ) -> None:
        """Создаёт новый экземпляр клиента.

        Args:
            base_url: Базовый URL Graph API.
            timeout: Таймаут запросов.
            concurrency_limit: Ограничение числа одновременных запросов.
            transport: Необязательный кастомный транспорт для httpx.
        """

        self._base_url = base_url.rstrip("/")
        self._api_prefix = f"/{api_version.strip('/')}" if api_version else ""
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout, transport=transport)
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._concurrency_limit = concurrency_limit
        self._posts_path = "/me/threads"
        self._posts_params: Dict[str, Any] = {
            "fields": "id,permalink,text,timestamp,media_type,media_url,like_count,repost_count,reply_count",
        }
        self._configure_posts_override(posts_url_override)

    async def close(self) -> None:
        """Закрывает клиент."""

        await self._client.aclose()

    @property
    def concurrency_limit(self) -> int:
        """Возвращает установленное ограничение параллелизма."""

        return self._concurrency_limit

    async def fetch_posts(
        self, access_token: str, after: Optional[str] = None, *, account_name: Optional[str] = None
    ) -> ThreadsFetchResult:
        """Загружает все посты для указанного токена доступа.

        Args:
            access_token: Токен доступа Threads.
            after: Курсор пагинации, с которого начинать выборку.

        Returns:
            Результат с постами и курсором продолжения.
        """

        posts: List[ThreadsPost] = []
        cursor = after
        params: Dict[str, Any] = dict(self._posts_params)
        if after:
            params["after"] = after

        while True:
            async with self._semaphore:
                response_data = await self._request(
                    self._posts_path,
                    access_token=access_token,
                    params=params,
                    account_name=account_name,
                )
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
            params = dict(self._posts_params)
            params["after"] = after_cursor

            if not next_url:
                break

        if cursor is None and posts:
            cursor = posts[-1].id
        return ThreadsFetchResult(posts=posts, next_cursor=cursor)

    async def _request(
        self,
        path: str,
        *,
        access_token: str,
        params: Optional[Dict[str, Any]] = None,
        account_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {access_token}"}

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_random_exponential(multiplier=1, max=30),
            retry=retry_if_exception_type(httpx.HTTPError),
            reraise=True,
        ):
            with attempt:
                url_path = self._build_url_path(path)
                response = await self._client.get(url_path, params=params, headers=headers)
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    attempt_number = attempt.retry_state.attempt_number
                    response_text = exc.response.text if exc.response else ""
                    context = {
                        "url": str(exc.request.url) if exc.request else None,
                        "status_code": exc.response.status_code if exc.response else None,
                        "response_text": response_text,
                        "attempt": attempt_number,
                        "account_name": account_name,
                    }
                    logger.error(
                        "Ответ Threads API со статусом %s",
                        exc.response.status_code if exc.response else "unknown",
                        extra={
                            "context": json.dumps(context, ensure_ascii=False),
                            "account_label": account_name,
                        },
                    )
                    raise
                return response.json()
        raise ThreadsAPIError("Не удалось получить ответ от Threads API")

    async def fetch_post_insights(
        self, access_token: str, post_id: str, *, account_name: Optional[str] = None
    ) -> Dict[str, int]:
        """Возвращает метрики Insights для указанного поста."""

        metrics = INSIGHTS_METRICS
        params = {"metric": ",".join(metrics)}
        async with self._semaphore:
            data = await self._request(
                f"/{post_id}/insights",
                access_token=access_token,
                params=params,
                account_name=account_name,
            )

        insights: Dict[str, int] = {metric: 0 for metric in metrics}
        for item in data.get("data", []):
            metric_name = item.get("name", "")
            if not metric_name:
                continue
            if metric_name not in insights:
                continue
            values = item.get("values") or []
            if not values:
                continue
            value = values[0].get("value")
            try:
                insights[metric_name] = int(value)
            except (TypeError, ValueError):
                continue
        return insights

    def _build_url_path(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        if self._api_prefix and not path.startswith(self._api_prefix):
            return f"{self._api_prefix}{path}"
        return path

    def build_absolute_url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Строит абсолютный URL запроса."""

        relative = self._build_url_path(path)
        request = self._client.build_request("GET", relative, params=params)
        return str(request.url)

    def _configure_posts_override(self, posts_url_override: Optional[str]) -> None:
        if not posts_url_override:
            return

        parsed = urlparse(posts_url_override)
        if parsed.scheme and parsed.netloc:
            override_base = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
            if override_base and override_base != self._base_url:
                logger.warning(
                    "Игнорируем override URL постов: домен не совпадает с базовым URL",
                )
                return

        if parsed.path:
            path = parsed.path
            if not path.startswith("/"):
                path = f"/{path}"
            self._posts_path = path

        query_params = parse_qs(parsed.query, keep_blank_values=True) if parsed.query else {}
        if not query_params:
            return

        filtered_params: Dict[str, Any] = {}
        for key, values in query_params.items():
            if not values:
                continue
            if key == "after":
                continue
            filtered_params[key] = values[0] if len(values) == 1 else values

        if filtered_params:
            self._posts_params = filtered_params

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
