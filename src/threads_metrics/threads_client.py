"""Клиент Threads API для сбора постов."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import httpx

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

    _MAX_ATTEMPTS = 5
    _DEFAULT_INITIAL_BACKOFF_SECONDS = 5.0
    _DEFAULT_BACKOFF_MULTIPLIER = 3.0
    _RATE_LIMIT_INITIAL_BACKOFF_SECONDS = 10.0
    _RATE_LIMIT_BACKOFF_MULTIPLIER = 2.0
    _RATE_LIMIT_ERROR_FRAGMENT = "There have been too many calls for this Threads profile"

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
        self._account_cooldowns: Dict[str, float] = {}

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
        last_exception: Optional[Exception] = None

        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            await self._respect_account_cooldown(account_name)
            url_path = self._build_url_path(path)
            try:
                response = await self._client.get(url_path, params=params, headers=headers)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                last_exception = exc
                response_text = exc.response.text if exc.response else ""
                status_code = exc.response.status_code if exc.response else None
                context = {
                    "url": str(exc.request.url) if exc.request else None,
                    "status_code": status_code,
                    "response_text": response_text,
                    "attempt": attempt,
                    "account_name": account_name,
                }
                logger.error(
                    "Ответ Threads API со статусом %s",
                    status_code if status_code is not None else "unknown",
                    extra={
                        "context": json.dumps(context, ensure_ascii=False),
                        "account_label": account_name,
                    },
                )
                if attempt >= self._MAX_ATTEMPTS:
                    raise
                wait_seconds, reason, source = self._resolve_wait_for_status_error(
                    attempt, exc
                )
                self._schedule_account_cooldown(account_name, wait_seconds)
                await self._sleep_with_logging(
                    wait_seconds,
                    account_name,
                    next_attempt=attempt + 1,
                    reason=reason,
                    source=source,
                    status_code=status_code,
                )
                continue
            except httpx.HTTPError as exc:
                last_exception = exc
                context = {
                    "url": str(getattr(exc, "request", {}).url)
                    if getattr(exc, "request", None)
                    else None,
                    "attempt": attempt,
                    "account_name": account_name,
                }
                logger.error(
                    "Ошибка HTTP при обращении к Threads API",
                    extra={
                        "context": json.dumps(context, ensure_ascii=False),
                        "account_label": account_name,
                    },
                )
                if attempt >= self._MAX_ATTEMPTS:
                    raise
                wait_seconds = self._compute_default_wait(attempt)
                self._schedule_account_cooldown(account_name, wait_seconds)
                await self._sleep_with_logging(
                    wait_seconds,
                    account_name,
                    next_attempt=attempt + 1,
                    reason="http_error",
                    source="static_backoff",
                    status_code=None,
                )
                continue

            self._clear_account_cooldown(account_name)
            return response.json()

        raise ThreadsAPIError("Не удалось получить ответ от Threads API") from last_exception

    async def _respect_account_cooldown(self, account_name: Optional[str]) -> None:
        if not account_name:
            return
        wait_until = self._account_cooldowns.get(account_name)
        if wait_until is None:
            return
        now = self._current_time()
        if wait_until <= now:
            self._account_cooldowns.pop(account_name, None)
            return
        wait_seconds = wait_until - now
        await self._sleep_with_logging(
            wait_seconds,
            account_name,
            next_attempt=None,
            reason="account_cooldown",
            source="scheduled",
            status_code=None,
        )

    def _clear_account_cooldown(self, account_name: Optional[str]) -> None:
        if account_name and account_name in self._account_cooldowns:
            self._account_cooldowns.pop(account_name, None)

    def _schedule_account_cooldown(self, account_name: Optional[str], wait_seconds: float) -> None:
        if not account_name:
            return
        if wait_seconds <= 0:
            self._account_cooldowns.pop(account_name, None)
            return
        self._account_cooldowns[account_name] = self._current_time() + wait_seconds

    async def _sleep_with_logging(
        self,
        wait_seconds: float,
        account_name: Optional[str],
        *,
        next_attempt: Optional[int],
        reason: str,
        source: str,
        status_code: Optional[int],
    ) -> None:
        wait_seconds = max(wait_seconds, 0.0)
        if wait_seconds <= 0:
            return
        wait_milliseconds = int(wait_seconds * 1000)
        context = {
            "account_name": account_name,
            "wait_seconds": wait_seconds,
            "wait_milliseconds": wait_milliseconds,
            "next_attempt": next_attempt,
            "reason": reason,
            "source": source,
            "status_code": status_code,
        }
        logger.info(
            "Ожидание перед повторной попыткой запроса",
            extra={
                "context": json.dumps(context, ensure_ascii=False),
                "account_label": account_name,
            },
        )
        await asyncio.sleep(wait_seconds)

    def _resolve_wait_for_status_error(
        self, attempt: int, exc: httpx.HTTPStatusError
    ) -> Tuple[float, str, str]:
        response = exc.response
        if response is not None and response.status_code == 403:
            response_text = response.text or ""
            if self._RATE_LIMIT_ERROR_FRAGMENT in response_text:
                header_wait, header_source = self._extract_rate_limit_wait(response.headers)
                if header_wait is not None:
                    return header_wait, "threads_profile_rate_limit", header_source
                fallback = self._compute_rate_limit_wait(attempt)
                return fallback, "threads_profile_rate_limit", "fallback_backoff"
        default_wait = self._compute_default_wait(attempt)
        return default_wait, "http_status", "static_backoff"

    def _extract_rate_limit_wait(self, headers: httpx.Headers) -> Tuple[Optional[float], str]:
        retry_after = headers.get("Retry-After")
        if retry_after:
            wait_seconds = self._parse_retry_after(retry_after)
            if wait_seconds is not None:
                return wait_seconds, "retry_after"
        business_usage = headers.get("X-Business-Use-Case-Usage")
        if business_usage:
            wait_seconds = self._parse_usage_header(business_usage)
            if wait_seconds is not None:
                return wait_seconds, "business_use_case"
        return None, "unknown"

    @staticmethod
    def _parse_retry_after(raw_value: str) -> Optional[float]:
        raw_value = raw_value.strip()
        if not raw_value:
            return None
        try:
            return max(float(raw_value), 0.0)
        except ValueError:
            return None

    def _parse_usage_header(self, raw_value: str) -> Optional[float]:
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return None
        estimated = self._find_estimated_time(payload)
        if estimated is None:
            return None
        try:
            return max(float(estimated), 0.0)
        except (TypeError, ValueError):
            return None

    def _find_estimated_time(self, data: Any) -> Optional[float]:
        if isinstance(data, dict):
            if "estimated_time_to_regain_access" in data:
                return data.get("estimated_time_to_regain_access")
            for value in data.values():
                nested = self._find_estimated_time(value)
                if nested is not None:
                    return nested
        elif isinstance(data, list):
            for item in data:
                nested = self._find_estimated_time(item)
                if nested is not None:
                    return nested
        return None

    def _compute_default_wait(self, attempt: int) -> float:
        exponent = max(attempt - 1, 0)
        return self._DEFAULT_INITIAL_BACKOFF_SECONDS * (
            self._DEFAULT_BACKOFF_MULTIPLIER ** exponent
        )

    def _compute_rate_limit_wait(self, attempt: int) -> float:
        exponent = max(attempt - 1, 0)
        return self._RATE_LIMIT_INITIAL_BACKOFF_SECONDS * (
            self._RATE_LIMIT_BACKOFF_MULTIPLIER ** exponent
        )

    @staticmethod
    def _current_time() -> float:
        try:
            loop = asyncio.get_running_loop()
            return loop.time()
        except RuntimeError:
            return time.monotonic()

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
