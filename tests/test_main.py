"""Тесты для функций основного модуля."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

import httpx
import pytest

from threads_metrics.aggregation import aggregate_posts
from threads_metrics.constants import PUBLISH_TIME_COLUMN
from threads_metrics.google_sheets import AccountToken
from threads_metrics.main import collect_posts
from threads_metrics.threads_client import ThreadsFetchResult, ThreadsPost


def test_aggregate_posts_merges_insights() -> None:
    """Проверяет объединение постов с данными Insights."""

    posts = [
        {
            "id": "1",
            "account_name": "acc",
            "permalink": "https://example.com/1",
            "text": "post 1",
            "like_count": 10,
            "reply_count": 2,
            "repost_count": 3,
            "timestamp": "2025-10-06T19:16:42+0000",
        },
        {
            "id": "2",
            "account_name": "acc",
            "permalink": "https://example.com/2",
            "text": "post 2",
            "like_count": 5,
            "reply_count": 1,
            "repost_count": 0,
            "timestamp": "2025-10-07T01:00:00+0000",
        },
    ]
    insights = {
        "1": {
            "views": 120,
            "likes": 15,
            "replies": 4,
            "reposts": 6,
            "quotes": 2,
            "shares": 3,
        }
    }

    aggregated = aggregate_posts(posts, insights)

    first = next(item for item in aggregated if item["post_id"] == "1")
    second = next(item for item in aggregated if item["post_id"] == "2")

    assert first[PUBLISH_TIME_COLUMN] == "2025-10-06T22:16:42+03:00"
    assert first["views"] == 120
    assert first["likes"] == 15
    assert first["replies"] == 4
    assert first["reposts"] == 6
    assert first["quotes"] == 2
    assert first["shares"] == 3
    assert "like_count" not in first
    assert "reply_count" not in first
    assert "repost_count" not in first

    assert second[PUBLISH_TIME_COLUMN] == "2025-10-07T04:00:00+03:00"
    assert second["views"] is None
    assert second["likes"] == 5
    assert second["replies"] == 1
    assert second["reposts"] == 0
    assert second["quotes"] is None
    assert second["shares"] is None
    assert "like_count" not in second
    assert "reply_count" not in second
    assert "repost_count" not in second


@dataclass
class _StubClient:
    responses: Dict[str, object]
    concurrency_limit: int = 2

    async def fetch_posts(self, token: str, after: Optional[str] = None) -> ThreadsFetchResult:
        result = self.responses[token]
        if isinstance(result, Exception):
            raise result
        return result


class _StubSheets:
    def __init__(self, cursors: Optional[Dict[str, Optional[str]]] = None) -> None:
        self.cursors = cursors or {}

    def get_last_processed_cursor(self, account_name: str) -> Optional[str]:
        return self.cursors.get(account_name)

    def set_last_processed_cursor(self, account_name: str, cursor: str) -> None:
        self.cursors[account_name] = cursor


def test_collect_posts_skips_accounts_on_http_error(caplog: pytest.LogCaptureFixture) -> None:
    """Проверяет обработку ошибок Threads API при сборе постов."""

    tokens = [
        AccountToken(account_name="error_account", token="token-error"),
        AccountToken(account_name="ok_account", token="token-ok"),
    ]

    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status_code=403, request=request)
    error = httpx.HTTPStatusError("Forbidden", request=request, response=response)

    successful_result = ThreadsFetchResult(
        posts=[
            ThreadsPost(
                id="42",
                permalink="https://threads.net/p/42",
                data={"id": "42", "text": "hello"},
            )
        ],
        next_cursor="next-cursor",
    )

    client = _StubClient(responses={"token-error": error, "token-ok": successful_result})
    sheets = _StubSheets(cursors={"error_account": "prev-cursor"})

    with caplog.at_level(logging.WARNING):
        posts = asyncio.run(collect_posts(tokens, client, sheets))

    assert posts == [
        {
            "id": "42",
            "permalink": "https://threads.net/p/42",
            "text": "hello",
            "account_name": "ok_account",
        }
    ]

    assert sheets.cursors["error_account"] == "prev-cursor"
    assert sheets.cursors["ok_account"] == "next-cursor"

    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert warnings, "Ожидалось предупреждение в логах"
    assert any("Не удалось получить посты" in record.message for record in warnings)
