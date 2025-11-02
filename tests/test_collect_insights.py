"""Тесты для сбора инсайтов по постам."""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List

import httpx
import pytest

from threads_metrics.main import RetrySettings, collect_insights


@dataclass
class DummyStateStore:
    """Простая заглушка для проверки обновления метрик."""

    refresh_calls: List[str] = field(default_factory=list)
    updates: List[Dict[str, object]] = field(default_factory=list)

    def should_refresh_post_metrics(self, post_id: str, ttl_minutes: int) -> bool:
        self.refresh_calls.append(post_id)
        return True

    def update_post_metrics_many(self, timestamps: Dict[str, object]) -> None:
        self.updates.append(dict(timestamps))


class DummyClient:
    """Клиент Threads, имитирующий ошибку для одного поста."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, str]] = []

    async def fetch_post_insights(
        self, token: str, post_id: str, *, account_name: str | None = None
    ) -> Dict[str, int]:
        self.calls.append({"token": token, "post_id": post_id, "account_name": account_name or ""})
        if post_id == "1":
            request = httpx.Request("GET", "https://example.com/1")
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError("boom", request=request, response=response)
        return {"views": 100, "likes": 5}

    def build_absolute_url(self, path: str, params: Dict[str, str]) -> str:
        return f"https://example.com{path}?metric={params.get('metric', '')}"


def test_collect_insights_skips_failed_posts(caplog: pytest.LogCaptureFixture) -> None:
    """Проверяет, что ошибки клиента не прерывают сбор инсайтов."""

    posts = [
        {"id": "1", "account_name": "acc1"},
        {"id": "2", "account_name": "acc2"},
    ]
    tokens = {"acc1": "token1", "acc2": "token2"}
    client = DummyClient()
    state_store = DummyStateStore()

    with caplog.at_level(logging.ERROR):
        insights = asyncio.run(
            collect_insights(
                posts,
                tokens,
                client,
                state_store,
                ttl_minutes=60,
                retry_settings=RetrySettings(pause_range=(0, 0)),
            )
        )

    assert "1" not in insights
    assert insights["2"] == {"views": 100, "likes": 5}

    assert state_store.updates
    assert set(state_store.updates[0].keys()) == {"2"}

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    contexts = [json.loads(record.context) for record in error_records]
    assert any(context.get("post_id") == "1" for context in contexts)
