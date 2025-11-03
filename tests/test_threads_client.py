import asyncio
from typing import Any

import httpx
import pytest

from src.threads_metrics.threads_client import ThreadsClient


@pytest.mark.parametrize(
    "permalink, expected",
    [
        (
            "https://www.threads.net/@example/post/123?utm_source=test",
            "/@example/post/123",
        ),
        (
            "https://www.threads.com/@example/post/456",
            "/@example/post/456",
        ),
        (
            "/@example/post/789?foo=bar",
            "/@example/post/789",
        ),
        (
            "https://external.site/@example/post/000?utm_source=test",
            "https://external.site/@example/post/000",
        ),
    ],
)
def test_sanitize_permalink(permalink: str, expected: str) -> None:
    assert ThreadsClient._sanitize_permalink(permalink) == expected


def test_fetch_posts_uses_override_fields() -> None:
    captured_url = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_url["value"] = str(request.url)
        return httpx.Response(200, json={"data": [], "paging": {}})

    async def runner() -> None:
        transport = httpx.MockTransport(handler)
        client = ThreadsClient(
            base_url="https://graph.threads.net",
            timeout=10,
            transport=transport,
            posts_url_override="https://graph.threads.net/v1.0/me/threads?fields=id,permalink",
        )

        try:
            await client.fetch_posts("token")
        finally:
            await client.close()

    asyncio.run(runner())

    assert "value" in captured_url
    assert "fields=id%2Cpermalink" in captured_url["value"]
    assert "text" not in captured_url["value"]


def test_request_respects_retry_after_for_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    current_time = {"value": 0.0}

    async def fake_sleep(duration: float) -> None:
        waits.append(duration)
        current_time["value"] += duration

    monkeypatch.setattr(
        "src.threads_metrics.threads_client.asyncio.sleep",
        fake_sleep,
    )
    monkeypatch.setattr(
        "src.threads_metrics.threads_client.ThreadsClient._current_time",
        staticmethod(lambda: current_time["value"]),
    )

    attempts = {"value": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts["value"] += 1
        if attempts["value"] == 1:
            headers = {"Retry-After": "12", "Content-Type": "application/json"}
            payload: dict[str, Any] = {
                "error": {
                    "message": "There have been too many calls for this Threads profile. Wait a bit and try again.",
                    "code": 80016,
                }
            }
            return httpx.Response(403, json=payload, headers=headers, request=request)
        return httpx.Response(200, json={"data": [], "paging": {}}, request=request)

    async def runner() -> None:
        transport = httpx.MockTransport(handler)
        client = ThreadsClient(
            base_url="https://graph.threads.net",
            timeout=10,
            transport=transport,
        )

        try:
            await client.fetch_posts("token", account_name="acc")
        finally:
            await client.close()

    asyncio.run(runner())

    assert waits == [12.0]


def test_request_uses_rate_limit_backoff_without_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    current_time = {"value": 0.0}

    async def fake_sleep(duration: float) -> None:
        waits.append(duration)
        current_time["value"] += duration

    monkeypatch.setattr(
        "src.threads_metrics.threads_client.asyncio.sleep",
        fake_sleep,
    )
    monkeypatch.setattr(
        "src.threads_metrics.threads_client.ThreadsClient._current_time",
        staticmethod(lambda: current_time["value"]),
    )

    attempts = {"value": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts["value"] += 1
        if attempts["value"] == 1:
            payload = {
                "error": {
                    "message": "There have been too many calls for this Threads profile. Wait a bit and try again.",
                    "code": 80016,
                }
            }
            return httpx.Response(403, json=payload, request=request)
        return httpx.Response(200, json={"data": [], "paging": {}}, request=request)

    async def runner() -> None:
        transport = httpx.MockTransport(handler)
        client = ThreadsClient(
            base_url="https://graph.threads.net",
            timeout=10,
            transport=transport,
        )

        try:
            await client.fetch_posts("token", account_name="acc")
        finally:
            await client.close()

    asyncio.run(runner())

    assert waits == [10.0]


def test_request_uses_default_backoff_for_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    current_time = {"value": 0.0}

    async def fake_sleep(duration: float) -> None:
        waits.append(duration)
        current_time["value"] += duration

    monkeypatch.setattr(
        "src.threads_metrics.threads_client.asyncio.sleep",
        fake_sleep,
    )
    monkeypatch.setattr(
        "src.threads_metrics.threads_client.ThreadsClient._current_time",
        staticmethod(lambda: current_time["value"]),
    )

    attempts = {"value": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts["value"] += 1
        if attempts["value"] == 1:
            return httpx.Response(500, json={"error": {"message": "oops"}}, request=request)
        return httpx.Response(200, json={"data": [], "paging": {}}, request=request)

    async def runner() -> None:
        transport = httpx.MockTransport(handler)
        client = ThreadsClient(
            base_url="https://graph.threads.net",
            timeout=10,
            transport=transport,
        )

        try:
            await client.fetch_posts("token", account_name="acc")
        finally:
            await client.close()

    asyncio.run(runner())

    assert waits == [5.0]
