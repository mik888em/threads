import asyncio
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
