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
