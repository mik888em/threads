"""Тесты для функций основного модуля."""
from __future__ import annotations

from threads_metrics.aggregation import aggregate_posts


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
        },
        {
            "id": "2",
            "account_name": "acc",
            "permalink": "https://example.com/2",
            "text": "post 2",
            "like_count": 5,
            "reply_count": 1,
            "repost_count": 0,
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

    assert first["views"] == 120
    assert first["likes"] == 15
    assert first["replies"] == 4
    assert first["reposts"] == 6
    assert first["like_count"] == 15
    assert first["reply_count"] == 4
    assert first["repost_count"] == 6
    assert first["quotes"] == 2
    assert first["shares"] == 3

    assert second["views"] is None
    assert second["likes"] == 5
    assert second["replies"] == 1
    assert second["reposts"] == 0
    assert second["like_count"] == 5
    assert second["reply_count"] == 1
    assert second["repost_count"] == 0
    assert second["quotes"] is None
    assert second["shares"] is None
