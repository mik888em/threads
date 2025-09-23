"""Функции агрегации метрик постов."""
from __future__ import annotations

from typing import Any, Dict, List, Mapping


def aggregate_posts(
    posts: List[Dict[str, Any]],
    insights: Mapping[str, Dict[str, int]],
) -> List[Dict[str, Any]]:
    """Агрегирует постовые данные и метрики Insights."""

    aggregated: List[Dict[str, Any]] = []
    for post in posts:
        raw_post_id = post.get("id")
        if not raw_post_id:
            continue
        post_id = str(raw_post_id)
        insight = insights.get(post_id, {})
        has_insight = post_id in insights
        aggregated.append(
            {
                "account_name": post.get("account_name"),
                "post_id": post_id,
                "permalink": post.get("permalink"),
                "text": post.get("text"),
                "like_count": post.get("like_count", 0),
                "repost_count": post.get("repost_count", 0),
                "reply_count": post.get("reply_count", 0),
                "views": insight.get("views") if has_insight else None,
                "likes": insight.get("likes", post.get("like_count", 0)),
                "replies": insight.get("replies", post.get("reply_count", 0)),
                "reposts": insight.get("reposts", post.get("repost_count", 0)),
                "quotes": insight.get("quotes") if has_insight else None,
                "shares": insight.get("shares") if has_insight else None,
            }
        )
    return aggregated


__all__ = ["aggregate_posts"]
