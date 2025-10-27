"""Функции агрегации метрик постов."""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Mapping

from .constants import PUBLISH_TIME_COLUMN
from .state_store import TIMEZONE


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

        post_like_count = post.get("like_count")
        if post_like_count is None:
            post_like_count = 0
        post_reply_count = post.get("reply_count")
        if post_reply_count is None:
            post_reply_count = 0
        post_repost_count = post.get("repost_count")
        if post_repost_count is None:
            post_repost_count = 0

        like_value = (
            insight.get("likes", post_like_count) if has_insight else post_like_count
        )
        reply_value = (
            insight.get("replies", post_reply_count)
            if has_insight
            else post_reply_count
        )
        repost_value = (
            insight.get("reposts", post_repost_count)
            if has_insight
            else post_repost_count
        )
        aggregated.append(
            {
                PUBLISH_TIME_COLUMN: _convert_timestamp(post.get("timestamp")),
                "account_name": post.get("account_name"),
                "post_id": post_id,
                "permalink": post.get("permalink"),
                "text": post.get("text"),
                "views": insight.get("views") if has_insight else None,
                "likes": like_value,
                "replies": reply_value,
                "reposts": repost_value,
                "quotes": insight.get("quotes") if has_insight else None,
                "shares": insight.get("shares") if has_insight else None,
            }
        )
    return aggregated


def _convert_timestamp(raw_value: Any) -> str:
    """Конвертирует отметку публикации в часовой пояс Афин."""

    if not raw_value:
        return ""
    try:
        parsed = dt.datetime.strptime(str(raw_value), "%Y-%m-%dT%H:%M:%S%z")
    except (TypeError, ValueError):
        return str(raw_value)
    return parsed.astimezone(TIMEZONE).isoformat()


__all__ = ["aggregate_posts"]
