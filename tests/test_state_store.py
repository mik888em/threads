"""Тесты для хранилища состояния."""
from __future__ import annotations

import datetime as dt

from threads_metrics.state_store import StateStore, TIMEZONE


def test_state_store_post_metrics_ttl(tmp_path) -> None:
    """Проверяет применение TTL при обновлении метрик постов."""

    state_file = tmp_path / "state.json"
    store = StateStore(state_file)

    assert store.should_refresh_post_metrics("123", ttl_minutes=60)

    now = dt.datetime(2024, 1, 1, 12, 0, tzinfo=TIMEZONE)
    store.update_post_metrics_timestamp("123", timestamp=now)

    assert not store.should_refresh_post_metrics("123", ttl_minutes=120, now=now + dt.timedelta(minutes=30))
    assert store.should_refresh_post_metrics("123", ttl_minutes=60, now=now + dt.timedelta(minutes=90))

    # Пересоздаём стор, чтобы убедиться в сохранении состояния.
    store_reloaded = StateStore(state_file)
    assert not store_reloaded.should_refresh_post_metrics(
        "123", ttl_minutes=120, now=now + dt.timedelta(minutes=30)
    )


def test_state_store_bulk_update(tmp_path) -> None:
    """Проверяет массовое обновление меток времени."""

    state_file = tmp_path / "state.json"
    store = StateStore(state_file)

    timestamp = dt.datetime(2024, 1, 1, 15, 0, tzinfo=TIMEZONE)
    store.update_post_metrics_many({"1": timestamp, "2": timestamp + dt.timedelta(minutes=5)})

    assert not store.should_refresh_post_metrics("1", ttl_minutes=120, now=timestamp + dt.timedelta(minutes=30))
    assert store.should_refresh_post_metrics("2", ttl_minutes=1, now=timestamp + dt.timedelta(minutes=10))
