"""Хранилище состояния приложения."""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

TIMEZONE = dt.timezone(dt.timedelta(hours=3), name="Europe/Athens")


@dataclass(slots=True)
class AppState:
    """Структура состояния приложения."""

    cursors: Dict[str, str] = field(default_factory=dict)
    last_metrics_write: Optional[str] = None
    post_metrics_updated_at: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Преобразует состояние к словарю."""

        return {
            "cursors": self.cursors,
            "last_metrics_write": self.last_metrics_write,
            "post_metrics_updated_at": self.post_metrics_updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> "AppState":
        """Создаёт состояние из словаря."""

        cursors = data.get("cursors") or {}
        last_metrics_write = data.get("last_metrics_write")
        post_metrics_updated_at = data.get("post_metrics_updated_at") or {}
        return cls(
            cursors=dict(cursors),
            last_metrics_write=last_metrics_write,
            post_metrics_updated_at=dict(post_metrics_updated_at),
        )


class StateStore:
    """Файловое хранилище состояния."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._state = self._load()

    def get_account_cursor(self, account_name: str) -> Optional[str]:
        """Возвращает сохранённый курсор пагинации."""

        return self._state.cursors.get(account_name)

    def set_account_cursor(self, account_name: str, cursor: str) -> None:
        """Сохраняет курсор пагинации и пишет состояние на диск."""

        self._state.cursors[account_name] = cursor
        self._save()

    def get_last_metrics_write(self) -> Optional[dt.datetime]:
        """Возвращает время последней записи метрик."""

        if not self._state.last_metrics_write:
            return None
        return dt.datetime.fromisoformat(self._state.last_metrics_write)

    def update_last_metrics_write(self) -> None:
        """Обновляет отметку времени записи метрик."""

        now = dt.datetime.now(TIMEZONE).isoformat()
        self._state.last_metrics_write = now
        self._save()

    def get_post_metrics_timestamp(self, post_id: str) -> Optional[dt.datetime]:
        """Возвращает время последнего обновления метрик поста."""

        timestamp = self._state.post_metrics_updated_at.get(post_id)
        if not timestamp:
            return None
        return dt.datetime.fromisoformat(timestamp)

    def should_refresh_post_metrics(
        self, post_id: str, ttl_minutes: int, *, now: Optional[dt.datetime] = None
    ) -> bool:
        """Определяет, нужно ли обновлять метрики поста."""

        now_dt = now or dt.datetime.now(TIMEZONE)
        last_update = self.get_post_metrics_timestamp(post_id)
        if not last_update:
            return True
        return now_dt - last_update >= dt.timedelta(minutes=ttl_minutes)

    def update_post_metrics_timestamp(
        self, post_id: str, timestamp: Optional[dt.datetime] = None
    ) -> None:
        """Сохраняет время обновления метрик поста."""

        moment = timestamp or dt.datetime.now(TIMEZONE)
        self._state.post_metrics_updated_at[post_id] = moment.isoformat()
        self._save()

    def update_post_metrics_many(self, timestamps: Dict[str, dt.datetime]) -> None:
        """Массово обновляет отметки времени метрик постов."""

        for post_id, moment in timestamps.items():
            self._state.post_metrics_updated_at[post_id] = moment.isoformat()
        if timestamps:
            self._save()

    def _load(self) -> AppState:
        if not self._path.exists():
            return AppState()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return AppState()
        return AppState.from_dict(data)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


__all__ = ["StateStore", "AppState", "TIMEZONE"]
