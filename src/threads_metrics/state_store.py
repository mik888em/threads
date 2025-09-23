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

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Преобразует состояние к словарю."""

        return {
            "cursors": self.cursors,
            "last_metrics_write": self.last_metrics_write,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> "AppState":
        """Создаёт состояние из словаря."""

        cursors = data.get("cursors") or {}
        last_metrics_write = data.get("last_metrics_write")
        return cls(cursors=dict(cursors), last_metrics_write=last_metrics_write)


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


__all__ = ["StateStore", "AppState"]
