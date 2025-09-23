"""Модуль конфигурации приложения."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigError(RuntimeError):
    """Ошибка загрузки конфигурации."""


@dataclass(slots=True)
class Config:
    """Настройки приложения, загружаемые из переменных окружения.

    Attributes:
        google_table_id: Идентификатор Google-таблицы.
        gas_deployment_url: URL развёрнутого Google Apps Script.
        service_account_info: Данные сервисного аккаунта Google.
        threads_api_base_url: Базовый URL API Threads.
        request_timeout: Таймаут HTTP-запросов.
        concurrency_limit: Ограничение параллелизма для запросов.
        state_file: Путь к файлу состояния.
        metrics_ttl_minutes: Допустимая "давность" закэшированных метрик.
    """

    google_table_id: str
    gas_deployment_url: str
    service_account_info: Dict[str, Any]
    threads_api_base_url: str
    request_timeout: float
    concurrency_limit: int
    state_file: Path
    metrics_ttl_minutes: int

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "Config":
        """Создаёт конфигурацию на основе переменных окружения.

        Args:
            env: Необязательное отображение переменных окружения.

        Returns:
            Загруженная конфигурация.

        Raises:
            ConfigError: Если обязательные переменные не заданы или некорректны.
        """

        env_map = env or os.environ

        google_table_id = cls._require(env_map, "ID_GOOGLE_TABLE")
        gas_deployment_url = cls._require(env_map, "URL_GAS_RAZVERTIVANIA")
        service_account_json = cls._require(env_map, "GOOGLE_SERVICE_ACCOUNT_JSON")

        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as exc:
            raise ConfigError("Невозможно разобрать JSON сервисного аккаунта") from exc

        threads_api_base_url = env_map.get("THREADS_API_BASE_URL", "https://graph.threads.net")
        request_timeout = cls._parse_float(env_map.get("THREADS_REQUEST_TIMEOUT", "30"),
                                           "THREADS_REQUEST_TIMEOUT")
        concurrency_limit = cls._parse_int(env_map.get("THREADS_CONCURRENCY", "5"),
                                           "THREADS_CONCURRENCY")
        state_file = Path(env_map.get("THREADS_STATE_FILE", "state.json"))
        metrics_ttl_minutes = cls._parse_int(env_map.get("THREADS_METRICS_TTL_MIN", "60"),
                                             "THREADS_METRICS_TTL_MIN")

        return cls(
            google_table_id=google_table_id,
            gas_deployment_url=gas_deployment_url,
            service_account_info=service_account_info,
            threads_api_base_url=threads_api_base_url.rstrip("/"),
            request_timeout=request_timeout,
            concurrency_limit=concurrency_limit,
            state_file=state_file,
            metrics_ttl_minutes=metrics_ttl_minutes,
        )

    @staticmethod
    def _require(env: Dict[str, str], key: str) -> str:
        value = env.get(key)
        if not value:
            raise ConfigError(f"Переменная окружения {key} должна быть задана")
        return value

    @staticmethod
    def _parse_int(value: str, key: str) -> int:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise ConfigError(f"Переменная {key} должна быть целым числом") from exc
        if parsed <= 0:
            raise ConfigError(f"Переменная {key} должна быть положительным числом")
        return parsed

    @staticmethod
    def _parse_float(value: str, key: str) -> float:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise ConfigError(f"Переменная {key} должна быть числом") from exc
        if parsed <= 0:
            raise ConfigError(f"Переменная {key} должна быть положительным числом")
        return parsed


__all__ = ["Config", "ConfigError"]
