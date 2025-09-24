"""Тесты для конфигурации."""
from __future__ import annotations

import pytest

from threads_metrics.config import Config, ConfigError


def test_config_from_env_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Проверяет успешную загрузку конфигурации."""

    monkeypatch.setenv("ID_GOOGLE_TABLE", "table")
    monkeypatch.setenv("URL_GAS_RAZVERTIVANIA", "https://example.com")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")

    config = Config.from_env()

    assert config.google_table_id == "table"
    assert config.threads_api_base_url == "https://graph.threads.net"
    assert config.run_timeout_minutes == 35


def test_config_respects_run_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Проверяет чтение пользовательского таймаута работы."""

    monkeypatch.setenv("ID_GOOGLE_TABLE", "table")
    monkeypatch.setenv("URL_GAS_RAZVERTIVANIA", "https://example.com")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    monkeypatch.setenv("THREADS_RUN_TIMEOUT_MIN", "120")

    config = Config.from_env()

    assert config.run_timeout_minutes == 120


@pytest.mark.parametrize(
    "missing_key",
    ["ID_GOOGLE_TABLE", "URL_GAS_RAZVERTIVANIA", "GOOGLE_SERVICE_ACCOUNT_JSON"],
)
def test_config_missing_required(monkeypatch: pytest.MonkeyPatch, missing_key: str) -> None:
    """Проверяет ошибку при отсутствии обязательных переменных."""

    monkeypatch.delenv("ID_GOOGLE_TABLE", raising=False)
    monkeypatch.delenv("URL_GAS_RAZVERTIVANIA", raising=False)
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)

    env = {
        "ID_GOOGLE_TABLE": "table",
        "URL_GAS_RAZVERTIVANIA": "https://example.com",
        "GOOGLE_SERVICE_ACCOUNT_JSON": "{}",
    }
    env.pop(missing_key)

    with pytest.raises(ConfigError):
        Config.from_env(env)
