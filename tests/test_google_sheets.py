"""Тесты для клиента Google Sheets."""
from __future__ import annotations

import sys
import types
from dataclasses import dataclass

import pytest


if "gspread" not in sys.modules:
    gspread_stub = types.ModuleType("gspread")
    gspread_stub.authorize = lambda credentials: None
    sys.modules["gspread"] = gspread_stub

if "pandas" not in sys.modules:
    pandas_stub = types.ModuleType("pandas")

    class _StubDataFrame:
        def __init__(self, *args, **kwargs) -> None:  # noqa: D401 - простая заглушка
            """Инициализатор заглушки DataFrame."""

    pandas_stub.DataFrame = _StubDataFrame  # type: ignore[attr-defined]
    sys.modules["pandas"] = pandas_stub

if "google" not in sys.modules:
    google_module = types.ModuleType("google")
    google_module.__path__ = []  # type: ignore[attr-defined]
    sys.modules["google"] = google_module

if "google.oauth2" not in sys.modules:
    oauth2_module = types.ModuleType("google.oauth2")
    oauth2_module.__path__ = []  # type: ignore[attr-defined]
    sys.modules["google.oauth2"] = oauth2_module

if "google.oauth2.service_account" not in sys.modules:
    service_account_module = types.ModuleType("google.oauth2.service_account")

    class _StubCredentials:
        @classmethod
        def from_service_account_info(cls, info: dict[str, str], scopes: list[str]) -> "_StubCredentials":
            return cls()

    service_account_module.Credentials = _StubCredentials  # type: ignore[attr-defined]
    sys.modules["google.oauth2.service_account"] = service_account_module

from src.threads_metrics.google_sheets import AccountToken, GoogleSheetsClient


@dataclass
class DummyStateStore:
    """Заглушка хранилища состояния для тестов."""


class DummyWorksheet:
    def __init__(self, records: list[dict[str, str]]) -> None:
        self._records = records

    def get_all_records(self) -> list[dict[str, str]]:
        return self._records


class DummySpreadsheet:
    def __init__(self, records: list[dict[str, str]]) -> None:
        self._records = records

    def worksheet(self, worksheet_name: str) -> DummyWorksheet:
        assert worksheet_name == "accounts_threads"
        return DummyWorksheet(self._records)


class DummyClient:
    def __init__(self, records: list[dict[str, str]]) -> None:
        self._records = records

    def open_by_key(self, table_id: str) -> DummySpreadsheet:
        assert table_id == "test-table"
        return DummySpreadsheet(self._records)


class DummyCredentials:
    """Заглушка учётных данных Google."""


@pytest.fixture()
def google_client_mocks(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [{" NickName ": "Account", "BEARER   TOKEN": "token-value"}]

    def fake_authorize(credentials: DummyCredentials) -> DummyClient:
        return DummyClient(records)

    def fake_from_service_account_info(info: dict[str, str], scopes: list[str]) -> DummyCredentials:
        return DummyCredentials()

    monkeypatch.setattr("src.threads_metrics.google_sheets.gspread.authorize", fake_authorize)
    monkeypatch.setattr(
        "src.threads_metrics.google_sheets.Credentials.from_service_account_info",
        fake_from_service_account_info,
    )


def test_read_account_tokens_supports_bearer_headers(google_client_mocks: None) -> None:
    client = GoogleSheetsClient(table_id="test-table", service_account_info={}, state_store=DummyStateStore())

    tokens = client.read_account_tokens()

    assert tokens == [AccountToken(account_name="Account", token="token-value")]
