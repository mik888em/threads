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

    utils_module = types.ModuleType("gspread.utils")

    def _rowcol_to_a1(row: int, col: int) -> str:
        letters = ""
        current = col
        while current:
            current, remainder = divmod(current - 1, 26)
            letters = chr(65 + remainder) + letters
        return f"{letters}{row}"

    utils_module.rowcol_to_a1 = _rowcol_to_a1  # type: ignore[attr-defined]
    gspread_stub.utils = utils_module  # type: ignore[attr-defined]
    sys.modules["gspread.utils"] = utils_module

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
        def from_service_account_info(
            cls, info: dict[str, str], scopes: list[str]
        ) -> "_StubCredentials":
            return cls()

    service_account_module.Credentials = _StubCredentials  # type: ignore[attr-defined]
    sys.modules["google.oauth2.service_account"] = service_account_module

from src.threads_metrics.google_sheets import AccountToken, GoogleSheetsClient


@dataclass
class DummyStateStore:
    """Заглушка хранилища состояния для тестов."""

    last_metrics_updated: bool = False

    def update_last_metrics_write(self) -> None:
        self.last_metrics_updated = True


class DummySpreadsheetBackend:
    """Заглушка API Google Sheets для batch_update."""

    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def batch_update(self, payload: dict[str, object]) -> None:
        self.requests.append(payload)


class DummyWorksheet:
    """Заглушка листа Google Sheets для проверки операций."""

    def __init__(self, records: list[dict[str, object]], *, sheet_id: int = 1) -> None:
        self._records = records
        self.id = sheet_id
        self.cleared = False
        self.updated_values: list[list[str]] | None = None
        self.formats: list[tuple[str, dict[str, str]]] = []
        self._backend = DummySpreadsheetBackend()

    def get_all_records(self) -> list[dict[str, object]]:
        return self._records

    def clear(self) -> None:
        self.cleared = True

    def update(self, values: list[list[str]]) -> None:
        self.updated_values = values

    def format(self, range_label: str, fmt: dict[str, str]) -> None:
        self.formats.append((range_label, fmt))

    @property
    def spreadsheet(self) -> DummySpreadsheetBackend:
        return self._backend


class DummySpreadsheet:
    """Простая обёртка над заглушками листов."""

    def __init__(self, worksheets: dict[str, DummyWorksheet]) -> None:
        self._worksheets = worksheets

    def worksheet(self, worksheet_name: str) -> DummyWorksheet:
        return self._worksheets[worksheet_name]


class DummyClient:
    """Клиент Google Sheets, возвращающий подготовленные листы."""

    def __init__(self, worksheets: dict[str, DummyWorksheet]) -> None:
        self._worksheets = worksheets

    def open_by_key(self, table_id: str) -> DummySpreadsheet:
        assert table_id == "test-table"
        return DummySpreadsheet(self._worksheets)


class DummyCredentials:
    """Заглушка учётных данных Google."""


def test_read_account_tokens_supports_bearer_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [{" NickName ": "Account", "BEARER   TOKEN": "token-value"}]
    worksheets = {"accounts_threads": DummyWorksheet(records)}

    monkeypatch.setattr(
        "src.threads_metrics.google_sheets.gspread.authorize",
        lambda credentials: DummyClient(worksheets),
    )
    monkeypatch.setattr(
        "src.threads_metrics.google_sheets.Credentials.from_service_account_info",
        lambda info, scopes: DummyCredentials(),
    )

    client = GoogleSheetsClient(
        table_id="test-table", service_account_info={}, state_store=DummyStateStore()
    )

    tokens = client.read_account_tokens()

    assert tokens == [AccountToken(account_name="Account", token="token-value")]


def test_write_posts_metrics_updates_existing_rows_and_formats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_records = [
        {
            "account_name": "acc",
            "post_id": 123,
            "permalink": "https://example.com/post",
            "text": "old text",
            "like_count": 1,
            "repost_count": 1,
            "reply_count": 0,
            "views": 10,
            "likes": 1,
            "replies": 0,
            "reposts": 0,
            "quotes": 0,
            "shares": 0,
            "updated_at": "2024-01-01T00:00:00+03:00",
        }
    ]

    data_sheet = DummyWorksheet(existing_records, sheet_id=42)
    worksheets = {"Data_Po_kagdomy_posty": data_sheet}

    monkeypatch.setattr(
        "src.threads_metrics.google_sheets.gspread.authorize",
        lambda credentials: DummyClient(worksheets),
    )
    monkeypatch.setattr(
        "src.threads_metrics.google_sheets.Credentials.from_service_account_info",
        lambda info, scopes: DummyCredentials(),
    )

    state_store = DummyStateStore()
    client = GoogleSheetsClient(
        table_id="test-table", service_account_info={}, state_store=state_store
    )

    client.write_posts_metrics(
        [
            {
                "account_name": "acc",
                "post_id": "123",
                "permalink": "https://example.com/post",
                "text": "new text",
                "like_count": 5,
                "repost_count": 2,
                "reply_count": 1,
                "views": 15,
                "likes": 6,
                "replies": 1,
                "reposts": 2,
                "quotes": 0,
                "shares": 0,
            }
        ]
    )

    assert data_sheet.cleared is True
    assert state_store.last_metrics_updated is True
    assert data_sheet.updated_values is not None

    header, *rows = data_sheet.updated_values
    assert len(rows) == 1
    row = rows[0]

    like_index = header.index("like_count")
    text_index = header.index("text")
    updated_at_index = header.index("updated_at")

    assert row[like_index] == "5"
    assert row[text_index] == "new text"
    assert row[updated_at_index] != "2024-01-01T00:00:00+03:00"

    assert data_sheet.formats == [("A1:N2", {"wrapStrategy": "OVERFLOW_CELL"})]
    assert data_sheet.spreadsheet.requests
    update_request = data_sheet.spreadsheet.requests[0]["requests"][0]
    assert update_request["updateDimensionProperties"]["properties"]["pixelSize"] == 21
