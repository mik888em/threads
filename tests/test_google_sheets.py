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
        self.id = sheet_id
        self.cleared = False
        self.formats: list[tuple[str, dict[str, str]]] = []
        self._backend = DummySpreadsheetBackend()
        self.batch_update_calls: list[list[dict[str, object]]] = []
        self._grid: list[list[str]] = []
        if records:
            header = list(records[0].keys())
            self._grid.append([str(column) for column in header])
            for record in records:
                row = [str(record.get(column, "")) for column in header]
                self._grid.append(row)

    def _current_width(self) -> int:
        return max((len(row) for row in self._grid), default=0)

    def _ensure_size(self, rows: int, cols: int) -> None:
        current_width = max(self._current_width(), cols)
        while len(self._grid) < rows:
            self._grid.append([""] * current_width)
        for row in self._grid:
            if len(row) < current_width:
                row.extend([""] * (current_width - len(row)))

    @staticmethod
    def _a1_to_rowcol(label: str) -> tuple[int, int]:
        label = label.upper()
        letters = ""
        digits = ""
        for char in label:
            if char.isalpha():
                letters += char
            elif char.isdigit():
                digits += char
        col = 0
        for char in letters:
            col = col * 26 + (ord(char) - 64)
        return int(digits), col

    def _write_range(self, range_label: str, values: list[list[str]]) -> None:
        if ":" in range_label:
            start, end = range_label.split(":", 1)
        else:
            start = end = range_label
        start_row, start_col = self._a1_to_rowcol(start)
        end_row, end_col = self._a1_to_rowcol(end)
        self._ensure_size(end_row, end_col)
        for row_offset, value_row in enumerate(values):
            target_row = start_row - 1 + row_offset
            for col_offset, value in enumerate(value_row):
                target_col = start_col - 1 + col_offset
                self._grid[target_row][target_col] = str(value)

    def get_all_records(self) -> list[dict[str, object]]:
        if not self._grid:
            return []
        header = self._grid[0]
        records: list[dict[str, object]] = []
        for row in self._grid[1:]:
            record = {
                header[index]: row[index] if index < len(row) else ""
                for index in range(len(header))
            }
            records.append(record)
        return records

    def clear(self) -> None:
        self.cleared = True
        self._grid = []

    def update(self, values: list[list[str]]) -> None:
        raise AssertionError("Метод update не должен вызываться в новых тестах")

    def batch_update(self, data: list[dict[str, object]]) -> None:
        self.batch_update_calls.append(data)
        for item in data:
            self._write_range(item["range"], item["values"])  # type: ignore[index]

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
        },
        {
            "account_name": "acc",
            "post_id": 456,
            "permalink": "https://example.com/post2",
            "text": "keep text",
            "like_count": 2,
            "repost_count": 0,
            "reply_count": 0,
            "views": 20,
            "likes": 2,
            "replies": 0,
            "reposts": 0,
            "quotes": 0,
            "shares": 0,
            "updated_at": "2024-01-02T00:00:00+03:00",
        },
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
                "like_count": 6,
                "repost_count": 2,
                "reply_count": 1,
                "views": 15,
                "likes": 6,
                "replies": 1,
                "reposts": 2,
                "quotes": 0,
                "shares": 0,
            },
            {
                "account_name": "acc",
                "post_id": "789",
                "permalink": "https://example.com/post3",
                "text": "brand new",
                "like_count": 3,
                "repost_count": 0,
                "reply_count": 0,
                "views": 5,
                "likes": 3,
                "replies": 0,
                "reposts": 0,
                "quotes": 0,
                "shares": 0,
            },
        ]
    )

    assert data_sheet.cleared is False
    assert state_store.last_metrics_updated is True
    assert data_sheet.batch_update_calls

    updated_records = data_sheet.get_all_records()
    assert len(updated_records) == 3

    first_row = updated_records[0]
    second_row = updated_records[1]
    third_row = updated_records[2]

    assert first_row["text"] == "new text"
    assert first_row["like_count"] == "6"
    assert first_row["reply_count"] == "1"
    assert first_row["repost_count"] == "2"
    assert first_row["updated_at"] != "2024-01-01T00:00:00+03:00"

    assert second_row["text"] == "keep text"
    assert second_row["like_count"] == "2"

    assert third_row["post_id"] == "789"
    assert third_row["like_count"] == "3"

    batch_payload = data_sheet.batch_update_calls[0]
    assert any(item["range"].startswith("A2") for item in batch_payload)
    assert any(item["range"].startswith("A4") for item in batch_payload)

    assert data_sheet.formats == [("A4:N4", {"wrapStrategy": "OVERFLOW_CELL"})]
    assert data_sheet.spreadsheet.requests
    update_request = data_sheet.spreadsheet.requests[0]["requests"][0]
    assert update_request["updateDimensionProperties"]["properties"]["pixelSize"] == 21
    assert update_request["updateDimensionProperties"]["range"]["startIndex"] == 3
    assert update_request["updateDimensionProperties"]["range"]["endIndex"] == 4


def test_write_posts_metrics_updates_without_new_rows(monkeypatch: pytest.MonkeyPatch) -> None:
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

    data_sheet = DummyWorksheet(existing_records, sheet_id=99)
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
                "like_count": 6,
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

    assert data_sheet.cleared is False
    assert state_store.last_metrics_updated is True
    assert data_sheet.formats == []
    assert data_sheet.batch_update_calls
    batch_ranges = [item["range"] for item in data_sheet.batch_update_calls[0]]
    assert batch_ranges == [batch_ranges[0]]
