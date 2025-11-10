"""Тесты для модуля синхронизации Google Sheets."""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, call

import pytest

from src.threads_metrics.sync_sheets import (
    ROW_HEIGHT_PIXELS,
    _copy_values,
    _parse_max_rows,
)


class SourceWorksheetStub:
    """Заглушка листа-источника."""

    def __init__(self, values: List[List[str]]) -> None:
        self._values = values

    def get_all_values(self) -> List[List[str]]:
        return self._values


class TargetWorksheetStub:
    """Заглушка листа-приёмника."""

    def __init__(self) -> None:
        self.id = 7
        self.cleared = False
        self.resize_calls: list[tuple[int, int]] = []
        self.update_calls: list[tuple[str, list[list[str]], str]] = []
        self.spreadsheet = MagicMock()
        self.spreadsheet.batch_update = MagicMock()

    def clear(self) -> None:
        self.cleared = True

    def resize(self, rows: int, cols: int) -> None:
        self.resize_calls.append((rows, cols))

    def update(self, range_label: str, values: List[List[str]], *, value_input_option: str) -> None:
        self.update_calls.append((range_label, values, value_input_option))


def test_parse_max_rows() -> None:
    """Проверяет разбор ограничения строк из окружения."""

    assert _parse_max_rows(None) is None
    assert _parse_max_rows("") is None
    assert _parse_max_rows("0") is None
    assert _parse_max_rows("-5") is None
    assert _parse_max_rows("15") == 15
    with pytest.raises(SystemExit):
        _parse_max_rows("not-a-number")


def test_copy_values_respects_limit() -> None:
    """Проверяет, что копирование учитывает максимальное количество строк."""

    source = SourceWorksheetStub([
        ["A1", "B1"],
        ["A2", "B2"],
        ["A3", "B3"],
    ])
    target = TargetWorksheetStub()

    _copy_values(source, target, max_rows=2)

    assert target.cleared is True
    assert target.resize_calls == [(2, 2)]
    assert target.update_calls == [("A1", [["A1", "B1"], ["A2", "B2"]], "USER_ENTERED")]

    assert target.spreadsheet.batch_update.call_args_list == [
        call(
            {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": target.id,
                                "startRowIndex": 0,
                                "endRowIndex": 2,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "numberFormat": {"type": "TEXT"}
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat",
                        }
                    }
                ]
            }
        ),
        call(
            {
                "requests": [
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": target.id,
                                "dimension": "ROWS",
                                "startIndex": 0,
                                "endIndex": 2,
                            },
                            "properties": {"pixelSize": ROW_HEIGHT_PIXELS},
                            "fields": "pixelSize",
                        }
                    }
                ]
            }
        ),
    ]


def test_copy_values_without_limit_keeps_all_rows() -> None:
    """Проверяет, что при отсутствии ограничения копируются все строки."""

    rows = [["C1"], ["C2"], ["C3"], ["C4"]]
    source = SourceWorksheetStub(rows)
    target = TargetWorksheetStub()

    _copy_values(source, target, max_rows=None)

    assert target.update_calls[0][1] == rows
    target.spreadsheet.batch_update.assert_called_once()
