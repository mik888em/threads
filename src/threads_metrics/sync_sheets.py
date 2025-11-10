"""Скрипт синхронизации листов Google Sheets."""

from __future__ import annotations

import json
import logging
import os
from typing import List

import gspread
from google.oauth2.service_account import Credentials

DEFAULT_WORKSHEET_NAME = "Data_Po_kagdomy_posty"
ROW_HEIGHT_PIXELS = 21
COLUMN_B_INDEX = 1

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_env(name: str) -> str:
    """Возвращает значение переменной окружения или завершает работу."""

    value = os.getenv(name)
    if not value:
        logging.error("Переменная окружения %s не задана", name)
        raise SystemExit(1)
    return value


def _authorize(service_account_json: str) -> gspread.Client:
    """Создаёт клиента gspread из JSON сервисного аккаунта."""

    try:
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError as error:
        logging.error("Не удалось декодировать JSON сервисного аккаунта: %s", error)
        raise SystemExit(1) from error
    credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return gspread.authorize(credentials)


def _pad_rows(rows: List[List[str]]) -> List[List[str]]:
    """Дополняет строки до одинаковой длины пустыми значениями."""

    if not rows:
        return rows
    max_columns = max(len(row) for row in rows)
    return [row + [""] * (max_columns - len(row)) for row in rows]


def _set_row_height(worksheet: gspread.Worksheet, rows_count: int) -> None:
    """Устанавливает высоту строк на листе."""

    if rows_count <= 0:
        return
    sheet_id = worksheet.id
    worksheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": 0,
                            "endIndex": rows_count,
                        },
                        "properties": {"pixelSize": ROW_HEIGHT_PIXELS},
                        "fields": "pixelSize",
                    }
                }
            ]
        }
    )


def _set_column_text_format(
    worksheet: gspread.Worksheet,
    rows_count: int,
    column_index: int = COLUMN_B_INDEX,
) -> None:
    """Принудительно задаёт текстовый формат столбца, чтобы сохранить длинные значения."""

    if rows_count <= 0:
        return
    sheet_id = worksheet.id
    worksheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": rows_count,
                            "startColumnIndex": column_index,
                            "endColumnIndex": column_index + 1,
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
    )


def _parse_max_rows(value: str | None) -> int | None:
    """Преобразует ограничение количества строк из переменной окружения."""

    if value is None or value.strip() == "":
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        logging.error("Некорректное значение GOOGLE_MAX_STRING_PARSING: %s", value)
        raise SystemExit(1) from error
    if parsed <= 0:
        return None
    return parsed


def _copy_values(
    source_sheet: gspread.Worksheet,
    target_sheet: gspread.Worksheet,
    max_rows: int | None = None,
) -> None:
    """Копирует значения с листа источника на целевой лист."""

    rows = source_sheet.get_all_values()
    if max_rows is not None:
        rows = rows[:max_rows]
    padded_rows = _pad_rows(rows)
    target_sheet.clear()
    if padded_rows:
        rows_count = len(padded_rows)
        cols_count = len(padded_rows[0])
        target_sheet.resize(rows=rows_count, cols=cols_count)
        if cols_count > COLUMN_B_INDEX:
            _set_column_text_format(target_sheet, rows_count, column_index=COLUMN_B_INDEX)
        target_sheet.update("A1", padded_rows, value_input_option="USER_ENTERED")
        _set_row_height(target_sheet, rows_count)
    else:
        target_sheet.resize(rows=1, cols=1)
        _set_row_height(target_sheet, 1)


def main() -> None:
    """Точка входа в программу."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    service_account_json = _get_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    source_table_id = _get_env("ID_GOOGLE_TABLE")
    target_table_id = _get_env("ID_GOOGLE_TABLE_PUBLIC_DANNYE")
    worksheet_name = os.getenv("SOURCE_WORKSHEET_NAME", DEFAULT_WORKSHEET_NAME)

    max_rows = _parse_max_rows(os.getenv("GOOGLE_MAX_STRING_PARSING"))

    client = _authorize(service_account_json)
    source_table = client.open_by_key(source_table_id)
    target_table = client.open_by_key(target_table_id)

    source_sheet = source_table.worksheet(worksheet_name)
    target_sheet = target_table.worksheet(worksheet_name)

    logging.info(
        "Копирование данных листа %s из таблицы %s в таблицу %s", worksheet_name, source_table_id, target_table_id
    )
    if max_rows is not None:
        logging.info("Будет скопировано не более %s строк", max_rows)
    _copy_values(source_sheet, target_sheet, max_rows=max_rows)
    logging.info("Копирование завершено")


if __name__ == "__main__":
    try:
        main()
    except gspread.GSpreadException as error:
        logging.exception("Ошибка работы с Google Sheets: %s", error)
        raise SystemExit(1) from error
    except Exception as error:  # noqa: BLE001
        logging.exception("Необработанная ошибка: %s", error)
        raise SystemExit(1) from error
