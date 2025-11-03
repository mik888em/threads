"""Работа с Google Sheets."""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

from .constants import PUBLISH_TIME_COLUMN
from .state_store import StateStore

TIMEZONE = dt.timezone(dt.timedelta(hours=3), name="Europe/Athens")

IGNORED_BACKGROUND_COLOR = "#9fc5e8"
DEFAULT_BACKGROUND_COLOR = "#ffffff"
NOT_DETERMINED_COLOR = "not determinate"

DEFAULT_THEME_COLOR_HEX = {
    "ACCENT1": "#4285f4",
    "ACCENT2": "#ea4335",
    "ACCENT3": "#fbbc04",
    "ACCENT4": "#34a853",
    "ACCENT5": "#46bdc6",
    "ACCENT6": "#ab47bc",
    "BACKGROUND": DEFAULT_BACKGROUND_COLOR,
    "TEXT": "#000000",
}


@dataclass(slots=True)
class AccountToken:
    """Токен Threads из Google Sheets."""

    account_name: str
    token: str


class GoogleSheetsClient:
    """Обёртка для доступа к Google Sheets."""

    _SHEETS_MAX_ATTEMPTS = 5
    _SHEETS_INITIAL_WAIT_SECONDS = 2.0
    _SHEETS_BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        *,
        table_id: str,
        service_account_info: Dict[str, Any],
        state_store: StateStore,
    ) -> None:
        """Создаёт клиента для взаимодействия с Google Sheets.

        Args:
            table_id: Идентификатор таблицы.
            service_account_info: Данные сервисного аккаунта Google.
            state_store: Хранилище состояния приложения.
        """

        self._table_id = table_id
        self._credentials = Credentials.from_service_account_info(
            service_account_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        self._client = gspread.authorize(self._credentials)
        self._state_store = state_store

    def read_account_tokens(
        self, worksheet: str = "accounts_threads"
    ) -> List[AccountToken]:
        """Считывает токены аккаунтов Threads.

        Args:
            worksheet: Имя листа с токенами.

        Returns:
            Список токенов аккаунтов.
        """

        sheet = self._get_worksheet(worksheet)
        try:
            records = sheet.get_all_records()
        except Exception:
            logging.exception(
                "Не удалось прочитать данные из Google Sheets",
                extra={"context": json.dumps({"worksheet": worksheet})},
            )
            raise
        background_colors = self._get_column_background_colors(
            sheet,
            column="A",
            start_row=2,
            rows_count=len(records),
            worksheet_name=worksheet,
        )
        ignored_color = IGNORED_BACKGROUND_COLOR.lower()
        tokens: List[AccountToken] = []
        sanitized_rows: List[Dict[str, Any]] = []
        for index, row in enumerate(records, start=2):
            normalized_row = {
                "_".join(str(key).strip().lower().split()): value
                for key, value in row.items()
            }
            token = self._get_first_present(
                normalized_row, ("token", "access_token", "bearer_token")
            )
            account = self._get_first_present(
                normalized_row, ("account", "name", "nickname")
            )
            account_id = self._get_first_present(
                normalized_row, ("id", "account_id", "user_id")
            )
            background_color = background_colors.get(index, NOT_DETERMINED_COLOR)
            if background_color is None:
                background_color = NOT_DETERMINED_COLOR
            sanitized_info = {
                "row": index,
                "nickname": str(account) if account else None,
                "has_id": bool(account_id),
                "has_token": bool(token),
                "background_color": background_color,
            }
            sanitized_rows.append(sanitized_info)
            logging.info(
                "Прочитана строка листа accounts_threads",
                extra={
                    "context": json.dumps(sanitized_info),
                    "account_label": sanitized_info["nickname"],
                },
            )
            if background_color and background_color.lower() == ignored_color:
                logging.info(
                    "Аккаунт пропущен из-за заливки в Google Sheets",
                    extra={
                        "context": json.dumps(
                            {
                                "row": index,
                                "nickname": sanitized_info["nickname"],
                                "background_color": background_color,
                            }
                        ),
                        "account_label": sanitized_info["nickname"],
                    },
                )
                continue
            if token and account:
                tokens.append(AccountToken(account_name=str(account), token=str(token)))
        nicknames = [row["nickname"] for row in sanitized_rows if row["nickname"]]
        ignored_accounts = [
            row["nickname"]
            for row in sanitized_rows
            if row["nickname"]
            and row.get("background_color")
            and row["background_color"].lower() == ignored_color
        ]
        usable_accounts = [
            row["nickname"]
            for row in sanitized_rows
            if row["nickname"]
            and (
                not row.get("background_color")
                or row["background_color"].lower() != ignored_color
            )
        ]
        logging.info(
            "Сводка никнеймов из Google Sheets",
            extra={
                "context": json.dumps(
                    {
                        "worksheet": worksheet,
                        "total_rows": len(sanitized_rows),
                        "nicknames": nicknames,
                        "with_tokens": [
                            row["nickname"]
                            for row in sanitized_rows
                            if row["nickname"] and row["has_token"]
                        ],
                        "ignored_due_to_color": ignored_accounts,
                        "ignored_color_hex": IGNORED_BACKGROUND_COLOR,
                        "usable_accounts": usable_accounts,
                        "usable_accounts_count": len(usable_accounts),
                    }
                )
            },
        )
        return tokens

    @staticmethod
    def _get_first_present(row: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
        for key in keys:
            value = row.get(key)
            if value:
                return value
        return None

    def _get_column_background_colors(
        self,
        sheet: Any,
        *,
        column: str,
        start_row: int,
        rows_count: int,
        worksheet_name: str,
    ) -> Dict[int, str]:
        if rows_count <= 0:
            return {}
        end_row = start_row + rows_count - 1
        sheet_title = getattr(sheet, "title", worksheet_name)
        range_label = f"'{sheet_title}'!{column}{start_row}:{column}{end_row}"
        spreadsheet = getattr(sheet, "spreadsheet", None)
        if not spreadsheet or not hasattr(spreadsheet, "fetch_sheet_metadata"):
            return {}
        request_payload = {
            "includeGridData": True,
            "ranges": [range_label],
        }
        try:
            metadata = spreadsheet.fetch_sheet_metadata(request_payload)
        except Exception:
            logging.exception(
                "Не удалось получить цвета ячеек Google Sheets",
                extra={
                    "context": json.dumps(
                        {
                            "sheet": sheet_title,
                            "column": column,
                            "start_row": start_row,
                            "end_row": end_row,
                        }
                    )
                },
            )
            return {}
        sheet_data = self._extract_sheet_data(metadata, sheet.id)
        if not sheet_data:
            return {}
        row_data = self._collect_row_data(sheet_data)
        theme_palette = self._build_theme_palette(metadata.get("spreadsheetTheme", {}))
        colors: Dict[int, str] = {}
        for offset, row in enumerate(row_data, start=start_row):
            values = row.get("values", [])
            resolved_color: str = DEFAULT_BACKGROUND_COLOR
            if values:
                first_value = values[0]
                resolved_color = self._resolve_background_color(
                    first_value, theme_palette
                )
            colors[offset] = resolved_color or DEFAULT_BACKGROUND_COLOR
        if row_data:
            first_missing_row = start_row + len(row_data)
        else:
            first_missing_row = start_row
        for row_index in range(first_missing_row, end_row + 1):
            colors[row_index] = DEFAULT_BACKGROUND_COLOR
        return colors

    @staticmethod
    def _collect_row_data(sheet_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        for section in sheet_data.get("data", []) or []:
            for row in section.get("rowData", []) or []:
                collected.append(row)
        return collected

    @staticmethod
    def _extract_sheet_data(
        metadata: Dict[str, Any], sheet_id: int
    ) -> Optional[Dict[str, Any]]:
        for data in metadata.get("sheets", []) or []:
            if data.get("properties", {}).get("sheetId") == sheet_id:
                return data
        return None

    @staticmethod
    def _build_theme_palette(
        spreadsheet_theme: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        palette: Dict[str, Dict[str, Any]] = {}
        for entry in spreadsheet_theme.get("themeColors", []) or []:
            color_type = entry.get("colorType")
            color_style = entry.get("color", {}) or {}
            rgb_color = color_style.get("rgbColor")
            if color_type and rgb_color:
                palette[color_type] = rgb_color
        for color_type, hex_color in DEFAULT_THEME_COLOR_HEX.items():
            if color_type not in palette:
                converted = GoogleSheetsClient._hex_to_color_dict(hex_color)
                if converted:
                    palette[color_type] = converted
        return palette

    def _resolve_background_color(
        self, cell_value: Dict[str, Any], theme_palette: Dict[str, Dict[str, Any]]
    ) -> str:
        effective_format = cell_value.get("effectiveFormat", {}) or {}
        user_entered_format = cell_value.get("userEnteredFormat", {}) or {}

        color_candidates = [
            effective_format.get("backgroundColor"),
            effective_format.get("backgroundColorStyle"),
            user_entered_format.get("backgroundColor"),
            user_entered_format.get("backgroundColorStyle"),
        ]

        had_any_candidate = False
        for candidate in color_candidates:
            if not candidate:
                continue
            had_any_candidate = True
            resolved = self._resolve_color_candidate(candidate, theme_palette)
            if resolved:
                return resolved

        if had_any_candidate:
            return NOT_DETERMINED_COLOR
        return DEFAULT_BACKGROUND_COLOR

    def _resolve_color_candidate(
        self, candidate: Dict[str, Any], theme_palette: Dict[str, Dict[str, Any]]
    ) -> Optional[str]:
        if not candidate:
            return None
        if {"red", "green", "blue"}.intersection(candidate.keys()):
            return self._convert_color_to_hex(candidate)

        rgb_color = candidate.get("rgbColor")
        if rgb_color:
            return self._convert_color_to_hex(rgb_color)

        theme_color = candidate.get("themeColor")
        if theme_color:
            palette_color = theme_palette.get(theme_color)
            if not palette_color:
                palette_color = GoogleSheetsClient._hex_to_color_dict(
                    DEFAULT_THEME_COLOR_HEX.get(theme_color, "")
                )
            if palette_color:
                tinted = GoogleSheetsClient._apply_tint_to_color(
                    palette_color, candidate.get("tint")
                )
                return self._convert_color_to_hex(tinted)
        return None

    @staticmethod
    def _hex_to_color_dict(hex_color: str) -> Dict[str, float]:
        hex_value = hex_color.lstrip("#")
        if len(hex_value) != 6:
            return {}
        red = int(hex_value[0:2], 16) / 255
        green = int(hex_value[2:4], 16) / 255
        blue = int(hex_value[4:6], 16) / 255
        return {"red": red, "green": green, "blue": blue}

    @staticmethod
    def _apply_tint_to_color(
        base_color: Dict[str, Any], tint: Optional[float]
    ) -> Dict[str, Any]:
        if tint is None:
            return dict(base_color)

        def clamp(value: float) -> float:
            return max(0.0, min(1.0, value))

        adjusted: Dict[str, float] = {}
        for channel in ("red", "green", "blue"):
            component = float(base_color.get(channel, 0.0))
            component = clamp(component)
            if tint >= 0:
                component = component + (1.0 - component) * min(tint, 1.0)
            else:
                component = component * (1.0 + max(tint, -1.0))
            adjusted[channel] = clamp(component)
        return adjusted

    @staticmethod
    def _convert_color_to_hex(color: Optional[Dict[str, Any]]) -> Optional[str]:
        if not color:
            return None

        def normalize_component(value: Any) -> Optional[int]:
            if value is None:
                return None
            try:
                component = float(value)
            except (TypeError, ValueError):
                return None
            if component < 0:
                component = 0
            if component > 1:
                # Если значение уже в диапазоне 0-255.
                if component > 255:
                    component = 255
                return int(round(component))
            return int(round(component * 255))

        red = normalize_component(color.get("red"))
        green = normalize_component(color.get("green"))
        blue = normalize_component(color.get("blue"))
        if red is None or green is None or blue is None:
            return None
        return f"#{red:02x}{green:02x}{blue:02x}"

    def write_posts_metrics(
        self,
        rows: Iterable[Dict[str, Any]],
        worksheet: str = "Data_Po_kagdomy_posty",
        timestamp_column: str = "updated_at",
    ) -> None:
        """Записывает агрегированные метрики в Google Sheets.

        Args:
            rows: Коллекция словарей с данными по постам.
            worksheet: Имя листа для записи.
            timestamp_column: Колонка с отметкой времени обновления.
        """

        sheet = self._get_worksheet(worksheet)
        rows_list = list(rows)
        try:
            df = pd.DataFrame(rows_list)
            if df.empty:
                self._state_store.update_last_metrics_write()
                return
            now = dt.datetime.now(TIMEZONE).isoformat()
            df[timestamp_column] = now
            df = self._deduplicate(df, timestamp_column)

            existing_records = sheet.get_all_records()
            existing_df = pd.DataFrame(existing_records)

            if not existing_df.empty:
                merged_df = self._merge_existing(
                    existing_df, df, timestamp_column=timestamp_column
                )
            else:
                merged_df = df

            merged_df = self._align_columns(existing_df, merged_df)
            merged_df = self._sort_by_publish_time(
                merged_df, publish_column=PUBLISH_TIME_COLUMN
            )

            columns = list(merged_df.columns)
            final_values = merged_df.map(self._stringify_value)

            total_rows = max(len(existing_df.index), len(final_values.index))
            padded_rows = final_values.to_numpy().tolist()
            if total_rows > len(padded_rows):
                padded_rows.extend(
                    [[""] * len(columns) for _ in range(total_rows - len(padded_rows))]
                )

            all_values = [columns] + padded_rows
            total_rows_needed = len(all_values)
            if total_rows_needed > sheet.row_count:
                sheet.add_rows(total_rows_needed - sheet.row_count)

            end_cell = rowcol_to_a1(total_rows_needed, len(columns))
            sheet.batch_update([{"range": f"A1:{end_cell}", "values": all_values}])

            if final_values.index.size > 0:
                self._apply_formatting(
                    sheet,
                    start_row=2,
                    rows_count=final_values.index.size,
                    columns=len(columns),
                )

            self._state_store.update_last_metrics_write()
        except Exception:
            logging.exception(
                "Не удалось записать метрики в Google Sheets",
                extra={
                    "context": json.dumps(
                        {"worksheet": worksheet, "rows": len(rows_list)}
                    )
                },
            )
            raise

    def _get_worksheet(self, worksheet: str) -> Any:
        for attempt in range(1, self._SHEETS_MAX_ATTEMPTS + 1):
            try:
                spreadsheet = self._client.open_by_key(self._table_id)
                return spreadsheet.worksheet(worksheet)
            except Exception as error:
                if self._should_retry_sheets_error(error) and attempt < self._SHEETS_MAX_ATTEMPTS:
                    wait_seconds = self._compute_sheets_wait(attempt)
                    wait_milliseconds = int(wait_seconds * 1000)
                    logging.warning(
                        "Повторное обращение к Google Sheets из-за временной ошибки",
                        extra={
                            "context": json.dumps(
                                {
                                    "worksheet": worksheet,
                                    "attempt": attempt + 1,
                                    "wait_seconds": wait_seconds,
                                    "wait_milliseconds": wait_milliseconds,
                                }
                            )
                        },
                    )
                    time.sleep(wait_seconds)
                    continue
                logging.exception(
                    "Не удалось получить лист Google Sheets",
                    extra={"context": json.dumps({"worksheet": worksheet})},
                )
                raise
        raise RuntimeError("Не удалось получить лист Google Sheets после повторных попыток")

    def _should_retry_sheets_error(self, error: Exception) -> bool:
        if isinstance(error, APIError):
            response = getattr(error, "response", None)
            status_code = None
            if response is not None:
                status_code = getattr(response, "status_code", None) or getattr(
                    response, "status", None
                )
            if status_code == 503:
                return True
            message = str(error)
            if "503" in message or "UNAVAILABLE" in message.upper():
                return True
        message = str(error)
        if "UNAVAILABLE" in message.upper() or "503" in message:
            return True
        return False

    def _compute_sheets_wait(self, attempt: int) -> float:
        exponent = max(attempt - 1, 0)
        return self._SHEETS_INITIAL_WAIT_SECONDS * (
            self._SHEETS_BACKOFF_MULTIPLIER ** exponent
        )

    def _merge_existing(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame,
        *,
        timestamp_column: str,
    ) -> pd.DataFrame:
        key_columns = [
            col for col in ("account_name", "post_id") if col in new_df.columns
        ]
        if not key_columns:
            key_columns = [
                col for col in new_df.columns if col not in {timestamp_column}
            ]
        if not key_columns:
            return new_df

        key_columns = list(key_columns)

        existing_df = existing_df.copy()
        new_df = new_df.copy()

        for column in key_columns:
            if column not in existing_df.columns:
                existing_df[column] = pd.NA
            existing_df[column] = existing_df[column].map(self._normalize_key)
            new_df[column] = new_df[column].map(self._normalize_key)

        existing_df = existing_df.drop_duplicates(subset=key_columns, keep="last")
        new_df = new_df.drop_duplicates(subset=key_columns, keep="last")

        existing_df = existing_df.set_index(key_columns)
        new_df = new_df.set_index(key_columns)

        for column in new_df.columns:
            if column not in existing_df.columns:
                existing_df[column] = pd.NA
        for column in existing_df.columns:
            if column not in new_df.columns:
                new_df[column] = pd.NA

        merged = existing_df.combine_first(new_df)
        merged.update(new_df)
        return merged.reset_index()

    def _deduplicate(self, df: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
        key_columns = [col for col in ("account_name", "post_id") if col in df.columns]
        if not key_columns:
            key_columns = [col for col in df.columns if col not in {timestamp_column}]
        if not key_columns:
            return df

        df = df.copy()
        for column in key_columns:
            df[column] = df[column].map(self._normalize_key)
        return df.drop_duplicates(subset=key_columns, keep="last")

    def _align_columns(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> pd.DataFrame:
        desired_order: List[str] = list(new_df.columns)
        if not existing_df.empty:
            for column in existing_df.columns:
                if column not in desired_order:
                    desired_order.append(column)
        return new_df.reindex(columns=desired_order, fill_value=pd.NA)

    @staticmethod
    def _sort_by_publish_time(df: pd.DataFrame, *, publish_column: str) -> pd.DataFrame:
        if publish_column not in df.columns:
            return df.reset_index(drop=True)

        df_sorted = df.copy()
        sort_key = pd.to_datetime(df_sorted[publish_column], errors="coerce")
        df_sorted = df_sorted.assign(_sort_key=sort_key)
        df_sorted = df_sorted.sort_values(
            by="_sort_key", kind="mergesort", na_position="first"
        )
        df_sorted = df_sorted.drop(columns="_sort_key")
        return df_sorted.reset_index(drop=True)

    @staticmethod
    def _normalize_key(value: Any) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, float) and float(value).is_integer():
            return str(int(value))
        return str(value)

    def _apply_formatting(
        self, sheet: Any, *, start_row: int, rows_count: int, columns: int
    ) -> None:
        if rows_count <= 0 or columns <= 0:
            return
        try:
            end_row = start_row + rows_count - 1
            start_cell = rowcol_to_a1(start_row, 1)
            end_cell = rowcol_to_a1(end_row, columns)
            sheet.format(f"{start_cell}:{end_cell}", {"wrapStrategy": "OVERFLOW_CELL"})
            sheet.spreadsheet.batch_update(
                {
                    "requests": [
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet.id,
                                    "dimension": "ROWS",
                                    "startIndex": start_row - 1,
                                    "endIndex": end_row,
                                },
                                "properties": {"pixelSize": 21},
                                "fields": "pixelSize",
                            }
                        }
                    ]
                }
            )
        except Exception:
            logging.exception(
                "Не удалось применить форматирование листа Google Sheets",
                extra={"context": json.dumps({"rows": rows_count, "columns": columns})},
            )

    def get_last_processed_cursor(self, account_name: str) -> Optional[str]:
        """Возвращает последний курсор пагинации для аккаунта."""

        return self._state_store.get_account_cursor(account_name)

    def set_last_processed_cursor(self, account_name: str, cursor: str) -> None:
        """Сохраняет последний курсор пагинации для аккаунта."""

        self._state_store.set_account_cursor(account_name, cursor)

    def should_refresh_metrics(self, *, ttl_minutes: int) -> bool:
        """Определяет, нужно ли обновлять метрики."""

        last_update = self._state_store.get_last_metrics_write()
        if not last_update:
            return True
        now = dt.datetime.now(TIMEZONE)
        return now - last_update >= dt.timedelta(minutes=ttl_minutes)


__all__ = ["GoogleSheetsClient", "AccountToken"]
