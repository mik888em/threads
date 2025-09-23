"""Работа с Google Sheets."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from .state_store import StateStore

TIMEZONE = dt.timezone(dt.timedelta(hours=3), name="Europe/Athens")


@dataclass(slots=True)
class AccountToken:
    """Токен Threads из Google Sheets."""

    account_name: str
    token: str


class GoogleSheetsClient:
    """Обёртка для доступа к Google Sheets."""

    def __init__(self, *, table_id: str, service_account_info: Dict[str, Any], state_store: StateStore) -> None:
        """Создаёт клиента для взаимодействия с Google Sheets.

        Args:
            table_id: Идентификатор таблицы.
            service_account_info: Данные сервисного аккаунта Google.
            state_store: Хранилище состояния приложения.
        """

        self._table_id = table_id
        self._credentials = Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        self._client = gspread.authorize(self._credentials)
        self._state_store = state_store

    def read_account_tokens(self, worksheet: str = "accounts_threads") -> List[AccountToken]:
        """Считывает токены аккаунтов Threads.

        Args:
            worksheet: Имя листа с токенами.

        Returns:
            Список токенов аккаунтов.
        """

        sheet = self._client.open_by_key(self._table_id).worksheet(worksheet)
        records = sheet.get_all_records()
        tokens: List[AccountToken] = []
        for row in records:
            normalized_row = {
                "_".join(str(key).strip().lower().split()): value for key, value in row.items()
            }
            token = self._get_first_present(normalized_row, ("token", "access_token", "bearer_token"))
            account = self._get_first_present(normalized_row, ("account", "name", "nickname"))
            if token and account:
                tokens.append(AccountToken(account_name=str(account), token=str(token)))
        return tokens

    @staticmethod
    def _get_first_present(row: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
        for key in keys:
            value = row.get(key)
            if value:
                return value
        return None

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

        sheet = self._client.open_by_key(self._table_id).worksheet(worksheet)
        df = pd.DataFrame(rows)
        if df.empty:
            self._state_store.update_last_metrics_write()
            return
        now = dt.datetime.now(TIMEZONE).isoformat()
        df[timestamp_column] = now

        existing = sheet.get_all_records()
        existing_df = pd.DataFrame(existing)
        if not existing_df.empty:
            for column in df.columns:
                if column not in existing_df.columns:
                    existing_df[column] = pd.NA
            df = self._merge_existing(existing_df, df)

        sheet.clear()
        sheet.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
        self._state_store.update_last_metrics_write()

    def _merge_existing(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        timestamp_column = "updated_at"
        key_columns = [col for col in ("account_name", "post_id") if col in new_df.columns]
        if not key_columns:
            key_columns = [col for col in new_df.columns if col not in {timestamp_column}]
        if not key_columns:
            return new_df

        existing_df = existing_df.set_index(key_columns)
        new_df = new_df.set_index(key_columns)
        merged = existing_df.combine_first(new_df)
        merged.update(new_df)
        return merged.reset_index()

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
