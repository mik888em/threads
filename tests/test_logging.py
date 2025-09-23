"""Тесты для настройки логирования."""

from __future__ import annotations

import io
import json
import logging
from contextlib import contextmanager
from typing import Any, Iterator

from threads_metrics.main import setup_logging


def _reset_logging() -> None:
    """Очищает обработчики корневого логгера."""

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


@contextmanager
def _capture_root_stream() -> Iterator[tuple[logging.Handler, io.StringIO]]:
    """Подменяет поток первого обработчика корневого логгера."""

    if not logging.root.handlers:
        raise AssertionError("Ожидался хотя бы один обработчик логирования")
    handler = logging.root.handlers[0]
    stream = io.StringIO()
    original_stream = handler.stream
    handler.setStream(stream)
    try:
        yield handler, stream
    finally:
        handler.setStream(original_stream)


def test_setup_logging_sets_default_context() -> None:
    """Проверяет, что форматтер добавляет контекст по умолчанию."""

    payload: dict[str, Any] | None = None
    _reset_logging()
    try:
        setup_logging()
        with _capture_root_stream() as (handler, stream):
            logging.info("test message without extra")
            handler.flush()
            payload = json.loads(stream.getvalue())
    finally:
        _reset_logging()
    assert payload is not None
    assert payload["msg"] == "test message without extra"
    assert payload["context"] == {}


def test_setup_logging_with_custom_context() -> None:
    """Проверяет корректное форматирование пользовательского контекста."""

    payload: dict[str, Any] | None = None
    _reset_logging()
    try:
        setup_logging()
        custom_context = {"foo": "bar"}
        with _capture_root_stream() as (handler, stream):
            logging.info("test message", extra={"context": json.dumps(custom_context)})
            handler.flush()
            payload = json.loads(stream.getvalue())
    finally:
        _reset_logging()
    assert payload is not None
    assert payload["msg"] == "test message"
    assert payload["context"] == custom_context
