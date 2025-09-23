"""Тесты для настройки логирования."""

from __future__ import annotations

import io
import logging

from threads_metrics.main import setup_logging


def test_setup_logging_sets_default_context() -> None:
    """Проверяет, что фабрика логов добавляет контекст по умолчанию."""

    original_factory = logging.getLogRecordFactory()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logger = logging.getLogger("threads.test.logger")
    handler = logging.StreamHandler(io.StringIO())
    handler.setFormatter(logging.Formatter("%(context)s"))
    logger.addHandler(handler)
    logger.propagate = False

    try:
        setup_logging()
        logger.info("test message without extra")
        handler.flush()
        assert handler.stream.getvalue().strip() == "{}"
    finally:
        logging.setLogRecordFactory(original_factory)
        logger.removeHandler(handler)
        logging.root.handlers.clear()
