"""Точка входа приложения для сбора метрик Threads."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import random
import signal
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping

import httpx

from .aggregation import aggregate_posts
from .config import Config, ConfigError
from .google_sheets import AccountToken, GoogleSheetsClient
from .gh_cancel import DEFAULT_INTERVAL_SECONDS, cancel_pending_workflow_runs
from .state_store import StateStore, TIMEZONE
from .threads_client import ThreadsClient, ThreadsAPIError, INSIGHTS_METRICS


@dataclass(frozen=True)
class RetrySettings:
    """Настройки повторных запросов Insights."""

    max_attempts: int = 3
    pause_range: tuple[float, float] = (20.0, 30.0)

    def normalized_pause_range(self) -> tuple[float, float]:
        """Возвращает отсортированные границы паузы."""

        start, end = self.pause_range
        if end < start:
            return end, start
        return start, end

HEARTBEAT_INTERVAL = 30


class ContextJsonFormatter(logging.Formatter):
    """Форматтер, добавляющий пустой контекст при необходимости."""

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "context"):
            record.context = json.dumps({})
        formatted = super().format(record)
        account_label = getattr(record, "account_label", None)
        if account_label:
            return f'| nick account: "{account_label}" {formatted}'
        return formatted


def setup_logging() -> None:
    """Настраивает вывод логов в формате JSON."""

    handler = logging.StreamHandler()
    handler.setFormatter(
        ContextJsonFormatter(
            fmt='{"ts":"%(asctime)sZ","level":"%(levelname)s","msg":"%(message)s","context":%(context)s}',
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def _require_github_env() -> tuple[str, str, str]:
    """Возвращает параметры репозитория из окружения."""

    required = ("GITHUB_OWNER", "GITHUB_REPO", "GITHUB_TOKEN")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        message = f"Отсутствуют переменные окружения: {', '.join(missing)}"
        logging.error(message, extra={"context": json.dumps({})})
        raise ConfigError(message)

    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]
    token = os.environ["GITHUB_TOKEN"]
    return owner, repo, token


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Разбирает аргументы командной строки."""

    parser = argparse.ArgumentParser(description="Сборщик метрик Threads")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Сбор метрик Threads")
    run_parser.set_defaults(command="run")

    cancel_parser = subparsers.add_parser(
        "cancel-pending", help="Отмена ожидающих запусков workflow threads-metrics"
    )
    cancel_parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Интервал проверки GitHub Actions (секунды)",
    )
    cancel_parser.set_defaults(command="cancel-pending")

    args = parser.parse_args(argv)
    if args.command is None:
        setattr(args, "command", "run")
    return args


@asynccontextmanager
async def app_dependencies(config: Config) -> Any:
    """Создаёт и освобождает ресурсы приложения."""

    state_store = StateStore(config.state_file)
    client = ThreadsClient(
        base_url=config.threads_api_base_url,
        timeout=config.request_timeout,
        concurrency_limit=config.concurrency_limit,
        posts_url_override=config.threads_posts_url_override,
    )
    sheets = GoogleSheetsClient(
        table_id=config.google_table_id,
        service_account_info=config.service_account_info,
        state_store=state_store,
    )

    try:
        yield {
            "config": config,
            "state_store": state_store,
            "threads_client": client,
            "sheets_client": sheets,
        }
    finally:
        await client.close()


async def run_service(config: Config) -> None:
    """Основной сценарий работы сервиса."""

    async with app_dependencies(config) as deps:
        config = deps["config"]
        sheets: GoogleSheetsClient = deps["sheets_client"]
        threads_client: ThreadsClient = deps["threads_client"]
        state_store: StateStore = deps["state_store"]

        lock_acquired = state_store.try_acquire_run_lock(
            max_age=dt.timedelta(minutes=config.run_timeout_minutes)
        )
        if not lock_acquired:
            logging.info(
                "Предыдущий запуск ещё выполняется, завершаемся",
                extra={"context": json.dumps({})},
            )
            return

        try:
            if not sheets.should_refresh_metrics(ttl_minutes=config.metrics_ttl_minutes):
                logging.info(
                    "Метрики актуальны, обновление не требуется",
                    extra={"context": json.dumps({})},
                )
                return

            tokens = sheets.read_account_tokens()
            logging.info(
                "Найдено аккаунтов: %d", len(tokens), extra={"context": json.dumps({})}
            )

            posts = await collect_posts(tokens, threads_client, sheets)
            token_map = {token.account_name: token.token for token in tokens}
            insights = await collect_insights(
                posts,
                token_map,
                threads_client,
                state_store,
                ttl_minutes=config.metrics_ttl_minutes,
            )
            metrics = aggregate_posts(posts, insights)
            sheets.write_posts_metrics(metrics)
            logging.info(
                "Метрики обновлены", extra={"context": json.dumps({"posts": len(posts)})}
            )
        finally:
            state_store.release_run_lock()


async def collect_posts(
    tokens: List[AccountToken],
    client: ThreadsClient,
    sheets: GoogleSheetsClient,
) -> List[Dict[str, Any]]:
    """Собирает посты для всех аккаунтов."""

    async def _collect_for_account(token: AccountToken) -> List[Dict[str, Any]]:
        cursor = sheets.get_last_processed_cursor(token.account_name)
        logging.info(
            "Начинаем загрузку постов для аккаунта",
            extra={
                "context": json.dumps(
                    {
                        "account": token.account_name,
                        "has_saved_cursor": bool(cursor),
                    }
                ),
                "account_label": token.account_name,
            },
        )
        try:
            result = await client.fetch_posts(
                token.token, after=cursor, account_name=token.account_name
            )
        except (httpx.HTTPStatusError, ThreadsAPIError) as exc:
            logging.warning(
                "Не удалось получить посты для аккаунта %s: %s",
                token.account_name,
                exc,
                extra={
                    "context": json.dumps({"account": token.account_name}),
                    "account_label": token.account_name,
                },
            )
            return []
        posts_data = []
        for post in result.posts:
            post_data = post.data | {"permalink": post.permalink, "account_name": token.account_name}
            posts_data.append(post_data)
        if result.next_cursor:
            sheets.set_last_processed_cursor(token.account_name, result.next_cursor)
        logging.info(
            "Получены посты для аккаунта",
            extra={
                "context": json.dumps(
                    {
                        "account": token.account_name,
                        "posts": len(posts_data),
                        "has_next_cursor": bool(result.next_cursor),
                    }
                ),
                "account_label": token.account_name,
            },
        )
        return posts_data

    semaphore = asyncio.Semaphore(client.concurrency_limit)

    async def _bounded(task: AccountToken) -> List[Dict[str, Any]]:
        async with semaphore:
            return await _collect_for_account(task)

    tasks = [asyncio.create_task(_bounded(token)) for token in tokens]
    results: List[List[Dict[str, Any]]] = await asyncio.gather(*tasks, return_exceptions=False)
    flat: List[Dict[str, Any]] = [item for sublist in results for item in sublist]
    return flat


async def collect_insights(
    posts: List[Dict[str, Any]],
    tokens: Mapping[str, str],
    client: ThreadsClient,
    state_store: StateStore,
    ttl_minutes: int,
    retry_settings: RetrySettings | None = None,
) -> Dict[str, Dict[str, int]]:
    """Параллельно собирает Insights для постов."""

    retry_settings = retry_settings or RetrySettings()
    failed_requests: List[tuple[str, str, str]] = []

    async def _fetch(
        post_id: str, token: str, account_name: str
    ) -> tuple[str, Dict[str, int], dt.datetime] | None:
        logging.info(
            "Запрашиваем инсайты для поста",
            extra={
                "context": json.dumps(
                    {"post_id": post_id, "account_name": account_name}
                ),
                "account_label": account_name,
            },
        )
        try:
            insights = await client.fetch_post_insights(
                token, post_id, account_name=account_name
            )
        except Exception:
            logging.exception(
                "Не удалось получить инсайты для поста",
                extra={
                    "context": json.dumps(
                        {"post_id": post_id, "account_name": account_name}
                    ),
                    "account_label": account_name,
                },
            )
            failed_requests.append((post_id, token, account_name))
            return None

        fetched_at = dt.datetime.now(TIMEZONE)
        logging.info(
            "Инсайты успешно получены",
            extra={
                "context": json.dumps(
                    {"post_id": post_id, "account_name": account_name}
                ),
                "account_label": account_name,
            },
        )
        return post_id, insights, fetched_at

    tasks: List[asyncio.Task[tuple[str, Dict[str, int], dt.datetime] | None]] = []
    for post in posts:
        raw_post_id = post.get("id")
        account_name = post.get("account_name")
        if not raw_post_id or not account_name:
            continue
        post_id = str(raw_post_id)
        token = tokens.get(str(account_name))
        if not token:
            continue
        if not state_store.should_refresh_post_metrics(post_id, ttl_minutes):
            continue

        tasks.append(asyncio.create_task(_fetch(post_id, token, str(account_name))))

    insights_map: Dict[str, Dict[str, int]] = {}
    if not tasks:
        return insights_map

    results = await asyncio.gather(*tasks)
    updates: Dict[str, dt.datetime] = {}
    for result in results:
        if result is None:
            continue
        post_id, insights, fetched_at = result
        insights_map[post_id] = insights
        updates[post_id] = fetched_at

    if failed_requests:
        extra_insights, extra_updates = await retry_failed_insights(
            failed_requests, client, retry_settings
        )
        insights_map.update(extra_insights)
        updates.update(extra_updates)

    if updates:
        state_store.update_post_metrics_many(updates)
    return insights_map


async def retry_failed_insights(
    failed_requests: List[tuple[str, str, str]],
    client: ThreadsClient,
    retry_settings: RetrySettings,
) -> tuple[Dict[str, Dict[str, int]], Dict[str, dt.datetime]]:
    """Выполняет дополнительные попытки запросов Insights."""

    if not failed_requests:
        return {}, {}

    logging.info(
        "================ Повторные попытки запросов Insights ================",
        extra={
            "context": json.dumps(
                {
                    "count": len(failed_requests),
                    "post_ids": [post_id for post_id, _, _ in failed_requests],
                }
            ),
        },
    )

    async def _retry_single(
        post_id: str, token: str, account_name: str
    ) -> tuple[str, Dict[str, int], dt.datetime] | None:
        max_attempts = max(1, retry_settings.max_attempts)
        params = {"metric": ",".join(INSIGHTS_METRICS)}
        url = client.build_absolute_url(f"/{post_id}/insights", params=params)

        logging.info(
            "Запускаем повторные попытки запроса",
            extra={
                "context": json.dumps(
                    {"post_id": post_id, "account_name": account_name, "url": url}
                ),
                "account_label": account_name,
            },
        )

        pause_start, pause_end = retry_settings.normalized_pause_range()

        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                pause = random.uniform(pause_start, pause_end)
                logging.info(
                    "Пауза перед повторной попыткой %.2f секунд",
                    pause,
                    extra={
                        "context": json.dumps(
                            {
                                "post_id": post_id,
                                "account_name": account_name,
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "url": url,
                                "pause": pause,
                            }
                        ),
                        "account_label": account_name,
                    },
                )
                await asyncio.sleep(pause)

            logging.info(
                "Дополнительная попытка %d из %d",
                attempt,
                max_attempts,
                extra={
                    "context": json.dumps(
                        {
                            "post_id": post_id,
                            "account_name": account_name,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "url": url,
                        }
                    ),
                    "account_label": account_name,
                },
            )

            try:
                insights = await client.fetch_post_insights(
                    token, post_id, account_name=account_name
                )
            except Exception as exc:
                logging.warning(
                    "Дополнительная попытка %d из %d завершилась ошибкой: %s",
                    attempt,
                    max_attempts,
                    exc,
                    extra={
                        "context": json.dumps(
                            {
                                "post_id": post_id,
                                "account_name": account_name,
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "url": url,
                            }
                        ),
                        "account_label": account_name,
                    },
                )
                if attempt == max_attempts:
                    logging.error(
                        "Инсайты не получены после %d дополнительных попыток",
                        max_attempts,
                        extra={
                            "context": json.dumps(
                                {
                                    "post_id": post_id,
                                    "account_name": account_name,
                                    "attempt": attempt,
                                    "max_attempts": max_attempts,
                                    "url": url,
                                }
                            ),
                            "account_label": account_name,
                        },
                    )
                continue

            fetched_at = dt.datetime.now(TIMEZONE)
            logging.info(
                "Инсайты получены на дополнительной попытке",
                extra={
                    "context": json.dumps(
                        {
                            "post_id": post_id,
                            "account_name": account_name,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "url": url,
                        }
                    ),
                    "account_label": account_name,
                },
            )
            return post_id, insights, fetched_at

        return None

    tasks = [
        asyncio.create_task(_retry_single(post_id, token, account_name))
        for post_id, token, account_name in failed_requests
    ]
    results = await asyncio.gather(*tasks)

    insights_map: Dict[str, Dict[str, int]] = {}
    updates: Dict[str, dt.datetime] = {}
    for item in results:
        if not item:
            continue
        post_id, insights, fetched_at = item
        insights_map[post_id] = insights
        updates[post_id] = fetched_at

    return insights_map, updates


async def heartbeat() -> None:
    """Периодически пишет heartbeat-логи."""

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        logging.info("heartbeat", extra={"context": json.dumps({})})


async def main_async(config: Config) -> None:
    """Запускает сервис с таймаутом и обработкой сигналов."""

    stop_event = asyncio.Event()
    timeout_seconds = config.run_timeout_minutes * 60

    def _handle_signal(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    heartbeat_task = asyncio.create_task(heartbeat())
    service_task = asyncio.create_task(run_service(config))
    timeout_task = asyncio.create_task(asyncio.sleep(timeout_seconds))
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        {service_task, timeout_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )

    if timeout_task in done and not service_task.done():
        logging.warning("Таймаут работы сервиса", extra={"context": json.dumps({})})
        service_task.cancel()
    if stop_task in done and not service_task.done():
        logging.info("Получен сигнал остановки", extra={"context": json.dumps({})})
        service_task.cancel()

    try:
        await service_task
    except asyncio.CancelledError:
        logging.info("Сервис остановлен до завершения", extra={"context": json.dumps({})})

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    timeout_task.cancel()
    stop_task.cancel()

    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass


def main(argv: Iterable[str] | None = None) -> None:
    """CLI-обёртка над асинхронным запуском."""

    setup_logging()
    args = parse_args(argv)
    command = getattr(args, "command", "run")

    if command == "run":
        try:
            config = Config.from_env()
        except ConfigError as exc:
            logging.error(
                "Ошибка конфигурации: %s", exc, extra={"context": json.dumps({})}
            )
            raise
        asyncio.run(main_async(config))
        return

    if command == "cancel-pending":
        owner, repo, token = _require_github_env()
        interval = getattr(args, "interval", DEFAULT_INTERVAL_SECONDS)
        if interval < 0:
            raise ConfigError("Интервал не может быть отрицательным")
        logging.info(
            "Запуск отмены очереди GitHub Actions",
            extra={"context": json.dumps({"owner": owner, "repo": repo})},
        )
        asyncio.run(
            cancel_pending_workflow_runs(owner, repo, token, interval_seconds=interval)
        )
        return

    raise ConfigError(f"Неизвестная команда: {command}")


if __name__ == "__main__":
    main()
