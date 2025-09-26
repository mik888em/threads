"""Инструменты для отмены ожидающих запусков GitHub Actions."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Dict, List, Optional

import httpx

GITHUB_API_URL = "https://api.github.com"
WORKFLOW_FILE = "threads-metrics.yml"
DEFAULT_INTERVAL_SECONDS = 10
MAX_BACKOFF_SECONDS = 600


def _context(data: Optional[Dict[str, object]] = None) -> Dict[str, str]:
    """Возвращает контекст для JSON-логирования."""

    return {"context": json.dumps(data or {})}


async def _fetch_runs(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    status: str,
) -> List[Dict[str, object]]:
    """Получает список запусков workflow в заданном статусе."""

    response = await client.get(
        f"/repos/{owner}/{repo}/actions/runs",
        params={"workflow_id": WORKFLOW_FILE, "status": status},
    )
    remaining = response.headers.get("X-RateLimit-Remaining")
    response.raise_for_status()
    payload = response.json()
    runs = payload.get("workflow_runs", [])
    logging.info(
        "Получены запуски workflow",
        extra=_context(
            {
                "status": status,
                "runs": len(runs),
                "rate_limit_remaining": remaining,
            }
        ),
    )
    return runs


async def _cancel_run(
    client: httpx.AsyncClient, owner: str, repo: str, run_id: int | str
) -> None:
    """Отправляет запрос на отмену конкретного запуска."""

    response = await client.post(f"/repos/{owner}/{repo}/actions/runs/{run_id}/cancel")
    remaining = response.headers.get("X-RateLimit-Remaining")
    response.raise_for_status()
    logging.info(
        "Запуск отменён",
        extra=_context(
            {
                "run_id": str(run_id),
                "rate_limit_remaining": remaining,
            }
        ),
    )


async def _process_iteration(client: httpx.AsyncClient, owner: str, repo: str) -> None:
    """Проводит одну итерацию проверки и отмены очереди."""

    in_progress_runs = await _fetch_runs(client, owner, repo, status="in_progress")
    queued_runs = await _fetch_runs(client, owner, repo, status="queued")

    if not in_progress_runs:
        logging.info(
            "Активные запуски не найдены",
            extra=_context({"queued": len(queued_runs)}),
        )
        return

    if not queued_runs:
        logging.info(
            "Очередь запусков пуста",
            extra=_context({"active_runs": len(in_progress_runs)}),
        )
        return

    cancellation_tasks = []
    for run in queued_runs:
        run_id = run.get("id")
        if run_id is None:
            logging.warning(
                "Пропущен запуск без идентификатора",
                extra=_context({"run": run}),
            )
            continue
        cancellation_tasks.append(_cancel_run(client, owner, repo, run_id))

    if not cancellation_tasks:
        logging.info(
            "Запуски для отмены не найдены",
            extra=_context({"queued": len(queued_runs)}),
        )
        return

    await asyncio.gather(*cancellation_tasks)
    logging.info(
        "Отменены ожидающие запуски",
        extra=_context(
            {
                "cancelled": len(cancellation_tasks),
                "active_runs": len(in_progress_runs),
                "queued": len(queued_runs),
            }
        ),
    )


async def cancel_pending_workflow_runs(
    owner: str,
    repo: str,
    token: str,
    *,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    max_iterations: Optional[int] = None,
    stop_event: Optional[asyncio.Event] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> None:
    """Отслеживает и отменяет очереди для workflow threads-metrics."""

    if interval_seconds < 0:
        raise ValueError("Интервал ожидания не может быть отрицательным")

    local_stop_event = stop_event or asyncio.Event()

    loop = asyncio.get_running_loop()

    def _handle_signal(signum: int) -> None:
        signal_name = signal.Signals(signum).name
        logging.info(
            "Получен сигнал остановки",
            extra=_context({"signal": signal_name}),
        )
        local_stop_event.set()

    registered_signals: List[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig.value)
        except (NotImplementedError, RuntimeError):
            continue
        registered_signals.append(sig)

    close_client = False
    if client is None:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "threads-metrics-cancel/1.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        client = httpx.AsyncClient(
            base_url=GITHUB_API_URL, headers=headers, timeout=10.0
        )
        close_client = True

    backoff_seconds = interval_seconds or DEFAULT_INTERVAL_SECONDS
    iteration = 0

    try:
        while True:
            if local_stop_event.is_set():
                logging.info("Остановка цикла отмены запрошена", extra=_context({}))
                break

            iteration += 1
            try:
                await _process_iteration(client, owner, repo)
                backoff_seconds = interval_seconds or DEFAULT_INTERVAL_SECONDS
            except httpx.HTTPStatusError as exc:  # pragma: no cover - разбор статусов
                status_code = exc.response.status_code if exc.response else None
                remaining = (
                    exc.response.headers.get("X-RateLimit-Remaining")
                    if exc.response
                    else None
                )
                if status_code in {403, 429}:
                    logging.warning(
                        "Получен ответ об ограничении API",
                        extra=_context(
                            {
                                "status": status_code,
                                "backoff": backoff_seconds,
                                "rate_limit_remaining": remaining,
                            }
                        ),
                    )
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)
                    continue
                logging.error(
                    "Ошибка GitHub API", extra=_context({"status": status_code})
                )
                await asyncio.sleep(backoff_seconds)
                continue
            except httpx.HTTPError as exc:  # pragma: no cover - сетевые ошибки
                logging.error(
                    "Сетевая ошибка при обращении к GitHub API: %s",
                    exc,
                    extra=_context({}),
                )
                await asyncio.sleep(backoff_seconds)
                continue

            if max_iterations is not None and iteration >= max_iterations:
                break

            if interval_seconds == 0:
                continue

            try:
                await asyncio.wait_for(
                    local_stop_event.wait(), timeout=interval_seconds
                )
            except asyncio.TimeoutError:
                continue
    finally:
        if close_client:
            await client.aclose()
        for sig in registered_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                continue
