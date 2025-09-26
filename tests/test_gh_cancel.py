"""Тесты для отмены очереди GitHub Actions."""

from __future__ import annotations

import asyncio
from typing import Dict, List

import httpx

from threads_metrics.gh_cancel import WORKFLOW_FILE, cancel_pending_workflow_runs

BASE_URL = "https://api.github.com"


def _make_response(
    method: str,
    url: str,
    *,
    status_code: int = 200,
    json_body: Dict[str, object] | None = None,
) -> httpx.Response:
    """Создаёт httpx.Response для тестов."""

    return httpx.Response(
        status_code=status_code,
        json=json_body or {},
        headers={"X-RateLimit-Remaining": "4999"},
        request=httpx.Request(method, url),
    )


def _runs_payload(run_ids: List[int]) -> Dict[str, object]:
    """Возвращает полезную нагрузку с идентификаторами запусков."""

    return {"workflow_runs": [{"id": run_id} for run_id in run_ids]}


class _DummyClient:
    """Простейший асинхронный клиент для подмены httpx.AsyncClient."""

    def __init__(self, owner: str, repo: str, responses: Dict[str, httpx.Response]):
        self.owner = owner
        self.repo = repo
        self._responses = responses
        self.get_calls: List[tuple[str, str]] = []
        self.post_calls: List[str] = []

    async def get(
        self, url: str, params: Dict[str, object] | None = None
    ) -> httpx.Response:
        status = params.get("status") if params else ""
        self.get_calls.append((url, str(status)))
        return self._responses[str(status)]

    async def post(self, url: str) -> httpx.Response:
        self.post_calls.append(url)
        return _make_response(
            "POST",
            f"{BASE_URL}{url}",
            status_code=202,
            json_body={},
        )

    async def aclose(self) -> None:
        return None


def test_cancel_pending_when_active_run_exists() -> None:
    """При активном запуске отменяются все элементы очереди."""

    owner = "octo"
    repo = "threads"
    workflow_runs_url = (
        f"{BASE_URL}/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/runs"
    )
    responses = {
        "in_progress": _make_response(
            "GET",
            workflow_runs_url,
            json_body=_runs_payload([101]),
        ),
        "queued": _make_response(
            "GET",
            workflow_runs_url,
            json_body=_runs_payload([202, 303]),
        ),
    }

    client = _DummyClient(owner, repo, responses)

    asyncio.run(
        cancel_pending_workflow_runs(
            owner,
            repo,
            token="dummy",
            interval_seconds=0,
            max_iterations=1,
            client=client,
        )
    )

    expected_path = f"/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/runs"
    assert client.get_calls == [
        (expected_path, "in_progress"),
        (expected_path, "queued"),
    ]
    assert client.post_calls == [
        f"/repos/{owner}/{repo}/actions/runs/202/cancel",
        f"/repos/{owner}/{repo}/actions/runs/303/cancel",
    ]


def test_skip_cancel_when_no_active_runs() -> None:
    """При отсутствии активных запусков очередь не трогается."""

    owner = "octo"
    repo = "threads"
    workflow_runs_url = (
        f"{BASE_URL}/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/runs"
    )
    responses = {
        "in_progress": _make_response(
            "GET",
            workflow_runs_url,
            json_body=_runs_payload([]),
        ),
        "queued": _make_response(
            "GET",
            workflow_runs_url,
            json_body=_runs_payload([404, 505]),
        ),
    }

    expected_path = f"/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/runs"

    client = _DummyClient(owner, repo, responses)

    asyncio.run(
        cancel_pending_workflow_runs(
            owner,
            repo,
            token="dummy",
            interval_seconds=0,
            max_iterations=1,
            client=client,
        )
    )

    assert client.post_calls == []
    assert client.get_calls == [
        (expected_path, "in_progress"),
        (expected_path, "queued"),
    ]
