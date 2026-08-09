"""Shared fixtures for the real-infrastructure integration suite.

These tests require a live PostgreSQL database, a live Redis broker, a
running FastAPI server (uvicorn), and running Celery workers bound to the
high/medium/low/control queues — the same topology docker-compose.yml
describes. They are never run by the default `pytest` invocation (see the
`integration` marker registered in pyproject.toml); CI's `integration` job
starts all of that first, then runs `pytest tests/integration -m integration`.

There is no local equivalent on a Windows development machine without
Docker Desktop — this suite exists specifically to give that verification
in GitHub Actions instead.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import httpx2 as httpx
import pytest

from app.workers.celery_app import celery_app

BASE_URL = "http://127.0.0.1:8000"
EXPECTED_WORKER_QUEUES = {"worker-high", "worker-medium", "worker-low", "worker-control"}


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 60.0,
    interval: float = 1.0,
    description: str = "condition",
) -> None:
    """Poll `predicate` with a bounded timeout instead of a fixed sleep.

    Raises TimeoutError (failing the test, and therefore the CI job) rather
    than hanging or silently passing if the condition is never met.
    """

    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # transient errors during startup are expected
            last_error = exc
        time.sleep(interval)
    suffix = f": {last_error!r}" if last_error is not None else ""
    raise TimeoutError(f"Timed out after {timeout}s waiting for {description}{suffix}")


@pytest.fixture(scope="session", autouse=True)
def live_stack_ready() -> None:
    def api_healthy() -> bool:
        response = httpx.get(f"{BASE_URL}/health", timeout=2.0)
        return response.status_code == 200

    wait_until(
        api_healthy,
        timeout=60.0,
        interval=1.0,
        description="the API server to become reachable",
    )

    def workers_ready() -> bool:
        pings = celery_app.control.inspect(timeout=2.0).ping() or {}
        hostnames = {name.split("@")[0] for name in pings}
        return hostnames >= EXPECTED_WORKER_QUEUES

    wait_until(
        workers_ready,
        timeout=60.0,
        interval=2.0,
        description=f"all of {sorted(EXPECTED_WORKER_QUEUES)} to register with the broker",
    )


@pytest.fixture
def api_client() -> httpx.Client:
    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        yield client


def wait_for_task_status(
    client: httpx.Client,
    task_id: str,
    expected_statuses: set[str],
    *,
    timeout: float = 30.0,
    interval: float = 0.5,
) -> dict:
    """Poll GET /tasks/{id} until it reaches one of expected_statuses."""

    last_body: dict = {}

    def reached_expected_status() -> bool:
        nonlocal last_body
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200, response.text
        last_body = response.json()
        return last_body["status"] in expected_statuses

    wait_until(
        reached_expected_status,
        timeout=timeout,
        interval=interval,
        description=f"task {task_id} to reach one of {sorted(expected_statuses)}",
    )
    return last_body
