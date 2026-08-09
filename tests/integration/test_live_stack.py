"""End-to-end proof against real infrastructure: PostgreSQL + Redis + Celery
workers + a running FastAPI server, exactly as docker-compose.yml describes
(one dedicated worker per queue). See tests/integration/conftest.py for how
the stack is expected to already be running before this file executes.

Each test proves one specific, previously-only-manually-verified claim.
Nothing here is faked or bypasses the real system: every assertion goes
through the public HTTP API or reads back what the API/worker actually
persisted.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import Task, TaskExecutionAttempt, TaskPriority, TaskStatus
from tests.integration.conftest import wait_for_task_status

pytestmark = pytest.mark.integration

WORKER_HOSTNAME_BY_PRIORITY = {
    "HIGH": "worker-high",
    "MEDIUM": "worker-medium",
    "LOW": "worker-low",
}


def test_alembic_upgrade_head_actually_applied_to_postgres() -> None:
    """Proof point 3: alembic upgrade head succeeded against PostgreSQL (not
    just SQLite, which is all the deterministic suite ever exercises)."""

    engine = create_engine(get_settings().database_url)
    try:
        with engine.connect() as connection:
            version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar()
            assert version == "20260809_0002"

            columns = {
                row[0]
                for row in connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'tasks'"
                    )
                )
            }
            assert {"lease_expires_at", "status", "priority", "retry_count"} <= columns
    finally:
        engine.dispose()


@pytest.mark.parametrize("priority", ["HIGH", "MEDIUM", "LOW"])
def test_task_submitted_through_api_is_routed_to_its_dedicated_worker_and_completes(
    api_client, priority: str
) -> None:
    """Proof points 5-9: submit through the real API boundary, the message
    reaches its priority queue, the *correct dedicated worker* (not just any
    worker) receives and processes it, persisted state reaches a terminal
    status, and execution-attempt history exists.

    Routing correctness is proven without racing to observe the queue
    mid-flight (which would be flaky): each worker process was started with
    a hostname matching its queue (worker-high@..., etc — see the CI
    workflow), so the completed attempt's worker_identifier is direct,
    non-flaky proof that this HIGH task was executed by worker-high and not
    by worker-medium or worker-low.
    """

    response = api_client.post(
        "/tasks",
        json={
            "task_type": "echo",
            "payload": {"message": f"live-stack-{priority}-{uuid.uuid4()}"},
            "priority": priority,
        },
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]

    body = wait_for_task_status(api_client, task_id, {"COMPLETED", "FAILED", "DEAD_LETTER"})

    assert body["status"] == "COMPLETED", body
    assert body["attempt_count"] == 1

    detail = api_client.get(f"/tasks/{task_id}").json()
    assert len(detail["execution_attempts"]) == 1
    attempt = detail["execution_attempts"][0]
    assert attempt["finished_at"] is not None
    assert attempt["error"] is None
    expected_worker = WORKER_HOSTNAME_BY_PRIORITY[priority]
    assert attempt["worker_identifier"].startswith(expected_worker), (
        f"expected a {priority} task to be processed by {expected_worker}, "
        f"got {attempt['worker_identifier']!r}"
    )


def test_malformed_payload_fails_deterministically_through_real_worker(api_client) -> None:
    """A real, meaningful failure path: the echo handler rejects a
    non-string message, and this is a *permanent* (non-retryable) error, so
    it reaches FAILED immediately and deterministically — no backoff timing
    to wait out, unlike the retryable path."""

    response = api_client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"message": 12345}, "priority": "MEDIUM"},
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]

    body = wait_for_task_status(api_client, task_id, {"FAILED", "COMPLETED", "DEAD_LETTER"})

    assert body["status"] == "FAILED", body
    assert body["last_error"] is not None
    assert "echo payload" in body["last_error"]
    assert body["attempt_count"] == 1


def test_dead_letter_requeue_is_redispatched_and_completes_on_real_worker(api_client) -> None:
    """A real recovery path against real infrastructure: seed a task that
    has already exhausted its retries (DEAD_LETTER), call the requeue
    endpoint, and prove the *real* Celery broker delivers it to a *real*
    worker that completes it — not just that the DB row flips state.
    """

    engine = create_engine(get_settings().database_url)
    try:
        with Session(engine) as session:
            task = Task(
                task_type="echo",
                payload={"message": "requeue-through-real-broker"},
                priority=TaskPriority.LOW,
                status=TaskStatus.DEAD_LETTER,
                retry_count=3,
                max_retries=3,
                last_error="previously exhausted retries",
            )
            task.execution_attempts.append(
                TaskExecutionAttempt(attempt_number=1, worker_identifier="worker-low@ci-prior-run")
            )
            session.add(task)
            session.commit()
            task_id = str(task.id)
    finally:
        engine.dispose()

    response = api_client.post(f"/tasks/{task_id}/requeue")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "QUEUED"

    body = wait_for_task_status(api_client, task_id, {"COMPLETED", "FAILED", "DEAD_LETTER"})

    assert body["status"] == "COMPLETED", body
    assert body["retry_count"] == 0
    assert body["attempt_count"] == 2  # original attempt preserved + the requeued one

    detail = api_client.get(f"/tasks/{task_id}").json()
    attempts = detail["execution_attempts"]
    assert [a["attempt_number"] for a in attempts] == [1, 2]
    assert attempts[0]["worker_identifier"] == "worker-low@ci-prior-run"  # history untouched
    assert attempts[1]["worker_identifier"].startswith("worker-low")  # LOW priority -> worker-low


def test_redispatch_republishes_queued_task_through_real_broker(api_client) -> None:
    """A task that was persisted but never actually dispatched (simulating a
    broker outage at submission time — see the README's consistency-window
    table) should be recoverable via POST /tasks/{id}/redispatch, proven by
    a *real* worker actually completing it, not just a 200 response.
    """

    engine = create_engine(get_settings().database_url)
    try:
        with Session(engine) as session:
            task = Task(
                task_type="echo",
                payload={"message": "redispatch-through-real-broker"},
                priority=TaskPriority.MEDIUM,
                status=TaskStatus.QUEUED,
            )
            session.add(task)
            session.commit()
            task_id = str(task.id)
    finally:
        engine.dispose()

    response = api_client.post(f"/tasks/{task_id}/redispatch")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "QUEUED"

    body = wait_for_task_status(api_client, task_id, {"COMPLETED", "FAILED", "DEAD_LETTER"})

    assert body["status"] == "COMPLETED", body
    assert body["attempt_count"] == 1

    detail = api_client.get(f"/tasks/{task_id}").json()
    assert detail["execution_attempts"][0]["worker_identifier"].startswith("worker-medium")


def test_stale_running_task_is_reclaimed_and_completes_on_real_worker(api_client) -> None:
    """The full lease-recovery path against real infrastructure: a task
    whose lease has already expired while still RUNNING (simulating a
    crashed worker — a real crash-and-wait would make this test slow and
    still wouldn't test anything the expired-lease state itself doesn't
    already fully determine) is reclaimed by POST /recovery/stale-running,
    redispatched through the *real* broker, and completed by a *real*
    worker — proving the whole chain, not just the DB-state transition that
    tests/test_recovery.py already covers under SQLite.
    """

    engine = create_engine(get_settings().database_url)
    try:
        with Session(engine) as session:
            task = Task(
                task_type="echo",
                payload={"message": "stale-recovery-through-real-broker"},
                priority=TaskPriority.HIGH,
                status=TaskStatus.RUNNING,
                started_at=datetime.now(UTC) - timedelta(minutes=10),
                lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )
            task.execution_attempts.append(
                TaskExecutionAttempt(
                    attempt_number=1, worker_identifier="worker-high@ci-presumed-crashed"
                )
            )
            session.add(task)
            session.commit()
            task_id = str(task.id)
    finally:
        engine.dispose()

    response = api_client.post("/recovery/stale-running")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["reclaimed_count"] == 1
    assert body["reclaimed"][0]["task_id"] == task_id
    assert body["reclaimed"][0]["outcome"] == "QUEUED"
    assert body["reclaimed"][0]["redispatched"] is True

    detail_body = wait_for_task_status(api_client, task_id, {"COMPLETED", "FAILED", "DEAD_LETTER"})

    assert detail_body["status"] == "COMPLETED", detail_body
    assert detail_body["attempt_count"] == 2  # abandoned attempt preserved + the reclaimed one

    detail = api_client.get(f"/tasks/{task_id}").json()
    attempts = detail["execution_attempts"]
    assert attempts[0]["worker_identifier"] == "worker-high@ci-presumed-crashed"
    assert attempts[0]["error"] == "Execution lease expired; worker presumed lost"
    assert attempts[1]["worker_identifier"].startswith("worker-high")


def test_monitoring_reflects_real_infrastructure(api_client) -> None:
    """Proof point 10: /monitoring/summary is backed by the real stack, not
    fakes — worker count matches the 4 real Celery processes, queues report
    available, and database reports available."""

    response = api_client.get("/monitoring/summary")
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["database"]["available"] is True
    assert body["queues"]["available"] is True
    assert body["workers"]["available"] is True
    assert body["workers"]["count"] == 4
    reported_identifiers = {item["identifier"].split("@")[0] for item in body["workers"]["items"]}
    assert reported_identifiers == {"worker-high", "worker-medium", "worker-low", "worker-control"}
    assert body["tasks"] is not None  # DB is up, so real counts, not None


def test_recovery_endpoint_runs_cleanly_against_real_stack(api_client) -> None:
    """The administrative recovery endpoint should be a safe no-op when
    nothing is actually stale, proven against real infrastructure rather
    than the SQLite-backed unit tests."""

    response = api_client.post("/recovery/stale-running")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["reclaimed_count"] == 0
    assert body["reclaimed"] == []


def test_task_listing_reflects_persisted_state(api_client) -> None:
    response = api_client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"message": "listing-check"}, "priority": "HIGH"},
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]
    wait_for_task_status(api_client, task_id, {"COMPLETED", "FAILED", "DEAD_LETTER"})

    listed = api_client.get("/tasks", params={"limit": 100})
    assert listed.status_code == 200
    ids = {task["id"] for task in listed.json()}
    assert task_id in ids
