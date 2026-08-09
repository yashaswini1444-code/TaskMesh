from collections.abc import Generator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models import Task, TaskStatus
from app.schemas.monitoring import QueueStatus, WorkerItem, WorkerStatus
from app.services.monitoring import (
    CeleryWorkerMonitor,
    RedisQueueMonitor,
    build_monitoring_summary,
    get_queue_monitor,
    get_worker_monitor,
)


class FakeWorkerMonitor:
    def inspect(self) -> WorkerStatus:
        return WorkerStatus(
            available=True,
            count=1,
            items=[WorkerItem(identifier="worker-1", active_tasks=2, reserved_tasks=1)],
        )


class FakeQueueMonitor:
    def inspect(self) -> QueueStatus:
        return QueueStatus(available=True, high=1, medium=2, low=3, total=6)


@pytest.fixture
def monitoring_client() -> Generator[TestClient, None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    now = datetime.now(UTC)
    with Session(engine) as seed:
        seed.add_all(
            [
                Task(task_type="echo", payload={"message": "q"}),
                Task(task_type="echo", payload={"message": "r"}, status=TaskStatus.RUNNING),
                Task(
                    task_type="echo",
                    payload={"message": "c"},
                    status=TaskStatus.COMPLETED,
                    completed_at=now - timedelta(seconds=10),
                ),
                Task(task_type="bad", payload={}, status=TaskStatus.FAILED, last_error="bad"),
                Task(
                    task_type="echo",
                    payload={"message": "d"},
                    status=TaskStatus.DEAD_LETTER,
                    last_error="transient",
                ),
            ]
        )
        seed.commit()

    def override_get_db() -> Generator[Session, None, None]:
        with Session(engine, expire_on_commit=False) as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker_monitor] = FakeWorkerMonitor
    app.dependency_overrides[get_queue_monitor] = FakeQueueMonitor
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()
    engine.dispose()


def test_monitoring_summary_combines_durable_and_infrastructure_metrics(
    monitoring_client: TestClient,
) -> None:
    response = monitoring_client.get("/monitoring/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["database"] == {"available": True, "error": None}
    assert body["workers"] == {
        "available": True,
        "count": 1,
        "items": [{"identifier": "worker-1", "active_tasks": 2, "reserved_tasks": 1}],
        "error": None,
    }
    assert body["queues"]["total"] == 6
    assert body["tasks"] == {
        "queued": 1,
        "running": 1,
        "completed": 1,
        "failed": 1,
        "dead_letter": 1,
        "stale_running": 0,
    }
    assert body["throughput"]["window_seconds"] == 60
    assert body["throughput"]["completed"] == 1
    assert body["throughput"]["per_minute"] == 1.0
    assert len(body["recent_tasks"]) == 5
    assert {task["status"] for task in body["recent_failures"]} == {
        "FAILED",
        "DEAD_LETTER",
    }


def test_database_metrics_use_completed_timestamp_window() -> None:
    engine = create_engine("sqlite://", poolclass=StaticPool)
    Base.metadata.create_all(engine)
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.add_all(
            [
                Task(
                    task_type="echo",
                    payload={"message": "recent"},
                    status=TaskStatus.COMPLETED,
                    completed_at=now - timedelta(seconds=30),
                ),
                Task(
                    task_type="echo",
                    payload={"message": "old"},
                    status=TaskStatus.COMPLETED,
                    completed_at=now - timedelta(seconds=61),
                ),
            ]
        )
        session.commit()
        summary = build_monitoring_summary(
            session,
            WorkerStatus(available=False, count=0, items=[]),
            QueueStatus(available=False, high=0, medium=0, low=0, total=0),
            now=now,
        )
    assert summary.tasks.completed == 2
    assert summary.throughput.completed == 1


def test_infrastructure_monitors_degrade_without_leaking_errors(monkeypatch) -> None:
    class BrokenInspector:
        def ping(self):
            raise RuntimeError("sensitive internal diagnostic")

    monkeypatch.setattr(
        "app.services.monitoring.celery_app.control.inspect",
        lambda timeout: BrokenInspector(),
    )
    workers = CeleryWorkerMonitor().inspect()
    assert workers.available is False
    assert workers.error == "Worker monitoring unavailable"
    assert "sensitive" not in workers.error

    monkeypatch.setattr(
        "app.services.monitoring.Redis.from_url",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("internal")),
    )
    queues = RedisQueueMonitor().inspect()
    assert queues.available is False
    assert queues.error == "Queue monitoring unavailable"


class BrokenSession:
    """Simulates a database that cannot be queried (connection refused, pool
    exhausted, etc.) without depending on a real broken engine."""

    def execute(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("connection refused: password authentication failed")

    def scalar(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("connection refused: password authentication failed")

    def scalars(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("connection refused: password authentication failed")


def test_database_outage_degrades_monitoring_without_fabricating_or_leaking(
    caplog: pytest.LogCaptureFixture,
) -> None:
    summary = build_monitoring_summary(
        BrokenSession(),  # type: ignore[arg-type]
        WorkerStatus(available=True, count=0, items=[]),
        QueueStatus(available=True, high=0, medium=0, low=0, total=0),
    )

    assert summary.database.available is False
    assert summary.database.error == "Database monitoring unavailable"
    assert summary.tasks is None
    assert summary.throughput is None
    assert summary.recent_tasks == []
    assert summary.recent_failures == []
    assert "password" not in (summary.database.error or "")


@pytest.fixture
def broken_db_monitoring_client() -> Generator[TestClient, None, None]:
    def override_get_db() -> Generator[BrokenSession, None, None]:
        yield BrokenSession()

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_worker_monitor] = FakeWorkerMonitor
    app.dependency_overrides[get_queue_monitor] = FakeQueueMonitor
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def test_monitoring_endpoint_returns_200_when_database_is_unavailable(
    broken_db_monitoring_client: TestClient,
) -> None:
    response = broken_db_monitoring_client.get("/monitoring/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["database"] == {
        "available": False,
        "error": "Database monitoring unavailable",
    }
    assert body["tasks"] is None
    assert body["throughput"] is None
    assert body["recent_tasks"] == []
    assert body["recent_failures"] == []
    assert body["workers"]["available"] is True
    assert body["queues"]["available"] is True
