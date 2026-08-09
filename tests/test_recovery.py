import threading
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from app.db.base import Base
from app.db.session import get_session_factory
from app.main import app
from app.models import Task, TaskExecutionAttempt, TaskPriority, TaskStatus
from app.services import task_lifecycle
from app.services.dispatcher import (
    TaskDispatcher,
    TaskDispatchError,
    get_task_dispatcher,
)
from app.services.recovery import STALE_LEASE_ERROR, recover_stale_tasks


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def make_running_task(
    session_factory: sessionmaker[Session],
    *,
    lease_expires_at: datetime,
    retry_count: int = 0,
    max_retries: int = 3,
    priority: TaskPriority = TaskPriority.HIGH,
) -> object:
    with session_factory() as session:
        task = Task(
            task_type="echo",
            payload={"message": "hi"},
            priority=priority,
            status=TaskStatus.RUNNING,
            retry_count=retry_count,
            max_retries=max_retries,
            started_at=datetime.now(UTC) - timedelta(minutes=10),
            lease_expires_at=lease_expires_at,
        )
        task.execution_attempts.append(
            TaskExecutionAttempt(attempt_number=1, worker_identifier="worker-a")
        )
        session.add(task)
        session.commit()
        return task.id


def load(
    session_factory: sessionmaker[Session], task_id: object
) -> tuple[Task, list[TaskExecutionAttempt]]:
    with session_factory() as session:
        task = session.get(Task, task_id)
        assert task is not None
        attempts = list(
            session.scalars(
                select(TaskExecutionAttempt)
                .where(TaskExecutionAttempt.task_id == task_id)
                .order_by(TaskExecutionAttempt.attempt_number)
            )
        )
        session.expunge_all()
        return task, attempts


def test_stale_running_task_is_reclaimed_requeued_and_redispatched(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(session_factory, lease_expires_at=now - timedelta(seconds=1))
    dispatcher = Mock(spec=TaskDispatcher)

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)

    assert len(reclaimed) == 1
    result = reclaimed[0]
    assert result.task_id == task_id
    assert result.outcome is TaskStatus.QUEUED
    assert result.retry_count == 1
    assert result.redispatched is True
    dispatcher.dispatch.assert_called_once_with(task_id, TaskPriority.HIGH)

    task, attempts = load(session_factory, task_id)
    assert task.status is TaskStatus.QUEUED
    assert task.retry_count == 1
    assert task.lease_expires_at is None
    assert task.last_error == STALE_LEASE_ERROR
    assert len(attempts) == 1
    assert attempts[0].finished_at is not None
    assert attempts[0].error == STALE_LEASE_ERROR


def test_non_stale_running_task_is_left_untouched(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(session_factory, lease_expires_at=now + timedelta(minutes=5))
    dispatcher = Mock(spec=TaskDispatcher)

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)

    assert reclaimed == []
    dispatcher.dispatch.assert_not_called()
    task, attempts = load(session_factory, task_id)
    assert task.status is TaskStatus.RUNNING
    assert task.lease_expires_at is not None
    assert attempts[0].finished_at is None


def test_stale_running_task_with_exhausted_retries_becomes_dead_letter(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(
        session_factory, lease_expires_at=now - timedelta(seconds=1), retry_count=3, max_retries=3
    )
    dispatcher = Mock(spec=TaskDispatcher)

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)

    assert len(reclaimed) == 1
    assert reclaimed[0].outcome is TaskStatus.DEAD_LETTER
    assert reclaimed[0].retry_count == 3
    assert reclaimed[0].redispatched is False
    dispatcher.dispatch.assert_not_called()
    task, _ = load(session_factory, task_id)
    assert task.status is TaskStatus.DEAD_LETTER
    assert task.retry_count == 3


def test_reclaim_with_failed_redispatch_leaves_task_queued_for_manual_redispatch(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(session_factory, lease_expires_at=now - timedelta(seconds=1))
    dispatcher = Mock(spec=TaskDispatcher)
    dispatcher.dispatch.side_effect = TaskDispatchError("broker unavailable")

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)

    assert reclaimed[0].outcome is TaskStatus.QUEUED
    assert reclaimed[0].redispatched is False
    task, _ = load(session_factory, task_id)
    assert task.status is TaskStatus.QUEUED


def test_repeated_recovery_scan_is_a_no_op_after_first_reclaim(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(session_factory, lease_expires_at=now - timedelta(seconds=1))
    dispatcher = Mock(spec=TaskDispatcher)

    first = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)
    second = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)

    assert len(first) == 1
    assert second == []
    assert dispatcher.dispatch.call_count == 1
    task, attempts = load(session_factory, task_id)
    assert task.retry_count == 1
    assert len(attempts) == 1


def test_concurrent_recovery_scans_reclaim_a_task_exactly_once(tmp_path: Path) -> None:
    """SQLite-level concurrency proof: two threads, each with their own
    connection to a shared on-disk database, racing to reclaim the same
    stale task via the compare-and-swap UPDATE. SQLite serializes writers at
    the database-file level, so exactly one transaction must win and the
    other must safely no-op — this proves the guard logic itself is correct.

    (A single shared in-memory StaticPool connection cannot be used here:
    driving one sqlite3 connection object from two threads concurrently is
    unsafe at the DBAPI level and produces unrelated corruption, not a
    meaningful concurrency proof. True multi-connection row-locking behavior
    under PostgreSQL is proven separately by the CI integration suite —
    tests/integration/test_postgres_concurrency.py.)
    """

    db_path = tmp_path / "recovery-concurrency.db"
    engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 30},
        poolclass=QueuePool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    now = datetime.now(UTC)
    task_id = make_running_task(factory, lease_expires_at=now - timedelta(seconds=1))

    results: list[list] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def run() -> None:
        barrier.wait()
        dispatcher = Mock(spec=TaskDispatcher)
        try:
            results.append(
                recover_stale_tasks(session_factory=factory, dispatcher=dispatcher, now=now)
            )
        except BaseException as exc:  # surfaced via assertion below, not swallowed
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    total_reclaimed = sum(len(result) for result in results)
    assert total_reclaimed == 1
    task, attempts = load(factory, task_id)
    assert task.retry_count == 1
    assert len(attempts) == 1
    engine.dispose()


def test_late_completion_after_lease_reclaim_is_rejected_not_corrupting_recovered_state(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(session_factory, lease_expires_at=now - timedelta(seconds=1))
    dispatcher = Mock(spec=TaskDispatcher)

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)
    assert reclaimed[0].outcome is TaskStatus.QUEUED

    with session_factory() as session:
        original_attempt_id = session.scalar(
            select(TaskExecutionAttempt.id).where(
                TaskExecutionAttempt.task_id == task_id,
                TaskExecutionAttempt.attempt_number == 1,
            )
        )

    # The presumed-dead worker was actually still alive and now tries to
    # complete the attempt that recovery already closed out.
    with pytest.raises(task_lifecycle.TaskReclaimedError):
        task_lifecycle._complete_task(task_id, original_attempt_id, session_factory)

    task, attempts = load(session_factory, task_id)
    assert task.status is TaskStatus.QUEUED  # recovery's decision stands
    assert task.retry_count == 1
    assert attempts[0].error == STALE_LEASE_ERROR  # not overwritten


def test_late_failure_after_lease_reclaim_is_rejected_not_corrupting_recovered_state(
    session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    task_id = make_running_task(
        session_factory, lease_expires_at=now - timedelta(seconds=1), retry_count=3, max_retries=3
    )
    dispatcher = Mock(spec=TaskDispatcher)

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher, now=now)
    assert reclaimed[0].outcome is TaskStatus.DEAD_LETTER

    with session_factory() as session:
        original_attempt_id = session.scalar(
            select(TaskExecutionAttempt.id).where(
                TaskExecutionAttempt.task_id == task_id,
                TaskExecutionAttempt.attempt_number == 1,
            )
        )

    with pytest.raises(task_lifecycle.TaskReclaimedError):
        task_lifecycle._finalize_failure(
            task_id, original_attempt_id, "late failure", False, session_factory
        )

    task, _ = load(session_factory, task_id)
    assert task.status is TaskStatus.DEAD_LETTER  # not reopened/overwritten


@pytest.fixture
def recovery_client() -> Generator[tuple[TestClient, Mock, sessionmaker[Session]], None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    dispatcher = Mock(spec=TaskDispatcher)

    app.dependency_overrides[get_session_factory] = lambda: factory
    app.dependency_overrides[get_task_dispatcher] = lambda: dispatcher
    with TestClient(app) as client:
        yield client, dispatcher, factory
    app.dependency_overrides.pop(get_session_factory, None)
    app.dependency_overrides.pop(get_task_dispatcher, None)
    engine.dispose()


def test_recovery_endpoint_reclaims_stale_task_and_returns_summary(
    recovery_client: tuple[TestClient, Mock, sessionmaker[Session]],
) -> None:
    client, dispatcher, factory = recovery_client
    now = datetime.now(UTC)
    task_id = make_running_task(factory, lease_expires_at=now - timedelta(seconds=1))

    response = client.post("/recovery/stale-running")

    assert response.status_code == 200
    body = response.json()
    assert body["reclaimed_count"] == 1
    assert body["reclaimed"][0]["task_id"] == str(task_id)
    assert body["reclaimed"][0]["outcome"] == "QUEUED"
    assert body["reclaimed"][0]["redispatched"] is True
    dispatcher.dispatch.assert_called_once()


def test_recovery_endpoint_is_a_no_op_with_no_stale_tasks(
    recovery_client: tuple[TestClient, Mock, sessionmaker[Session]],
) -> None:
    client, dispatcher, _ = recovery_client

    response = client.post("/recovery/stale-running")

    assert response.status_code == 200
    assert response.json() == {"reclaimed_count": 0, "reclaimed": []}
    dispatcher.dispatch.assert_not_called()
