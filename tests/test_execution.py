from collections.abc import Generator
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.models import Task, TaskExecutionAttempt, TaskStatus
from app.services.execution import TaskExecutionError, execute_job
from app.services.task_lifecycle import (
    InvalidTaskIdError,
    LifecycleOutcome,
    TaskExecutionFailedError,
    TaskNotFoundError,
    process_task,
)


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


def create_task(
    session_factory: sessionmaker[Session],
    *,
    task_type: str = "echo",
    payload: dict[str, object] | None = None,
    status: TaskStatus = TaskStatus.QUEUED,
) -> Task:
    with session_factory() as session:
        task = Task(
            task_type=task_type,
            payload=payload if payload is not None else {"message": "hello"},
            status=status,
        )
        session.add(task)
        session.commit()
        return task


def load_task_with_attempts(
    session_factory: sessionmaker[Session], task_id: object
) -> Task:
    with session_factory() as session:
        task = session.scalar(
            select(Task)
            .where(Task.id == task_id)
            .execution_options(populate_existing=True)
        )
        assert task is not None
        _ = task.execution_attempts
        session.expunge_all()
        return task


def test_running_and_attempt_are_committed_before_execution(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    def assert_committed_running(task_type: str, payload: object) -> None:
        with session_factory() as observer:
            persisted = observer.get(Task, task.id)
            assert persisted is not None
            assert persisted.status is TaskStatus.RUNNING
            assert persisted.started_at is not None
            assert persisted.completed_at is None
            assert persisted.last_error is None
            attempts = observer.scalars(
                select(TaskExecutionAttempt).where(
                    TaskExecutionAttempt.task_id == task.id
                )
            ).all()
            assert len(attempts) == 1
            assert attempts[0].attempt_number == 1
            assert attempts[0].worker_identifier == "worker-test"
            assert attempts[0].started_at is not None
            assert attempts[0].finished_at is None
            assert attempts[0].error is None

    outcome = process_task(
        task.id,
        worker_identifier="worker-test",
        session_factory=session_factory,
        executor=assert_committed_running,
    )

    assert outcome is LifecycleOutcome.COMPLETED


def test_successful_execution_completes_task_and_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    outcome = process_task(
        task.id,
        worker_identifier="worker-success",
        session_factory=session_factory,
    )

    persisted = load_task_with_attempts(session_factory, task.id)
    assert outcome is LifecycleOutcome.COMPLETED
    assert persisted.status is TaskStatus.COMPLETED
    assert persisted.started_at is not None
    assert persisted.completed_at is not None
    assert persisted.last_error is None
    assert persisted.retry_count == 0
    assert len(persisted.execution_attempts) == 1
    attempt = persisted.execution_attempts[0]
    assert attempt.attempt_number == 1
    assert attempt.worker_identifier == "worker-success"
    assert attempt.started_at is not None
    assert attempt.finished_at is not None
    assert attempt.error is None


def test_execution_failure_fails_task_and_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    def fail(task_type: str, payload: object) -> None:
        raise TaskExecutionError("safe job failure")

    with pytest.raises(TaskExecutionFailedError, match="safe job failure"):
        process_task(
            task.id,
            worker_identifier="worker-failure",
            session_factory=session_factory,
            executor=fail,
        )

    persisted = load_task_with_attempts(session_factory, task.id)
    assert persisted.status is TaskStatus.FAILED
    assert persisted.started_at is not None
    assert persisted.completed_at is None
    assert persisted.last_error == "safe job failure"
    assert persisted.retry_count == 0
    assert len(persisted.execution_attempts) == 1
    attempt = persisted.execution_attempts[0]
    assert attempt.finished_at is not None
    assert attempt.error == "safe job failure"


@pytest.mark.parametrize(
    "terminal_status",
    [TaskStatus.RUNNING, TaskStatus.COMPLETED, TaskStatus.FAILED],
)
def test_duplicate_nonqueued_task_is_not_reexecuted(
    session_factory: sessionmaker[Session], terminal_status: TaskStatus
) -> None:
    task = create_task(session_factory, status=terminal_status)
    executor = Mock()

    outcome = process_task(
        task.id,
        worker_identifier="duplicate-worker",
        session_factory=session_factory,
        executor=executor,
    )

    assert outcome is LifecycleOutcome.SKIPPED
    executor.assert_not_called()
    with session_factory() as session:
        attempt_count = session.scalar(
            select(func.count(TaskExecutionAttempt.id)).where(
                TaskExecutionAttempt.task_id == task.id
            )
        )
    assert attempt_count == 0


def test_nonexistent_task_is_handled_without_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    missing_id = uuid4()

    with pytest.raises(TaskNotFoundError, match="does not exist"):
        process_task(
            missing_id,
            worker_identifier="worker-test",
            session_factory=session_factory,
        )

    with session_factory() as session:
        assert session.scalar(select(func.count(TaskExecutionAttempt.id))) == 0


def test_malformed_uuid_is_rejected_without_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    with pytest.raises(InvalidTaskIdError, match="invalid task UUID"):
        process_task(
            "not-a-uuid",
            worker_identifier="worker-test",
            session_factory=session_factory,
        )

    with session_factory() as session:
        assert session.scalar(select(func.count(TaskExecutionAttempt.id))) == 0


def test_supported_echo_handler_succeeds() -> None:
    execute_job("echo", {"message": "hello"})


def test_unsupported_task_type_is_recorded_as_failed(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory, task_type="unknown.job")

    with pytest.raises(TaskExecutionFailedError, match="Unsupported task type"):
        process_task(
            task.id,
            worker_identifier="worker-test",
            session_factory=session_factory,
        )

    persisted = load_task_with_attempts(session_factory, task.id)
    assert persisted.status is TaskStatus.FAILED
    assert persisted.retry_count == 0
    assert persisted.execution_attempts[0].error == persisted.last_error


def test_malformed_echo_payload_is_recorded_as_failed(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory, payload={"message": 123})

    with pytest.raises(TaskExecutionFailedError, match="echo payload"):
        process_task(
            task.id,
            worker_identifier="worker-test",
            session_factory=session_factory,
        )

    persisted = load_task_with_attempts(session_factory, task.id)
    assert persisted.status is TaskStatus.FAILED
    assert persisted.retry_count == 0
    assert persisted.execution_attempts[0].finished_at is not None
    assert "echo payload" in persisted.execution_attempts[0].error
