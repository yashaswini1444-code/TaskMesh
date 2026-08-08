from collections.abc import Generator
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.models import Task, TaskExecutionAttempt, TaskStatus
from app.services.execution import RetryableTaskExecutionError
from app.services.task_lifecycle import (
    LifecycleOutcome,
    TaskExecutionFailedError,
    TaskRetryRequested,
    process_task,
    retry_countdown,
)
from app.workers.tasks import execute_task


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
    session_factory: sessionmaker[Session], *, max_retries: int = 3
) -> Task:
    with session_factory() as session:
        task = Task(
            task_type="echo",
            payload={"message": "hello"},
            max_retries=max_retries,
        )
        session.add(task)
        session.commit()
        return task


def load_task(
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


def transient_failure(task_type: str, payload: object) -> None:
    raise RetryableTaskExecutionError("temporary dependency failure")


@pytest.mark.parametrize(
    ("retry_number", "expected_countdown"),
    [(1, 2), (2, 4), (3, 8), (4, 16), (5, 16)],
)
def test_retry_countdown_is_bounded(
    retry_number: int, expected_countdown: int
) -> None:
    assert retry_countdown(retry_number) == expected_countdown


def test_retryable_failures_create_durable_attempts_and_backoff(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    for retry_number, expected_countdown in [(1, 2), (2, 4), (3, 8)]:
        with pytest.raises(TaskRetryRequested) as raised:
            process_task(
                task.id,
                worker_identifier="retry-worker",
                session_factory=session_factory,
                executor=transient_failure,
            )

        request = raised.value
        assert request.task_id == task.id
        assert request.retry_number == retry_number
        assert request.countdown == expected_countdown
        assert request.max_retries == 3

        persisted, attempts = load_task(session_factory, task.id)
        assert persisted.status is TaskStatus.QUEUED
        assert persisted.retry_count == retry_number
        assert persisted.completed_at is None
        assert persisted.last_error == "temporary dependency failure"
        assert len(attempts) == retry_number
        assert [attempt.attempt_number for attempt in attempts] == list(
            range(1, retry_number + 1)
        )
        assert all(attempt.finished_at is not None for attempt in attempts)
        assert all(
            attempt.error == "temporary dependency failure" for attempt in attempts
        )


def test_retry_state_is_committed_before_retry_signal(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    with pytest.raises(TaskRetryRequested):
        process_task(
            task.id,
            worker_identifier="retry-worker",
            session_factory=session_factory,
            executor=transient_failure,
        )

    with session_factory() as observer:
        persisted = observer.get(Task, task.id)
        assert persisted is not None
        assert persisted.status is TaskStatus.QUEUED
        assert persisted.retry_count == 1
        attempt = observer.scalar(
            select(TaskExecutionAttempt).where(
                TaskExecutionAttempt.task_id == task.id
            )
        )
        assert attempt is not None
        assert attempt.finished_at is not None
        assert attempt.error == "temporary dependency failure"


def test_exhausted_retries_become_terminal_without_exceeding_limit(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory, max_retries=2)

    for _ in range(2):
        with pytest.raises(TaskRetryRequested):
            process_task(
                task.id,
                worker_identifier="retry-worker",
                session_factory=session_factory,
                executor=transient_failure,
            )

    before_terminal, prior_attempts = load_task(session_factory, task.id)
    prior_errors = [attempt.error for attempt in prior_attempts]
    assert before_terminal.retry_count == 2

    with pytest.raises(TaskExecutionFailedError):
        process_task(
            task.id,
            worker_identifier="retry-worker",
            session_factory=session_factory,
            executor=transient_failure,
        )

    terminal, attempts = load_task(session_factory, task.id)
    assert terminal.status is TaskStatus.DEAD_LETTER
    assert terminal.retry_count == terminal.max_retries == 2
    assert len(attempts) == terminal.max_retries + 1
    assert [attempt.error for attempt in attempts[:2]] == prior_errors
    assert attempts[-1].finished_at is not None
    assert attempts[-1].error == "temporary dependency failure"


def test_zero_retries_fails_on_initial_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory, max_retries=0)

    with pytest.raises(TaskExecutionFailedError):
        process_task(
            task.id,
            worker_identifier="retry-worker",
            session_factory=session_factory,
            executor=transient_failure,
        )

    persisted, attempts = load_task(session_factory, task.id)
    assert persisted.status is TaskStatus.DEAD_LETTER
    assert persisted.retry_count == 0
    assert len(attempts) == 1


def test_success_after_retry_preserves_retry_history(
    session_factory: sessionmaker[Session],
) -> None:
    task = create_task(session_factory)

    with pytest.raises(TaskRetryRequested):
        process_task(
            task.id,
            worker_identifier="retry-worker",
            session_factory=session_factory,
            executor=transient_failure,
        )

    outcome = process_task(
        task.id,
        worker_identifier="retry-worker",
        session_factory=session_factory,
    )

    persisted, attempts = load_task(session_factory, task.id)
    assert outcome is LifecycleOutcome.COMPLETED
    assert persisted.status is TaskStatus.COMPLETED
    assert persisted.retry_count == 1
    assert persisted.completed_at is not None
    assert persisted.last_error is None
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]
    assert attempts[0].error == "temporary dependency failure"
    assert attempts[1].error is None


def test_celery_wrapper_translates_retry_signal_to_self_retry() -> None:
    task_id = "00000000-0000-0000-0000-000000000001"
    retry_request = TaskRetryRequested(
        task_id=UUID(task_id),
        retry_number=1,
        countdown=2,
        max_retries=3,
        error_message="temporary dependency failure",
    )

    with (
        patch("app.workers.tasks.process_task", side_effect=retry_request) as process,
        patch.object(execute_task, "retry", side_effect=RuntimeError("scheduled")) as retry,
        pytest.raises(RuntimeError, match="scheduled"),
    ):
        execute_task.run(task_id)

    process.assert_called_once()
    assert process.call_args.args[0] == task_id
    retry.assert_called_once_with(
        exc=retry_request,
        countdown=2,
        max_retries=3,
    )


def test_celery_wrapper_does_not_retry_success_or_permanent_failure() -> None:
    task_id = "00000000-0000-0000-0000-000000000001"

    with (
        patch("app.workers.tasks.process_task", return_value=LifecycleOutcome.COMPLETED),
        patch.object(execute_task, "retry") as retry,
    ):
        execute_task.run(task_id)
    retry.assert_not_called()

    with (
        patch(
            "app.workers.tasks.process_task",
            side_effect=TaskExecutionFailedError("permanent failure"),
        ),
        patch.object(execute_task, "retry") as retry,
        pytest.raises(TaskExecutionFailedError, match="permanent failure"),
    ):
        execute_task.run(task_id)
    retry.assert_not_called()
