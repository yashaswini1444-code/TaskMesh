from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.models import Task, TaskExecutionAttempt, TaskPriority, TaskStatus


@pytest.fixture
def session() -> Generator[Session, None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    with Session(engine) as database_session:
        yield database_session
    engine.dispose()


def test_task_persistence_and_defaults(session: Session) -> None:
    task = Task(task_type="email.send", payload={"recipient_id": 42})
    session.add(task)
    session.commit()
    session.refresh(task)

    assert task.id is not None
    assert task.payload == {"recipient_id": 42}
    assert task.priority is TaskPriority.MEDIUM
    assert task.status is TaskStatus.QUEUED
    assert task.retry_count == 0
    assert task.max_retries == 3
    assert task.created_at is not None
    assert task.started_at is None
    assert task.completed_at is None
    assert task.last_error is None


def test_execution_attempt_persistence_and_relationship(session: Session) -> None:
    task = Task(task_type="report.generate", payload={"report_id": "weekly"})
    task.execution_attempts.append(
        TaskExecutionAttempt(attempt_number=1, worker_identifier="worker-01")
    )
    session.add(task)
    session.commit()
    session.expire_all()

    persisted_attempt = session.scalars(select(TaskExecutionAttempt)).one()

    assert persisted_attempt.task.task_type == "report.generate"
    assert persisted_attempt in persisted_attempt.task.execution_attempts
    assert persisted_attempt.worker_identifier == "worker-01"
    assert persisted_attempt.started_at is not None
    assert persisted_attempt.finished_at is None
    assert persisted_attempt.error is None


def test_retry_count_cannot_exceed_max_retries(session: Session) -> None:
    session.add(
        Task(
            task_type="invalid.retry",
            payload={},
            retry_count=2,
            max_retries=1,
        )
    )

    with pytest.raises(IntegrityError):
        session.commit()


def test_attempt_numbers_are_unique_per_task(session: Session) -> None:
    task = Task(task_type="duplicate.attempt", payload={})
    task.execution_attempts.extend(
        [
            TaskExecutionAttempt(attempt_number=1),
            TaskExecutionAttempt(attempt_number=1),
        ]
    )
    session.add(task)

    with pytest.raises(IntegrityError):
        session.commit()
