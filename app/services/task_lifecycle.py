from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models import Task, TaskExecutionAttempt, TaskStatus
from app.services.execution import (
    RetryableTaskExecutionError,
    TaskExecutionError,
    execute_job,
)

SessionFactory = Callable[[], Session]
JobExecutor = Callable[[str, Mapping[str, Any]], None]
RETRY_BACKOFF_BASE_SECONDS = 2
RETRY_BACKOFF_MAX_SECONDS = 16


class InvalidTaskIdError(ValueError):
    """Raised when a worker message does not contain a valid task UUID."""


class TaskNotFoundError(LookupError):
    """Raised when a valid task UUID has no persisted task."""


class TaskExecutionFailedError(TaskExecutionError):
    """Raised after a job failure has been durably recorded."""


class TaskRetryRequested(TaskExecutionError):
    """Signals Celery after a retryable failure is durably queued."""

    def __init__(
        self,
        *,
        task_id: UUID,
        retry_number: int,
        countdown: int,
        max_retries: int,
        error_message: str,
    ) -> None:
        super().__init__(error_message)
        self.task_id = task_id
        self.retry_number = retry_number
        self.countdown = countdown
        self.max_retries = max_retries


class LifecycleOutcome(str, Enum):
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def retry_countdown(retry_number: int) -> int:
    """Return bounded exponential delay for a 1-based retry number."""

    if retry_number < 1:
        raise ValueError("retry_number must be at least 1")
    delay = RETRY_BACKOFF_BASE_SECONDS**retry_number
    return min(delay, RETRY_BACKOFF_MAX_SECONDS)


def _parse_task_id(task_id: str | UUID) -> UUID:
    if isinstance(task_id, UUID):
        return task_id
    try:
        return UUID(task_id)
    except (TypeError, ValueError, AttributeError) as exc:
        raise InvalidTaskIdError("Worker message contains an invalid task UUID") from exc


def _safe_error_message(exc: Exception) -> str:
    if isinstance(exc, TaskExecutionError):
        message = str(exc).strip() or "Task execution failed"
    else:
        message = f"{type(exc).__name__}: task execution failed"
    return message[:2000]


def _claim_task(
    task_id: UUID,
    worker_identifier: str,
    session_factory: SessionFactory,
) -> tuple[UUID, str, dict[str, Any]] | None:
    claimed_at = _utc_now()
    with session_factory() as session:
        claim = session.execute(
            update(Task)
            .where(Task.id == task_id, Task.status == TaskStatus.QUEUED)
            .values(
                status=TaskStatus.RUNNING,
                started_at=claimed_at,
                completed_at=None,
                last_error=None,
            )
        )
        if claim.rowcount != 1:
            task_exists = session.scalar(select(Task.id).where(Task.id == task_id))
            session.rollback()
            if task_exists is None:
                raise TaskNotFoundError(f"Task {task_id} does not exist")
            return None

        attempt_number = session.scalar(
            select(func.coalesce(func.max(TaskExecutionAttempt.attempt_number), 0) + 1)
            .where(TaskExecutionAttempt.task_id == task_id)
        )
        task = session.get(Task, task_id)
        if task is None:  # Defensive: the successful UPDATE guarantees existence.
            session.rollback()
            raise TaskNotFoundError(f"Task {task_id} does not exist")

        attempt = TaskExecutionAttempt(
            task_id=task_id,
            attempt_number=int(attempt_number),
            worker_identifier=worker_identifier[:255],
            started_at=claimed_at,
        )
        session.add(attempt)
        session.commit()

        return attempt.id, task.task_type, deepcopy(task.payload)


def _complete_task(
    task_id: UUID,
    attempt_id: UUID,
    session_factory: SessionFactory,
) -> None:
    finished_at = _utc_now()
    with session_factory() as session:
        task = session.get(Task, task_id)
        attempt = session.get(TaskExecutionAttempt, attempt_id)
        if task is None or attempt is None:
            raise TaskNotFoundError("Claimed task or execution attempt no longer exists")

        task.status = TaskStatus.COMPLETED
        task.completed_at = finished_at
        task.last_error = None
        attempt.finished_at = finished_at
        attempt.error = None
        session.commit()


def _finalize_failure(
    task_id: UUID,
    attempt_id: UUID,
    error_message: str,
    retryable: bool,
    session_factory: SessionFactory,
) -> TaskRetryRequested | None:
    finished_at = _utc_now()
    with session_factory() as session:
        task = session.get(Task, task_id)
        attempt = session.get(TaskExecutionAttempt, attempt_id)
        if task is None or attempt is None:
            raise TaskNotFoundError("Claimed task or execution attempt no longer exists")

        retry_requested = retryable and task.retry_count < task.max_retries
        if retry_requested:
            task.retry_count += 1
            task.status = TaskStatus.QUEUED
        else:
            task.status = TaskStatus.FAILED
        task.completed_at = None
        task.last_error = error_message
        attempt.finished_at = finished_at
        attempt.error = error_message
        session.commit()

        if retry_requested:
            return TaskRetryRequested(
                task_id=task_id,
                retry_number=task.retry_count,
                countdown=retry_countdown(task.retry_count),
                max_retries=task.max_retries,
                error_message=error_message,
            )
        return None


def process_task(
    task_id: str | UUID,
    *,
    worker_identifier: str,
    session_factory: SessionFactory = SessionLocal,
    executor: JobExecutor = execute_job,
) -> LifecycleOutcome:
    """Claim and execute one queued task using explicit durable transactions.

    A non-QUEUED duplicate delivery is skipped. RUNNING and its attempt are
    committed before the executor is called. A worker crash after that commit
    can leave an unfinished RUNNING task; recovery is intentionally deferred.
    """

    parsed_task_id = _parse_task_id(task_id)
    claimed = _claim_task(parsed_task_id, worker_identifier, session_factory)
    if claimed is None:
        return LifecycleOutcome.SKIPPED

    attempt_id, task_type, payload = claimed
    try:
        executor(task_type, payload)
    except Exception as exc:
        error_message = _safe_error_message(exc)
        retry_request = _finalize_failure(
            parsed_task_id,
            attempt_id,
            error_message,
            isinstance(exc, RetryableTaskExecutionError),
            session_factory,
        )
        if retry_request is not None:
            # The retry state is committed before control returns to Celery.
            raise retry_request from exc
        raise TaskExecutionFailedError(error_message) from exc

    _complete_task(parsed_task_id, attempt_id, session_factory)
    return LifecycleOutcome.COMPLETED
