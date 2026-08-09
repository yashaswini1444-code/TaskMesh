from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
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


class TaskReclaimedError(TaskExecutionError):
    """Raised when a completion/failure arrives for an attempt that lease
    recovery already reclaimed (the task moved off RUNNING while this worker
    was still executing it). The late outcome is intentionally discarded:
    recovery's decision — and the fresh delivery it already published — is
    authoritative. See app/services/recovery.py."""


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
    return datetime.now(UTC)


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
    lease_expires_at = claimed_at + timedelta(seconds=get_settings().task_lease_seconds)
    with session_factory() as session:
        claim = session.execute(
            update(Task)
            .where(Task.id == task_id, Task.status == TaskStatus.QUEUED)
            .values(
                status=TaskStatus.RUNNING,
                started_at=claimed_at,
                completed_at=None,
                last_error=None,
                lease_expires_at=lease_expires_at,
            )
        )
        # SQLAlchemy's Session.execute() return type (Result[Any]) isn't
        # narrowed to CursorResult by the stubs, though UPDATE always
        # returns one with a real rowcount at runtime.
        if claim.rowcount != 1:  # type: ignore[attr-defined]
            task_exists = session.scalar(select(Task.id).where(Task.id == task_id))
            session.rollback()
            if task_exists is None:
                raise TaskNotFoundError(f"Task {task_id} does not exist")
            return None

        attempt_number = session.scalar(
            select(func.coalesce(func.max(TaskExecutionAttempt.attempt_number), 0) + 1)
            .where(TaskExecutionAttempt.task_id == task_id)
        )
        assert attempt_number is not None  # COALESCE(..., 0) + 1 is never NULL
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
    """Transition a claimed task to COMPLETED.

    Guarded by the same compare-and-swap pattern as ``_claim_task``: the
    UPDATE only applies while the task is still RUNNING. If lease recovery
    already reclaimed this task (worker presumed lost, a fresh attempt
    already queued), the guard fails and this late completion is rejected
    rather than silently overwriting the recovered state.
    """

    finished_at = _utc_now()
    with session_factory() as session:
        result = session.execute(
            update(Task)
            .where(Task.id == task_id, Task.status == TaskStatus.RUNNING)
            .values(
                status=TaskStatus.COMPLETED,
                completed_at=finished_at,
                last_error=None,
                lease_expires_at=None,
            )
        )
        if result.rowcount != 1:  # type: ignore[attr-defined]
            session.rollback()
            raise TaskReclaimedError(
                f"Task {task_id} was reclaimed before this attempt completed"
            )

        attempt = session.get(TaskExecutionAttempt, attempt_id)
        if attempt is None:
            raise TaskNotFoundError("Claimed execution attempt no longer exists")
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
    """Transition a claimed task to QUEUED (retry), DEAD_LETTER, or FAILED.

    Guarded the same way as ``_complete_task``: the terminal/retry UPDATE
    only applies while the task is still RUNNING, so a late failure arriving
    after lease recovery already reclaimed the task is rejected instead of
    corrupting the recovered state.
    """

    finished_at = _utc_now()
    with session_factory() as session:
        current = session.execute(
            select(Task.retry_count, Task.max_retries).where(
                Task.id == task_id, Task.status == TaskStatus.RUNNING
            )
        ).first()
        if current is None:
            existing = session.scalar(select(Task.id).where(Task.id == task_id))
            session.rollback()
            if existing is None:
                raise TaskNotFoundError(f"Task {task_id} does not exist")
            raise TaskReclaimedError(
                f"Task {task_id} was reclaimed before this attempt failed"
            )
        retry_count, max_retries = current

        retry_requested = retryable and retry_count < max_retries
        if retry_requested:
            new_retry_count = retry_count + 1
            new_status = TaskStatus.QUEUED
        elif retryable:
            new_retry_count = retry_count
            new_status = TaskStatus.DEAD_LETTER
        else:
            new_retry_count = retry_count
            new_status = TaskStatus.FAILED

        result = session.execute(
            update(Task)
            .where(Task.id == task_id, Task.status == TaskStatus.RUNNING)
            .values(
                status=new_status,
                retry_count=new_retry_count,
                completed_at=None,
                last_error=error_message,
                lease_expires_at=None,
            )
        )
        if result.rowcount != 1:  # type: ignore[attr-defined]
            session.rollback()
            raise TaskReclaimedError(
                f"Task {task_id} was reclaimed before this attempt failed"
            )

        attempt = session.get(TaskExecutionAttempt, attempt_id)
        if attempt is None:
            raise TaskNotFoundError("Claimed execution attempt no longer exists")
        attempt.finished_at = finished_at
        attempt.error = error_message
        session.commit()

        if retry_requested:
            return TaskRetryRequested(
                task_id=task_id,
                retry_number=new_retry_count,
                countdown=retry_countdown(new_retry_count),
                max_retries=max_retries,
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
    committed before the executor is called, with an execution lease
    (``lease_expires_at``). A worker crash after that commit leaves the task
    RUNNING until ``app.services.recovery.recover_stale_tasks`` reclaims it
    once the lease expires; that reclaim is what actually resolves the task,
    not this function. If this same worker resurfaces after being reclaimed,
    ``_complete_task``/``_finalize_failure`` will raise ``TaskReclaimedError``
    instead of corrupting the recovered state.
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
