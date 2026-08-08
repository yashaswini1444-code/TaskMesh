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
from app.services.execution import TaskExecutionError, execute_job

SessionFactory = Callable[[], Session]
JobExecutor = Callable[[str, Mapping[str, Any]], None]


class InvalidTaskIdError(ValueError):
    """Raised when a worker message does not contain a valid task UUID."""


class TaskNotFoundError(LookupError):
    """Raised when a valid task UUID has no persisted task."""


class TaskExecutionFailedError(TaskExecutionError):
    """Raised after a job failure has been durably recorded."""


class LifecycleOutcome(str, Enum):
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def _fail_task(
    task_id: UUID,
    attempt_id: UUID,
    error_message: str,
    session_factory: SessionFactory,
) -> None:
    finished_at = _utc_now()
    with session_factory() as session:
        task = session.get(Task, task_id)
        attempt = session.get(TaskExecutionAttempt, attempt_id)
        if task is None or attempt is None:
            raise TaskNotFoundError("Claimed task or execution attempt no longer exists")

        task.status = TaskStatus.FAILED
        task.completed_at = None
        task.last_error = error_message
        attempt.finished_at = finished_at
        attempt.error = error_message
        session.commit()


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
        _fail_task(parsed_task_id, attempt_id, error_message, session_factory)
        raise TaskExecutionFailedError(error_message) from exc

    _complete_task(parsed_task_id, attempt_id, session_factory)
    return LifecycleOutcome.COMPLETED
