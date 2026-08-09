"""Stale-RUNNING task recovery ("lease recovery").

Each claimed task carries a ``lease_expires_at`` timestamp set by
``task_lifecycle._claim_task``. If a worker crashes mid-execution, the task
is left RUNNING forever with no lease-based follow-up unless something scans
for it — that scan is what this module does.

Design goals (deliberately NOT a Kubernetes-grade lease system):

- A single timestamp column, no per-worker heartbeat/lease-owner tracking.
- Recovery is a plain, testable Python function with no Celery dependency of
  its own; it is *triggered* by Celery Beat (see workers/celery_app.py) and
  by an administrative API endpoint (POST /recovery/stale-running), but the
  decision logic lives here and is unit-testable without either.
- Concurrency safety reuses the exact compare-and-swap pattern already used
  throughout task_lifecycle: every state transition is a single
  ``UPDATE ... WHERE status = 'RUNNING'`` (optionally with an additional
  lease-expiry guard), so two overlapping recovery scans — or a recovery
  scan racing the original worker's own completion — can never both "win"
  the same task. See task_lifecycle.TaskReclaimedError for the other side of
  that race.

What this guarantees:
- A task is only reclaimed after its lease has genuinely expired.
- Reclaiming a task is atomic and idempotent: at most one caller transitions
  a given task out of RUNNING for a given staleness window.
- The abandoned execution attempt is closed out truthfully (finished_at +
  error set), never deleted or silently altered.
- Retry budget is respected using the same retry_count/max_retries rule as
  a normal in-worker failure: exhausted budget -> DEAD_LETTER, otherwise a
  new attempt is queued (and, unlike a normal Celery-scheduled retry,
  explicitly redispatched here, since no Celery process is alive to do it).

What this does NOT guarantee:
- Exactly-once execution. If the original worker was not actually dead — merely
  slower than the configured lease — it may complete the job's real side
  effects *after* recovery has already redispatched a second attempt. This
  is a deliberate, documented trade-off: a lease that is too short relative
  to real job duration trades "task stuck forever" for "task possibly
  executed twice". Set TASKMESH_TASK_LEASE_SECONDS well above the slowest
  expected job duration to keep this rare, and prefer idempotent handlers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models import Task, TaskExecutionAttempt, TaskPriority, TaskStatus
from app.services.dispatcher import (
    TaskDispatcher,
    TaskDispatchError,
    get_task_dispatcher,
)
from app.services.task_lifecycle import SessionFactory, retry_countdown

logger = logging.getLogger("taskmesh.recovery")

STALE_LEASE_ERROR = "Execution lease expired; worker presumed lost"


@dataclass(frozen=True)
class ReclaimedTask:
    task_id: UUID
    outcome: TaskStatus
    retry_count: int
    redispatched: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _reclaim_one(
    task_id: UUID, now: datetime, session_factory: SessionFactory
) -> ReclaimedTask | None:
    """Attempt to reclaim a single stale task. Returns None if it was not
    actually reclaimed (already finished normally, or another recovery pass
    won the race first) — never raises for that case."""

    with session_factory() as session:
        current = session.execute(
            select(Task.retry_count, Task.max_retries).where(
                Task.id == task_id,
                Task.status == TaskStatus.RUNNING,
                Task.lease_expires_at.is_not(None),
                Task.lease_expires_at < now,
            )
        ).first()
        if current is None:
            return None
        retry_count, max_retries = current

        retry_requested = retry_count < max_retries
        new_retry_count = retry_count + 1 if retry_requested else retry_count
        new_status = TaskStatus.QUEUED if retry_requested else TaskStatus.DEAD_LETTER

        result = session.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.status == TaskStatus.RUNNING,
                Task.lease_expires_at.is_not(None),
                Task.lease_expires_at < now,
            )
            .values(
                status=new_status,
                retry_count=new_retry_count,
                started_at=None,
                completed_at=None,
                last_error=STALE_LEASE_ERROR,
                lease_expires_at=None,
            )
        )
        if result.rowcount != 1:
            # Lost the race: the original worker finished (or another
            # recovery pass reclaimed it) between the SELECT above and here.
            session.rollback()
            return None

        open_attempt = session.execute(
            select(TaskExecutionAttempt)
            .where(
                TaskExecutionAttempt.task_id == task_id,
                TaskExecutionAttempt.finished_at.is_(None),
            )
            .order_by(TaskExecutionAttempt.attempt_number.desc())
            .limit(1)
        ).scalar_one_or_none()
        if open_attempt is not None:
            open_attempt.finished_at = now
            open_attempt.error = STALE_LEASE_ERROR
        session.commit()

        return ReclaimedTask(
            task_id=task_id,
            outcome=new_status,
            retry_count=new_retry_count,
            redispatched=False,
        )


def recover_stale_tasks(
    *,
    session_factory: SessionFactory = SessionLocal,
    dispatcher: TaskDispatcher | None = None,
    now: datetime | None = None,
) -> list[ReclaimedTask]:
    """Scan for RUNNING tasks whose lease has expired and reclaim each one.

    Reclaimed tasks that still have retry budget are requeued *and*
    redispatched to the broker here (no Celery `self.retry` call is possible
    — the original worker process is presumed gone). If redispatch itself
    fails (broker unavailable), the task is left QUEUED for manual
    redispatch via POST /tasks/{id}/redispatch — the same documented
    persist-then-dispatch consistency window as task submission.
    """

    current_time = now or _utc_now()
    active_dispatcher = dispatcher or get_task_dispatcher()

    with session_factory() as session:
        candidate_ids = list(
            session.scalars(
                select(Task.id).where(
                    Task.status == TaskStatus.RUNNING,
                    Task.lease_expires_at.is_not(None),
                    Task.lease_expires_at < current_time,
                )
            )
        )

    reclaimed: list[ReclaimedTask] = []
    for task_id in candidate_ids:
        result = _reclaim_one(task_id, current_time, session_factory)
        if result is None:
            continue

        redispatched = False
        if result.outcome is TaskStatus.QUEUED:
            with session_factory() as session:
                priority = session.scalar(
                    select(Task.priority).where(Task.id == task_id)
                )
            try:
                active_dispatcher.dispatch(task_id, priority or TaskPriority.MEDIUM)
                redispatched = True
            except TaskDispatchError:
                logger.warning(
                    "recovery redispatch failed task_id=%s; task remains QUEUED "
                    "for manual redispatch",
                    task_id,
                )
            result = ReclaimedTask(
                task_id=result.task_id,
                outcome=result.outcome,
                retry_count=result.retry_count,
                redispatched=redispatched,
            )

        logger.info(
            "stale task reclaimed task_id=%s outcome=%s retry_count=%s "
            "redispatched=%s lease_countdown=%s",
            task_id,
            result.outcome.value,
            result.retry_count,
            redispatched,
            retry_countdown(result.retry_count) if result.outcome is TaskStatus.QUEUED else None,
        )
        reclaimed.append(result)

    return reclaimed


def count_stale_running(session: Session, *, now: datetime | None = None) -> int:
    """Read-only count of RUNNING tasks whose lease has already expired, for
    monitoring. Does not mutate any state."""

    current_time = now or _utc_now()
    return (
        session.scalar(
            select(func.count(Task.id)).where(
                Task.status == TaskStatus.RUNNING,
                Task.lease_expires_at.is_not(None),
                Task.lease_expires_at < current_time,
            )
        )
        or 0
    )
