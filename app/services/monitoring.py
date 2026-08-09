import logging
from datetime import UTC, datetime, timedelta
from typing import Protocol

from redis import Redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.models import Task, TaskStatus
from app.schemas.monitoring import (
    DatabaseStatus,
    MonitoringSummary,
    QueueStatus,
    TaskCounts,
    ThroughputMetrics,
    WorkerItem,
    WorkerStatus,
)
from app.schemas.task import TaskRead
from app.services.recovery import count_stale_running
from app.workers.celery_app import celery_app

logger = logging.getLogger("taskmesh.monitoring")

MONITORING_WINDOW_SECONDS = 60
MONITORED_QUEUES = ("high", "medium", "low")
DATABASE_UNAVAILABLE_MESSAGE = "Database monitoring unavailable"


class WorkerMonitor(Protocol):
    def inspect(self) -> WorkerStatus: ...


class QueueMonitor(Protocol):
    def inspect(self) -> QueueStatus: ...


class CeleryWorkerMonitor:
    def inspect(self) -> WorkerStatus:
        try:
            inspector = celery_app.control.inspect(timeout=1.0)
            ping = inspector.ping() or {}
            active = inspector.active() or {}
            reserved = inspector.reserved() or {}
            items = [
                WorkerItem(
                    identifier=identifier,
                    active_tasks=len(active.get(identifier, [])),
                    reserved_tasks=len(reserved.get(identifier, [])),
                )
                for identifier in sorted(ping)
            ]
            return WorkerStatus(available=True, count=len(items), items=items)
        except Exception:
            return WorkerStatus(
                available=False,
                count=0,
                items=[],
                error="Worker monitoring unavailable",
            )


class RedisQueueMonitor:
    def inspect(self) -> QueueStatus:
        try:
            settings = get_settings()
            client = Redis.from_url(
                settings.celery_broker_url,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
                decode_responses=True,
            )
            # redis-py's stubs type llen() as Awaitable[int] | int to cover
            # both its sync and async clients; this is always int here.
            depths = {
                queue: int(client.llen(queue))  # type: ignore[arg-type]
                for queue in MONITORED_QUEUES
            }
            return QueueStatus(
                available=True,
                high=depths["high"],
                medium=depths["medium"],
                low=depths["low"],
                total=sum(depths.values()),
            )
        except Exception:
            return QueueStatus(
                available=False,
                high=0,
                medium=0,
                low=0,
                total=0,
                error="Queue monitoring unavailable",
            )


worker_monitor = CeleryWorkerMonitor()
queue_monitor = RedisQueueMonitor()


def get_worker_monitor() -> WorkerMonitor:
    return worker_monitor


def get_queue_monitor() -> QueueMonitor:
    return queue_monitor


def _query_task_metrics(
    session: Session, current_time: datetime
) -> tuple[TaskCounts, ThroughputMetrics, list[Task], list[Task]]:
    """Run the DB-dependent monitoring queries. Raises on connection/query failure;
    callers are responsible for catching and degrading gracefully."""

    status_rows = session.execute(
        select(Task.status, func.count(Task.id)).group_by(Task.status)
    ).all()
    counts = {status: count for status, count in status_rows}
    stale_running = count_stale_running(session, now=current_time)
    task_counts = TaskCounts(
        queued=counts.get(TaskStatus.QUEUED, 0),
        running=counts.get(TaskStatus.RUNNING, 0),
        completed=counts.get(TaskStatus.COMPLETED, 0),
        failed=counts.get(TaskStatus.FAILED, 0),
        dead_letter=counts.get(TaskStatus.DEAD_LETTER, 0),
        stale_running=stale_running,
    )

    window_start = current_time - timedelta(seconds=MONITORING_WINDOW_SECONDS)
    completed_in_window = (
        session.scalar(
            select(func.count(Task.id)).where(
                Task.status == TaskStatus.COMPLETED,
                Task.completed_at >= window_start,
            )
        )
        or 0
    )
    throughput = ThroughputMetrics(
        window_seconds=MONITORING_WINDOW_SECONDS,
        completed=completed_in_window,
        per_minute=float(completed_in_window),
    )

    recent_tasks = list(
        session.scalars(
            select(Task)
            .options(selectinload(Task.execution_attempts))
            .order_by(Task.created_at.desc(), Task.id.desc())
            .limit(10)
        )
    )
    recent_failures = list(
        session.scalars(
            select(Task)
            .options(selectinload(Task.execution_attempts))
            .where(Task.status.in_([TaskStatus.FAILED, TaskStatus.DEAD_LETTER]))
            .order_by(Task.created_at.desc(), Task.id.desc())
            .limit(10)
        )
    )
    return task_counts, throughput, recent_tasks, recent_failures


def build_monitoring_summary(
    session: Session,
    workers: WorkerStatus,
    queues: QueueStatus,
    *,
    now: datetime | None = None,
) -> MonitoringSummary:
    """Build the operational snapshot. A database outage degrades this endpoint
    instead of raising: ``database.available`` becomes ``False`` and ``tasks``/
    ``throughput`` are left unset rather than fabricated as zero. Worker and
    queue telemetry (already gathered by the caller) are reported independently
    of database health, matching how ``CeleryWorkerMonitor``/``RedisQueueMonitor``
    already degrade."""

    current_time = now or datetime.now(UTC)
    task_counts: TaskCounts | None
    throughput: ThroughputMetrics | None
    recent_tasks: list[Task]
    recent_failures: list[Task]
    try:
        task_counts, throughput, recent_tasks, recent_failures = _query_task_metrics(
            session, current_time
        )
        database_status = DatabaseStatus(available=True)
    except Exception:
        logger.exception("Database monitoring query failed")
        task_counts = None
        throughput = None
        recent_tasks = []
        recent_failures = []
        database_status = DatabaseStatus(
            available=False, error=DATABASE_UNAVAILABLE_MESSAGE
        )

    return MonitoringSummary(
        database=database_status,
        workers=workers,
        queues=queues,
        tasks=task_counts,
        throughput=throughput,
        recent_tasks=[TaskRead.model_validate(task) for task in recent_tasks],
        recent_failures=[TaskRead.model_validate(task) for task in recent_failures],
    )
