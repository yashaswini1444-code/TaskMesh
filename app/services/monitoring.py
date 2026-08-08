from datetime import datetime, timedelta, timezone
from typing import Protocol

from redis import Redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import Task, TaskStatus
from app.schemas.monitoring import (
    MonitoringSummary,
    QueueStatus,
    TaskCounts,
    ThroughputMetrics,
    WorkerItem,
    WorkerStatus,
)
from app.workers.celery_app import celery_app

MONITORING_WINDOW_SECONDS = 60
MONITORED_QUEUES = ("high", "medium", "low")


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
            depths = {queue: int(client.llen(queue)) for queue in MONITORED_QUEUES}
            return QueueStatus(
                available=True,
                **depths,
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


def build_monitoring_summary(
    session: Session,
    workers: WorkerStatus,
    queues: QueueStatus,
    *,
    now: datetime | None = None,
) -> MonitoringSummary:
    status_rows = session.execute(
        select(Task.status, func.count(Task.id)).group_by(Task.status)
    ).all()
    counts = {status: count for status, count in status_rows}
    task_counts = TaskCounts(
        queued=counts.get(TaskStatus.QUEUED, 0),
        running=counts.get(TaskStatus.RUNNING, 0),
        completed=counts.get(TaskStatus.COMPLETED, 0),
        failed=counts.get(TaskStatus.FAILED, 0),
        dead_letter=counts.get(TaskStatus.DEAD_LETTER, 0),
    )

    current_time = now or datetime.now(timezone.utc)
    window_start = current_time - timedelta(seconds=MONITORING_WINDOW_SECONDS)
    completed_in_window = session.scalar(
        select(func.count(Task.id)).where(
            Task.status == TaskStatus.COMPLETED,
            Task.completed_at >= window_start,
        )
    ) or 0
    throughput = ThroughputMetrics(
        window_seconds=MONITORING_WINDOW_SECONDS,
        completed=completed_in_window,
        per_minute=float(completed_in_window),
    )

    recent_tasks = list(
        session.scalars(
            select(Task).order_by(Task.created_at.desc(), Task.id.desc()).limit(10)
        )
    )
    recent_failures = list(
        session.scalars(
            select(Task)
            .where(Task.status.in_([TaskStatus.FAILED, TaskStatus.DEAD_LETTER]))
            .order_by(Task.created_at.desc(), Task.id.desc())
            .limit(10)
        )
    )
    return MonitoringSummary(
        workers=workers,
        queues=queues,
        tasks=task_counts,
        throughput=throughput,
        recent_tasks=recent_tasks,
        recent_failures=recent_failures,
    )
