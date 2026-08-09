from pydantic import BaseModel, Field

from app.schemas.task import TaskRead


class WorkerItem(BaseModel):
    identifier: str
    active_tasks: int
    reserved_tasks: int


class WorkerStatus(BaseModel):
    available: bool
    count: int
    items: list[WorkerItem]
    error: str | None = None


class QueueStatus(BaseModel):
    available: bool
    high: int
    medium: int
    low: int
    total: int
    error: str | None = None


class DatabaseStatus(BaseModel):
    """Reachability of the persistence layer itself, not application data."""

    available: bool
    error: str | None = None


class TaskCounts(BaseModel):
    queued: int
    running: int
    completed: int
    failed: int
    dead_letter: int


class ThroughputMetrics(BaseModel):
    window_seconds: int
    completed: int
    per_minute: float


class MonitoringSummary(BaseModel):
    """Operational snapshot. ``tasks``/``throughput`` are ``None`` only when
    ``database.available`` is ``False`` — counts are never fabricated."""

    database: DatabaseStatus
    workers: WorkerStatus
    queues: QueueStatus
    tasks: TaskCounts | None = None
    throughput: ThroughputMetrics | None = None
    recent_tasks: list[TaskRead] = Field(default_factory=list)
    recent_failures: list[TaskRead] = Field(default_factory=list)
