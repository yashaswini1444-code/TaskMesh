from pydantic import BaseModel

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
    workers: WorkerStatus
    queues: QueueStatus
    tasks: TaskCounts
    throughput: ThroughputMetrics
    recent_tasks: list[TaskRead]
    recent_failures: list[TaskRead]
