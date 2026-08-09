from uuid import UUID

from pydantic import BaseModel

from app.models import TaskStatus


class ReclaimedTaskResult(BaseModel):
    task_id: UUID
    outcome: TaskStatus
    retry_count: int
    redispatched: bool


class RecoveryRunResult(BaseModel):
    reclaimed_count: int
    reclaimed: list[ReclaimedTaskResult]
