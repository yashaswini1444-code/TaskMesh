from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from app.models import TaskPriority, TaskStatus

TaskType = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_type: TaskType
    payload: dict[str, Any]
    priority: TaskPriority = TaskPriority.MEDIUM


class TaskRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_type: str
    payload: dict[str, Any]
    priority: TaskPriority
    status: TaskStatus
    retry_count: int
    max_retries: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    last_error: str | None
