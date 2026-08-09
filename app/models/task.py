import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.task_execution_attempt import TaskExecutionAttempt


class TaskPriority(str, enum.Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint("retry_count >= 0", name="retry_count_nonnegative"),
        CheckConstraint("max_retries >= 0", name="max_retries_nonnegative"),
        CheckConstraint("retry_count <= max_retries", name="retry_count_within_limit"),
        # Matches the lease-recovery scan: WHERE status='RUNNING' AND
        # lease_expires_at < :now. No standalone index on lease_expires_at
        # alone; every real query filters on status first.
        Index("ix_tasks_status_lease_expires_at", "status", "lease_expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    priority: Mapped[TaskPriority] = mapped_column(
        Enum(TaskPriority, native_enum=False, create_constraint=True),
        nullable=False,
        default=TaskPriority.MEDIUM,
        server_default=TaskPriority.MEDIUM.value,
        index=True,
    )
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, native_enum=False, create_constraint=True),
        nullable=False,
        default=TaskStatus.QUEUED,
        server_default=TaskStatus.QUEUED.value,
        index=True,
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    max_retries: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default="3"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    execution_attempts: Mapped[list["TaskExecutionAttempt"]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskExecutionAttempt.attempt_number",
    )
