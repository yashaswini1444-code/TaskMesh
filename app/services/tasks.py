from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models import Task, TaskPriority, TaskStatus
from app.schemas.task import TaskCreate


def create_task(session: Session, task_data: TaskCreate) -> Task:
    task = Task(
        task_type=task_data.task_type,
        payload=task_data.payload,
        priority=task_data.priority,
        status=TaskStatus.QUEUED,
    )
    session.add(task)
    try:
        session.commit()
    except Exception:
        session.rollback()
        raise
    session.refresh(task)
    return task


def get_task(session: Session, task_id: UUID) -> Task | None:
    return session.get(Task, task_id)


def list_tasks(
    session: Session,
    *,
    offset: int,
    limit: int,
    priority: TaskPriority | None = None,
    task_status: TaskStatus | None = None,
    task_type: str | None = None,
) -> list[Task]:
    statement: Select[tuple[Task]] = select(Task)
    if priority is not None:
        statement = statement.where(Task.priority == priority)
    if task_status is not None:
        statement = statement.where(Task.status == task_status)
    if task_type is not None:
        statement = statement.where(Task.task_type == task_type)

    statement = statement.order_by(Task.created_at.desc(), Task.id.desc())
    statement = statement.offset(offset).limit(limit)
    return list(session.scalars(statement))
