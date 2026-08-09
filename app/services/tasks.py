from uuid import UUID

from sqlalchemy import Select, select, update
from sqlalchemy.orm import Session, selectinload

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
    return session.scalar(
        select(Task)
        .where(Task.id == task_id)
        .options(selectinload(Task.execution_attempts))
    )


def requeue_dead_letter_task(session: Session, task_id: UUID) -> Task | None:
    result = session.execute(
        update(Task)
        .where(Task.id == task_id, Task.status == TaskStatus.DEAD_LETTER)
        .values(
            status=TaskStatus.QUEUED,
            retry_count=0,
            started_at=None,
            completed_at=None,
            last_error=None,
        )
    )
    # See task_lifecycle.py for why this needs a type: ignore.
    if result.rowcount != 1:  # type: ignore[attr-defined]
        session.rollback()
        return None
    session.commit()
    return get_task(session, task_id)


def list_tasks(
    session: Session,
    *,
    offset: int,
    limit: int,
    priority: TaskPriority | None = None,
    task_status: TaskStatus | None = None,
    task_type: str | None = None,
) -> list[Task]:
    statement: Select[tuple[Task]] = select(Task).options(
        selectinload(Task.execution_attempts)
    )
    if priority is not None:
        statement = statement.where(Task.priority == priority)
    if task_status is not None:
        statement = statement.where(Task.status == task_status)
    if task_type is not None:
        statement = statement.where(Task.task_type == task_type)

    statement = statement.order_by(Task.created_at.desc(), Task.id.desc())
    statement = statement.offset(offset).limit(limit)
    return list(session.scalars(statement))
