from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models import Task, TaskPriority, TaskStatus
from app.schemas.task import TaskCreate, TaskRead
from app.services.tasks import create_task, get_task, list_tasks

router = APIRouter(prefix="/tasks", tags=["tasks"])
DatabaseSession = Annotated[Session, Depends(get_db)]


@router.post("", response_model=TaskRead, status_code=status.HTTP_201_CREATED)
def submit_task(task_data: TaskCreate, session: DatabaseSession) -> Task:
    return create_task(session, task_data)


@router.get("", response_model=list[TaskRead])
def read_tasks(
    session: DatabaseSession,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    priority: TaskPriority | None = None,
    task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    task_type: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
) -> list[Task]:
    return list_tasks(
        session,
        offset=offset,
        limit=limit,
        priority=priority,
        task_status=task_status,
        task_type=task_type,
    )


@router.get("/{task_id}", response_model=TaskRead)
def read_task(task_id: UUID, session: DatabaseSession) -> Task:
    task = get_task(session, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    return task
