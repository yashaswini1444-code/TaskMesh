from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models import Task, TaskPriority, TaskStatus
from app.schemas.task import TaskCreate, TaskRead
from app.services.dispatcher import (
    TaskDispatcher,
    TaskDispatchError,
    get_task_dispatcher,
)
from app.services.tasks import create_task, get_task, list_tasks

router = APIRouter(prefix="/tasks", tags=["tasks"])
DatabaseSession = Annotated[Session, Depends(get_db)]
Dispatcher = Annotated[TaskDispatcher, Depends(get_task_dispatcher)]


@router.post("", response_model=TaskRead, status_code=status.HTTP_201_CREATED)
def submit_task(
    task_data: TaskCreate,
    session: DatabaseSession,
    dispatcher: Dispatcher,
) -> Task:
    task = create_task(session, task_data)
    try:
        dispatcher.dispatch(task.id)
    except TaskDispatchError as exc:
        # Persistence and broker publication are intentionally separate in
        # Milestone 4. The committed QUEUED task remains traceable if Redis is
        # unavailable; a durable outbox is deferred to a later milestone.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Task was persisted but could not be dispatched",
                "task_id": str(task.id),
            },
        ) from exc
    return task


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
