from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models import Task, TaskPriority, TaskStatus
from app.schemas.task import TaskCreate, TaskDetail, TaskRead
from app.services.dispatcher import (
    TaskDispatcher,
    TaskDispatchError,
    get_task_dispatcher,
)
from app.services.tasks import create_task, get_task, list_tasks, requeue_dead_letter_task

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
        dispatcher.dispatch(task.id, task.priority)
    except TaskDispatchError as exc:
        # Persistence and broker publication are intentionally separate, not
        # atomic (no transactional outbox). The committed QUEUED task remains
        # traceable and redispatchable (POST /tasks/{id}/redispatch) if the
        # broker was unavailable at submission time.
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


@router.get("/{task_id}", response_model=TaskDetail)
def read_task(task_id: UUID, session: DatabaseSession) -> Task:
    task = get_task(session, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    return task


@router.post("/{task_id}/requeue", response_model=TaskDetail)
def requeue_task(
    task_id: UUID,
    session: DatabaseSession,
    dispatcher: Dispatcher,
) -> Task:
    task = requeue_dead_letter_task(session, task_id)
    if task is None:
        existing = get_task(session, task_id)
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only dead-letter tasks can be requeued",
        )

    try:
        dispatcher.dispatch(task.id, task.priority)
    except TaskDispatchError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Task was requeued but could not be dispatched",
                "task_id": str(task.id),
            },
        ) from exc
    return task


@router.post("/{task_id}/redispatch", response_model=TaskDetail)
def redispatch_task(
    task_id: UUID,
    session: DatabaseSession,
    dispatcher: Dispatcher,
) -> Task:
    """Republish a broker message for a task that is already persisted and
    QUEUED, without creating a new task or resetting any state.

    Exists for the consistency window documented on POST /tasks: dispatch
    can fail after the task is durably persisted (broker unavailable), and
    there was previously no way to recover that specific task short of
    manual database access. This does not implement a transactional
    outbox — it is a manual/administrative recovery action, not an
    automatic guarantee.

    Safe to call more than once, including concurrently: the underlying
    Celery message carries only the task id, and a worker's claim
    (`UPDATE ... WHERE status = 'QUEUED'`) is idempotent, so an extra
    redispatch for a task another delivery already claimed is a wasted
    broker message, never a duplicate execution.
    """

    task = get_task(session, task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    if task.status is not TaskStatus.QUEUED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only QUEUED tasks can be redispatched",
        )

    try:
        dispatcher.dispatch(task.id, task.priority)
    except TaskDispatchError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Task remains persisted but redispatch failed",
                "task_id": str(task.id),
            },
        ) from exc
    return task
