from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_session_factory
from app.schemas.recovery import ReclaimedTaskResult, RecoveryRunResult
from app.services.dispatcher import TaskDispatcher, get_task_dispatcher
from app.services.recovery import recover_stale_tasks

router = APIRouter(prefix="/recovery", tags=["recovery"])
Dispatcher = Annotated[TaskDispatcher, Depends(get_task_dispatcher)]
SessionFactoryDep = Annotated[Callable[[], Session], Depends(get_session_factory)]


@router.post("/stale-running", response_model=RecoveryRunResult)
def run_stale_running_recovery(
    dispatcher: Dispatcher, session_factory: SessionFactoryDep
) -> RecoveryRunResult:
    """Administrative recovery path: reclaim RUNNING tasks whose execution
    lease has expired without waiting for the periodic Celery Beat scan.
    Safe to call repeatedly — reclaiming is a no-op for tasks that are not
    actually stale (see app.services.recovery for the concurrency guarantees).
    """

    reclaimed = recover_stale_tasks(session_factory=session_factory, dispatcher=dispatcher)
    return RecoveryRunResult(
        reclaimed_count=len(reclaimed),
        reclaimed=[
            ReclaimedTaskResult(
                task_id=item.task_id,
                outcome=item.outcome,
                retry_count=item.retry_count,
                redispatched=item.redispatched,
            )
            for item in reclaimed
        ],
    )
