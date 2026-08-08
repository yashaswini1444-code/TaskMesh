from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.monitoring import MonitoringSummary
from app.services.monitoring import (
    QueueMonitor,
    WorkerMonitor,
    build_monitoring_summary,
    get_queue_monitor,
    get_worker_monitor,
)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/summary", response_model=MonitoringSummary)
def monitoring_summary(
    session: Annotated[Session, Depends(get_db)],
    worker_monitor: Annotated[WorkerMonitor, Depends(get_worker_monitor)],
    queue_monitor: Annotated[QueueMonitor, Depends(get_queue_monitor)],
) -> MonitoringSummary:
    return build_monitoring_summary(
        session,
        worker_monitor.inspect(),
        queue_monitor.inspect(),
    )
