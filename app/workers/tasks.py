from celery import Task as CeleryTask

from app.services.task_lifecycle import TaskRetryRequested, process_task
from app.workers.celery_app import celery_app


@celery_app.task(bind=True, name="taskmesh.execute_task", ignore_result=True)
def execute_task(self: CeleryTask, task_id: str) -> None:
    """Thin Celery entry point for durable task lifecycle processing."""

    request = getattr(self, "request", None)
    worker_identifier = getattr(request, "hostname", None) or "celery-worker"
    try:
        process_task(task_id, worker_identifier=worker_identifier)
    except TaskRetryRequested as retry_request:
        # Celery preserves the current delivery's routing information when
        # retrying, so HIGH/MEDIUM/LOW queue placement remains intact.
        raise self.retry(
            exc=retry_request,
            countdown=retry_request.countdown,
            max_retries=retry_request.max_retries,
        ) from retry_request


@celery_app.task(name="taskmesh.recover_stale_tasks", ignore_result=True)
def recover_stale_tasks_task() -> None:
    """Celery Beat entry point for lease recovery. See beat_schedule in
    celery_app.py and app.services.recovery for the actual logic.

    Imported locally (not at module scope) to avoid a circular import:
    services.recovery -> services.dispatcher -> workers.tasks.
    """

    from app.services.recovery import recover_stale_tasks

    recover_stale_tasks()
