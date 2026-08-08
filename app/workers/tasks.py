from app.workers.celery_app import celery_app
from app.services.task_lifecycle import TaskRetryRequested, process_task


@celery_app.task(bind=True, name="taskmesh.execute_task", ignore_result=True)
def execute_task(self: object, task_id: str) -> None:
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
        )
