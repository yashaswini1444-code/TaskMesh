from app.workers.celery_app import celery_app
from app.services.task_lifecycle import process_task


@celery_app.task(bind=True, name="taskmesh.execute_task", ignore_result=True)
def execute_task(self: object, task_id: str) -> None:
    """Thin Celery entry point for durable task lifecycle processing."""

    request = getattr(self, "request", None)
    worker_identifier = getattr(request, "hostname", None) or "celery-worker"
    process_task(task_id, worker_identifier=worker_identifier)
