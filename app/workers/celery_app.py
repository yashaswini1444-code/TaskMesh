from celery import Celery
from kombu import Queue

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "taskmesh",
    broker=settings.celery_broker_url,
    include=["app.workers.tasks"],
)
celery_app.conf.update(
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    enable_utc=True,
    timezone="UTC",
    task_always_eager=False,
    task_ignore_result=True,
    task_queues=(
        Queue("high"),
        Queue("medium"),
        Queue("low"),
        # Administrative/periodic work (lease recovery) is isolated on its
        # own queue so it never competes with priority task capacity.
        Queue("control"),
    ),
    task_default_queue="medium",
    task_create_missing_queues=False,
    # Periodically reclaim RUNNING tasks whose execution lease has expired
    # (crashed/killed worker). See app.services.recovery.recover_stale_tasks
    # and app.workers.tasks.recover_stale_tasks_task.
    beat_schedule={
        "recover-stale-running-tasks": {
            "task": "taskmesh.recover_stale_tasks",
            "schedule": settings.task_recovery_interval_seconds,
            "options": {"queue": "control"},
        },
    },
)
