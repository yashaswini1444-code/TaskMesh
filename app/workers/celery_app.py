from celery import Celery

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
)
