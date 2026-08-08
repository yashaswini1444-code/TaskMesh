from app.workers.celery_app import celery_app
from app.workers.tasks import execute_task


def test_celery_uses_redis_json_and_utc_without_eager_execution() -> None:
    assert celery_app.conf.broker_url == "redis://localhost:6379/0"
    assert celery_app.conf.accept_content == ["json"]
    assert celery_app.conf.task_serializer == "json"
    assert celery_app.conf.enable_utc is True
    assert celery_app.conf.timezone == "UTC"
    assert celery_app.conf.task_always_eager is False
    assert celery_app.conf.task_ignore_result is True
    assert celery_app.conf.result_backend is None


def test_execute_task_is_registered_without_running_a_worker() -> None:
    assert execute_task.name == "taskmesh.execute_task"
