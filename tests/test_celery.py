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


def test_celery_declares_priority_and_control_queues() -> None:
    assert {queue.name for queue in celery_app.conf.task_queues} == {
        "high",
        "medium",
        "low",
        "control",
    }
    assert celery_app.conf.task_default_queue == "medium"
    assert celery_app.conf.task_create_missing_queues is False


def test_execute_task_is_registered_without_running_a_worker() -> None:
    assert execute_task.name == "taskmesh.execute_task"


def test_recovery_beat_schedule_targets_control_queue() -> None:
    from app.core.config import get_settings

    entry = celery_app.conf.beat_schedule["recover-stale-running-tasks"]
    assert entry["task"] == "taskmesh.recover_stale_tasks"
    assert entry["options"] == {"queue": "control"}
    assert entry["schedule"] == get_settings().task_recovery_interval_seconds


def test_recover_stale_tasks_task_is_registered() -> None:
    from app.workers.tasks import recover_stale_tasks_task

    assert recover_stale_tasks_task.name == "taskmesh.recover_stale_tasks"
