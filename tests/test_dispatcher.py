from unittest.mock import patch
from uuid import uuid4

import pytest

from app.models import TaskPriority
from app.services.dispatcher import CeleryTaskDispatcher, queue_for_priority


@pytest.mark.parametrize(
    ("priority", "expected_queue"),
    [
        (TaskPriority.HIGH, "high"),
        (TaskPriority.MEDIUM, "medium"),
        (TaskPriority.LOW, "low"),
    ],
)
def test_priority_maps_to_queue(
    priority: TaskPriority, expected_queue: str
) -> None:
    assert queue_for_priority(priority) == expected_queue


@pytest.mark.parametrize(
    ("priority", "expected_queue"),
    [
        (TaskPriority.HIGH, "high"),
        (TaskPriority.MEDIUM, "medium"),
        (TaskPriority.LOW, "low"),
    ],
)
def test_celery_dispatcher_publishes_uuid_string_to_priority_queue(
    priority: TaskPriority, expected_queue: str
) -> None:
    task_id = uuid4()
    dispatcher = CeleryTaskDispatcher()

    with patch("app.services.dispatcher.execute_task.apply_async") as publish:
        dispatcher.dispatch(task_id, priority)

    publish.assert_called_once_with(
        args=[str(task_id)],
        queue=expected_queue,
    )
