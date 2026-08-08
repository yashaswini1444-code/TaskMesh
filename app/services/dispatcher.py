from typing import Final, Protocol
from uuid import UUID

from app.models import TaskPriority
from app.workers.tasks import execute_task

PRIORITY_QUEUES: Final[dict[TaskPriority, str]] = {
    TaskPriority.HIGH: "high",
    TaskPriority.MEDIUM: "medium",
    TaskPriority.LOW: "low",
}


class TaskDispatchError(RuntimeError):
    """Raised when a persisted task cannot be published to the broker."""


class TaskDispatcher(Protocol):
    def dispatch(self, task_id: UUID, priority: TaskPriority) -> None: ...


def queue_for_priority(priority: TaskPriority) -> str:
    return PRIORITY_QUEUES[priority]


class CeleryTaskDispatcher:
    def dispatch(self, task_id: UUID, priority: TaskPriority) -> None:
        try:
            execute_task.apply_async(
                args=[str(task_id)],
                queue=queue_for_priority(priority),
            )
        except Exception as exc:
            raise TaskDispatchError("Celery task publication failed") from exc


task_dispatcher = CeleryTaskDispatcher()


def get_task_dispatcher() -> TaskDispatcher:
    return task_dispatcher
