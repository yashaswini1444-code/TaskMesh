import logging
from typing import Final, Protocol
from uuid import UUID

from app.models import TaskPriority
from app.workers.tasks import execute_task

logger = logging.getLogger("taskmesh.dispatch")

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
        queue = queue_for_priority(priority)
        try:
            execute_task.apply_async(args=[str(task_id)], queue=queue)
        except Exception as exc:
            logger.warning(
                "dispatch failed task_id=%s priority=%s queue=%s error=%s",
                task_id,
                priority.value,
                queue,
                type(exc).__name__,
            )
            raise TaskDispatchError("Celery task publication failed") from exc
        logger.info(
            "task dispatched task_id=%s priority=%s queue=%s", task_id, priority.value, queue
        )


task_dispatcher = CeleryTaskDispatcher()


def get_task_dispatcher() -> TaskDispatcher:
    return task_dispatcher
