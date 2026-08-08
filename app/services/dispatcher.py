from typing import Protocol
from uuid import UUID

from app.workers.tasks import execute_task


class TaskDispatchError(RuntimeError):
    """Raised when a persisted task cannot be published to the broker."""


class TaskDispatcher(Protocol):
    def dispatch(self, task_id: UUID) -> None: ...


class CeleryTaskDispatcher:
    def dispatch(self, task_id: UUID) -> None:
        try:
            execute_task.delay(str(task_id))
        except Exception as exc:
            raise TaskDispatchError("Celery task publication failed") from exc


task_dispatcher = CeleryTaskDispatcher()


def get_task_dispatcher() -> TaskDispatcher:
    return task_dispatcher
