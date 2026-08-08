from collections.abc import Callable, Mapping
from typing import Any, Final


class TaskExecutionError(RuntimeError):
    """A safe domain error produced while executing task application logic."""


TaskHandler = Callable[[Mapping[str, Any]], None]


def handle_echo(payload: Mapping[str, Any]) -> None:
    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        raise TaskExecutionError("echo payload requires a non-empty string 'message'")


TASK_HANDLERS: Final[dict[str, TaskHandler]] = {
    "echo": handle_echo,
}


def execute_job(task_type: str, payload: Mapping[str, Any]) -> None:
    handler = TASK_HANDLERS.get(task_type)
    if handler is None:
        raise TaskExecutionError(f"Unsupported task type: {task_type}")
    handler(payload)
