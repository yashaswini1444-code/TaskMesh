import logging

import app.core.logging as app_logging


def test_configure_logging_attaches_handler_and_does_not_propagate_to_root() -> None:
    app_logging._configured = False
    taskmesh_logger = logging.getLogger("taskmesh")
    taskmesh_logger.handlers.clear()

    app_logging.configure_logging()

    assert taskmesh_logger.level == logging.INFO
    assert len(taskmesh_logger.handlers) == 1
    assert taskmesh_logger.propagate is False


def test_configure_logging_is_idempotent() -> None:
    app_logging._configured = False
    taskmesh_logger = logging.getLogger("taskmesh")
    taskmesh_logger.handlers.clear()

    app_logging.configure_logging()
    app_logging.configure_logging()
    app_logging.configure_logging()

    assert len(taskmesh_logger.handlers) == 1


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_lifecycle_logger_emits_expected_fields() -> None:
    """Attaches directly to the "taskmesh.lifecycle" logger rather than
    using pytest's caplog: caplog's capture handler lives on the root
    logger, and configure_logging() deliberately sets propagate=False on
    the "taskmesh" namespace (to avoid duplicate lines when a host process
    like a Celery worker also configures the root logger), so root-level
    capture would never see these records regardless of what actually
    happened."""

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session, sessionmaker
    from sqlalchemy.pool import StaticPool

    from app.db.base import Base
    from app.models import Task
    from app.services.task_lifecycle import process_task

    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    factory: sessionmaker[Session] = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        task = Task(task_type="echo", payload={"message": "log-check"})
        session.add(task)
        session.commit()
        task_id = task.id

    lifecycle_logger = logging.getLogger("taskmesh.lifecycle")
    handler = _ListHandler()
    previous_level = lifecycle_logger.level
    lifecycle_logger.addHandler(handler)
    lifecycle_logger.setLevel(logging.INFO)
    try:
        process_task(task_id, worker_identifier="log-test-worker", session_factory=factory)
    finally:
        lifecycle_logger.removeHandler(handler)
        lifecycle_logger.setLevel(previous_level)
        engine.dispose()

    messages = [record.getMessage() for record in handler.records]
    assert any("task claimed" in message and str(task_id) in message for message in messages)
    assert any("task completed" in message and str(task_id) in message for message in messages)
    assert not any("log-check" in message for message in messages)  # payload never logged
