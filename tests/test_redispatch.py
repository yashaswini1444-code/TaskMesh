from collections.abc import Generator
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models import Task, TaskPriority, TaskStatus
from app.services.dispatcher import TaskDispatcher, TaskDispatchError, get_task_dispatcher


@pytest.fixture
def redispatch_client() -> Generator[tuple[TestClient, Mock, Session], None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    dispatcher = Mock(spec=TaskDispatcher)

    def override_get_db() -> Generator[Session, None, None]:
        yield session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_task_dispatcher] = lambda: dispatcher
    with TestClient(app) as client:
        yield client, dispatcher, session
    app.dependency_overrides.pop(get_db, None)
    app.dependency_overrides.pop(get_task_dispatcher, None)
    session.close()
    engine.dispose()


def make_task(session: Session, status: TaskStatus, priority: TaskPriority = TaskPriority.HIGH) -> Task:
    task = Task(
        task_type="echo",
        payload={"message": "hi"},
        priority=priority,
        status=status,
    )
    session.add(task)
    session.commit()
    return task


def test_redispatch_republishes_queued_task_without_changing_state(
    redispatch_client: tuple[TestClient, Mock, Session],
) -> None:
    client, dispatcher, session = redispatch_client
    task = make_task(session, TaskStatus.QUEUED, TaskPriority.HIGH)

    response = client.post(f"/tasks/{task.id}/redispatch")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(task.id)
    assert body["status"] == "QUEUED"
    dispatcher.dispatch.assert_called_once_with(task.id, TaskPriority.HIGH)


@pytest.mark.parametrize(
    "status",
    [TaskStatus.RUNNING, TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.DEAD_LETTER],
)
def test_redispatch_rejects_non_queued_states(
    redispatch_client: tuple[TestClient, Mock, Session],
    status: TaskStatus,
) -> None:
    client, dispatcher, session = redispatch_client
    task = make_task(session, status)

    response = client.post(f"/tasks/{task.id}/redispatch")

    assert response.status_code == 409
    assert response.json() == {"detail": "Only QUEUED tasks can be redispatched"}
    dispatcher.dispatch.assert_not_called()


def test_redispatch_returns_404_for_unknown_task(
    redispatch_client: tuple[TestClient, Mock, Session],
) -> None:
    client, _, _ = redispatch_client

    response = client.post("/tasks/00000000-0000-0000-0000-000000000000/redispatch")

    assert response.status_code == 404


def test_redispatch_failure_returns_sanitized_503(
    redispatch_client: tuple[TestClient, Mock, Session],
) -> None:
    client, dispatcher, session = redispatch_client
    task = make_task(session, TaskStatus.QUEUED)
    dispatcher.dispatch.side_effect = TaskDispatchError("broker unavailable: password=hunter2")

    response = client.post(f"/tasks/{task.id}/redispatch")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["message"] == "Task remains persisted but redispatch failed"
    assert detail["task_id"] == str(task.id)
    assert "password" not in response.text

    persisted = client.get(f"/tasks/{task.id}")
    assert persisted.json()["status"] == "QUEUED"


def test_double_redispatch_is_safe_and_publishes_twice(
    redispatch_client: tuple[TestClient, Mock, Session],
) -> None:
    """Redispatch is idempotent at the execution level (the worker-side
    atomic claim de-duplicates), not at the message level — calling it twice
    is expected to publish two broker messages, both harmless."""

    client, dispatcher, session = redispatch_client
    task = make_task(session, TaskStatus.QUEUED)

    first = client.post(f"/tasks/{task.id}/redispatch")
    second = client.post(f"/tasks/{task.id}/redispatch")

    assert first.status_code == 200
    assert second.status_code == 200
    assert dispatcher.dispatch.call_count == 2
