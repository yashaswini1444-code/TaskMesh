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
from app.models import Task, TaskExecutionAttempt, TaskPriority, TaskStatus
from app.services.dispatcher import TaskDispatcher, TaskDispatchError, get_task_dispatcher


@pytest.fixture
def dead_letter_client() -> Generator[tuple[TestClient, Mock, Session], None, None]:
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


def make_dead_letter(session: Session, priority: TaskPriority = TaskPriority.HIGH) -> Task:
    task = Task(
        task_type="echo",
        payload={"message": "retry me"},
        priority=priority,
        status=TaskStatus.DEAD_LETTER,
        retry_count=3,
        max_retries=3,
        last_error="temporary failure",
    )
    task.execution_attempts.extend(
        [
            TaskExecutionAttempt(
                attempt_number=number,
                worker_identifier="worker-1",
                error="temporary failure",
            )
            for number in range(1, 5)
        ]
    )
    session.add(task)
    session.commit()
    return task


def test_dead_letter_filter_and_task_history(
    dead_letter_client: tuple[TestClient, Mock, Session],
) -> None:
    client, _, session = dead_letter_client
    task = make_dead_letter(session)

    listed = client.get("/tasks", params={"status": "DEAD_LETTER"})
    detail = client.get(f"/tasks/{task.id}")

    assert listed.status_code == 200
    assert [item["id"] for item in listed.json()] == [str(task.id)]
    assert detail.status_code == 200
    assert len(detail.json()["execution_attempts"]) == 4
    assert detail.json()["execution_attempts"][0]["attempt_number"] == 1


def test_requeue_resets_active_state_preserves_history_and_dispatches_priority(
    dead_letter_client: tuple[TestClient, Mock, Session],
) -> None:
    client, dispatcher, session = dead_letter_client
    task = make_dead_letter(session, TaskPriority.HIGH)

    response = client.post(f"/tasks/{task.id}/requeue")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "QUEUED"
    assert body["retry_count"] == 0
    assert body["last_error"] is None
    assert len(body["execution_attempts"]) == 4
    dispatcher.dispatch.assert_called_once_with(task.id, TaskPriority.HIGH)


def test_requeue_rejects_non_dead_letter_and_double_requeue(
    dead_letter_client: tuple[TestClient, Mock, Session],
) -> None:
    client, dispatcher, session = dead_letter_client
    queued = Task(task_type="echo", payload={"message": "hello"})
    session.add(queued)
    session.commit()

    assert client.post(f"/tasks/{queued.id}/requeue").status_code == 409
    task = make_dead_letter(session)
    assert client.post(f"/tasks/{task.id}/requeue").status_code == 200
    assert client.post(f"/tasks/{task.id}/requeue").status_code == 409
    assert dispatcher.dispatch.call_count == 1


def test_requeue_dispatch_failure_leaves_truthful_queued_state(
    dead_letter_client: tuple[TestClient, Mock, Session],
) -> None:
    client, dispatcher, session = dead_letter_client
    task = make_dead_letter(session)
    dispatcher.dispatch.side_effect = TaskDispatchError("broker unavailable")

    response = client.post(f"/tasks/{task.id}/requeue")

    assert response.status_code == 503
    assert response.json()["detail"]["task_id"] == str(task.id)
    persisted = client.get(f"/tasks/{task.id}").json()
    assert persisted["status"] == "QUEUED"
    assert persisted["retry_count"] == 0
