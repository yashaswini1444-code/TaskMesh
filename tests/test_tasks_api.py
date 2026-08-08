from collections.abc import Generator
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.services.dispatcher import (
    TaskDispatcher,
    TaskDispatchError,
    get_task_dispatcher,
)


@pytest.fixture
def dispatcher() -> Mock:
    return Mock(spec=TaskDispatcher)


@pytest.fixture
def client(dispatcher: Mock) -> Generator[TestClient, None, None]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    def override_get_db() -> Generator[Session, None, None]:
        with Session(engine, expire_on_commit=False) as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_task_dispatcher] = lambda: dispatcher
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.pop(get_db, None)
    app.dependency_overrides.pop(get_task_dispatcher, None)
    engine.dispose()


def task_request(
    task_type: str = "email.send", priority: str = "MEDIUM"
) -> dict[str, object]:
    return {
        "task_type": task_type,
        "payload": {"recipient_id": 42},
        "priority": priority,
    }


def test_create_task_starts_queued(client: TestClient, dispatcher: Mock) -> None:
    response = client.post("/tasks", json=task_request())

    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["task_type"] == "email.send"
    assert body["payload"] == {"recipient_id": 42}
    assert body["priority"] == "MEDIUM"
    assert body["status"] == "QUEUED"
    assert body["retry_count"] == 0
    assert body["max_retries"] == 3
    assert body["created_at"] is not None
    assert body["started_at"] is None
    assert body["completed_at"] is None
    assert body["last_error"] is None
    dispatcher.dispatch.assert_called_once()
    dispatched_id, dispatched_priority = dispatcher.dispatch.call_args.args
    assert str(dispatched_id) == body["id"]
    assert dispatched_priority.value == "MEDIUM"


def test_task_is_persisted_before_dispatch(
    client: TestClient, dispatcher: Mock
) -> None:
    def assert_task_is_readable(task_id: object, priority: object) -> None:
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200

    dispatcher.dispatch.side_effect = assert_task_is_readable

    response = client.post("/tasks", json=task_request())

    assert response.status_code == 201
    dispatcher.dispatch.assert_called_once()


def test_dispatch_failure_preserves_queued_task(
    client: TestClient, dispatcher: Mock
) -> None:
    dispatcher.dispatch.side_effect = TaskDispatchError("broker unavailable")

    response = client.post("/tasks", json=task_request())

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["message"] == "Task was persisted but could not be dispatched"
    persisted = client.get(f"/tasks/{detail['task_id']}")
    assert persisted.status_code == 200
    assert persisted.json()["status"] == "QUEUED"


@pytest.mark.parametrize("priority", ["HIGH", "MEDIUM", "LOW"])
def test_create_task_accepts_supported_priorities(
    client: TestClient, dispatcher: Mock, priority: str
) -> None:
    response = client.post("/tasks", json=task_request(priority=priority))

    assert response.status_code == 201
    assert response.json()["priority"] == priority
    dispatched_id, dispatched_priority = dispatcher.dispatch.call_args.args
    assert str(dispatched_id) == response.json()["id"]
    assert dispatched_priority.value == priority


def test_create_task_rejects_invalid_priority(client: TestClient) -> None:
    response = client.post("/tasks", json=task_request(priority="URGENT"))

    assert response.status_code == 422


def test_create_task_requires_object_payload(client: TestClient) -> None:
    request = task_request()
    request["payload"] = ["not", "an", "object"]

    response = client.post("/tasks", json=request)

    assert response.status_code == 422


def test_create_task_rejects_server_controlled_fields(client: TestClient) -> None:
    request = task_request()
    request["status"] = "COMPLETED"

    response = client.post("/tasks", json=request)

    assert response.status_code == 422


@pytest.mark.parametrize("task_type", ["", "   "])
def test_create_task_rejects_empty_task_type(
    client: TestClient, task_type: str
) -> None:
    response = client.post("/tasks", json=task_request(task_type=task_type))

    assert response.status_code == 422


def test_get_task_returns_persisted_task(client: TestClient) -> None:
    created = client.post("/tasks", json=task_request()).json()

    response = client.get(f"/tasks/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_get_task_returns_404_for_unknown_uuid(client: TestClient) -> None:
    response = client.get(f"/tasks/{uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Task not found"}


def test_list_tasks_returns_persisted_tasks(client: TestClient) -> None:
    first = client.post("/tasks", json=task_request("email.send")).json()
    second = client.post("/tasks", json=task_request("report.generate")).json()

    response = client.get("/tasks")

    assert response.status_code == 200
    assert {task["id"] for task in response.json()} == {first["id"], second["id"]}


def test_list_tasks_pagination(client: TestClient) -> None:
    created_ids = {
        client.post("/tasks", json=task_request(f"task.{number}")).json()["id"]
        for number in range(3)
    }

    first_page = client.get("/tasks", params={"offset": 0, "limit": 2})
    second_page = client.get("/tasks", params={"offset": 2, "limit": 2})

    assert first_page.status_code == 200
    assert second_page.status_code == 200
    assert len(first_page.json()) == 2
    assert len(second_page.json()) == 1
    listed_ids = {task["id"] for task in first_page.json() + second_page.json()}
    assert listed_ids == created_ids


def test_list_tasks_filters_by_priority_status_and_type(client: TestClient) -> None:
    client.post("/tasks", json=task_request("email.send", "HIGH"))
    client.post("/tasks", json=task_request("email.send", "LOW"))
    client.post("/tasks", json=task_request("report.generate", "HIGH"))

    response = client.get(
        "/tasks",
        params={"priority": "HIGH", "status": "QUEUED", "task_type": "email.send"},
    )

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["priority"] == "HIGH"
    assert response.json()[0]["status"] == "QUEUED"
    assert response.json()[0]["task_type"] == "email.send"
