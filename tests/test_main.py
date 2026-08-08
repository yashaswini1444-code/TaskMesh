from fastapi.testclient import TestClient

from app.main import app


def test_read_root() -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "name": "TaskMesh",
        "message": "Asynchronous job processing system",
    }


def test_read_health() -> None:
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
