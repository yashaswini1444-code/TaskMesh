from fastapi.testclient import TestClient

from app.main import app


def test_dashboard_and_static_assets_are_served() -> None:
    with TestClient(app) as client:
        dashboard = client.get("/dashboard")
        css = client.get("/static/styles.css")
        javascript = client.get("/static/app.js")

    assert dashboard.status_code == 200
    assert "TaskMesh Operations" in dashboard.text
    assert "System overview" in dashboard.text
    assert "Priority queues" in dashboard.text
    assert "Recent tasks" in dashboard.text
    assert "Failures & dead letters" in dashboard.text
    assert css.status_code == 200
    assert "--cyan" in css.text
    assert javascript.status_code == 200
    assert 'fetch("/monitoring/summary")' in javascript.text
    assert "execution_attempts" in javascript.text
