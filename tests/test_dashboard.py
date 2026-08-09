from fastapi.testclient import TestClient

from app.main import app


def test_dashboard_and_static_assets_are_served() -> None:
    with TestClient(app) as client:
        dashboard = client.get("/dashboard")
        css = client.get("/static/styles.css")
        javascript = client.get("/static/app.js")

    assert dashboard.status_code == 200
    assert "TaskMesh — Distributed Task Operations" in dashboard.text
    assert "Task Operations" in dashboard.text
    assert "Task Lifecycle Overview" in dashboard.text
    assert "Priority Queue Traffic" in dashboard.text
    assert "Worker Fleet" in dashboard.text
    assert "Recent Executions" in dashboard.text
    assert "Dead Letter Queue" in dashboard.text
    assert "System Health" in dashboard.text
    assert "Recover Stale Tasks" in dashboard.text
    assert 'id="redispatch-dialog"' in dashboard.text
    assert 'id="recovery-dialog"' in dashboard.text
    assert css.status_code == 200
    assert "--obsidian" in css.text
    assert "--violet" in css.text
    assert "prefers-reduced-motion" in css.text
    assert javascript.status_code == 200
    assert 'fetch("/monitoring/summary")' in javascript.text
    assert 'fetch(`/tasks/${encodeURIComponent(id)}`)' in javascript.text
    assert 'method: "POST"' in javascript.text
    assert "execution_attempts" in javascript.text
    assert "task.attempt_count" in javascript.text
    assert '/redispatch`' in javascript.text
    assert '"/recovery/stale-running"' in javascript.text
    assert "data.tasks.stale_running" in javascript.text
