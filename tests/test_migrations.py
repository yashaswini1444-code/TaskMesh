from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

from app.core.config import get_settings


def test_initial_migration_upgrades_sqlite_database(
    tmp_path: Path, monkeypatch
) -> None:
    database_path = tmp_path / "migration.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("TASKMESH_DATABASE_URL", database_url)
    get_settings.cache_clear()

    config = Config("alembic.ini")
    command.upgrade(config, "head")

    inspector = inspect(create_engine(database_url))
    assert {
        "alembic_version",
        "tasks",
        "task_execution_attempts",
    } <= set(inspector.get_table_names())
    assert {column["name"] for column in inspector.get_columns("tasks")} >= {
        "id",
        "task_type",
        "payload",
        "priority",
        "status",
        "retry_count",
        "max_retries",
        "created_at",
        "started_at",
        "completed_at",
        "last_error",
    }
    task_checks = {
        constraint["name"] for constraint in inspector.get_check_constraints("tasks")
    }
    assert {
        "ck_tasks_retry_count_nonnegative",
        "ck_tasks_max_retries_nonnegative",
        "ck_tasks_retry_count_within_limit",
        "ck_tasks_taskpriority",
        "ck_tasks_taskstatus",
    } <= task_checks

    attempt_unique_constraints = inspector.get_unique_constraints(
        "task_execution_attempts"
    )
    assert any(
        constraint["column_names"] == ["task_id", "attempt_number"]
        for constraint in attempt_unique_constraints
    )

    get_settings.cache_clear()
