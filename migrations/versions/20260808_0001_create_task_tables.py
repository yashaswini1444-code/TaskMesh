"""Create task persistence tables.

Revision ID: 20260808_0001
Revises:
Create Date: 2026-08-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260808_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_type", sa.String(length=100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "priority",
            sa.Enum(
                "HIGH",
                "MEDIUM",
                "LOW",
                name="taskpriority",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="MEDIUM",
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "QUEUED",
                "RUNNING",
                "COMPLETED",
                "FAILED",
                "DEAD_LETTER",
                name="taskstatus",
                native_enum=False,
                create_constraint=True,
            ),
            server_default="QUEUED",
            nullable=False,
        ),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_retries", sa.Integer(), server_default="3", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "max_retries >= 0", name=op.f("ck_tasks_max_retries_nonnegative")
        ),
        sa.CheckConstraint(
            "retry_count >= 0", name=op.f("ck_tasks_retry_count_nonnegative")
        ),
        sa.CheckConstraint(
            "retry_count <= max_retries",
            name=op.f("ck_tasks_retry_count_within_limit"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tasks")),
    )
    op.create_index(op.f("ix_tasks_priority"), "tasks", ["priority"])
    op.create_index(op.f("ix_tasks_status"), "tasks", ["status"])
    op.create_index(op.f("ix_tasks_task_type"), "tasks", ["task_type"])

    op.create_table(
        "task_execution_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_identifier", sa.String(length=255), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "attempt_number >= 1",
            name=op.f("ck_task_execution_attempts_attempt_number_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_task_execution_attempts_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_execution_attempts")),
        sa.UniqueConstraint(
            "task_id",
            "attempt_number",
            name=op.f("uq_task_execution_attempts_task_id"),
        ),
    )
    op.create_index(
        op.f("ix_task_execution_attempts_task_id"),
        "task_execution_attempts",
        ["task_id"],
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_task_execution_attempts_task_id"),
        table_name="task_execution_attempts",
    )
    op.drop_table("task_execution_attempts")
    op.drop_index(op.f("ix_tasks_task_type"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_status"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_priority"), table_name="tasks")
    op.drop_table("tasks")
