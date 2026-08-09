"""Add task lease column and recovery index.

Revision ID: 20260809_0002
Revises: 20260808_0001
Create Date: 2026-08-09
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260809_0002"
down_revision: str | None = "20260808_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_tasks_status_lease_expires_at",
        "tasks",
        ["status", "lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_tasks_status_lease_expires_at", table_name="tasks")
    op.drop_column("tasks", "lease_expires_at")
