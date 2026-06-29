"""add order task order lookup index

Revision ID: 20260629_0004
Revises: 20260629_0003
Create Date: 2026-06-29
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260629_0004"
down_revision: str | None = "20260629_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


TASK_ORDER_LOOKUP_INDEX_NAME = "ix_order_tasks_order_id_created_at"


def upgrade() -> None:
    """Add the index used by dashboard order detail task history."""
    # The order detail page loads task rows by order_id and newest task first.
    # Postgres does not automatically index foreign keys, so without this index
    # a single clicked order can scan the full order_tasks table during large
    # load tests.
    with op.get_context().autocommit_block():
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY {TASK_ORDER_LOOKUP_INDEX_NAME}
            ON order_tasks (order_id, created_at DESC, updated_at DESC, id DESC)
            """
        )


def downgrade() -> None:
    """Drop the dashboard order detail task lookup index."""
    with op.get_context().autocommit_block():
        op.execute(
            f"DROP INDEX CONCURRENTLY IF EXISTS {TASK_ORDER_LOOKUP_INDEX_NAME}"
        )
