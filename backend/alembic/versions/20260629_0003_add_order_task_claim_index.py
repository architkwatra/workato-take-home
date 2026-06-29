"""add order task claim index

Revision ID: 20260629_0003
Revises: 20260625_0002
Create Date: 2026-06-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "20260629_0003"
down_revision: str | None = "20260625_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


CLAIM_INDEX_NAME = "ix_order_tasks_claim_order"


def upgrade() -> None:
    """Add the queue-order index used by worker task claims."""
    # Workers claim active queue rows in updated_at/next_run_at/created_at/id
    # order. The older status/next_run_at index can find due pending rows, but
    # it cannot satisfy that ordering, so large due backlogs force a table scan
    # and external sort before every claim. This partial index keeps only
    # claimable-status rows in worker claim order.
    with op.get_context().autocommit_block():
        op.create_index(
            CLAIM_INDEX_NAME,
            "order_tasks",
            ["updated_at", "next_run_at", "created_at", "id"],
            postgresql_concurrently=True,
            postgresql_where=sa.text(
                "status in ('pending'::task_status, 'running'::task_status)"
            ),
        )


def downgrade() -> None:
    """Drop the worker claim-order index."""
    with op.get_context().autocommit_block():
        op.drop_index(
            CLAIM_INDEX_NAME,
            table_name="order_tasks",
            postgresql_concurrently=True,
        )
