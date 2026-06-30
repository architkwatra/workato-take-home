"""add active order created_at index

Revision ID: 20260630_0005
Revises: 20260629_0004
Create Date: 2026-06-30
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260630_0005"
down_revision: str | None = "20260629_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the lookup index used by dashboard end-to-end SLA checks."""
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_orders_active_created_at
        ON orders (created_at)
        WHERE state NOT IN (
            'delivered'::order_state,
            'cancelled'::order_state,
            'failed'::order_state
        )
        """
    )


def downgrade() -> None:
    """Drop the dashboard end-to-end SLA lookup index."""
    op.execute("DROP INDEX IF EXISTS ix_orders_active_created_at")
