"""add payment check lifecycle state

Revision ID: 20260630_0006
Revises: 20260630_0005
Create Date: 2026-06-30
"""

from collections.abc import Sequence

from alembic import op


revision: str = "20260630_0006"
down_revision: str | None = "20260630_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the payment authorization lifecycle state."""
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TYPE order_state ADD VALUE IF NOT EXISTS "
            "'payment_check' AFTER 'placed'"
        )


def downgrade() -> None:
    """Keep enum values in place.

    PostgreSQL cannot remove enum values without rebuilding dependent columns.
    Leaving the values is safer than rewriting hot operational tables during a
    local rollback.
    """
