"""create orders table

Revision ID: 20260625_0001
Revises:
Create Date: 2026-06-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "20260625_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


ORDER_STATES = (
    "placed",
    "confirmed",
    "preparing",
    "ready",
    "out_for_delivery",
    "delivered",
    "cancelled",
    "failed",
)


def upgrade() -> None:
    """Create the first real schema object used by order intake."""
    order_state = postgresql.ENUM(*ORDER_STATES, name="order_state")
    order_state.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column(
            "state",
            postgresql.ENUM(name="order_state", create_type=False),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("customer_ref", sa.Text(), nullable=True),
        sa.Column("restaurant_ref", sa.Text(), nullable=False),
        sa.Column("courier_ref", sa.Text(), nullable=True),
        sa.Column("terminal_reason", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_orders_idempotency_key"),
    )
    op.create_index("ix_orders_state_updated_at", "orders", ["state", "updated_at"])


def downgrade() -> None:
    """Drop the initial order intake table and enum."""
    op.drop_index("ix_orders_state_updated_at", table_name="orders")
    op.drop_table("orders")

    order_state = postgresql.ENUM(*ORDER_STATES, name="order_state")
    order_state.drop(op.get_bind(), checkfirst=True)
