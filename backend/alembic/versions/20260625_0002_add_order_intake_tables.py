"""add order intake support tables

Revision ID: 20260625_0002
Revises: 20260625_0001
Create Date: 2026-06-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "20260625_0002"
down_revision: str | None = "20260625_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Task types describe the unit of work a worker should execute. They are
# separate from order states because some work is a delayed check, not an
# immediate lifecycle transition.
TASK_TYPES = (
    "advance_state",  # Move the order to target_state after required work succeeds.
    "check_ready",  # Poll restaurant prep without blocking a worker between checks.
    "check_pickup",  # Poll courier pickup progress while order stays out_for_delivery.
    "check_delivery",  # Poll courier delivery progress until the order can be delivered.
)

# Task statuses track the durable queue row, not the customer-visible order
# state. Workers use these values to find runnable work, recover expired leases,
# and avoid retrying work that is already terminal.
TASK_STATUSES = (
    "pending",  # Runnable later; worker may claim it when next_run_at is due.
    "running",  # Claimed by a worker lease; reclaimable after locked_until expires.
    "completed",  # Finished successfully or safely no-op'd after a race.
    "failed",  # Exhausted real error retries and should not be claimed again.
    "cancelled",  # Invalidated because the order moved to a terminal state.
)
EVENT_TYPES = (
    "order_created",
    "state_transition",
    "courier_picked_up",
    "retry_scheduled",
    "task_cancelled",
    "order_cancelled",
    "order_failed",
)


def upgrade() -> None:
    """Create durable task, event, and worker heartbeat tables."""
    task_type = postgresql.ENUM(*TASK_TYPES, name="task_type")
    task_status = postgresql.ENUM(*TASK_STATUSES, name="task_status")
    event_type = postgresql.ENUM(*EVENT_TYPES, name="event_type")

    task_type.create(op.get_bind(), checkfirst=True)
    task_status.create(op.get_bind(), checkfirst=True)
    event_type.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "order_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "order_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orders.id"),
            nullable=False,
        ),
        sa.Column(
            "task_type",
            postgresql.ENUM(name="task_type", create_type=False),
            nullable=False,
        ),
        sa.Column(
            "target_state",
            postgresql.ENUM(name="order_state", create_type=False),
            nullable=True,
        ),
        sa.Column(
            "status",
            postgresql.ENUM(name="task_status", create_type=False),
            nullable=False,
            comment=(
                "Durable queue status: pending is claimable, running is leased, "
                "completed/failed/cancelled are terminal for the task row."
            ),
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dedupe_key", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
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
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "attempts >= 0",
            name="ck_order_tasks_attempts_nonnegative",
        ),
        sa.CheckConstraint(
            "max_attempts > 0",
            name="ck_order_tasks_max_attempts_positive",
        ),
    )
    op.create_index(
        "ix_order_tasks_status_next_run_at",
        "order_tasks",
        ["status", "next_run_at"],
    )
    op.create_index("ix_order_tasks_locked_until", "order_tasks", ["locked_until"])
    op.create_index(
        "uq_order_tasks_active_dedupe_key",
        "order_tasks",
        ["dedupe_key"],
        unique=True,
        postgresql_where=sa.text(
            "dedupe_key is not null and status in ('pending', 'running')"
        ),
    )

    op.create_table(
        "order_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "order_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orders.id"),
            nullable=False,
        ),
        sa.Column(
            "event_type",
            postgresql.ENUM(name="event_type", create_type=False),
            nullable=False,
        ),
        sa.Column(
            "from_state",
            postgresql.ENUM(name="order_state", create_type=False),
            nullable=True,
        ),
        sa.Column(
            "to_state",
            postgresql.ENUM(name="order_state", create_type=False),
            nullable=True,
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("order_tasks.id"),
            nullable=True,
        ),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index(
        "ix_order_events_order_id_occurred_at",
        "order_events",
        ["order_id", "occurred_at"],
    )
    op.create_index(
        "ix_order_events_event_type_occurred_at",
        "order_events",
        ["event_type", "occurred_at"],
    )

    op.create_table(
        "workers",
        sa.Column("worker_id", sa.Text(), primary_key=True),
        sa.Column("hostname", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index("ix_workers_last_seen_at", "workers", ["last_seen_at"])


def downgrade() -> None:
    """Drop order intake support tables and their enums."""
    op.drop_index("ix_workers_last_seen_at", table_name="workers")
    op.drop_table("workers")

    op.drop_index("ix_order_events_event_type_occurred_at", table_name="order_events")
    op.drop_index("ix_order_events_order_id_occurred_at", table_name="order_events")
    op.drop_table("order_events")

    op.drop_index("uq_order_tasks_active_dedupe_key", table_name="order_tasks")
    op.drop_index("ix_order_tasks_locked_until", table_name="order_tasks")
    op.drop_index("ix_order_tasks_status_next_run_at", table_name="order_tasks")
    op.drop_table("order_tasks")

    event_type = postgresql.ENUM(*EVENT_TYPES, name="event_type")
    task_status = postgresql.ENUM(*TASK_STATUSES, name="task_status")
    task_type = postgresql.ENUM(*TASK_TYPES, name="task_type")

    event_type.drop(op.get_bind(), checkfirst=True)
    task_status.drop(op.get_bind(), checkfirst=True)
    task_type.drop(op.get_bind(), checkfirst=True)
