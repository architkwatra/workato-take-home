from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_ORDER_CANCELLED, EVENT_TYPE_ORDER_CREATED
from common.state_machine import (
    ORDER_STATE_CANCELLED,
    ORDER_STATE_CONFIRMED,
    ORDER_STATE_PLACED,
    is_terminal_order_state,
)
from common.task_types import (
    TASK_STATUS_CANCELLED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_TYPE_ADVANCE_STATE,
)


ORDER_CANCELLED_REASON = "operator_cancelled"
ORDER_CANCELLED_TASK_ERROR = "order cancelled by operator"


class IdempotencyConflictError(RuntimeError):
    """Raised when a reused idempotency key has a different request body."""

    def __init__(
        self,
        *,
        idempotency_key: str,
        existing_order_id: str,
        existing_restaurant_ref: str,
        existing_customer_ref: str | None,
        requested_restaurant_ref: str,
        requested_customer_ref: str | None,
    ) -> None:
        super().__init__("idempotency key reused with a different order request")
        self.idempotency_key = idempotency_key
        self.existing_order_id = existing_order_id
        self.existing_restaurant_ref = existing_restaurant_ref
        self.existing_customer_ref = existing_customer_ref
        self.requested_restaurant_ref = requested_restaurant_ref
        self.requested_customer_ref = requested_customer_ref


def create_or_get_order(
    *,
    idempotency_key: str,
    restaurant_ref: str,
    customer_ref: str | None,
) -> tuple[dict[str, Any], bool]:
    """Create a placed order or return the existing row for the idempotency key."""
    order_id = uuid4()
    created_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    insert into orders (
                        id,
                        idempotency_key,
                        state,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        updated_at
                    )
                    values (%s, %s, %s::order_state, %s, %s, %s, %s)
                    on conflict (idempotency_key) do nothing
                    returning
                        id::text,
                        idempotency_key,
                        state::text,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        updated_at
                    """,
                    (
                        order_id,
                        idempotency_key,
                        ORDER_STATE_PLACED,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        created_at,
                    ),
                )
                inserted_order = cur.fetchone()
                if inserted_order is not None:
                    # The creation event is inserted in the same transaction as
                    # the order so the audit timeline cannot miss accepted
                    # orders. This is a creation milestone, not a state
                    # transition, so from_state/to_state stay null. Duplicate
                    # idempotency requests skip this path, so they do not create
                    # duplicate events.
                    cur.execute(
                        """
                        insert into order_events (
                            id,
                            order_id,
                            event_type,
                            occurred_at
                        )
                        values (%s, %s, %s::event_type, %s)
                        """,
                        (
                            uuid4(),
                            order_id,
                            EVENT_TYPE_ORDER_CREATED,
                            created_at,
                        ),
                    )
                    # The first durable task is created with the order so a
                    # crash after accepting an order still leaves work for a
                    # worker to claim. Duplicate idempotency requests skip this
                    # path, and the active dedupe_key guards against accidental
                    # duplicate initial tasks for the same order/target.
                    cur.execute(
                        """
                        insert into order_tasks (
                            id,
                            order_id,
                            task_type,
                            target_state,
                            status,
                            next_run_at,
                            dedupe_key,
                            created_at,
                            updated_at
                        )
                        values (
                            %s,
                            %s,
                            %s::task_type,
                            %s::order_state,
                            %s::task_status,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                        """,
                        (
                            uuid4(),
                            order_id,
                            TASK_TYPE_ADVANCE_STATE,
                            ORDER_STATE_CONFIRMED,
                            TASK_STATUS_PENDING,
                            created_at,
                            f"{order_id}:{TASK_TYPE_ADVANCE_STATE}:{ORDER_STATE_CONFIRMED}",
                            created_at,
                            created_at,
                        ),
                    )
                    return dict(inserted_order), True

                cur.execute(
                    """
                    select
                        id::text,
                        idempotency_key,
                        state::text,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        updated_at
                    from orders
                    where idempotency_key = %s
                    """,
                    (idempotency_key,),
                )
                existing_order = cur.fetchone()
                if existing_order is None:
                    raise RuntimeError("idempotent order lookup failed after conflict")

                # Idempotency keys are only valid for identical retries. If the
                # same key arrives with a different body, returning the existing
                # order would make the client think a different order was
                # accepted. Surface that as a conflict instead.
                if (
                    existing_order["restaurant_ref"] != restaurant_ref
                    or existing_order["customer_ref"] != customer_ref
                ):
                    raise IdempotencyConflictError(
                        idempotency_key=idempotency_key,
                        existing_order_id=existing_order["id"],
                        existing_restaurant_ref=existing_order["restaurant_ref"],
                        existing_customer_ref=existing_order["customer_ref"],
                        requested_restaurant_ref=restaurant_ref,
                        requested_customer_ref=customer_ref,
                    )

                return dict(existing_order), False


def cancel_order(*, order_id: str) -> dict[str, Any] | None:
    """Move a non-terminal order to cancelled and invalidate open work."""
    cancelled_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # Cancellation is an operator override, so lock the order row
                # before deciding whether it is still cancellable. This
                # serializes the decision with worker finalization updates that
                # may be moving the same order through the lifecycle.
                cur.execute(
                    """
                    select
                        id::text,
                        idempotency_key,
                        state::text,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        updated_at
                    from orders
                    where id = %s::uuid
                    for update
                    """,
                    (order_id,),
                )
                order = cur.fetchone()
                if order is None:
                    return None

                # Treat terminal orders as an idempotent no-op. A delivered,
                # failed, or already-cancelled order should not be rewritten by
                # a late dashboard click.
                from_state = order["state"]
                if is_terminal_order_state(from_state):
                    return dict(order)

                cur.execute(
                    """
                    update orders
                    set
                        state = %s::order_state,
                        version = version + 1,
                        terminal_reason = %s,
                        cancelled_at = coalesce(cancelled_at, %s),
                        updated_at = %s
                    where id = %s::uuid
                    returning
                        id::text,
                        idempotency_key,
                        state::text,
                        customer_ref,
                        restaurant_ref,
                        created_at,
                        updated_at
                    """,
                    (
                        ORDER_STATE_CANCELLED,
                        ORDER_CANCELLED_REASON,
                        cancelled_at,
                        cancelled_at,
                        order_id,
                    ),
                )
                cancelled_order = dict(cur.fetchone())

                # The order is now terminal, so pending/running queue rows are
                # no longer valid work. A worker that is already outside the DB
                # doing a downstream call will re-check task ownership/order
                # state before finalizing; clearing the lease here prevents
                # future workers from continuing that order.
                cur.execute(
                    """
                    update order_tasks
                    set
                        status = %s::task_status,
                        locked_by = null,
                        locked_until = null,
                        last_error = %s,
                        updated_at = %s
                    where
                        order_id = %s::uuid
                        and status = any(%s::task_status[])
                    returning id::text
                    """,
                    (
                        TASK_STATUS_CANCELLED,
                        ORDER_CANCELLED_TASK_ERROR,
                        cancelled_at,
                        order_id,
                        [TASK_STATUS_PENDING, TASK_STATUS_RUNNING],
                    ),
                )
                cancelled_task_ids = [row["id"] for row in cur.fetchall()]

                cur.execute(
                    """
                    insert into order_events (
                        id,
                        order_id,
                        event_type,
                        from_state,
                        to_state,
                        occurred_at,
                        metadata
                    )
                    values (
                        %s::uuid,
                        %s::uuid,
                        %s::event_type,
                        %s::order_state,
                        %s::order_state,
                        %s,
                        %s
                    )
                    """,
                    (
                        uuid4(),
                        order_id,
                        EVENT_TYPE_ORDER_CANCELLED,
                        from_state,
                        ORDER_STATE_CANCELLED,
                        cancelled_at,
                        Jsonb(
                            {
                                "source": "dashboard_cancel_order",
                                "cancelled_task_count": len(cancelled_task_ids),
                                "cancelled_task_ids": cancelled_task_ids,
                            }
                        ),
                    ),
                )

                return cancelled_order
