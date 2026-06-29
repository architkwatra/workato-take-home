from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg.rows import dict_row

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_ORDER_CREATED
from common.state_machine import ORDER_STATE_CONFIRMED, ORDER_STATE_PLACED
from common.task_types import TASK_STATUS_PENDING, TASK_TYPE_ADVANCE_STATE


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
