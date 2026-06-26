from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg.rows import dict_row

from common.db import open_db_connection
from common.state_machine import ORDER_STATE_PLACED


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

                return dict(existing_order), False
