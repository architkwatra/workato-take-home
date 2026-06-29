import os
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from common.db import open_db_connection
from common.state_machine import ORDER_STATE_PLACED, ORDER_STATES
from common.task_types import (
    ORDER_TASK_STATUSES,
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
)


ACTIVE_WORKER_THRESHOLD_SECONDS = 30
DEFAULT_CONFIGURED_WORKER_COUNT = 3
# The dashboard polls frequently, so row lists stay intentionally bounded while
# aggregate counts continue to reflect the whole database.
RECENT_ORDER_LIMIT = 12
RECENT_EVENT_LIMIT = 20
PROBLEM_TASK_LIMIT = 20
PLACED_ORDER_LIMIT = 5
ORDER_DETAIL_EVENT_LIMIT = 50
ORDER_DETAIL_TASK_LIMIT = 50
# Keep throughput derived from a short rolling window. This is intentionally
# read-model-only for the local dashboard; durable metrics storage can come
# later if we need historical charts.
THROUGHPUT_WINDOW_SECONDS = 30


def _configured_worker_count() -> int:
    """Return the configured worker count displayed by the dashboard.

    The API container cannot discover Docker Compose's worker replica count at
    runtime, so local compose sets this explicitly next to the worker replica
    setting. Invalid values fall back to the local-demo default.
    """
    raw_value = os.getenv(
        "DASHBOARD_CONFIGURED_WORKER_COUNT",
        str(DEFAULT_CONFIGURED_WORKER_COUNT),
    )
    try:
        configured_count = int(raw_value)
    except ValueError:
        return DEFAULT_CONFIGURED_WORKER_COUNT

    return configured_count if configured_count > 0 else DEFAULT_CONFIGURED_WORKER_COUNT


def _count_map(
    rows: list[dict[str, Any]],
    expected_keys: tuple[str, ...],
) -> dict[str, int]:
    """Return counts for every known enum value, including zero-count states."""
    counts = {key: 0 for key in expected_keys}
    for row in rows:
        counts[row["key"]] = row["count"]
    return counts


def get_dashboard_overview() -> dict[str, Any]:
    """Return a denormalized snapshot for the browser dashboard.

    The dashboard is intentionally backed by one purpose-built API response
    instead of making the browser join orders, tasks, events, and workers. That
    keeps UI polling cheap to reason about and keeps DB-specific enum/cast logic
    contained in the API service.
    """
    generated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select state::text as key, count(*)::int as count
                from orders
                group by state
                """
            )
            order_counts = _count_map(list(cur.fetchall()), ORDER_STATES)

            cur.execute(
                """
                select status::text as key, count(*)::int as count
                from order_tasks
                group by status
                """
            )
            task_counts = _count_map(list(cur.fetchall()), ORDER_TASK_STATUSES)

            cur.execute(
                """
                select
                    count(*) filter (
                        where
                            status = %s::task_status
                            and next_run_at <= now()
                    )::int as due_pending,
                    count(*) filter (
                        where
                            status = %s::task_status
                            and locked_until < now()
                    )::int as expired_running,
                    count(*) filter (
                        where
                            status = %s::task_status
                            and updated_at >= now() - (%s * interval '1 second')
                    )::int as completed_recent
                from order_tasks
                """,
                (
                    TASK_STATUS_PENDING,
                    TASK_STATUS_RUNNING,
                    TASK_STATUS_COMPLETED,
                    THROUGHPUT_WINDOW_SECONDS,
                ),
            )
            task_health = dict(cur.fetchone())

            workers = _load_worker_overview(cur)
            problem_tasks = _load_problem_tasks(cur)
            placed_orders = _load_placed_orders(cur)
            recent_orders = _load_recent_orders(cur)
            recent_events = _load_recent_events(cur)

    return {
        "generated_at": generated_at,
        "orders": {
            "total": sum(order_counts.values()),
            "by_state": order_counts,
        },
        "tasks": {
            "total": sum(task_counts.values()),
            "by_status": task_counts,
            "due_pending": task_health["due_pending"],
            "expired_running": task_health["expired_running"],
        },
        "throughput": {
            "window_seconds": THROUGHPUT_WINDOW_SECONDS,
            "tasks_completed": task_health["completed_recent"],
            "tasks_completed_per_second": round(
                task_health["completed_recent"] / THROUGHPUT_WINDOW_SECONDS,
                2,
            ),
        },
        "workers": workers,
        "problem_tasks": problem_tasks,
        "placed_orders": placed_orders,
        "recent_orders": recent_orders,
        "recent_events": recent_events,
    }


def get_dashboard_order_detail(*, order_id: str) -> dict[str, Any] | None:
    """Return one order's current state and audit trail for the detail view."""
    with open_db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select
                    id::text as order_id,
                    idempotency_key,
                    state::text,
                    customer_ref,
                    restaurant_ref,
                    courier_ref,
                    version,
                    created_at,
                    updated_at
                from orders
                where id = %s::uuid
                """,
                (order_id,),
            )
            order = cur.fetchone()
            if order is None:
                return None

            # Keep task history bounded but ordered newest-first. The detail
            # view is for operational inspection, not a full audit export.
            cur.execute(
                """
                select
                    id::text as task_id,
                    task_type::text,
                    target_state::text,
                    status::text,
                    attempts,
                    max_attempts,
                    next_run_at,
                    deadline_at,
                    locked_by,
                    locked_until,
                    last_error,
                    created_at,
                    updated_at,
                    completed_at
                from order_tasks
                where order_id = %s::uuid
                order by created_at desc, updated_at desc, id desc
                limit %s
                """,
                (order_id, ORDER_DETAIL_TASK_LIMIT),
            )
            tasks = [dict(row) for row in cur.fetchall()]

            # Events are returned chronological so the UI can render the
            # pipeline history in the same direction an order moves.
            cur.execute(
                """
                select
                    id::text as event_id,
                    event_type::text,
                    from_state::text,
                    to_state::text,
                    task_id::text,
                    worker_id,
                    occurred_at,
                    metadata
                from order_events
                where order_id = %s::uuid
                order by occurred_at asc, id asc
                limit %s
                """,
                (order_id, ORDER_DETAIL_EVENT_LIMIT),
            )
            events = [dict(row) for row in cur.fetchall()]

    return {
        "order": dict(order),
        "tasks": tasks,
        "events": events,
        "limits": {
            "tasks": ORDER_DETAIL_TASK_LIMIT,
            "events": ORDER_DETAIL_EVENT_LIMIT,
        },
    }


def _load_worker_overview(cur) -> dict[str, Any]:
    """Load aggregate worker heartbeat status for the dashboard summary."""
    # Worker heartbeats run every 10 seconds. A 30 second active window allows a
    # small amount of scheduling jitter while still making stopped workers
    # visible quickly during local demos.
    cur.execute(
        """
        select
            count(*)::int as total_seen,
            count(*) filter (
                where last_seen_at >= now() - (%s * interval '1 second')
            )::int as active_count
        from workers
        """,
        (ACTIVE_WORKER_THRESHOLD_SECONDS,),
    )
    counts = dict(cur.fetchone())

    return {
        "active_count": counts["active_count"],
        "active_threshold_seconds": ACTIVE_WORKER_THRESHOLD_SECONDS,
        "configured_count": _configured_worker_count(),
        "total_seen": counts["total_seen"],
    }


def _load_problem_tasks(cur) -> list[dict[str, Any]]:
    """Load the tasks an operator should inspect first."""
    # The dashboard only calls out tasks that are already failed, running past
    # their lease, or due again with an error from a previous attempt. Ordinary
    # future pending tasks are healthy scheduled work and are counted elsewhere.
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text as order_id,
            orders.idempotency_key,
            task.task_type::text,
            task.target_state::text,
            task.status::text,
            task.attempts,
            task.max_attempts,
            task.next_run_at,
            task.deadline_at,
            task.locked_by,
            task.locked_until,
            task.last_error,
            case
                when task.status = %s::task_status then 'failed'
                when
                    task.status = %s::task_status
                    and task.locked_until < now()
                    then 'expired_running'
                else 'due_pending_with_error'
            end as problem_reason
        from order_tasks as task
        join orders on orders.id = task.order_id
        where
            task.status = %s::task_status
            or (
                task.status = %s::task_status
                and task.locked_until < now()
            )
            or (
                task.status = %s::task_status
                and task.next_run_at <= now()
                and task.last_error is not null
            )
        order by
            case
                when task.status = %s::task_status then 0
                when
                    task.status = %s::task_status
                    and task.locked_until < now()
                    then 1
                else 2
            end,
            coalesce(task.locked_until, task.next_run_at, task.updated_at) asc
        limit %s
        """,
        (
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
            TASK_STATUS_PENDING,
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
            PROBLEM_TASK_LIMIT,
        ),
    )
    return [dict(row) for row in cur.fetchall()]


def _load_recent_orders(cur) -> list[dict[str, Any]]:
    """Load recently changed orders for the dashboard activity table."""
    cur.execute(
        """
        select
            id::text as order_id,
            idempotency_key,
            state::text,
            restaurant_ref,
            courier_ref,
            created_at,
            updated_at
        from orders
        order by updated_at desc
        limit %s
        """,
        (RECENT_ORDER_LIMIT,),
    )
    return [dict(row) for row in cur.fetchall()]


def _load_placed_orders(cur) -> list[dict[str, Any]]:
    """Load the newest placed orders so operators can click one early."""
    # This query intentionally orders by updated_at to use the existing
    # ix_orders_state_updated_at index. Placed orders have not advanced yet, so
    # updated_at is the timestamp that matters for this dashboard watch list.
    cur.execute(
        """
        select
            id::text as order_id,
            idempotency_key,
            state::text,
            restaurant_ref,
            courier_ref,
            created_at,
            updated_at
        from orders
        where state = %s::order_state
        order by updated_at desc
        limit %s
        """,
        (ORDER_STATE_PLACED, PLACED_ORDER_LIMIT),
    )
    return [dict(row) for row in cur.fetchall()]


def _load_recent_events(cur) -> list[dict[str, Any]]:
    """Load recent order events with order keys already joined in."""
    # order_events stores order_id, not the human-friendly idempotency key. The
    # API joins it here so the frontend can render one simple activity table.
    cur.execute(
        """
        select
            event.id::text as event_id,
            event.order_id::text,
            orders.idempotency_key,
            event.event_type::text,
            event.from_state::text,
            event.to_state::text,
            event.task_id::text,
            event.worker_id,
            event.occurred_at
        from order_events as event
        join orders on orders.id = event.order_id
        order by event.occurred_at desc
        limit %s
        """,
        (RECENT_EVENT_LIMIT,),
    )
    return [dict(row) for row in cur.fetchall()]
