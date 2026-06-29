import os
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_ORDER_CREATED, EVENT_TYPE_STATE_TRANSITION
from common.state_machine import (
    ORDER_STATE_DELIVERED,
    ORDER_STATE_PLACED,
    ORDER_STATES,
)
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
# Latency is also windowed, but over a longer interval so the dashboard remains
# responsive to a new loadgen run without going blank between deliveries.
DEFAULT_LATENCY_WINDOW_SECONDS = 300


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


def _positive_int_env(name: str, fallback: int) -> int:
    raw_value = os.getenv(name, str(fallback))
    try:
        parsed_value = int(raw_value)
    except ValueError:
        return fallback

    return parsed_value if parsed_value > 0 else fallback


def _latency_window_seconds() -> int:
    return _positive_int_env(
        "DASHBOARD_LATENCY_WINDOW_SECONDS",
        DEFAULT_LATENCY_WINDOW_SECONDS,
    )


def _count_map(
    rows: list[dict[str, Any]],
    expected_keys: tuple[str, ...],
) -> dict[str, int]:
    """Return counts for every known enum value, including zero-count states."""
    counts = {key: 0 for key in expected_keys}
    for row in rows:
        counts[row["key"]] = row["count"]
    return counts


def _rounded_float(value: Any) -> float | None:
    """Return a rounded JSON-friendly float for aggregate numeric values."""
    if value is None:
        return None
    return round(float(value), 2)


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
                            and last_error is not null
                    )::int as retrying_pending,
                    count(*) filter (
                        where
                            status = %s::task_status
                            and locked_until >= now()
                            and last_error is not null
                    )::int as retrying_running,
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
                    TASK_STATUS_PENDING,
                    TASK_STATUS_RUNNING,
                    TASK_STATUS_COMPLETED,
                    THROUGHPUT_WINDOW_SECONDS,
                ),
            )
            task_health = dict(cur.fetchone())

            order_throughput = _load_order_throughput(cur)
            latency = _load_latency_overview(cur, _latency_window_seconds())
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
            "retrying_pending": task_health["retrying_pending"],
            "retrying_running": task_health["retrying_running"],
        },
        "throughput": {
            "window_seconds": THROUGHPUT_WINDOW_SECONDS,
            "orders_created": order_throughput["orders_created"],
            "orders_created_per_minute": order_throughput[
                "orders_created_per_minute"
            ],
            "orders_delivered": order_throughput["orders_delivered"],
            "orders_delivered_per_minute": order_throughput[
                "orders_delivered_per_minute"
            ],
            "tasks_completed": task_health["completed_recent"],
            "tasks_completed_per_second": round(
                task_health["completed_recent"] / THROUGHPUT_WINDOW_SECONDS,
                2,
            ),
        },
        "latency": latency,
        "workers": workers,
        "problem_tasks": problem_tasks,
        "placed_orders": placed_orders,
        "recent_orders": recent_orders,
        "recent_events": recent_events,
    }


def _load_order_throughput(cur) -> dict[str, Any]:
    """Return order-level throughput over the dashboard rolling window."""
    # Business throughput is based on customer-visible events, not task rows.
    # A single order completes several tasks, so task completion rate is useful
    # system telemetry but not a substitute for order creation/delivery rate.
    cur.execute(
        """
        select
            count(*) filter (
                where
                    event_type = %s::event_type
                    and occurred_at >= now() - (%s * interval '1 second')
            )::int as orders_created,
            count(*) filter (
                where
                    event_type = %s::event_type
                    and to_state = %s::order_state
                    and occurred_at >= now() - (%s * interval '1 second')
            )::int as orders_delivered
        from order_events
        """,
        (
            EVENT_TYPE_ORDER_CREATED,
            THROUGHPUT_WINDOW_SECONDS,
            EVENT_TYPE_STATE_TRANSITION,
            ORDER_STATE_DELIVERED,
            THROUGHPUT_WINDOW_SECONDS,
        ),
    )
    row = dict(cur.fetchone())
    orders_created = row["orders_created"]
    orders_delivered = row["orders_delivered"]
    return {
        "orders_created": orders_created,
        "orders_created_per_minute": round(
            orders_created * 60 / THROUGHPUT_WINDOW_SECONDS,
            2,
        ),
        "orders_delivered": orders_delivered,
        "orders_delivered_per_minute": round(
            orders_delivered * 60 / THROUGHPUT_WINDOW_SECONDS,
            2,
        ),
    }


def _load_latency_overview(cur, window_seconds: int) -> dict[str, Any]:
    """Return end-to-end and per-stage latency from order event timestamps."""
    cur.execute(
        """
        with created as (
            select order_id, min(occurred_at) as created_at
            from order_events
            where event_type = %s::event_type
            group by order_id
        ),
        delivered as (
            select order_id, min(occurred_at) as delivered_at
            from order_events
            where
                event_type = %s::event_type
                and to_state = %s::order_state
                and occurred_at >= now() - (%s * interval '1 second')
            group by order_id
        ),
        durations as (
            select
                extract(epoch from delivered.delivered_at - created.created_at)
                    ::double precision as duration_seconds
            from created
            join delivered on delivered.order_id = created.order_id
            where delivered.delivered_at >= created.created_at
        )
        select
            count(*)::int as sample_count,
            avg(duration_seconds)::double precision as avg_seconds,
            percentile_cont(0.95) within group (order by duration_seconds)
                ::double precision as p95_seconds
        from durations
        """,
        (
            EVENT_TYPE_ORDER_CREATED,
            EVENT_TYPE_STATE_TRANSITION,
            ORDER_STATE_DELIVERED,
            window_seconds,
        ),
    )
    pipeline = dict(cur.fetchone())

    cur.execute(
        """
        with timeline as (
            select
                order_id,
                %s::text as reached_state,
                occurred_at,
                0 as event_order
            from order_events
            where event_type = %s::event_type

            union all

            select
                order_id,
                to_state::text as reached_state,
                occurred_at,
                1 as event_order
            from order_events
            where
                event_type = %s::event_type
                and to_state is not null
        ),
        sequenced as (
            select
                order_id,
                lag(reached_state) over event_sequence as from_state,
                reached_state as to_state,
                occurred_at as reached_at,
                extract(
                    epoch from occurred_at - lag(occurred_at) over event_sequence
                )::double precision as duration_seconds
            from timeline
            window event_sequence as (
                partition by order_id
                order by occurred_at, event_order, reached_state
            )
        )
        select
            from_state,
            to_state,
            count(*)::int as sample_count,
            avg(duration_seconds)::double precision as avg_seconds,
            percentile_cont(0.95) within group (order by duration_seconds)
                ::double precision as p95_seconds
        from sequenced
        where
            from_state is not null
            and duration_seconds is not null
            and duration_seconds >= 0
            and reached_at >= now() - (%s * interval '1 second')
        group by from_state, to_state
        """,
        (
            ORDER_STATE_PLACED,
            EVENT_TYPE_ORDER_CREATED,
            EVENT_TYPE_STATE_TRANSITION,
            window_seconds,
        ),
    )
    stages = [
        {
            "from_state": row["from_state"],
            "to_state": row["to_state"],
            "sample_count": row["sample_count"],
            "avg_seconds": _rounded_float(row["avg_seconds"]),
            "p95_seconds": _rounded_float(row["p95_seconds"]),
        }
        for row in cur.fetchall()
    ]

    return {
        "window_seconds": window_seconds,
        "pipeline": {
            "sample_count": pipeline["sample_count"],
            "avg_seconds": _rounded_float(pipeline["avg_seconds"]),
            "p95_seconds": _rounded_float(pipeline["p95_seconds"]),
        },
        "stages": stages,
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
    # their lease, or waiting for retry after a previous failed attempt.
    # Ordinary future pending tasks with no last_error are healthy scheduled
    # work and are not useful in this operator-focused list.
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
                when task.status = %s::task_status then 'retry_running'
                when task.next_run_at > now() then 'pending_retry'
                else 'due_retry'
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
                and task.last_error is not null
            )
            or (
                task.status = %s::task_status
                and task.last_error is not null
            )
        order by
            case
                when task.status = %s::task_status then 0
                when
                    task.status = %s::task_status
                    and task.locked_until < now()
                    then 1
                when task.status = %s::task_status then 2
                when task.next_run_at <= now() then 3
                else 4
            end,
            coalesce(task.locked_until, task.next_run_at, task.updated_at) asc
        limit %s
        """,
        (
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
            TASK_STATUS_RUNNING,
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
            TASK_STATUS_PENDING,
            TASK_STATUS_RUNNING,
            TASK_STATUS_FAILED,
            TASK_STATUS_RUNNING,
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
