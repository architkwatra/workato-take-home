import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
from psycopg.rows import dict_row

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_ORDER_CREATED, EVENT_TYPE_STATE_TRANSITION
from common.state_machine import (
    ORDER_STATE_CONFIRMED,
    ORDER_STATE_DELIVERED,
    ORDER_STATE_OUT_FOR_DELIVERY,
    ORDER_STATE_PAYMENT_CHECK,
    ORDER_STATE_PLACED,
    ORDER_STATE_PREPARING,
    ORDER_STATE_READY,
    is_terminal_order_state,
)
from common.task_types import (
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_TYPE_ADVANCE_STATE,
    TASK_TYPE_CHECK_PAYMENT,
    TASK_TYPE_CHECK_DELIVERY,
    TASK_TYPE_CHECK_READY,
)


logger = logging.getLogger("worker.tasks")

LEASE_SECONDS = 30
DEFAULT_DOWNSTREAM_SIM_BASE_URL = "http://downstream-sim:8000"
DEFAULT_DOWNSTREAM_REQUEST_TIMEOUT_SECONDS = 2.0
# Base delay for retryable errors. The first retry waits this long; later
# retryable failures grow exponentially until TASK_RETRY_MAX_DELAY_SECONDS.
DEFAULT_TASK_RETRY_DELAY_SECONDS = 5.0
DEFAULT_TASK_RETRY_MAX_DELAY_SECONDS = 60.0
DEFAULT_PAYMENT_CHECK_INTERVAL_SECONDS = 1.0
DEFAULT_PAYMENT_CHECK_DEADLINE_SECONDS = 60.0
DEFAULT_READY_CHECK_INITIAL_DELAY_SECONDS = 2.0
DEFAULT_READY_CHECK_INTERVAL_SECONDS = 2.0
DEFAULT_READY_CHECK_DEADLINE_SECONDS = 60.0
DEFAULT_DELIVERY_CHECK_INITIAL_DELAY_SECONDS = 3.0
DEFAULT_DELIVERY_CHECK_INTERVAL_SECONDS = 3.0
DEFAULT_DELIVERY_CHECK_DEADLINE_SECONDS = 120.0
# Future-slice tasks should stay visible but not spin the worker loop before
# their handler exists. This delay is only for unsupported task release.
DEFAULT_UNSUPPORTED_TASK_RELEASE_DELAY_SECONDS = 30.0
MAX_LAST_ERROR_LENGTH = 300
MAX_CLAIM_LOG_CACHE_SIZE = 1000
_logged_claimed_task_ids: set[str] = set()
_logged_claimed_task_order: deque[str] = deque()

# Extra columns written by generic transition finalization must be explicitly
# allowlisted because column names cannot be passed as SQL parameters.
ALLOWED_EXTRA_ORDER_COLUMNS = frozenset({"courier_ref"})

DELIVERY_CHECK_STATUS_DELIVERED = "delivered"
DELIVERY_CHECK_STATUS_IN_TRANSIT = "in_transit"
PAYMENT_CHECK_STATUS_AUTHORIZED = "authorized"
PAYMENT_CHECK_STATUS_PENDING = "pending"
TRANSITION_STATUS_NOT_CONFIRMED = "not_confirmed"
TRANSITION_STATUS_NOT_PREPARING = "not_preparing"
COURIER_ASSIGN_STATUS_ASSIGNED = "assigned"
COURIER_ASSIGN_STATUS_NOT_ASSIGNED = "not_assigned"

PROCESSING_ACTION_TRANSITIONED = "transitioned"
PROCESSING_ACTION_RETRY_SCHEDULED = "retry_scheduled"
PROCESSING_ACTION_FAILED = "failed"
PROCESSING_ACTION_MISSING_TASK = "missing_task"
PROCESSING_ACTION_NOT_RUNNING = "not_running"
PROCESSING_ACTION_LOST_OWNERSHIP = "lost_ownership"
PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP = "completed_terminal_noop"
PROCESSING_ACTION_COMPLETED_STALE_NOOP = "completed_stale_noop"
PROCESSING_ACTION_COMPLETED_OPTIMISTIC_NOOP = "completed_optimistic_noop"
PROCESSING_ACTION_POLL_SCHEDULED = "poll_scheduled"

COMPLETED_NOOP_ACTIONS = frozenset(
    {
        PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
        PROCESSING_ACTION_COMPLETED_STALE_NOOP,
        PROCESSING_ACTION_COMPLETED_OPTIMISTIC_NOOP,
    }
)
SKIPPED_AFTER_CLAIM_ACTIONS = frozenset(
    {
        PROCESSING_ACTION_MISSING_TASK,
        PROCESSING_ACTION_NOT_RUNNING,
        PROCESSING_ACTION_LOST_OWNERSHIP,
    }
)


@dataclass(frozen=True)
class ClaimedTask:
    """Task row claimed by this worker process."""

    id: str
    order_id: str
    task_type: str
    target_state: str | None
    status: str
    locked_by: str
    locked_until: datetime


@dataclass(frozen=True)
class ProcessingResult:
    """Outcome of processing a claimed task."""

    action: str
    order_id: str | None = None
    from_state: str | None = None
    to_state: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class RestaurantTransitionSpec:
    """One downstream transition handled by the worker.

    expected_status commits the local state change. pending_status is a valid
    business response meaning downstream has not reached the target yet; it
    requeues the task without consuming transport retry attempts.
    """

    source_state: str
    target_state: str
    endpoint_path: str
    expected_status: str
    pending_status: str
    action_name: str
    source_started_at_field: str
    next_task_type: str | None = None
    next_target_state: str | None = None


@dataclass(frozen=True)
class RestaurantTransitionCall:
    """Details needed for one restaurant simulator request."""

    order_id: str
    restaurant_ref: str
    source_started_at: datetime
    spec: RestaurantTransitionSpec


@dataclass(frozen=True)
class TransitionCallOutcome:
    """Validated command-style downstream response."""

    status: str
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class PaymentCheckCall:
    """Details needed for one payment authorization poll."""

    order_id: str
    placed_at: datetime


@dataclass(frozen=True)
class PaymentCheckOutcome:
    """Validated response from the payment simulator endpoint."""

    status: str
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class CourierAssignCall:
    """Details needed for one courier assignment request."""

    order_id: str
    restaurant_ref: str
    ready_at: datetime


@dataclass(frozen=True)
class CourierAssignOutcome:
    """Validated courier assignment response."""

    status: str
    retry_after_seconds: float | None = None
    courier_ref: str | None = None


@dataclass(frozen=True)
class RestaurantReadyCheckCall:
    """Details needed for one restaurant readiness poll."""

    order_id: str
    restaurant_ref: str
    prep_started_at: datetime


@dataclass(frozen=True)
class RestaurantReadyCheckOutcome:
    """Validated response from the restaurant readiness simulator endpoint."""

    status: str
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class CourierDeliveryCheckCall:
    """Details needed for one courier delivery status poll."""

    order_id: str
    courier_ref: str
    dispatched_at: datetime


@dataclass(frozen=True)
class CourierDeliveryCheckOutcome:
    """Validated response from the courier delivery simulator endpoint."""

    status: str
    retry_after_seconds: float | None = None


READY_CHECK_STATUS_READY = "ready"
READY_CHECK_STATUS_NOT_READY = "not_ready"


PAYMENT_FINALIZATION_SPEC = RestaurantTransitionSpec(
    source_state=ORDER_STATE_PLACED,
    target_state=ORDER_STATE_PAYMENT_CHECK,
    endpoint_path="/payment/check",
    expected_status=PAYMENT_CHECK_STATUS_AUTHORIZED,
    pending_status=PAYMENT_CHECK_STATUS_PENDING,
    action_name="payment check",
    source_started_at_field="placed_at",
    next_task_type=TASK_TYPE_ADVANCE_STATE,
    next_target_state=ORDER_STATE_CONFIRMED,
)

# Each transition is modeled as a quick downstream check. Downstream either
# returns the success status or a valid pending status with retry_after_seconds;
# workers never hold DB locks while waiting for the simulated delay to pass.
RESTAURANT_TRANSITION_SPECS = {
    ORDER_STATE_CONFIRMED: RestaurantTransitionSpec(
        source_state=ORDER_STATE_PAYMENT_CHECK,
        target_state=ORDER_STATE_CONFIRMED,
        endpoint_path="/restaurant/confirm",
        expected_status=ORDER_STATE_CONFIRMED,
        pending_status=TRANSITION_STATUS_NOT_CONFIRMED,
        action_name="restaurant confirm",
        source_started_at_field="payment_checked_at",
        next_task_type=TASK_TYPE_ADVANCE_STATE,
        next_target_state=ORDER_STATE_PREPARING,
    ),
    ORDER_STATE_PREPARING: RestaurantTransitionSpec(
        source_state=ORDER_STATE_CONFIRMED,
        target_state=ORDER_STATE_PREPARING,
        endpoint_path="/restaurant/start-prep",
        expected_status=ORDER_STATE_PREPARING,
        pending_status=TRANSITION_STATUS_NOT_PREPARING,
        action_name="restaurant start-prep",
        source_started_at_field="confirmed_at",
        next_task_type=TASK_TYPE_CHECK_READY,
        next_target_state=ORDER_STATE_READY,
    ),
}

READY_FINALIZATION_SPEC = RestaurantTransitionSpec(
    source_state=ORDER_STATE_PREPARING,
    target_state=ORDER_STATE_READY,
    endpoint_path="/restaurant/check-ready",
    expected_status=READY_CHECK_STATUS_READY,
    pending_status=READY_CHECK_STATUS_NOT_READY,
    action_name="restaurant check-ready",
    source_started_at_field="prep_started_at",
    # Once the restaurant marks food ready, the next durable command is courier
    # assignment. Insert it in the same transaction as preparing -> ready so
    # ready orders cannot be stranded without dispatch work.
    next_task_type=TASK_TYPE_ADVANCE_STATE,
    next_target_state=ORDER_STATE_OUT_FOR_DELIVERY,
)

# Courier assign is a command task like restaurant confirm/start-prep, but it
# also writes courier_ref to the order row. It is dispatched separately from
# process_transition_task so the HTTP response can be passed into finalization.
COURIER_ASSIGN_SPEC = RestaurantTransitionSpec(
    source_state=ORDER_STATE_READY,
    target_state=ORDER_STATE_OUT_FOR_DELIVERY,
    endpoint_path="/courier/assign",
    expected_status=COURIER_ASSIGN_STATUS_ASSIGNED,
    pending_status=COURIER_ASSIGN_STATUS_NOT_ASSIGNED,
    action_name="courier assign",
    source_started_at_field="ready_at",
    next_task_type=TASK_TYPE_CHECK_DELIVERY,
    next_target_state=ORDER_STATE_DELIVERED,
)

DELIVERY_FINALIZATION_SPEC = RestaurantTransitionSpec(
    source_state=ORDER_STATE_OUT_FOR_DELIVERY,
    target_state=ORDER_STATE_DELIVERED,
    endpoint_path="/courier/check-delivery",
    expected_status=DELIVERY_CHECK_STATUS_DELIVERED,
    pending_status=DELIVERY_CHECK_STATUS_IN_TRANSIT,
    action_name="courier check-delivery",
    source_started_at_field="dispatched_at",
)


def _read_positive_float_env(env_name: str, default: float) -> float:
    """Read a positive float environment setting."""
    raw_value = os.getenv(env_name)
    if raw_value is None:
        return default

    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{env_name} must be a number") from exc

    if value <= 0:
        raise RuntimeError(f"{env_name} must be greater than zero")

    return value


def _downstream_sim_base_url() -> str:
    """Return the configured downstream simulator base URL without a suffix slash."""
    return os.getenv(
        "DOWNSTREAM_SIM_BASE_URL",
        DEFAULT_DOWNSTREAM_SIM_BASE_URL,
    ).rstrip("/")


def _downstream_request_timeout_seconds() -> float:
    """Return the downstream request timeout in seconds."""
    return _read_positive_float_env(
        "DOWNSTREAM_REQUEST_TIMEOUT_SECONDS",
        DEFAULT_DOWNSTREAM_REQUEST_TIMEOUT_SECONDS,
    )


def _task_retry_base_delay_seconds() -> float:
    """Return the base wait before the first retryable-error retry."""
    return _read_positive_float_env(
        "TASK_RETRY_DELAY_SECONDS",
        DEFAULT_TASK_RETRY_DELAY_SECONDS,
    )


def _task_retry_max_delay_seconds() -> float:
    """Return the cap for exponential retryable-error backoff."""
    return _read_positive_float_env(
        "TASK_RETRY_MAX_DELAY_SECONDS",
        DEFAULT_TASK_RETRY_MAX_DELAY_SECONDS,
    )


def _task_retry_delay_seconds(*, attempts: int) -> float:
    """Return exponential backoff for the next retryable-error attempt.

    `attempts` is the number of retryable failures already recorded on the task.
    The current failure has not been written yet, so attempts=0 is the first
    failure and receives the base delay. Normal business polling such as
    not_ready/not_delivered does not use this helper and does not consume
    attempts.
    """
    prior_failures = max(attempts, 0)
    retry_delay = _task_retry_base_delay_seconds() * (2**prior_failures)
    return min(retry_delay, _task_retry_max_delay_seconds())


def _payment_check_interval_seconds() -> float:
    """Return the fallback wait between normal payment-pending polls."""
    return _read_positive_float_env(
        "PAYMENT_CHECK_INTERVAL_SECONDS",
        DEFAULT_PAYMENT_CHECK_INTERVAL_SECONDS,
    )


def _payment_check_deadline_seconds() -> float:
    """Return the max wall-clock wait for payment authorization polling."""
    return _read_positive_float_env(
        "PAYMENT_CHECK_DEADLINE_SECONDS",
        DEFAULT_PAYMENT_CHECK_DEADLINE_SECONDS,
    )


def _ready_check_initial_delay_seconds() -> float:
    """Return how long to wait before the first restaurant ready check."""
    return _read_positive_float_env(
        "READY_CHECK_INITIAL_DELAY_SECONDS",
        DEFAULT_READY_CHECK_INITIAL_DELAY_SECONDS,
    )


def _ready_check_interval_seconds() -> float:
    """Return the fallback wait between normal not-ready polls."""
    return _read_positive_float_env(
        "READY_CHECK_INTERVAL_SECONDS",
        DEFAULT_READY_CHECK_INTERVAL_SECONDS,
    )


def _ready_check_deadline_seconds() -> float:
    """Return the max wall-clock wait for restaurant readiness polling."""
    return _read_positive_float_env(
        "READY_CHECK_DEADLINE_SECONDS",
        DEFAULT_READY_CHECK_DEADLINE_SECONDS,
    )


def _delivery_check_initial_delay_seconds() -> float:
    """Return how long to wait before the first courier delivery check."""
    return _read_positive_float_env(
        "DELIVERY_CHECK_INITIAL_DELAY_SECONDS",
        DEFAULT_DELIVERY_CHECK_INITIAL_DELAY_SECONDS,
    )


def _delivery_check_interval_seconds() -> float:
    """Return the fallback wait between normal in-transit delivery polls."""
    return _read_positive_float_env(
        "DELIVERY_CHECK_INTERVAL_SECONDS",
        DEFAULT_DELIVERY_CHECK_INTERVAL_SECONDS,
    )


def _delivery_check_deadline_seconds() -> float:
    """Return the max wall-clock wait for courier delivery polling."""
    return _read_positive_float_env(
        "DELIVERY_CHECK_DEADLINE_SECONDS",
        DEFAULT_DELIVERY_CHECK_DEADLINE_SECONDS,
    )


def _unsupported_task_release_delay_seconds() -> float:
    """Return how long unsupported tasks wait before workers inspect them again."""
    return _read_positive_float_env(
        "UNSUPPORTED_TASK_RELEASE_DELAY_SECONDS",
        DEFAULT_UNSUPPORTED_TASK_RELEASE_DELAY_SECONDS,
    )


def _short_error(message: str) -> str:
    """Keep task last_error small enough for dashboards and logs."""
    cleaned = " ".join(message.split())
    if len(cleaned) <= MAX_LAST_ERROR_LENGTH:
        return cleaned
    return f"{cleaned[: MAX_LAST_ERROR_LENGTH - 3]}..."


def _should_log_claim_at_info(task_id: str) -> bool:
    """Return whether this task claim should use info-level logging."""
    if task_id in _logged_claimed_task_ids:
        return False

    # Reclaimed tasks can appear repeatedly during downstream outages. Keep
    # only a bounded recent-id cache so log suppression cannot grow forever.
    _logged_claimed_task_ids.add(task_id)
    _logged_claimed_task_order.append(task_id)
    while len(_logged_claimed_task_order) > MAX_CLAIM_LOG_CACHE_SIZE:
        expired_task_id = _logged_claimed_task_order.popleft()
        _logged_claimed_task_ids.discard(expired_task_id)
    return True


def _valid_positive_number(value) -> float | None:
    """Return a positive numeric JSON value, rejecting bools and strings."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value > 0 else None


def _bounded_payment_check_next_run_at(
    *,
    now: datetime,
    retry_after_seconds: float | None,
    deadline_at: datetime,
) -> datetime:
    """Return the next payment poll time, capped so the deadline gets checked."""
    poll_delay_seconds = retry_after_seconds or _payment_check_interval_seconds()
    proposed_next_run_at = now + timedelta(seconds=poll_delay_seconds)
    return min(proposed_next_run_at, deadline_at)


def _bounded_ready_check_next_run_at(
    *,
    now: datetime,
    retry_after_seconds: float | None,
    deadline_at: datetime,
) -> datetime:
    """Return the next poll time, capped so the deadline gets a final check."""
    poll_delay_seconds = retry_after_seconds or _ready_check_interval_seconds()
    proposed_next_run_at = now + timedelta(seconds=poll_delay_seconds)
    return min(proposed_next_run_at, deadline_at)


def _bounded_delivery_check_next_run_at(
    *,
    now: datetime,
    retry_after_seconds: float | None,
    deadline_at: datetime,
) -> datetime:
    """Return the next delivery poll time, capped so the deadline gets a final check."""
    poll_delay_seconds = retry_after_seconds or _delivery_check_interval_seconds()
    proposed_next_run_at = now + timedelta(seconds=poll_delay_seconds)
    return min(proposed_next_run_at, deadline_at)


def claim_one_task(*, worker_id: str) -> ClaimedTask | None:
    """Claim one eligible task for this worker, if any are runnable."""
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # The SELECT ... FOR UPDATE SKIP LOCKED subquery is the
                # concurrency boundary for competing workers: each worker skips
                # rows already locked by another transaction instead of waiting
                # behind it, so replicas can claim different tasks in parallel.
                # updated_at keeps rows released back to pending from being
                # repeatedly selected ahead of other due work.
                # The CTE selects only one task id; the outer UPDATE then claims
                # exactly that row in the same statement, avoiding a read/write
                # gap where another worker could claim it first.
                cur.execute(
                    """
                    with candidate as (
                        select id
                        from order_tasks
                        where
                            (
                                status = %s::task_status
                                and next_run_at <= now()
                            )
                            or (
                                status = %s::task_status
                                and locked_until < now()
                            )
                        order by updated_at, next_run_at, created_at, id
                        for update skip locked
                        limit 1
                    )
                    update order_tasks as task
                    set
                        status = %s::task_status,
                        locked_by = %s,
                        locked_until = now() + (%s * interval '1 second'),
                        updated_at = now()
                    from candidate
                    where task.id = candidate.id
                    returning
                        task.id::text,
                        task.order_id::text,
                        task.task_type::text,
                        task.target_state::text,
                        task.status::text,
                        task.locked_by,
                        task.locked_until
                    """,
                    (
                        TASK_STATUS_PENDING,
                        TASK_STATUS_RUNNING,
                        TASK_STATUS_RUNNING,
                        worker_id,
                        LEASE_SECONDS,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    return None

                return ClaimedTask(
                    id=row["id"],
                    order_id=row["order_id"],
                    task_type=row["task_type"],
                    target_state=row["target_state"],
                    status=row["status"],
                    locked_by=row["locked_by"],
                    locked_until=row["locked_until"],
                )


def release_claimed_task(
    *,
    task_id: str,
    worker_id: str,
    next_run_at: datetime | None = None,
    last_error: str | None = None,
) -> bool:
    """Release a claimed task back to pending without completing it."""
    updated_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update order_tasks
                    set
                        status = %s::task_status,
                        next_run_at = coalesce(%s, next_run_at),
                        last_error = coalesce(%s, last_error),
                        locked_by = null,
                        locked_until = null,
                        updated_at = %s
                    where
                        id = %s::uuid
                        and status = %s::task_status
                        and locked_by = %s
                    """,
                    (
                        TASK_STATUS_PENDING,
                        next_run_at,
                        last_error,
                        updated_at,
                        task_id,
                        TASK_STATUS_RUNNING,
                        worker_id,
                    ),
                )
                return cur.rowcount == 1


def _complete_claimed_task(cur, *, task_id: str, worker_id: str, completed_at) -> bool:
    """Mark a claimed task completed, clearing its worker lease."""
    # attempts still shows whether the task recovered after retries; clearing
    # last_error keeps completed tasks from looking actively broken in views.
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            completed_at = %s,
            last_error = null,
            locked_by = null,
            locked_until = null,
            updated_at = %s
        where
            id = %s::uuid
            and status = %s::task_status
            and locked_by = %s
        """,
        (
            TASK_STATUS_COMPLETED,
            completed_at,
            completed_at,
            task_id,
            TASK_STATUS_RUNNING,
            worker_id,
        ),
    )
    return cur.rowcount == 1


def _load_transition_task_row(
    cur,
    *,
    task_id: str,
    source_state: str,
) -> dict | None:
    """Lock the task row and return current order details for a transition."""
    # Lock only the task row here. The order row is intentionally left unlocked
    # so the later UPDATE ... WHERE version = <read_version> is the real
    # optimistic concurrency guard for cancellation races and stale workers.
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text,
            task.status::text as task_status,
            task.locked_by,
            orders.state::text as order_state,
            orders.version,
            orders.restaurant_ref,
            -- downstream-sim is stateless, so the worker sends the durable
            -- timestamp for when this source state began. For placed orders
            -- that is order_created; for all later states it is the latest
            -- state_transition into source_state. updated_at is a defensive
            -- fallback for old rows if an event is missing.
            coalesce(source_event.occurred_at, orders.updated_at) as source_started_at
        from order_tasks as task
        join orders on orders.id = task.order_id
        left join lateral (
            select occurred_at
            from order_events
            where
                order_id = orders.id
                and (
                    (
                        %s::order_state = %s::order_state
                        and event_type = %s::event_type
                    )
                    or (
                        %s::order_state <> %s::order_state
                        and event_type = %s::event_type
                        and to_state = %s::order_state
                    )
                )
            order by occurred_at desc
            limit 1
        ) as source_event on true
        where task.id = %s::uuid
        for update of task
        """,
        (
            source_state,
            ORDER_STATE_PLACED,
            EVENT_TYPE_ORDER_CREATED,
            source_state,
            ORDER_STATE_PLACED,
            EVENT_TYPE_STATE_TRANSITION,
            source_state,
            task_id,
        ),
    )
    return cur.fetchone()


def _load_payment_check_task_row(cur, *, task_id: str) -> dict | None:
    """Lock the check_payment task row and return current order details."""
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text,
            task.status::text as task_status,
            task.locked_by,
            task.attempts,
            task.max_attempts,
            task.deadline_at,
            orders.state::text as order_state,
            orders.version,
            created_event.occurred_at as placed_at
        from order_tasks as task
        join orders on orders.id = task.order_id
        left join lateral (
            select occurred_at
            from order_events
            where
                order_id = orders.id
                and event_type = %s::event_type
            order by occurred_at asc
            limit 1
        ) as created_event on true
        where task.id = %s::uuid
        for update of task
        """,
        (
            EVENT_TYPE_ORDER_CREATED,
            task_id,
        ),
    )
    return cur.fetchone()


def _load_ready_check_task_row(cur, *, task_id: str) -> dict | None:
    """Lock the check_ready task row and return current order/prep details."""
    # Keep the order row unlocked for the same reason as command transitions:
    # the final UPDATE ... WHERE version = <read_version> is the concurrency
    # guard, while FOR UPDATE OF task proves task ownership.
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text,
            task.status::text as task_status,
            task.locked_by,
            task.attempts,
            task.max_attempts,
            task.deadline_at,
            orders.state::text as order_state,
            orders.version,
            orders.restaurant_ref,
            prep_event.occurred_at as prep_started_at
        from order_tasks as task
        join orders on orders.id = task.order_id
        -- The preparing transition event is the durable clock source for the
        -- simulator. It was committed with the state change, so workers do not
        -- need simulator-side state to know when prep started.
        left join lateral (
            select occurred_at
            from order_events
            where
                order_id = orders.id
                and event_type = %s::event_type
                and from_state = %s::order_state
                and to_state = %s::order_state
            order by occurred_at desc
            limit 1
        ) as prep_event on true
        where task.id = %s::uuid
        for update of task
        """,
        (
            EVENT_TYPE_STATE_TRANSITION,
            ORDER_STATE_CONFIRMED,
            ORDER_STATE_PREPARING,
            task_id,
        ),
    )
    return cur.fetchone()


def _load_delivery_check_task_row(cur, *, task_id: str) -> dict | None:
    """Lock the check_delivery task row and return current order/dispatch details."""
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text,
            task.status::text as task_status,
            task.locked_by,
            task.attempts,
            task.max_attempts,
            task.deadline_at,
            orders.state::text as order_state,
            orders.version,
            orders.courier_ref,
            dispatch_event.occurred_at as dispatched_at
        from order_tasks as task
        join orders on orders.id = task.order_id
        -- The ready -> out_for_delivery transition event is the durable clock
        -- source for the delivery simulator. It was committed with the state
        -- change, so workers do not need simulator-side state to know when
        -- dispatch happened.
        left join lateral (
            select occurred_at
            from order_events
            where
                order_id = orders.id
                and event_type = %s::event_type
                and from_state = %s::order_state
                and to_state = %s::order_state
            order by occurred_at desc
            limit 1
        ) as dispatch_event on true
        where task.id = %s::uuid
        for update of task
        """,
        (
            EVENT_TYPE_STATE_TRANSITION,
            ORDER_STATE_READY,
            ORDER_STATE_OUT_FOR_DELIVERY,
            task_id,
        ),
    )
    return cur.fetchone()


def _owned_task_check_result(
    row: dict | None,
    *,
    worker_id: str,
) -> ProcessingResult | None:
    """Return a skip result when the claimed task is no longer ours."""
    if row is None:
        return ProcessingResult(action=PROCESSING_ACTION_MISSING_TASK)
    if row["task_status"] != TASK_STATUS_RUNNING:
        return ProcessingResult(
            action=PROCESSING_ACTION_NOT_RUNNING,
            order_id=row["order_id"],
        )
    if row["locked_by"] != worker_id:
        return ProcessingResult(
            action=PROCESSING_ACTION_LOST_OWNERSHIP,
            order_id=row["order_id"],
        )
    return None


def _reschedule_claimed_task(
    cur,
    *,
    task_id: str,
    worker_id: str,
    next_run_at: datetime,
    last_error: str,
    updated_at: datetime,
) -> bool:
    """Release a claimed task back to pending after a retryable failure."""
    # attempts counts consumed downstream tries, not claim attempts. Completing a
    # task successfully leaves attempts unchanged; only retryable failures and
    # final failure consume the retry budget.
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            attempts = attempts + 1,
            next_run_at = %s,
            last_error = %s,
            locked_by = null,
            locked_until = null,
            updated_at = %s
        where
            id = %s::uuid
            and status = %s::task_status
            and locked_by = %s
        """,
        (
            TASK_STATUS_PENDING,
            next_run_at,
            last_error,
            updated_at,
            task_id,
            TASK_STATUS_RUNNING,
            worker_id,
        ),
    )
    return cur.rowcount == 1


def _reschedule_poll_task(
    cur,
    *,
    task_id: str,
    worker_id: str,
    next_run_at: datetime,
    updated_at: datetime,
) -> bool:
    """Release a poll task after a normal not-ready response."""
    # not_ready is expected business progress, not a downstream error. Keep the
    # retry budget intact and clear any older transient last_error.
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            next_run_at = %s,
            last_error = null,
            locked_by = null,
            locked_until = null,
            updated_at = %s
        where
            id = %s::uuid
            and status = %s::task_status
            and locked_by = %s
        """,
        (
            TASK_STATUS_PENDING,
            next_run_at,
            updated_at,
            task_id,
            TASK_STATUS_RUNNING,
            worker_id,
        ),
    )
    return cur.rowcount == 1


def _fail_claimed_task(
    cur,
    *,
    task_id: str,
    worker_id: str,
    last_error: str,
    updated_at: datetime,
) -> bool:
    """Mark a claimed task failed after exhausting retry attempts."""
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            attempts = attempts + 1,
            last_error = %s,
            locked_by = null,
            locked_until = null,
            updated_at = %s
        where
            id = %s::uuid
            and status = %s::task_status
            and locked_by = %s
        """,
        (
            TASK_STATUS_FAILED,
            last_error,
            updated_at,
            task_id,
            TASK_STATUS_RUNNING,
            worker_id,
        ),
    )
    return cur.rowcount == 1


def _fail_claimed_task_without_attempt_increment(
    cur,
    *,
    task_id: str,
    worker_id: str,
    last_error: str,
    updated_at: datetime,
) -> bool:
    """Mark a claimed task failed for a non-transient business timeout."""
    # Deadline failure is a separate control from max_attempts. Preserve
    # attempts so dashboards can distinguish business timeout from transport
    # errors that consumed retry budget.
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            last_error = %s,
            locked_by = null,
            locked_until = null,
            updated_at = %s
        where
            id = %s::uuid
            and status = %s::task_status
            and locked_by = %s
        """,
        (
            TASK_STATUS_FAILED,
            last_error,
            updated_at,
            task_id,
            TASK_STATUS_RUNNING,
            worker_id,
        ),
    )
    return cur.rowcount == 1


def _retry_or_fail_claimed_task(
    cur,
    *,
    task_id: str,
    worker_id: str,
    order_id: str,
    from_state: str,
    to_state: str,
    attempts: int,
    max_attempts: int,
    next_run_at: datetime,
    last_error: str,
    updated_at: datetime,
) -> ProcessingResult:
    """Consume one retryable failure and either reschedule or fail the task."""
    next_attempts = attempts + 1
    if next_attempts >= max_attempts:
        failed = _fail_claimed_task(
            cur,
            task_id=task_id,
            worker_id=worker_id,
            last_error=last_error,
            updated_at=updated_at,
        )
        if failed:
            return ProcessingResult(
                action=PROCESSING_ACTION_FAILED,
                order_id=order_id,
                from_state=from_state,
                to_state=to_state,
                error=last_error,
            )

        return ProcessingResult(
            action=PROCESSING_ACTION_LOST_OWNERSHIP,
            order_id=order_id,
            from_state=from_state,
            to_state=to_state,
            error=last_error,
        )

    scheduled = _reschedule_claimed_task(
        cur,
        task_id=task_id,
        worker_id=worker_id,
        next_run_at=next_run_at,
        last_error=last_error,
        updated_at=updated_at,
    )
    if scheduled:
        return ProcessingResult(
            action=PROCESSING_ACTION_RETRY_SCHEDULED,
            order_id=order_id,
            from_state=from_state,
            to_state=to_state,
            error=last_error,
        )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=order_id,
        from_state=from_state,
        to_state=to_state,
        error=last_error,
    )


def _prepare_transition_call(
    *,
    task: ClaimedTask,
    worker_id: str,
    spec: RestaurantTransitionSpec,
) -> RestaurantTransitionCall | ProcessingResult:
    """Validate the claimed task and return details for the simulator call."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # This first DB phase is deliberately short: verify we still own
                # the task and either return the data needed for the HTTP call or
                # complete the task as a no-op. The network call happens later.
                row = _load_transition_task_row(
                    cur,
                    task_id=task.id,
                    source_state=spec.source_state,
                )
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != spec.source_state:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                return RestaurantTransitionCall(
                    order_id=order_id,
                    restaurant_ref=row["restaurant_ref"],
                    source_started_at=row["source_started_at"],
                    spec=spec,
                )


def _call_restaurant_transition(
    call: RestaurantTransitionCall,
) -> TransitionCallOutcome | str:
    """Call the simulator and return an outcome or short retryable error."""
    url = f"{_downstream_sim_base_url()}{call.spec.endpoint_path}"
    payload = {
        "order_id": call.order_id,
        "restaurant_ref": call.restaurant_ref,
        call.spec.source_started_at_field: call.source_started_at.isoformat(),
    }

    # This is intentionally not full durable downstream idempotency yet. These
    # simulator endpoints are deterministic and side-effect-free for this slice,
    # so retrying the same order_id is acceptable until downstream_calls exists.
    # Timeouts and connection drops are treated as retryable because the worker
    # cannot tell whether downstream received the request.
    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling {call.spec.action_name}: {exc}")
    except httpx.RequestError as exc:
        return _short_error(
            f"downstream request failed calling {call.spec.action_name}: {exc}"
        )

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from {call.spec.action_name}"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} "
            f"from {call.spec.action_name}"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict):
        return _short_error(
            f"downstream returned invalid {call.spec.action_name} body: {body!r}"
        )

    status = body.get("status")
    if status == call.spec.expected_status:
        return TransitionCallOutcome(status=call.spec.expected_status)
    if status == call.spec.pending_status:
        return TransitionCallOutcome(
            status=call.spec.pending_status,
            retry_after_seconds=_valid_positive_number(body.get("retry_after_seconds")),
        )

    return _short_error(
        f"downstream returned invalid {call.spec.action_name} body: {body!r}"
    )


def _prepare_courier_assign_call(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> CourierAssignCall | ProcessingResult:
    """Validate courier assignment work and return courier-named call details."""
    # Courier assignment uses the same short DB validation as command-style
    # restaurant transitions, then adapts the result to a courier-specific type
    # so the HTTP callsite does not look like a restaurant integration.
    prepared = _prepare_transition_call(
        task=task,
        worker_id=worker_id,
        spec=COURIER_ASSIGN_SPEC,
    )
    if isinstance(prepared, ProcessingResult):
        return prepared

    return CourierAssignCall(
        order_id=prepared.order_id,
        restaurant_ref=prepared.restaurant_ref,
        ready_at=prepared.source_started_at,
    )


def _call_courier_assign(
    call: CourierAssignCall,
) -> CourierAssignOutcome | str:
    """Call courier assign and return an outcome or retryable error."""
    url = f"{_downstream_sim_base_url()}/courier/assign"
    payload = {
        "order_id": call.order_id,
        "restaurant_ref": call.restaurant_ref,
        "ready_at": call.ready_at.isoformat(),
    }

    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling courier assign: {exc}")
    except httpx.RequestError as exc:
        return _short_error(
            f"downstream request failed calling courier assign: {exc}"
        )

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from courier assign"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} from courier assign"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict):
        return _short_error(
            f"downstream returned invalid courier assign body: {body!r}"
        )

    status = body.get("status")
    if status == COURIER_ASSIGN_STATUS_NOT_ASSIGNED:
        return CourierAssignOutcome(
            status=COURIER_ASSIGN_STATUS_NOT_ASSIGNED,
            retry_after_seconds=_valid_positive_number(body.get("retry_after_seconds")),
        )

    if status != COURIER_ASSIGN_STATUS_ASSIGNED:
        return _short_error(
            f"downstream returned invalid courier assign body: {body!r}"
        )

    courier_ref = body.get("courier_ref")
    if not courier_ref or not isinstance(courier_ref, str):
        return _short_error(
            f"downstream returned missing courier_ref in assign response: {body!r}"
        )

    return CourierAssignOutcome(
        status=COURIER_ASSIGN_STATUS_ASSIGNED,
        courier_ref=courier_ref,
    )


def _prepare_payment_check_call(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> PaymentCheckCall | ProcessingResult:
    """Validate the claimed check_payment task and return simulator call details."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_payment_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PLACED:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PLACED,
                        to_state=ORDER_STATE_PAYMENT_CHECK,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_payment task is missing deadline_at",
                        updated_at=occurred_at,
                    )

                if row["placed_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PLACED,
                        to_state=ORDER_STATE_PAYMENT_CHECK,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="missing order_created event for check_payment",
                        updated_at=occurred_at,
                    )

                return PaymentCheckCall(
                    order_id=order_id,
                    placed_at=row["placed_at"],
                )


def _call_payment_check(call: PaymentCheckCall) -> PaymentCheckOutcome | str:
    """Call the payment simulator and return an outcome or retryable error."""
    url = f"{_downstream_sim_base_url()}/payment/check"
    payload = {
        "order_id": call.order_id,
        "placed_at": call.placed_at.isoformat(),
    }

    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling payment check: {exc}")
    except httpx.RequestError as exc:
        return _short_error(f"downstream request failed calling payment check: {exc}")

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from payment check"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} from payment check"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict):
        return _short_error(f"downstream returned invalid payment check body: {body!r}")

    status = body.get("status")
    if status == PAYMENT_CHECK_STATUS_AUTHORIZED:
        return PaymentCheckOutcome(status=PAYMENT_CHECK_STATUS_AUTHORIZED)
    if status == PAYMENT_CHECK_STATUS_PENDING:
        return PaymentCheckOutcome(
            status=PAYMENT_CHECK_STATUS_PENDING,
            retry_after_seconds=_valid_positive_number(body.get("retry_after_seconds")),
        )

    return _short_error(f"downstream returned invalid payment check body: {body!r}")


def _reschedule_payment_check_after_error(
    *,
    task: ClaimedTask,
    worker_id: str,
    order_id: str,
    error: str,
) -> ProcessingResult:
    """Consume retry budget after a failed payment check request."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_payment_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PLACED:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                return _retry_or_fail_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    order_id=order_id,
                    from_state=ORDER_STATE_PLACED,
                    to_state=ORDER_STATE_PAYMENT_CHECK,
                    attempts=row["attempts"],
                    max_attempts=row["max_attempts"],
                    next_run_at=updated_at
                    + timedelta(
                        seconds=_task_retry_delay_seconds(attempts=row["attempts"])
                    ),
                    last_error=error,
                    updated_at=updated_at,
                )


def _reschedule_payment_check_after_pending(
    *,
    task: ClaimedTask,
    worker_id: str,
    outcome: PaymentCheckOutcome,
) -> ProcessingResult:
    """Release check_payment after a normal payment-pending response."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_payment_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PLACED:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PLACED,
                        to_state=ORDER_STATE_PAYMENT_CHECK,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=updated_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_payment task is missing deadline_at",
                        updated_at=updated_at,
                    )

                if updated_at >= row["deadline_at"]:
                    error = "payment check deadline exceeded"
                    failed = _fail_claimed_task_without_attempt_increment(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        last_error=error,
                        updated_at=updated_at,
                    )
                    if failed:
                        return ProcessingResult(
                            action=PROCESSING_ACTION_FAILED,
                            order_id=order_id,
                            from_state=ORDER_STATE_PLACED,
                            to_state=ORDER_STATE_PAYMENT_CHECK,
                            error=error,
                        )

                    return ProcessingResult(
                        action=PROCESSING_ACTION_LOST_OWNERSHIP,
                        order_id=order_id,
                        from_state=ORDER_STATE_PLACED,
                        to_state=ORDER_STATE_PAYMENT_CHECK,
                        error=error,
                    )

                next_run_at = _bounded_payment_check_next_run_at(
                    now=updated_at,
                    retry_after_seconds=outcome.retry_after_seconds,
                    deadline_at=row["deadline_at"],
                )
                scheduled = _reschedule_poll_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=next_run_at,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_POLL_SCHEDULED,
                        order_id=order_id,
                        from_state=ORDER_STATE_PLACED,
                        to_state=ORDER_STATE_PAYMENT_CHECK,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=task.order_id,
        from_state=ORDER_STATE_PLACED,
        to_state=ORDER_STATE_PAYMENT_CHECK,
    )


def _prepare_ready_check_call(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> RestaurantReadyCheckCall | ProcessingResult:
    """Validate the claimed check_ready task and return simulator call details."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_ready_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PREPARING:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PREPARING,
                        to_state=ORDER_STATE_READY,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_ready task is missing deadline_at",
                        updated_at=occurred_at,
                    )

                if row["prep_started_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PREPARING,
                        to_state=ORDER_STATE_READY,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="missing confirmed -> preparing event for check_ready",
                        updated_at=occurred_at,
                    )

                return RestaurantReadyCheckCall(
                    order_id=order_id,
                    restaurant_ref=row["restaurant_ref"],
                    prep_started_at=row["prep_started_at"],
                )


def _call_restaurant_ready_check(
    call: RestaurantReadyCheckCall,
) -> RestaurantReadyCheckOutcome | str:
    """Call the readiness simulator and return an outcome or retryable error."""
    url = f"{_downstream_sim_base_url()}/restaurant/check-ready"
    payload = {
        "order_id": call.order_id,
        "restaurant_ref": call.restaurant_ref,
        "prep_started_at": call.prep_started_at.isoformat(),
    }

    # no response is different from not_ready: no response consumes error retry
    # budget because the worker cannot trust downstream state; not_ready is a
    # valid business response and keeps attempts unchanged.
    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling restaurant check-ready: {exc}")
    except httpx.RequestError as exc:
        return _short_error(
            f"downstream request failed calling restaurant check-ready: {exc}"
        )

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from restaurant check-ready"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} "
            "from restaurant check-ready"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict):
        return _short_error(f"downstream returned invalid check-ready body: {body!r}")

    status = body.get("status")
    if status == READY_CHECK_STATUS_READY:
        return RestaurantReadyCheckOutcome(status=READY_CHECK_STATUS_READY)
    if status == READY_CHECK_STATUS_NOT_READY:
        return RestaurantReadyCheckOutcome(
            status=READY_CHECK_STATUS_NOT_READY,
            retry_after_seconds=_valid_positive_number(
                body.get("retry_after_seconds")
            ),
        )

    return _short_error(f"downstream returned invalid check-ready body: {body!r}")


def _reschedule_ready_check_after_error(
    *,
    task: ClaimedTask,
    worker_id: str,
    order_id: str,
    error: str,
) -> ProcessingResult:
    """Consume retry budget after a failed readiness check request."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_ready_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PREPARING:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                return _retry_or_fail_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    order_id=order_id,
                    from_state=ORDER_STATE_PREPARING,
                    to_state=ORDER_STATE_READY,
                    attempts=row["attempts"],
                    max_attempts=row["max_attempts"],
                    next_run_at=updated_at
                    + timedelta(
                        seconds=_task_retry_delay_seconds(attempts=row["attempts"])
                    ),
                    last_error=error,
                    updated_at=updated_at,
                )


def _reschedule_ready_check_after_not_ready(
    *,
    task: ClaimedTask,
    worker_id: str,
    outcome: RestaurantReadyCheckOutcome,
) -> ProcessingResult:
    """Release check_ready after a normal not-ready response."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_ready_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_PREPARING:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_PREPARING,
                        to_state=ORDER_STATE_READY,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=updated_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_ready task is missing deadline_at",
                        updated_at=updated_at,
                    )

                if updated_at >= row["deadline_at"]:
                    # Deadline failure is a business timeout after valid
                    # not_ready responses. It fails the task but keeps attempts
                    # unchanged because no transient downstream error occurred.
                    error = "restaurant ready deadline exceeded"
                    failed = _fail_claimed_task_without_attempt_increment(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        last_error=error,
                        updated_at=updated_at,
                    )
                    if failed:
                        return ProcessingResult(
                            action=PROCESSING_ACTION_FAILED,
                            order_id=order_id,
                            from_state=ORDER_STATE_PREPARING,
                            to_state=ORDER_STATE_READY,
                            error=error,
                        )

                    return ProcessingResult(
                        action=PROCESSING_ACTION_LOST_OWNERSHIP,
                        order_id=order_id,
                        from_state=ORDER_STATE_PREPARING,
                        to_state=ORDER_STATE_READY,
                        error=error,
                    )

                next_run_at = _bounded_ready_check_next_run_at(
                    now=updated_at,
                    retry_after_seconds=outcome.retry_after_seconds,
                    deadline_at=row["deadline_at"],
                )
                scheduled = _reschedule_poll_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=next_run_at,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_POLL_SCHEDULED,
                        order_id=order_id,
                        from_state=ORDER_STATE_PREPARING,
                        to_state=ORDER_STATE_READY,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=task.order_id,
        from_state=ORDER_STATE_PREPARING,
        to_state=ORDER_STATE_READY,
    )


def _insert_followup_task(
    cur,
    *,
    order_id: str,
    task_type: str,
    target_state: str,
    next_run_at: datetime,
    deadline_at: datetime | None,
    created_at: datetime,
) -> None:
    """Insert the next task for a committed lifecycle transition."""
    dedupe_key = f"{order_id}:{task_type}:{target_state}"
    # The partial unique index only prevents duplicate active follow-up tasks;
    # completed historical tasks with the same dedupe key remain queryable.
    cur.execute(
        """
        insert into order_tasks (
            id,
            order_id,
            task_type,
            target_state,
            status,
            next_run_at,
            deadline_at,
            dedupe_key,
            created_at,
            updated_at
        )
        values (
            %s,
            %s::uuid,
            %s::task_type,
            %s::order_state,
            %s::task_status,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        on conflict (dedupe_key)
            where dedupe_key is not null
                and status in (
                    'pending'::task_status,
                    'running'::task_status
                )
        do nothing
        """,
        (
            uuid4(),
            order_id,
            task_type,
            target_state,
            TASK_STATUS_PENDING,
            next_run_at,
            deadline_at,
            dedupe_key,
            created_at,
            created_at,
        ),
    )


def _followup_schedule_for_spec(
    *,
    spec: RestaurantTransitionSpec,
    occurred_at: datetime,
) -> tuple[datetime, datetime | None]:
    """Return next_run_at and deadline_at for a transition follow-up task."""
    if spec.next_task_type == TASK_TYPE_CHECK_READY:
        # Poll tasks need a business deadline independent from transport
        # max_attempts. A restaurant can keep saying "not ready" forever even
        # when every HTTP request succeeds.
        return (
            occurred_at + timedelta(seconds=_ready_check_initial_delay_seconds()),
            occurred_at + timedelta(seconds=_ready_check_deadline_seconds()),
        )

    if spec.next_task_type == TASK_TYPE_CHECK_DELIVERY:
        # Same rationale as check_ready: a courier that always says "in_transit"
        # would never exhaust max_attempts without a hard deadline.
        return (
            occurred_at + timedelta(seconds=_delivery_check_initial_delay_seconds()),
            occurred_at + timedelta(seconds=_delivery_check_deadline_seconds()),
        )

    return occurred_at, None


def _finalize_transition_task(
    *,
    task: ClaimedTask,
    worker_id: str,
    spec: RestaurantTransitionSpec,
    extra_order_fields: dict[str, object] | None = None,
) -> ProcessingResult:
    """Persist a restaurant or courier transition after downstream success.

    extra_order_fields is for caller-supplied columns that must be written
    alongside the state change (e.g. {"courier_ref": "..."} for courier assign).
    Column names come from internal callsites only and must be allowlisted here.
    """
    if extra_order_fields:
        unknown_columns = set(extra_order_fields) - ALLOWED_EXTRA_ORDER_COLUMNS
        if unknown_columns:
            raise ValueError(
                f"unknown extra_order_fields columns: {sorted(unknown_columns)}"
            )

    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # The downstream call may have taken longer than the task lease,
                # or another actor may have changed the order. Re-read and
                # re-check ownership before committing any local state change.
                row = _load_transition_task_row(
                    cur,
                    task_id=task.id,
                    source_state=spec.source_state,
                )
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != spec.source_state:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                # Optimistic locking on orders.version prevents a stale worker,
                # cancellation race, or expired-lease claimant from committing a
                # state transition after another actor already changed the row.
                extra_set_sql = (
                    "".join(
                        f",\n                        {col} = %s"
                        for col in extra_order_fields
                    )
                    if extra_order_fields
                    else ""
                )
                extra_set_params = (
                    tuple(extra_order_fields.values()) if extra_order_fields else ()
                )
                cur.execute(
                    f"""
                    update orders
                    set
                        state = %s::order_state,
                        version = version + 1,
                        updated_at = %s{extra_set_sql}
                    where
                        id = %s::uuid
                        and version = %s
                        and state = %s::order_state
                    """,
                    (spec.target_state, occurred_at)
                    + extra_set_params
                    + (order_id, row["version"], spec.source_state),
                )
                if cur.rowcount == 0:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_OPTIMISTIC_NOOP,
                        order_id=order_id,
                        from_state=spec.source_state,
                    )

                cur.execute(
                    """
                    insert into order_events (
                        id,
                        order_id,
                        event_type,
                        from_state,
                        to_state,
                        task_id,
                        worker_id,
                        occurred_at
                    )
                    values (
                        %s::uuid,
                        %s::uuid,
                        %s::event_type,
                        %s::order_state,
                        %s::order_state,
                        %s::uuid,
                        %s,
                        %s
                    )
                    """,
                    (
                        uuid4(),
                        order_id,
                        EVENT_TYPE_STATE_TRANSITION,
                        spec.source_state,
                        spec.target_state,
                        task.id,
                        worker_id,
                        occurred_at,
                    ),
                )
                if (
                    spec.next_task_type is not None
                    and spec.next_target_state is not None
                ):
                    # The follow-up task is created in the same transaction as
                    # the state transition. If the transition rolls back, the
                    # next stage cannot be claimed; if it commits, work exists.
                    next_run_at, deadline_at = _followup_schedule_for_spec(
                        spec=spec,
                        occurred_at=occurred_at,
                    )
                    _insert_followup_task(
                        cur,
                        order_id=order_id,
                        task_type=spec.next_task_type,
                        target_state=spec.next_target_state,
                        next_run_at=next_run_at,
                        deadline_at=deadline_at,
                        created_at=occurred_at,
                    )

                # Event insertion and task completion stay in the same
                # transaction as the order transition so the audit trail cannot
                # show a completed task without the matching state event.
                _complete_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    completed_at=occurred_at,
                )
                return ProcessingResult(
                    action=PROCESSING_ACTION_TRANSITIONED,
                    order_id=order_id,
                    from_state=spec.source_state,
                    to_state=spec.target_state,
                )


def _reschedule_transition_after_pending(
    *,
    task: ClaimedTask,
    worker_id: str,
    spec: RestaurantTransitionSpec,
    retry_after_seconds: float | None,
) -> ProcessingResult:
    """Release a transition task after a valid downstream "not yet" response."""
    updated_at = datetime.now(timezone.utc)
    # Pending statuses are normal business progress. Use downstream's retry
    # hint when present; otherwise fall back to the base retry delay to avoid a
    # hot loop while still leaving attempts untouched. Exponential backoff is
    # only for real retryable errors that consume attempts.
    poll_delay_seconds = retry_after_seconds or _task_retry_base_delay_seconds()
    next_run_at = updated_at + timedelta(seconds=poll_delay_seconds)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_transition_task_row(
                    cur,
                    task_id=task.id,
                    source_state=spec.source_state,
                )
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != spec.source_state:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                scheduled = _reschedule_poll_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=next_run_at,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_POLL_SCHEDULED,
                        order_id=order_id,
                        from_state=spec.source_state,
                        to_state=spec.target_state,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=task.order_id,
        from_state=spec.source_state,
        to_state=spec.target_state,
    )


def _reschedule_transition_task(
    *,
    task: ClaimedTask,
    worker_id: str,
    order_id: str,
    error: str,
    spec: RestaurantTransitionSpec,
) -> ProcessingResult:
    """Release a transition task for a later retry."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # Lock the task before deciding whether this failure consumes
                # the final attempt. That keeps concurrent expired-lease
                # claimants from double-counting retries.
                cur.execute(
                    """
                    select
                        id::text as task_id,
                        order_id::text,
                        status::text as task_status,
                        locked_by,
                        attempts,
                        max_attempts
                    from order_tasks
                    where id = %s::uuid
                    for update
                    """,
                    (task.id,),
                )
                row = cur.fetchone()
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                # A failed downstream call leaves the order in the spec's source
                # state. The only durable task change is either releasing it for
                # a later retry or marking it failed after max_attempts is
                # exhausted, without pretending the restaurant completed it.
                next_attempts = row["attempts"] + 1
                if next_attempts >= row["max_attempts"]:
                    failed = _fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        last_error=error,
                        updated_at=updated_at,
                    )
                    if failed:
                        return ProcessingResult(
                            action=PROCESSING_ACTION_FAILED,
                            order_id=order_id,
                            from_state=spec.source_state,
                            to_state=spec.target_state,
                            error=error,
                        )

                    return ProcessingResult(
                        action=PROCESSING_ACTION_LOST_OWNERSHIP,
                        order_id=order_id,
                        from_state=spec.source_state,
                        to_state=spec.target_state,
                        error=error,
                    )

                scheduled = _reschedule_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=updated_at
                    + timedelta(
                        seconds=_task_retry_delay_seconds(attempts=row["attempts"])
                    ),
                    last_error=error,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_RETRY_SCHEDULED,
                        order_id=order_id,
                        from_state=spec.source_state,
                        to_state=spec.target_state,
                        error=error,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=order_id,
        from_state=spec.source_state,
        to_state=spec.target_state,
        error=error,
    )


def process_payment_check_task(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> ProcessingResult:
    """Poll payment authorization and advance placed orders to payment_check."""
    prepared = _prepare_payment_check_call(task=task, worker_id=worker_id)
    if isinstance(prepared, ProcessingResult):
        return prepared

    outcome = _call_payment_check(prepared)
    if isinstance(outcome, str):
        return _reschedule_payment_check_after_error(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=outcome,
        )

    if outcome.status == PAYMENT_CHECK_STATUS_AUTHORIZED:
        return _finalize_transition_task(
            task=task,
            worker_id=worker_id,
            spec=PAYMENT_FINALIZATION_SPEC,
        )

    return _reschedule_payment_check_after_pending(
        task=task,
        worker_id=worker_id,
        outcome=outcome,
    )


def process_transition_task(
    *,
    task: ClaimedTask,
    worker_id: str,
    spec: RestaurantTransitionSpec,
) -> ProcessingResult:
    """Move a claimed restaurant advance task through its target transition."""
    prepared = _prepare_transition_call(task=task, worker_id=worker_id, spec=spec)
    if isinstance(prepared, ProcessingResult):
        return prepared

    # The HTTP request happens outside any transaction or row lock. A slow or
    # dead downstream service should not block other workers from claiming tasks
    # or reading/updating unrelated orders.
    outcome = _call_restaurant_transition(prepared)
    if isinstance(outcome, str):
        return _reschedule_transition_task(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=outcome,
            spec=spec,
        )

    if outcome.status == spec.pending_status:
        return _reschedule_transition_after_pending(
            task=task,
            worker_id=worker_id,
            spec=spec,
            retry_after_seconds=outcome.retry_after_seconds,
        )

    return _finalize_transition_task(task=task, worker_id=worker_id, spec=spec)


def process_courier_assign_task(*, task: ClaimedTask, worker_id: str) -> ProcessingResult:
    """Assign a courier and advance ready orders to out_for_delivery.

    Separated from process_transition_task so the courier_ref from the HTTP
    response can flow into the finalization transaction via extra_order_fields.
    """
    prepared = _prepare_courier_assign_call(task=task, worker_id=worker_id)
    if isinstance(prepared, ProcessingResult):
        return prepared

    outcome = _call_courier_assign(prepared)
    if isinstance(outcome, str):
        return _reschedule_transition_task(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=outcome,
            spec=COURIER_ASSIGN_SPEC,
        )

    if outcome.status == COURIER_ASSIGN_STATUS_NOT_ASSIGNED:
        return _reschedule_transition_after_pending(
            task=task,
            worker_id=worker_id,
            spec=COURIER_ASSIGN_SPEC,
            retry_after_seconds=outcome.retry_after_seconds,
        )

    return _finalize_transition_task(
        task=task,
        worker_id=worker_id,
        spec=COURIER_ASSIGN_SPEC,
        extra_order_fields={"courier_ref": outcome.courier_ref},
    )


def process_ready_check_task(*, task: ClaimedTask, worker_id: str) -> ProcessingResult:
    """Poll restaurant readiness and advance preparing orders to ready."""
    prepared = _prepare_ready_check_call(task=task, worker_id=worker_id)
    if isinstance(prepared, ProcessingResult):
        return prepared

    # The readiness poll is outside any transaction, just like command calls.
    # Workers must not hold DB locks while waiting on downstream.
    outcome = _call_restaurant_ready_check(prepared)
    if isinstance(outcome, str):
        return _reschedule_ready_check_after_error(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=outcome,
        )

    if outcome.status == READY_CHECK_STATUS_READY:
        return _finalize_transition_task(
            task=task,
            worker_id=worker_id,
            spec=READY_FINALIZATION_SPEC,
        )

    return _reschedule_ready_check_after_not_ready(
        task=task,
        worker_id=worker_id,
        outcome=outcome,
    )


def _prepare_delivery_check_call(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> CourierDeliveryCheckCall | ProcessingResult:
    """Validate the claimed check_delivery task and return simulator call details."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_delivery_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_OUT_FOR_DELIVERY:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=occurred_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_delivery task is missing deadline_at",
                        updated_at=occurred_at,
                    )

                if row["dispatched_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="missing ready -> out_for_delivery event for check_delivery",
                        updated_at=occurred_at,
                    )

                if row["courier_ref"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=occurred_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="missing courier_ref for check_delivery",
                        updated_at=occurred_at,
                    )

                return CourierDeliveryCheckCall(
                    order_id=order_id,
                    courier_ref=row["courier_ref"],
                    dispatched_at=row["dispatched_at"],
                )


def _call_courier_delivery_check(
    call: CourierDeliveryCheckCall,
) -> CourierDeliveryCheckOutcome | str:
    """Call the delivery simulator and return an outcome or retryable error."""
    url = f"{_downstream_sim_base_url()}/courier/check-delivery"
    payload = {
        "order_id": call.order_id,
        "courier_ref": call.courier_ref,
        "dispatched_at": call.dispatched_at.isoformat(),
    }

    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling courier check-delivery: {exc}")
    except httpx.RequestError as exc:
        return _short_error(
            f"downstream request failed calling courier check-delivery: {exc}"
        )

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from courier check-delivery"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} "
            "from courier check-delivery"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict):
        return _short_error(f"downstream returned invalid check-delivery body: {body!r}")

    status = body.get("status")
    if status == DELIVERY_CHECK_STATUS_DELIVERED:
        return CourierDeliveryCheckOutcome(status=DELIVERY_CHECK_STATUS_DELIVERED)
    if status == DELIVERY_CHECK_STATUS_IN_TRANSIT:
        return CourierDeliveryCheckOutcome(
            status=DELIVERY_CHECK_STATUS_IN_TRANSIT,
            retry_after_seconds=_valid_positive_number(body.get("retry_after_seconds")),
        )

    return _short_error(f"downstream returned invalid check-delivery body: {body!r}")


def _reschedule_delivery_check_after_error(
    *,
    task: ClaimedTask,
    worker_id: str,
    order_id: str,
    error: str,
) -> ProcessingResult:
    """Consume retry budget after a failed delivery check request."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_delivery_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_OUT_FOR_DELIVERY:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=row["order_id"],
                        from_state=order_state,
                    )

                return _retry_or_fail_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    order_id=order_id,
                    from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                    to_state=ORDER_STATE_DELIVERED,
                    attempts=row["attempts"],
                    max_attempts=row["max_attempts"],
                    next_run_at=updated_at
                    + timedelta(
                        seconds=_task_retry_delay_seconds(attempts=row["attempts"])
                    ),
                    last_error=error,
                    updated_at=updated_at,
                )


def _reschedule_delivery_check_after_in_transit(
    *,
    task: ClaimedTask,
    worker_id: str,
    outcome: CourierDeliveryCheckOutcome,
) -> ProcessingResult:
    """Release check_delivery after a normal in-transit response."""
    updated_at = datetime.now(timezone.utc)

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_delivery_check_task_row(cur, task_id=task.id)
                skip_result = _owned_task_check_result(row, worker_id=worker_id)
                if skip_result is not None:
                    return skip_result

                order_state = row["order_state"]
                order_id = row["order_id"]
                if is_terminal_order_state(order_state):
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if order_state != ORDER_STATE_OUT_FOR_DELIVERY:
                    _complete_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        completed_at=updated_at,
                    )
                    return ProcessingResult(
                        action=PROCESSING_ACTION_COMPLETED_STALE_NOOP,
                        order_id=order_id,
                        from_state=order_state,
                    )

                if row["deadline_at"] is None:
                    return _retry_or_fail_claimed_task(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        next_run_at=updated_at
                        + timedelta(
                            seconds=_task_retry_delay_seconds(
                                attempts=row["attempts"]
                            )
                        ),
                        last_error="check_delivery task is missing deadline_at",
                        updated_at=updated_at,
                    )

                if updated_at >= row["deadline_at"]:
                    # Deadline failure is a business timeout; the courier kept
                    # reporting in_transit past the window. Does not count as a
                    # transient error, so attempts is left unchanged.
                    error = "courier delivery deadline exceeded"
                    failed = _fail_claimed_task_without_attempt_increment(
                        cur,
                        task_id=task.id,
                        worker_id=worker_id,
                        last_error=error,
                        updated_at=updated_at,
                    )
                    if failed:
                        return ProcessingResult(
                            action=PROCESSING_ACTION_FAILED,
                            order_id=order_id,
                            from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                            to_state=ORDER_STATE_DELIVERED,
                            error=error,
                        )

                    return ProcessingResult(
                        action=PROCESSING_ACTION_LOST_OWNERSHIP,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                        error=error,
                    )

                next_run_at = _bounded_delivery_check_next_run_at(
                    now=updated_at,
                    retry_after_seconds=outcome.retry_after_seconds,
                    deadline_at=row["deadline_at"],
                )
                scheduled = _reschedule_poll_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=next_run_at,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_POLL_SCHEDULED,
                        order_id=order_id,
                        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
                        to_state=ORDER_STATE_DELIVERED,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=task.order_id,
        from_state=ORDER_STATE_OUT_FOR_DELIVERY,
        to_state=ORDER_STATE_DELIVERED,
    )


def process_delivery_check_task(*, task: ClaimedTask, worker_id: str) -> ProcessingResult:
    """Poll courier delivery status and advance out_for_delivery orders to delivered."""
    prepared = _prepare_delivery_check_call(task=task, worker_id=worker_id)
    if isinstance(prepared, ProcessingResult):
        return prepared

    outcome = _call_courier_delivery_check(prepared)
    if isinstance(outcome, str):
        return _reschedule_delivery_check_after_error(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=outcome,
        )

    if outcome.status == DELIVERY_CHECK_STATUS_DELIVERED:
        return _finalize_transition_task(
            task=task,
            worker_id=worker_id,
            spec=DELIVERY_FINALIZATION_SPEC,
        )

    return _reschedule_delivery_check_after_in_transit(
        task=task,
        worker_id=worker_id,
        outcome=outcome,
    )


def claim_and_process_one_task(*, worker_id: str) -> bool:
    """Claim one task and process the first supported lifecycle transition."""
    task = claim_one_task(worker_id=worker_id)
    if task is None:
        return False

    log = logger.info if _should_log_claim_at_info(task.id) else logger.debug
    log("claimed task %s for order %s by %s", task.id, task.order_id, worker_id)

    if task.task_type == TASK_TYPE_ADVANCE_STATE:
        if task.target_state == ORDER_STATE_OUT_FOR_DELIVERY:
            # Courier assign is handled separately to pass courier_ref from the
            # HTTP response into the finalization transaction.
            result = process_courier_assign_task(task=task, worker_id=worker_id)
        else:
            spec = RESTAURANT_TRANSITION_SPECS.get(task.target_state)
            if spec is not None:
                result = process_transition_task(
                    task=task,
                    worker_id=worker_id,
                    spec=spec,
                )
            else:
                result = None
    elif (
        task.task_type == TASK_TYPE_CHECK_PAYMENT
        and task.target_state == ORDER_STATE_PAYMENT_CHECK
    ):
        result = process_payment_check_task(task=task, worker_id=worker_id)
    elif (
        task.task_type == TASK_TYPE_CHECK_READY
        and task.target_state == ORDER_STATE_READY
    ):
        result = process_ready_check_task(task=task, worker_id=worker_id)
    elif (
        task.task_type == TASK_TYPE_CHECK_DELIVERY
        and task.target_state == ORDER_STATE_DELIVERED
    ):
        result = process_delivery_check_task(task=task, worker_id=worker_id)
    else:
        result = None

    if result is None:
        # Unsupported tasks are not failed here; a later slice may add the
        # handler. Push next_run_at forward so unsupported future work cannot
        # create a hot claim/release loop across all worker replicas.
        unsupported_error = _short_error(
            f"unsupported task type={task.task_type} target={task.target_state}; "
            "waiting for handler"
        )
        next_run_at = datetime.now(timezone.utc) + timedelta(
            seconds=_unsupported_task_release_delay_seconds()
        )
        logger.warning(
            "unsupported task %s type=%s target=%s; retrying after %s",
            task.id,
            task.task_type,
            task.target_state,
            next_run_at.isoformat(),
        )
        release_claimed_task(
            task_id=task.id,
            worker_id=worker_id,
            next_run_at=next_run_at,
            last_error=unsupported_error,
        )
        return True

    if result.action == PROCESSING_ACTION_TRANSITIONED:
        logger.info(
            "advanced order %s %s -> %s with task %s by %s",
            result.order_id,
            result.from_state,
            result.to_state,
            task.id,
            worker_id,
        )
    elif result.action == PROCESSING_ACTION_RETRY_SCHEDULED:
        logger.warning(
            "rescheduled task %s for order %s after downstream failure: %s",
            task.id,
            result.order_id,
            result.error,
        )
    elif result.action == PROCESSING_ACTION_POLL_SCHEDULED:
        logger.info(
            "rescheduled poll task %s for order %s after pending downstream response",
            task.id,
            result.order_id,
        )
    elif result.action == PROCESSING_ACTION_FAILED:
        logger.error(
            "failed task %s for order %s: %s",
            task.id,
            result.order_id,
            result.error,
        )
    elif result.action in COMPLETED_NOOP_ACTIONS:
        logger.info(
            "completed task %s as %s for order %s",
            task.id,
            result.action,
            result.order_id,
        )
    elif result.action in SKIPPED_AFTER_CLAIM_ACTIONS:
        logger.warning("skipped task %s after claim: %s", task.id, result.action)
    else:
        logger.warning("task %s returned unknown result %s", task.id, result.action)
    return True
