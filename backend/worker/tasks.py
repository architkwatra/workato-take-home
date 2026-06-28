import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
from psycopg.rows import dict_row

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_STATE_TRANSITION
from common.state_machine import (
    ORDER_STATE_CONFIRMED,
    ORDER_STATE_PLACED,
    is_terminal_order_state,
)
from common.task_types import (
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_TYPE_ADVANCE_STATE,
)


logger = logging.getLogger("worker.tasks")

LEASE_SECONDS = 30
DEFAULT_DOWNSTREAM_SIM_BASE_URL = "http://downstream-sim:8000"
DEFAULT_DOWNSTREAM_REQUEST_TIMEOUT_SECONDS = 2.0
DEFAULT_TASK_RETRY_DELAY_SECONDS = 5.0
MAX_LAST_ERROR_LENGTH = 300
_logged_claimed_task_ids: set[str] = set()

PROCESSING_ACTION_TRANSITIONED = "transitioned"
PROCESSING_ACTION_RETRY_SCHEDULED = "retry_scheduled"
PROCESSING_ACTION_FAILED = "failed"
PROCESSING_ACTION_MISSING_TASK = "missing_task"
PROCESSING_ACTION_NOT_RUNNING = "not_running"
PROCESSING_ACTION_LOST_OWNERSHIP = "lost_ownership"
PROCESSING_ACTION_COMPLETED_TERMINAL_NOOP = "completed_terminal_noop"
PROCESSING_ACTION_COMPLETED_STALE_NOOP = "completed_stale_noop"
PROCESSING_ACTION_COMPLETED_OPTIMISTIC_NOOP = "completed_optimistic_noop"

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
class RestaurantConfirmCall:
    """Details needed for one restaurant confirmation request."""

    order_id: str
    restaurant_ref: str


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


def _task_retry_delay_seconds() -> float:
    """Return how long a failed task waits before becoming claimable again."""
    return _read_positive_float_env(
        "TASK_RETRY_DELAY_SECONDS",
        DEFAULT_TASK_RETRY_DELAY_SECONDS,
    )


def _short_error(message: str) -> str:
    """Keep task last_error small enough for dashboards and logs."""
    cleaned = " ".join(message.split())
    if len(cleaned) <= MAX_LAST_ERROR_LENGTH:
        return cleaned
    return f"{cleaned[: MAX_LAST_ERROR_LENGTH - 3]}..."


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


def release_claimed_task(*, task_id: str, worker_id: str) -> bool:
    """Release a claimed task back to pending without completing it."""
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update order_tasks
                    set
                        status = %s::task_status,
                        locked_by = null,
                        locked_until = null,
                        updated_at = now()
                    where
                        id = %s::uuid
                        and status = %s::task_status
                        and locked_by = %s
                    """,
                    (
                        TASK_STATUS_PENDING,
                        task_id,
                        TASK_STATUS_RUNNING,
                        worker_id,
                    ),
                )
                return cur.rowcount == 1


def _complete_claimed_task(cur, *, task_id: str, worker_id: str, completed_at) -> bool:
    """Mark a claimed task completed, clearing its worker lease."""
    cur.execute(
        """
        update order_tasks
        set
            status = %s::task_status,
            completed_at = %s,
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


def _load_confirm_task_row(cur, *, task_id: str) -> dict | None:
    """Lock the task row and return current order details for confirmation."""
    cur.execute(
        """
        select
            task.id::text as task_id,
            task.order_id::text,
            task.status::text as task_status,
            task.locked_by,
            orders.state::text as order_state,
            orders.version,
            orders.restaurant_ref
        from order_tasks as task
        join orders on orders.id = task.order_id
        where task.id = %s::uuid
        for update of task
        """,
        (task_id,),
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


def _prepare_confirm_call(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> RestaurantConfirmCall | ProcessingResult:
    """Validate the claimed task and return details for the simulator call."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_confirm_task_row(cur, task_id=task.id)
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

                return RestaurantConfirmCall(
                    order_id=order_id,
                    restaurant_ref=row["restaurant_ref"],
                )


def _confirm_restaurant_order(confirm_call: RestaurantConfirmCall) -> str | None:
    """Call the simulator and return a short retryable error on failure."""
    url = f"{_downstream_sim_base_url()}/restaurant/confirm"
    payload = {
        "order_id": confirm_call.order_id,
        "restaurant_ref": confirm_call.restaurant_ref,
    }

    # This is intentionally not full durable downstream idempotency yet. The
    # simulator endpoint is deterministic and side-effect-free for this slice,
    # so retrying the same order_id is acceptable until downstream_calls exists.
    try:
        response = httpx.post(
            url,
            json=payload,
            timeout=_downstream_request_timeout_seconds(),
        )
    except httpx.TimeoutException as exc:
        return _short_error(f"downstream timeout calling restaurant confirm: {exc}")
    except httpx.RequestError as exc:
        return _short_error(f"downstream request failed calling restaurant confirm: {exc}")

    if response.status_code >= 500:
        return _short_error(
            f"downstream returned {response.status_code} from restaurant confirm"
        )
    if response.status_code != 200:
        return _short_error(
            f"downstream returned unexpected {response.status_code} from restaurant confirm"
        )

    try:
        body = response.json()
    except ValueError as exc:
        return _short_error(f"downstream returned invalid JSON: {exc}")

    if not isinstance(body, dict) or body.get("status") != "confirmed":
        return _short_error(f"downstream returned invalid confirmation body: {body!r}")

    return None


def _finalize_confirm_task(
    *,
    task: ClaimedTask,
    worker_id: str,
) -> ProcessingResult:
    """Persist the placed -> confirmed transition after downstream success."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                row = _load_confirm_task_row(cur, task_id=task.id)
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

                # Optimistic locking on orders.version prevents a stale worker,
                # cancellation race, or expired-lease claimant from committing a
                # state transition after another actor already changed the row.
                cur.execute(
                    """
                    update orders
                    set
                        state = %s::order_state,
                        version = version + 1,
                        updated_at = %s
                    where
                        id = %s::uuid
                        and version = %s
                        and state = %s::order_state
                    """,
                    (
                        ORDER_STATE_CONFIRMED,
                        occurred_at,
                        order_id,
                        row["version"],
                        ORDER_STATE_PLACED,
                    ),
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
                        from_state=ORDER_STATE_PLACED,
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
                        ORDER_STATE_PLACED,
                        ORDER_STATE_CONFIRMED,
                        task.id,
                        worker_id,
                        occurred_at,
                    ),
                )
                _complete_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    completed_at=occurred_at,
                )
                return ProcessingResult(
                    action=PROCESSING_ACTION_TRANSITIONED,
                    order_id=order_id,
                    from_state=ORDER_STATE_PLACED,
                    to_state=ORDER_STATE_CONFIRMED,
                )


def _reschedule_confirm_task(
    *,
    task: ClaimedTask,
    worker_id: str,
    order_id: str,
    error: str,
) -> ProcessingResult:
    """Release the confirm task for a later retry."""
    updated_at = datetime.now(timezone.utc)
    retry_delay = timedelta(seconds=_task_retry_delay_seconds())
    next_run_at = updated_at + retry_delay

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
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

                # A failed downstream call leaves the order in placed. The only
                # durable task change is either releasing it for a later retry
                # or marking it failed after max_attempts is exhausted, without
                # pretending the restaurant accepted the order.
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
                            error=error,
                        )

                    return ProcessingResult(
                        action=PROCESSING_ACTION_LOST_OWNERSHIP,
                        order_id=order_id,
                        error=error,
                    )

                scheduled = _reschedule_claimed_task(
                    cur,
                    task_id=task.id,
                    worker_id=worker_id,
                    next_run_at=next_run_at,
                    last_error=error,
                    updated_at=updated_at,
                )
                if scheduled:
                    return ProcessingResult(
                        action=PROCESSING_ACTION_RETRY_SCHEDULED,
                        order_id=order_id,
                        error=error,
                    )

    return ProcessingResult(
        action=PROCESSING_ACTION_LOST_OWNERSHIP,
        order_id=order_id,
        error=error,
    )


def process_confirm_task(*, task: ClaimedTask, worker_id: str) -> ProcessingResult:
    """Move a claimed initial advance task from placed to confirmed."""
    prepared = _prepare_confirm_call(task=task, worker_id=worker_id)
    if isinstance(prepared, ProcessingResult):
        return prepared

    # The HTTP request happens outside any transaction or row lock. A slow or
    # dead downstream service should not block other workers from claiming tasks
    # or reading/updating unrelated orders.
    error = _confirm_restaurant_order(prepared)
    if error is not None:
        return _reschedule_confirm_task(
            task=task,
            worker_id=worker_id,
            order_id=prepared.order_id,
            error=error,
        )

    return _finalize_confirm_task(task=task, worker_id=worker_id)


def claim_and_process_one_task(*, worker_id: str) -> bool:
    """Claim one task and process the first supported lifecycle transition."""
    task = claim_one_task(worker_id=worker_id)
    if task is None:
        return False

    log = logger.info if task.id not in _logged_claimed_task_ids else logger.debug
    _logged_claimed_task_ids.add(task.id)
    log("claimed task %s for order %s by %s", task.id, task.order_id, worker_id)

    if (
        task.task_type != TASK_TYPE_ADVANCE_STATE
        or task.target_state != ORDER_STATE_CONFIRMED
    ):
        logger.warning(
            "unsupported task %s type=%s target=%s; releasing for later slice",
            task.id,
            task.task_type,
            task.target_state,
        )
        release_claimed_task(task_id=task.id, worker_id=worker_id)
        return True

    result = process_confirm_task(task=task, worker_id=worker_id)
    if result.action == PROCESSING_ACTION_TRANSITIONED:
        logger.info(
            "confirmed order %s with task %s by %s",
            result.order_id,
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
    elif result.action == PROCESSING_ACTION_FAILED:
        logger.error(
            "failed task %s for order %s after exhausting retries: %s",
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
