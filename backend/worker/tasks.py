import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

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
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_TYPE_ADVANCE_STATE,
)


logger = logging.getLogger("worker.tasks")

LEASE_SECONDS = 30
_logged_claimed_task_ids: set[str] = set()


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


def process_confirm_task(*, task: ClaimedTask, worker_id: str) -> ProcessingResult:
    """Move a claimed initial advance task from placed to confirmed."""
    occurred_at = datetime.now(timezone.utc)
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    select
                        task.id::text as task_id,
                        task.order_id::text,
                        task.status::text as task_status,
                        task.locked_by,
                        orders.state::text as order_state,
                        orders.version
                    from order_tasks as task
                    join orders on orders.id = task.order_id
                    where task.id = %s::uuid
                    for update of task, orders
                    """,
                    (task.id,),
                )
                row = cur.fetchone()
                if row is None:
                    return ProcessingResult(action="missing_task")
                if row["task_status"] != TASK_STATUS_RUNNING:
                    return ProcessingResult(
                        action="not_running",
                        order_id=row["order_id"],
                    )
                if row["locked_by"] != worker_id:
                    return ProcessingResult(
                        action="lost_ownership",
                        order_id=row["order_id"],
                    )

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
                        action="completed_terminal_noop",
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
                        action="completed_stale_noop",
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
                        action="completed_optimistic_noop",
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
                    action="transitioned",
                    order_id=order_id,
                    from_state=ORDER_STATE_PLACED,
                    to_state=ORDER_STATE_CONFIRMED,
                )


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
    if result.action == "transitioned":
        logger.info(
            "confirmed order %s with task %s by %s",
            result.order_id,
            task.id,
            worker_id,
        )
    elif result.action in {
        "completed_terminal_noop",
        "completed_stale_noop",
        "completed_optimistic_noop",
    }:
        logger.info(
            "completed task %s as %s for order %s",
            task.id,
            result.action,
            result.order_id,
        )
    elif result.action in {"missing_task", "not_running", "lost_ownership"}:
        logger.warning("skipped task %s after claim: %s", task.id, result.action)
    else:
        logger.warning("task %s returned unknown result %s", task.id, result.action)
    return True
