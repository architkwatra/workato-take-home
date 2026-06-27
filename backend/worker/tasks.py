import logging
from dataclasses import dataclass
from datetime import datetime

from psycopg.rows import dict_row

from common.db import open_db_connection
from common.task_types import TASK_STATUS_PENDING, TASK_STATUS_RUNNING


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


def claim_one_task(*, worker_id: str) -> ClaimedTask | None:
    """Claim one eligible task for this worker, if any are runnable."""
    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                # The SELECT ... FOR UPDATE SKIP LOCKED subquery is the
                # concurrency boundary for competing workers: each worker skips
                # rows already locked by another transaction instead of waiting
                # behind it, so replicas can claim different tasks in parallel.
                # updated_at keeps Slice 7A's release-to-pending loop from
                # repeatedly selecting the same oldest row while other due tasks
                # wait behind it.
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
    """Release a Slice 7A claim back to pending without completing the task."""
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
                        id = %s
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


def claim_and_release_one_task(*, worker_id: str) -> bool:
    """Claim one task and immediately release it for the Slice 7A milestone."""
    task = claim_one_task(worker_id=worker_id)
    if task is None:
        return False

    log = logger.info if task.id not in _logged_claimed_task_ids else logger.debug
    _logged_claimed_task_ids.add(task.id)
    log("claimed task %s for order %s by %s", task.id, task.order_id, worker_id)

    released = release_claimed_task(task_id=task.id, worker_id=worker_id)
    if released:
        logger.debug("released task %s back to pending by %s", task.id, worker_id)
    else:
        logger.warning(
            "worker %s could not release task %s because ownership changed",
            worker_id,
            task.id,
        )
    return True
