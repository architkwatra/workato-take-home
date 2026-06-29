from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from common.db import open_db_connection
from common.event_types import EVENT_TYPE_RETRY_SCHEDULED
from common.task_types import (
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_TYPE_CHECK_READY,
)


def utc_now() -> datetime:
    """Return one timezone-aware timestamp for task recovery writes."""
    return datetime.now(timezone.utc)


def retry_failed_tasks_for_order(*, order_id: str) -> dict[str, Any] | None:
    """Reset failed tasks for an order so workers can try them again.

    This is an operator recovery path, not automatic retry. It keeps the order in
    its current state and only reopens failed task rows. Normal worker
    state/ownership checks still decide whether a reset task is useful or stale.
    """
    recovered_at = utc_now()

    with open_db_connection() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    select id::text
                    from orders
                    where id = %s::uuid
                    """,
                    (order_id,),
                )
                order_row = cur.fetchone()
                if order_row is None:
                    return None

                # Failed tasks are terminal until an operator explicitly resets
                # them. For poll tasks, start a fresh deadline window based on
                # the original window length; otherwise an expired check_ready
                # task would fail immediately after manual recovery.
                cur.execute(
                    """
                    with failed_task as (
                        select
                            id,
                            task_type,
                            target_state,
                            attempts,
                            max_attempts,
                            last_error,
                            created_at,
                            deadline_at
                        from order_tasks
                        where
                            order_id = %s::uuid
                            and status = %s::task_status
                        for update
                    )
                    update order_tasks as task
                    set
                        status = %s::task_status,
                        attempts = 0,
                        next_run_at = %s,
                        deadline_at = case
                            when failed_task.task_type = %s::task_type then
                                %s + greatest(
                                    coalesce(
                                        failed_task.deadline_at
                                            - failed_task.created_at,
                                        interval '60 seconds'
                                    ),
                                    interval '1 second'
                                )
                            else failed_task.deadline_at
                        end,
                        last_error = null,
                        locked_by = null,
                        locked_until = null,
                        completed_at = null,
                        updated_at = %s
                    from failed_task
                    where task.id = failed_task.id
                    returning
                        task.id::text as task_id,
                        task.task_type::text,
                        task.target_state::text,
                        task.next_run_at,
                        task.deadline_at,
                        failed_task.attempts as previous_attempts,
                        failed_task.max_attempts as previous_max_attempts,
                        failed_task.last_error as previous_last_error
                    """,
                    (
                        order_id,
                        TASK_STATUS_FAILED,
                        TASK_STATUS_PENDING,
                        recovered_at,
                        TASK_TYPE_CHECK_READY,
                        recovered_at,
                        recovered_at,
                    ),
                )
                retried_tasks = [dict(row) for row in cur.fetchall()]

                for task in retried_tasks:
                    cur.execute(
                        """
                        insert into order_events (
                            id,
                            order_id,
                            event_type,
                            task_id,
                            occurred_at,
                            metadata
                        )
                        values (
                            %s::uuid,
                            %s::uuid,
                            %s::event_type,
                            %s::uuid,
                            %s,
                            %s
                        )
                        """,
                        (
                            uuid4(),
                            order_id,
                            EVENT_TYPE_RETRY_SCHEDULED,
                            task["task_id"],
                            recovered_at,
                            Jsonb(
                                {
                                    "source": "operator_retry_failed_tasks",
                                    "task_type": task["task_type"],
                                    "target_state": task["target_state"],
                                    "previous_attempts": task["previous_attempts"],
                                    "previous_max_attempts": task[
                                        "previous_max_attempts"
                                    ],
                                    "previous_last_error": task[
                                        "previous_last_error"
                                    ],
                                }
                            ),
                        ),
                    )

                return {
                    "order_id": order_id,
                    "retried_count": len(retried_tasks),
                    "tasks": retried_tasks,
                }
