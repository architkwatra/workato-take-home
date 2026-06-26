"""Order task constants shared by API and worker code.

The string values must match the Postgres `task_type` and `task_status` enum
values. Migrations keep their own copy so historical migrations remain stable.
"""

# Task type constants describe what unit of work a worker should perform.
TASK_TYPE_ADVANCE_STATE = "advance_state"  # Move the order to target_state.
TASK_TYPE_CHECK_READY = "check_ready"  # Check restaurant prep without blocking.
TASK_TYPE_CHECK_PICKUP = "check_pickup"  # Check courier pickup progress.
TASK_TYPE_CHECK_DELIVERY = "check_delivery"  # Check courier delivery progress.

# Task statuses are durable queue row states, not customer-visible order states.
TASK_STATUS_PENDING = "pending"  # Waiting until next_run_at to be claimed.
TASK_STATUS_RUNNING = "running"  # Claimed by a worker lease.
TASK_STATUS_COMPLETED = "completed"  # Finished or safely no-op'd.
TASK_STATUS_FAILED = "failed"  # Exhausted real error retries.
TASK_STATUS_CANCELLED = "cancelled"  # Invalidated by terminal order state.

# Ordered lists are useful for validation/tests while keeping raw enum strings
# out of route and worker code.
ORDER_TASK_TYPES = (
    TASK_TYPE_ADVANCE_STATE,
    TASK_TYPE_CHECK_READY,
    TASK_TYPE_CHECK_PICKUP,
    TASK_TYPE_CHECK_DELIVERY,
)

ORDER_TASK_STATUSES = (
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
)
