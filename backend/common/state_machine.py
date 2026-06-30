"""Order lifecycle constants shared by API and worker code.

The string values must match the Postgres `order_state` enum values created by
Alembic. Migrations intentionally keep their own copy of the enum values so old
migrations stay self-contained and do not change when application code changes.
"""

# Individual constants avoid spreading raw enum strings across API and worker
# code. If a state name changes, most application code should only need to
# update imports, not hunt for string literals.
ORDER_STATE_PLACED = "placed"  # API accepted the order; downstream work has not started.
ORDER_STATE_PAYMENT_CHECK = "payment_check"  # Payment authorization has completed.
ORDER_STATE_CONFIRMED = "confirmed"  # Restaurant accepted the order.
ORDER_STATE_PREPARING = "preparing"  # Restaurant is preparing the food.
ORDER_STATE_READY = "ready"  # Restaurant marked the order ready for pickup.
ORDER_STATE_OUT_FOR_DELIVERY = "out_for_delivery"  # Courier has the delivery flow.
ORDER_STATE_DELIVERED = "delivered"  # Customer received the order; terminal success.
ORDER_STATE_CANCELLED = "cancelled"  # Order was cancelled; terminal stop.
ORDER_STATE_FAILED = "failed"  # Pipeline could not complete the order; terminal stop.

# Ordered list of all valid order states. This is useful for validation,
# response schemas, tests, and any UI/API code that needs to enumerate states
# in the same order the lifecycle normally follows.
ORDER_STATES = (
    ORDER_STATE_PLACED,
    ORDER_STATE_PAYMENT_CHECK,
    ORDER_STATE_CONFIRMED,
    ORDER_STATE_PREPARING,
    ORDER_STATE_READY,
    ORDER_STATE_OUT_FOR_DELIVERY,
    ORDER_STATE_DELIVERED,
    ORDER_STATE_CANCELLED,
    ORDER_STATE_FAILED,
)

# Normal happy-path state machine. Workers use this to decide what the next
# lifecycle step should be for an `advance_state` task. Terminal and exceptional
# states are intentionally absent because workers must not auto-advance them.
ORDER_TRANSITIONS = {
    ORDER_STATE_PLACED: ORDER_STATE_PAYMENT_CHECK,
    ORDER_STATE_PAYMENT_CHECK: ORDER_STATE_CONFIRMED,
    ORDER_STATE_CONFIRMED: ORDER_STATE_PREPARING,
    ORDER_STATE_PREPARING: ORDER_STATE_READY,
    ORDER_STATE_READY: ORDER_STATE_OUT_FOR_DELIVERY,
    ORDER_STATE_OUT_FOR_DELIVERY: ORDER_STATE_DELIVERED,
}

# Terminal states are final from the worker's perspective. A worker that sees an
# order in one of these states should complete/no-op its task rather than making
# another downstream call or inserting follow-up work.
TERMINAL_ORDER_STATES = frozenset(
    {
        ORDER_STATE_DELIVERED,
        ORDER_STATE_CANCELLED,
        ORDER_STATE_FAILED,
    }
)


def next_order_state(current_state: str) -> str | None:
    """Return the next normal lifecycle state, or None for terminal states."""
    return ORDER_TRANSITIONS.get(current_state)


def can_transition(from_state: str, to_state: str) -> bool:
    """Return whether a state change follows the supported lifecycle order."""
    return ORDER_TRANSITIONS.get(from_state) == to_state


def is_terminal_order_state(state: str) -> bool:
    """Return whether the order is in a state that workers must not advance."""
    return state in TERMINAL_ORDER_STATES
