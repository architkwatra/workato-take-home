"""Order lifecycle constants shared by API and worker code.

The string values must match the Postgres `order_state` enum values created by
Alembic. Migrations intentionally keep their own copy of the enum values so old
migrations stay self-contained and do not change when application code changes.
"""

ORDER_STATE_PLACED = "placed"  # API accepted the order; downstream work has not started.
ORDER_STATE_CONFIRMED = "confirmed"  # Restaurant accepted the order.
ORDER_STATE_PREPARING = "preparing"  # Restaurant is preparing the food.
ORDER_STATE_READY = "ready"  # Restaurant marked the order ready for pickup.
ORDER_STATE_OUT_FOR_DELIVERY = "out_for_delivery"  # Courier has the delivery flow.
ORDER_STATE_DELIVERED = "delivered"  # Customer received the order; terminal success.
ORDER_STATE_CANCELLED = "cancelled"  # Order was cancelled; terminal stop.
ORDER_STATE_FAILED = "failed"  # Pipeline could not complete the order; terminal stop.

ORDER_STATES = (
    ORDER_STATE_PLACED,
    ORDER_STATE_CONFIRMED,
    ORDER_STATE_PREPARING,
    ORDER_STATE_READY,
    ORDER_STATE_OUT_FOR_DELIVERY,
    ORDER_STATE_DELIVERED,
    ORDER_STATE_CANCELLED,
    ORDER_STATE_FAILED,
)

ORDER_TRANSITIONS = {
    ORDER_STATE_PLACED: ORDER_STATE_CONFIRMED,
    ORDER_STATE_CONFIRMED: ORDER_STATE_PREPARING,
    ORDER_STATE_PREPARING: ORDER_STATE_READY,
    ORDER_STATE_READY: ORDER_STATE_OUT_FOR_DELIVERY,
    ORDER_STATE_OUT_FOR_DELIVERY: ORDER_STATE_DELIVERED,
}

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
