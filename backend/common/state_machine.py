ORDER_STATES = (
    "placed",
    "confirmed",
    "preparing",
    "ready",
    "out_for_delivery",
    "delivered",
    "cancelled",
    "failed",
)

ORDER_TRANSITIONS = {
    "placed": "confirmed",
    "confirmed": "preparing",
    "preparing": "ready",
    "ready": "out_for_delivery",
    "out_for_delivery": "delivered",
}

TERMINAL_ORDER_STATES = frozenset({"delivered", "cancelled", "failed"})


def next_order_state(current_state: str) -> str | None:
    """Return the next normal lifecycle state, or None for terminal states."""
    return ORDER_TRANSITIONS.get(current_state)


def can_transition(from_state: str, to_state: str) -> bool:
    """Return whether a state change follows the supported lifecycle order."""
    return ORDER_TRANSITIONS.get(from_state) == to_state


def is_terminal_order_state(state: str) -> bool:
    """Return whether the order is in a state that workers must not advance."""
    return state in TERMINAL_ORDER_STATES
