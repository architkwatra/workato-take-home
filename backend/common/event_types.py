"""Order event constants shared by API, workers, and dashboard query code.

The string values must match the Postgres `event_type` enum values. Migrations
keep their own copy of those strings so historical migrations remain immutable.
"""

# Event constants avoid scattering raw DB enum strings across application code.
EVENT_TYPE_ORDER_CREATED = "order_created"  # API accepted a new order.
EVENT_TYPE_STATE_TRANSITION = "state_transition"  # Order moved between states.
EVENT_TYPE_COURIER_PICKED_UP = "courier_picked_up"  # Courier pickup milestone.
EVENT_TYPE_RETRY_SCHEDULED = "retry_scheduled"  # Retry was scheduled after an error.
EVENT_TYPE_ORDER_CANCELLED = "order_cancelled"  # Order reached cancelled terminal state.
EVENT_TYPE_ORDER_FAILED = "order_failed"  # Order reached failed terminal state.

# Ordered list of dashboard/audit event types. Internal task status changes stay
# in order_tasks and should not be added here unless they are user-visible.
ORDER_EVENT_TYPES = (
    EVENT_TYPE_ORDER_CREATED,
    EVENT_TYPE_STATE_TRANSITION,
    EVENT_TYPE_COURIER_PICKED_UP,
    EVENT_TYPE_RETRY_SCHEDULED,
    EVENT_TYPE_ORDER_CANCELLED,
    EVENT_TYPE_ORDER_FAILED,
)
