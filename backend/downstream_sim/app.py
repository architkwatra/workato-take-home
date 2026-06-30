import hashlib
import os
from datetime import datetime, timedelta, timezone
from threading import Lock

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


app = FastAPI(title="Downstream Simulator")

# Demo delays are deterministic per order and stage. The simulator responds
# immediately with a "not yet" status until now is past source_started_at +
# delay; it never sleeps inside an HTTP request.
DEFAULT_RESTAURANT_CONFIRM_AFTER_SECONDS = 3.0
DEFAULT_RESTAURANT_CONFIRM_RETRY_AFTER_SECONDS = 1.0
DEFAULT_PAYMENT_AUTHORIZE_AFTER_SECONDS = 2.0
DEFAULT_PAYMENT_AUTHORIZE_RETRY_AFTER_SECONDS = 1.0
DEFAULT_PAYMENT_UNAUTHORIZED_RATE = 0.0
DEFAULT_RESTAURANT_START_PREP_AFTER_SECONDS = 3.0
DEFAULT_RESTAURANT_START_PREP_RETRY_AFTER_SECONDS = 1.0
DEFAULT_RESTAURANT_READY_AFTER_SECONDS = 6.0
DEFAULT_RESTAURANT_READY_RETRY_AFTER_SECONDS = 2.0
DEFAULT_COURIER_ASSIGN_AFTER_SECONDS = 3.0
DEFAULT_COURIER_ASSIGN_RETRY_AFTER_SECONDS = 1.0
DEFAULT_COURIER_DELIVERED_AFTER_SECONDS = 8.0
DEFAULT_COURIER_DELIVERED_RETRY_AFTER_SECONDS = 3.0
SIMULATED_SERVICE_PAYMENT_AUTHORIZE = "payment_authorize"
SIMULATED_SERVICE_RESTAURANT_CONFIRM = "restaurant_confirm"
SIMULATED_SERVICE_RESTAURANT_START_PREP = "restaurant_start_prep"
SIMULATED_SERVICE_RESTAURANT_CHECK_READY = "restaurant_check_ready"
SIMULATED_SERVICE_COURIER_ASSIGN = "courier_assign"
SIMULATED_SERVICE_COURIER_CHECK_DELIVERY = "courier_check_delivery"
SIMULATED_SERVICES = (
    SIMULATED_SERVICE_PAYMENT_AUTHORIZE,
    SIMULATED_SERVICE_RESTAURANT_CONFIRM,
    SIMULATED_SERVICE_RESTAURANT_START_PREP,
    SIMULATED_SERVICE_RESTAURANT_CHECK_READY,
    SIMULATED_SERVICE_COURIER_ASSIGN,
    SIMULATED_SERVICE_COURIER_CHECK_DELIVERY,
)

_kill_switch_lock = Lock()
_kill_switches = {service_name: False for service_name in SIMULATED_SERVICES}


def _dashboard_cors_origins() -> list[str]:
    """Return browser origins allowed to use the simulator control API."""
    raw_origins = os.getenv("DOWNSTREAM_SIM_CORS_ORIGINS")
    if raw_origins is None:
        return ["http://localhost:3000"]

    origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    return origins or ["http://localhost:3000"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_dashboard_cors_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class RestaurantConfirmRequest(BaseModel):
    """Restaurant confirmation request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)
    payment_checked_at: datetime


class RestaurantConfirmResponse(BaseModel):
    """Deterministic restaurant confirmation response."""

    status: str
    retry_after_seconds: float | None = None


class PaymentAuthorizeRequest(BaseModel):
    """Payment authorization request sent by the worker."""

    order_id: str = Field(min_length=1)
    placed_at: datetime


class PaymentAuthorizeResponse(BaseModel):
    """Deterministic payment authorization response."""

    status: str
    retry_after_seconds: float | None = None
    reason: str | None = None


class RestaurantStartPrepRequest(BaseModel):
    """Restaurant start-prep request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)
    confirmed_at: datetime


class RestaurantStartPrepResponse(BaseModel):
    """Deterministic restaurant start-prep response."""

    status: str
    retry_after_seconds: float | None = None


class RestaurantCheckReadyRequest(BaseModel):
    """Restaurant readiness check request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)
    prep_started_at: datetime


class RestaurantCheckReadyResponse(BaseModel):
    """Deterministic restaurant readiness check response."""

    status: str
    retry_after_seconds: float | None = None


class CourierAssignRequest(BaseModel):
    """Courier assignment request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)
    ready_at: datetime


class CourierAssignResponse(BaseModel):
    """Deterministic courier assignment response."""

    status: str
    courier_ref: str | None = None
    retry_after_seconds: float | None = None


class CourierCheckDeliveryRequest(BaseModel):
    """Courier delivery status poll request sent by the worker."""

    order_id: str = Field(min_length=1)
    courier_ref: str = Field(min_length=1)
    dispatched_at: datetime


class CourierCheckDeliveryResponse(BaseModel):
    """Deterministic courier delivery status response."""

    status: str
    retry_after_seconds: float | None = None


class KillSwitchUpdateRequest(BaseModel):
    """Operator request to kill or restore one simulated downstream service."""

    killed: bool


class KillSwitchState(BaseModel):
    """Current kill-switch state for one simulated downstream service."""

    service: str
    killed: bool
    status: str


class KillSwitchOverviewResponse(BaseModel):
    """Dashboard response listing all simulator kill-switch states."""

    services: list[KillSwitchState]


def _kill_switch_state(service_name: str) -> dict[str, bool | str]:
    """Return the dashboard-facing state for one simulated dependency."""
    killed = _kill_switches[service_name]
    return {
        "service": service_name,
        "killed": killed,
        "status": "killed" if killed else "online",
    }


def _kill_switch_snapshot() -> list[dict[str, bool | str]]:
    """Return a consistent copy of all in-memory kill switches."""
    with _kill_switch_lock:
        return [_kill_switch_state(service_name) for service_name in SIMULATED_SERVICES]


def _set_kill_switch(service_name: str, killed: bool) -> dict[str, bool | str]:
    """Set one simulator kill switch after validating the service name."""
    if service_name not in _kill_switches:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown simulated service: {service_name}",
        )

    with _kill_switch_lock:
        _kill_switches[service_name] = killed
        return _kill_switch_state(service_name)


def _raise_if_service_killed(service_name: str) -> None:
    """Make worker-facing endpoints fail fast while a kill switch is enabled.

    The simulator process stays healthy so the dashboard can restore the switch.
    Kill switches are intentionally per endpoint so demos can break one stage
    without taking every payment, restaurant, or courier operation down at once.
    Workers see this as a real downstream 503 and exercise their retry paths.
    """
    with _kill_switch_lock:
        killed = _kill_switches[service_name]

    if killed:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"{service_name.replace('_', ' ')} simulator is killed",
        )


def _read_positive_float_env(env_name: str, default: float) -> float:
    """Read a positive float env var while keeping deterministic defaults."""
    raw_value = os.getenv(env_name)
    if raw_value is None:
        return default

    try:
        value = float(raw_value)
    except ValueError:
        return default

    return value if value > 0 else default


def _read_probability_env(env_name: str, default: float) -> float:
    """Read a 0..1 probability env var while keeping deterministic defaults."""
    raw_value = os.getenv(env_name)
    if raw_value is None:
        return default

    try:
        value = float(raw_value)
    except ValueError:
        return default

    return value if 0 <= value <= 1 else default


def _read_delay_range(
    *,
    min_env_name: str,
    max_env_name: str,
    fixed_env_name: str,
    default: float,
) -> tuple[float, float]:
    """Return the configured delay window for one simulator phase.

    A fixed delay env var remains supported for existing configs. When min/max
    env vars are present, the simulator uses the range to stagger orders while
    keeping the response contract unchanged for workers.
    """
    fixed_delay = _read_positive_float_env(fixed_env_name, default)
    raw_min = os.getenv(min_env_name)
    raw_max = os.getenv(max_env_name)
    if raw_min is None and raw_max is None:
        return fixed_delay, fixed_delay

    minimum = _read_positive_float_env(min_env_name, fixed_delay)
    maximum = _read_positive_float_env(max_env_name, minimum)
    if maximum < minimum:
        # Treat a misordered range as a fixed delay rather than failing every
        # simulator request. The bad config remains visible because all orders
        # collapse to the configured minimum.
        return minimum, minimum

    return minimum, maximum


def _stable_unit_interval(value: str) -> float:
    """Map a string to a stable 0..1 value for deterministic jitter."""
    digest = hashlib.sha256(value.encode()).digest()
    bucket = int.from_bytes(digest[:8], "big")
    return bucket / ((1 << 64) - 1)


def _deterministic_delay_seconds(
    *,
    order_id: str,
    salt: str,
    min_env_name: str,
    max_env_name: str,
    fixed_env_name: str,
    default: float,
) -> float:
    """Return a stable per-order delay inside the configured range.

    This is intentionally not random per request. A given order_id always gets
    the same delay for the same phase, so worker retries remain predictable and
    dashboard demos show real staggered state instead of fake response data.
    """
    minimum, maximum = _read_delay_range(
        min_env_name=min_env_name,
        max_env_name=max_env_name,
        fixed_env_name=fixed_env_name,
        default=default,
    )
    if minimum == maximum:
        return minimum

    return minimum + (
        _stable_unit_interval(f"{salt}:{order_id}") * (maximum - minimum)
    )


def _delayed_status_response(
    *,
    order_id: str,
    source_started_at: datetime,
    salt: str,
    min_env_name: str,
    max_env_name: str,
    fixed_env_name: str,
    default_delay_seconds: float,
    retry_after_env_name: str,
    default_retry_after_seconds: float,
    ready_status: str,
    pending_status: str,
) -> dict[str, float | str]:
    """Return success after the deterministic per-order delay has elapsed."""
    delay_seconds = _deterministic_delay_seconds(
        order_id=order_id,
        salt=salt,
        min_env_name=min_env_name,
        max_env_name=max_env_name,
        fixed_env_name=fixed_env_name,
        default=default_delay_seconds,
    )
    retry_after_seconds = _read_positive_float_env(
        retry_after_env_name,
        default_retry_after_seconds,
    )

    ready_at = _utc_datetime(source_started_at) + timedelta(seconds=delay_seconds)
    if datetime.now(timezone.utc) >= ready_at:
        return {"status": ready_status}

    return {
        "status": pending_status,
        "retry_after_seconds": retry_after_seconds,
    }


def _utc_datetime(value: datetime) -> datetime:
    """Normalize incoming timestamps so readiness math is timezone-safe."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the downstream simulator process is alive."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "downstream-sim")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report simulator readiness and current placeholder subsystem state."""
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "downstream-sim"),
        "simulator": {
            service["service"]: service["status"]
            for service in _kill_switch_snapshot()
        },
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the simulator root."""
    return {"message": "Downstream simulator scaffold"}


@app.get("/control/kill-switches", response_model=KillSwitchOverviewResponse)
async def get_kill_switches() -> dict[str, list[dict[str, bool | str]]]:
    """Return current kill-switch state for dashboard operators."""
    return {"services": _kill_switch_snapshot()}


@app.post(
    "/control/kill-switches/{service_name}",
    response_model=KillSwitchState,
)
async def update_kill_switch(
    service_name: str,
    request: KillSwitchUpdateRequest,
) -> dict[str, bool | str]:
    """Kill or restore one simulated downstream service for outage demos."""
    return _set_kill_switch(service_name, request.killed)


@app.post("/payment/authorize", response_model=PaymentAuthorizeResponse)
async def authorize_payment(
    request: PaymentAuthorizeRequest,
) -> dict[str, float | str]:
    """Authorize payment for an order after a deterministic per-order delay."""
    _raise_if_service_killed(SIMULATED_SERVICE_PAYMENT_AUTHORIZE)
    delay_response = _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.placed_at,
        salt="payment-authorize",
        min_env_name="PAYMENT_AUTHORIZE_AFTER_SECONDS_MIN",
        max_env_name="PAYMENT_AUTHORIZE_AFTER_SECONDS_MAX",
        fixed_env_name="PAYMENT_AUTHORIZE_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_PAYMENT_AUTHORIZE_AFTER_SECONDS,
        retry_after_env_name="PAYMENT_AUTHORIZE_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_PAYMENT_AUTHORIZE_RETRY_AFTER_SECONDS,
        ready_status="authorized",
        pending_status="pending",
    )
    if delay_response["status"] != "authorized":
        return delay_response

    unauthorized_rate = _read_probability_env(
        "PAYMENT_UNAUTHORIZED_RATE",
        DEFAULT_PAYMENT_UNAUTHORIZED_RATE,
    )
    unauthorized_bucket = _stable_unit_interval(
        f"payment-unauthorized:{request.order_id}"
    )
    if unauthorized_bucket < unauthorized_rate:
        return {
            "status": "unauthorized",
            "reason": "payment authorization failed",
        }

    return {"status": "authorized"}


@app.post("/restaurant/confirm", response_model=RestaurantConfirmResponse)
async def confirm_restaurant_order(
    request: RestaurantConfirmRequest,
) -> dict[str, float | str]:
    """Confirm that a restaurant accepted an order.

    Real restaurants may not acknowledge immediately. The simulator models that
    as quick "not_confirmed" responses until this order's deterministic delay
    from payment_checked_at has elapsed.
    """
    _raise_if_service_killed(SIMULATED_SERVICE_RESTAURANT_CONFIRM)
    return _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.payment_checked_at,
        salt="restaurant-confirm",
        min_env_name="RESTAURANT_CONFIRM_AFTER_SECONDS_MIN",
        max_env_name="RESTAURANT_CONFIRM_AFTER_SECONDS_MAX",
        fixed_env_name="RESTAURANT_CONFIRM_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_RESTAURANT_CONFIRM_AFTER_SECONDS,
        retry_after_env_name="RESTAURANT_CONFIRM_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_RESTAURANT_CONFIRM_RETRY_AFTER_SECONDS,
        ready_status="confirmed",
        pending_status="not_confirmed",
    )


@app.post("/restaurant/start-prep", response_model=RestaurantStartPrepResponse)
async def start_restaurant_prep(
    request: RestaurantStartPrepRequest,
) -> dict[str, float | str]:
    """Start restaurant preparation for an order.

    Starting prep is also delayed to make every lifecycle hop visible in the
    dashboard without blocking a worker thread or DB transaction.
    """
    _raise_if_service_killed(SIMULATED_SERVICE_RESTAURANT_START_PREP)
    return _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.confirmed_at,
        salt="restaurant-start-prep",
        min_env_name="RESTAURANT_START_PREP_AFTER_SECONDS_MIN",
        max_env_name="RESTAURANT_START_PREP_AFTER_SECONDS_MAX",
        fixed_env_name="RESTAURANT_START_PREP_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_RESTAURANT_START_PREP_AFTER_SECONDS,
        retry_after_env_name="RESTAURANT_START_PREP_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_RESTAURANT_START_PREP_RETRY_AFTER_SECONDS,
        ready_status="preparing",
        pending_status="not_preparing",
    )


@app.post("/restaurant/check-ready", response_model=RestaurantCheckReadyResponse)
async def check_restaurant_ready(
    request: RestaurantCheckReadyRequest,
) -> dict[str, float | str]:
    """Return whether enough deterministic prep time has elapsed for an order."""
    _raise_if_service_killed(SIMULATED_SERVICE_RESTAURANT_CHECK_READY)
    return _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.prep_started_at,
        salt="restaurant-ready",
        min_env_name="RESTAURANT_READY_AFTER_SECONDS_MIN",
        max_env_name="RESTAURANT_READY_AFTER_SECONDS_MAX",
        fixed_env_name="RESTAURANT_READY_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_RESTAURANT_READY_AFTER_SECONDS,
        retry_after_env_name="RESTAURANT_READY_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_RESTAURANT_READY_RETRY_AFTER_SECONDS,
        ready_status="ready",
        pending_status="not_ready",
    )


@app.post("/courier/assign", response_model=CourierAssignResponse)
async def assign_courier(request: CourierAssignRequest) -> dict[str, float | str]:
    """Assign a deterministic courier ref to an order.

    The ref is derived from order_id so repeated calls for the same order always
    return the same courier. No persistence is needed; the worker writes the ref
    to orders.courier_ref in the finalization transaction.
    """
    _raise_if_service_killed(SIMULATED_SERVICE_COURIER_ASSIGN)
    delay_response = _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.ready_at,
        salt="courier-assign",
        min_env_name="COURIER_ASSIGN_AFTER_SECONDS_MIN",
        max_env_name="COURIER_ASSIGN_AFTER_SECONDS_MAX",
        fixed_env_name="COURIER_ASSIGN_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_COURIER_ASSIGN_AFTER_SECONDS,
        retry_after_env_name="COURIER_ASSIGN_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_COURIER_ASSIGN_RETRY_AFTER_SECONDS,
        ready_status="assigned",
        pending_status="not_assigned",
    )
    if delay_response["status"] != "assigned":
        return delay_response

    digest = hashlib.md5(
        request.order_id.encode(),
        usedforsecurity=False,
    ).hexdigest()
    courier_ref = f"courier-{digest[:12]}"
    return {"status": "assigned", "courier_ref": courier_ref}


@app.post("/courier/check-delivery", response_model=CourierCheckDeliveryResponse)
async def check_courier_delivery(
    request: CourierCheckDeliveryRequest,
) -> dict[str, float | str]:
    """Return whether enough deterministic delivery time has elapsed for an order."""
    _raise_if_service_killed(SIMULATED_SERVICE_COURIER_CHECK_DELIVERY)
    return _delayed_status_response(
        order_id=request.order_id,
        source_started_at=request.dispatched_at,
        salt="courier-delivery",
        min_env_name="COURIER_DELIVERED_AFTER_SECONDS_MIN",
        max_env_name="COURIER_DELIVERED_AFTER_SECONDS_MAX",
        fixed_env_name="COURIER_DELIVERED_AFTER_SECONDS",
        default_delay_seconds=DEFAULT_COURIER_DELIVERED_AFTER_SECONDS,
        retry_after_env_name="COURIER_DELIVERED_RETRY_AFTER_SECONDS",
        default_retry_after_seconds=DEFAULT_COURIER_DELIVERED_RETRY_AFTER_SECONDS,
        ready_status="delivered",
        pending_status="in_transit",
    )
