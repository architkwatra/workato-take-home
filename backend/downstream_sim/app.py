import hashlib
import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="Downstream Simulator")

# Demo prep duration used when RESTAURANT_READY_AFTER_SECONDS is not set. The
# simulator returns "ready" once now >= prep_started_at + this many seconds.
DEFAULT_RESTAURANT_READY_AFTER_SECONDS = 6.0
# Poll hint used when RESTAURANT_READY_RETRY_AFTER_SECONDS is not set. Workers
# cap this by the task deadline before scheduling the next check_ready attempt.
DEFAULT_RESTAURANT_READY_RETRY_AFTER_SECONDS = 2.0
# Courier delivery gate: "delivered" once now >= dispatched_at + this many seconds.
DEFAULT_COURIER_DELIVERED_AFTER_SECONDS = 8.0
# Poll hint returned with "in_transit". Workers cap this by the task deadline.
DEFAULT_COURIER_DELIVERED_RETRY_AFTER_SECONDS = 3.0


class RestaurantConfirmRequest(BaseModel):
    """Restaurant confirmation request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)


class RestaurantConfirmResponse(BaseModel):
    """Deterministic restaurant confirmation response."""

    status: str


class RestaurantStartPrepRequest(BaseModel):
    """Restaurant start-prep request sent by the worker."""

    order_id: str = Field(min_length=1)
    restaurant_ref: str = Field(min_length=1)


class RestaurantStartPrepResponse(BaseModel):
    """Deterministic restaurant start-prep response."""

    status: str


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


class CourierAssignResponse(BaseModel):
    """Deterministic courier assignment response."""

    status: str
    courier_ref: str


class CourierCheckDeliveryRequest(BaseModel):
    """Courier delivery status poll request sent by the worker."""

    order_id: str = Field(min_length=1)
    courier_ref: str = Field(min_length=1)
    dispatched_at: datetime


class CourierCheckDeliveryResponse(BaseModel):
    """Deterministic courier delivery status response."""

    status: str
    retry_after_seconds: float | None = None


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
        "simulator": {"restaurant": "idle", "courier": "idle"},
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the simulator root."""
    return {"message": "Downstream simulator scaffold"}


@app.post("/restaurant/confirm", response_model=RestaurantConfirmResponse)
async def confirm_restaurant_order(
    request: RestaurantConfirmRequest,
) -> dict[str, str]:
    """Confirm that a restaurant accepted an order.

    This first simulator slice is deterministic and side-effect-free. Repeated
    requests for the same order return the same response until durable
    downstream idempotency is added in a later slice.
    """
    return {"status": "confirmed"}


@app.post("/restaurant/start-prep", response_model=RestaurantStartPrepResponse)
async def start_restaurant_prep(
    request: RestaurantStartPrepRequest,
) -> dict[str, str]:
    """Start restaurant preparation for an order.

    Like confirmation, this is a deterministic command endpoint for the current
    demo slice. Repeated requests for the same order return the same response;
    durable idempotency records remain a later step.
    """
    return {"status": "preparing"}


@app.post("/restaurant/check-ready", response_model=RestaurantCheckReadyResponse)
async def check_restaurant_ready(
    request: RestaurantCheckReadyRequest,
) -> dict[str, float | str]:
    """Return whether enough deterministic prep time has elapsed for an order."""
    ready_after_seconds = _read_positive_float_env(
        "RESTAURANT_READY_AFTER_SECONDS",
        DEFAULT_RESTAURANT_READY_AFTER_SECONDS,
    )
    retry_after_seconds = _read_positive_float_env(
        "RESTAURANT_READY_RETRY_AFTER_SECONDS",
        DEFAULT_RESTAURANT_READY_RETRY_AFTER_SECONDS,
    )

    prep_started_at = _utc_datetime(request.prep_started_at)
    ready_at = prep_started_at + timedelta(seconds=ready_after_seconds)
    if datetime.now(timezone.utc) >= ready_at:
        return {"status": "ready"}

    return {
        "status": "not_ready",
        "retry_after_seconds": retry_after_seconds,
    }


@app.post("/courier/assign", response_model=CourierAssignResponse)
async def assign_courier(request: CourierAssignRequest) -> dict[str, str]:
    """Assign a deterministic courier ref to an order.

    The ref is derived from order_id so repeated calls for the same order always
    return the same courier. No persistence is needed; the worker writes the ref
    to orders.courier_ref in the finalization transaction.
    """
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
    delivered_after_seconds = _read_positive_float_env(
        "COURIER_DELIVERED_AFTER_SECONDS",
        DEFAULT_COURIER_DELIVERED_AFTER_SECONDS,
    )
    retry_after_seconds = _read_positive_float_env(
        "COURIER_DELIVERED_RETRY_AFTER_SECONDS",
        DEFAULT_COURIER_DELIVERED_RETRY_AFTER_SECONDS,
    )

    dispatched_at = _utc_datetime(request.dispatched_at)
    delivered_at = dispatched_at + timedelta(seconds=delivered_after_seconds)
    if datetime.now(timezone.utc) >= delivered_at:
        return {"status": "delivered"}

    return {
        "status": "in_transit",
        "retry_after_seconds": retry_after_seconds,
    }
