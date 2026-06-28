import os

from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="Downstream Simulator")


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
