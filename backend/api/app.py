import os
from datetime import datetime

import psycopg
from fastapi import FastAPI, Header, HTTPException, Response, status
from pydantic import BaseModel, Field

from api.order_store import create_or_get_order
from common.db import DatabaseConfigError, check_database_ready


app = FastAPI(title="Order Pipeline API")


class CreateOrderRequest(BaseModel):
    """Request body for the first order-intake slice."""

    restaurant_ref: str = Field(min_length=1)
    customer_ref: str | None = None


class OrderResponse(BaseModel):
    """Stable API shape returned when an order is created or reused."""

    id: str
    idempotency_key: str
    state: str
    restaurant_ref: str
    customer_ref: str | None
    created_at: datetime
    updated_at: datetime


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the API process is alive and able to serve requests."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "api")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report API readiness by checking the Postgres connection."""
    try:
        check_database_ready()
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "api"),
        "dependencies": {"postgres": "ok"},
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the API root."""
    return {"message": "Order Pipeline API scaffold"}


@app.post("/orders", response_model=OrderResponse)
async def create_order(
    request: CreateOrderRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, object]:
    """Create an order once, returning the existing row on duplicate submits."""
    cleaned_key = idempotency_key.strip() if idempotency_key else ""
    if not cleaned_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key header is required",
        )

    try:
        order, was_created = create_or_get_order(
            idempotency_key=cleaned_key,
            restaurant_ref=request.restaurant_ref,
            customer_ref=request.customer_ref,
        )
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    response.status_code = (
        status.HTTP_201_CREATED if was_created else status.HTTP_200_OK
    )
    return order
