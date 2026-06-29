import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from api.dashboard_store import get_dashboard_order_detail, get_dashboard_overview
from api.order_store import IdempotencyConflictError, cancel_order, create_or_get_order
from api.task_store import retry_failed_tasks_for_order
from common.db import (
    DatabaseConfigError,
    check_database_ready,
    close_db_pool,
    configure_db_pool,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the sync DB pool once per API process and close it on shutdown."""
    # The API uses synchronous psycopg from normal `def` handlers. FastAPI runs
    # those handlers in a threadpool, so blocking DB waits do not freeze the
    # event loop that accepts other requests.
    # Pool startup/shutdown are synchronous too, so run them off the event loop
    # to keep Uvicorn responsive to shutdown signals while Postgres is slow.
    await asyncio.to_thread(configure_db_pool)
    try:
        yield
    finally:
        await asyncio.to_thread(close_db_pool)


app = FastAPI(title="Order Pipeline API", lifespan=lifespan)


def _dashboard_cors_origins() -> list[str]:
    """Return browser origins allowed to read dashboard API data."""
    raw_origins = os.getenv("DASHBOARD_CORS_ORIGINS")
    if raw_origins is None:
        # Keep the no-config local demo working without using wildcard CORS.
        # The dashboard reads operational data, so a specific default origin is
        # safer than allowing every browser origin by accident.
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


class RetriedTaskResponse(BaseModel):
    """One failed task that an operator reset for another worker attempt."""

    task_id: str
    task_type: str
    target_state: str | None
    next_run_at: datetime
    deadline_at: datetime | None
    previous_attempts: int
    previous_max_attempts: int
    previous_last_error: str | None


class RetryFailedTasksResponse(BaseModel):
    """Operator response after reopening failed task rows for one order."""

    order_id: str
    retried_count: int
    tasks: list[RetriedTaskResponse]


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Report that the API process is alive and able to serve requests."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "api")}


@app.get("/readyz")
def readyz() -> dict[str, object]:
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
def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the API root."""
    return {"message": "Order Pipeline API scaffold"}


@app.get("/dashboard/overview")
def dashboard_overview() -> dict[str, Any]:
    """Return a read-only operations snapshot for the local dashboard."""
    try:
        return get_dashboard_overview()
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc


@app.get("/dashboard/orders/{order_id}")
def dashboard_order_detail(order_id: UUID) -> dict[str, Any]:
    """Return one order's current state, task rows, and event timeline."""
    try:
        result = get_dashboard_order_detail(order_id=str(order_id))
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="order not found",
        )

    return result


@app.post("/orders", response_model=OrderResponse)
def create_order(
    request: CreateOrderRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    """Create an order once, returning the existing row on duplicate submits."""
    # The client/loadgen owns this key so request retries cannot accidentally
    # create multiple orders when the API response is slow or dropped.
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
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Idempotency-Key is already used for a different order request",
                "idempotency_key": exc.idempotency_key,
                "existing_order_id": exc.existing_order_id,
                "existing": {
                    "restaurant_ref": exc.existing_restaurant_ref,
                    "customer_ref": exc.existing_customer_ref,
                },
                "requested": {
                    "restaurant_ref": exc.requested_restaurant_ref,
                    "customer_ref": exc.requested_customer_ref,
                },
            },
        ) from exc
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    response.status_code = (
        status.HTTP_201_CREATED if was_created else status.HTTP_200_OK
    )
    return order


@app.post("/orders/{order_id}/cancel", response_model=OrderResponse)
def cancel_existing_order(order_id: UUID) -> dict[str, Any]:
    """Cancel a non-terminal order and invalidate its open task rows."""
    try:
        result = cancel_order(order_id=str(order_id))
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="order not found",
        )

    return result


@app.post(
    "/orders/{order_id}/tasks/retry-failed",
    response_model=RetryFailedTasksResponse,
)
def retry_failed_order_tasks(order_id: UUID) -> dict[str, Any]:
    """Reopen failed tasks for an order without mutating the order state."""
    try:
        result = retry_failed_tasks_for_order(order_id=str(order_id))
    except DatabaseConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="postgres is not reachable") from exc

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="order not found",
        )

    return result
