import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field


def utc_now() -> datetime:
    """Return one timezone-aware timestamp for status and run metadata."""
    return datetime.now(timezone.utc)


def format_timestamp(value: datetime | None) -> str | None:
    """Return JSON-native timestamps for normal responses and error details."""
    return value.isoformat() if value else None


def api_base_url() -> str:
    """Return the target API base URL configured for this loadgen service."""
    return os.getenv("API_BASE_URL", "http://api:8000").rstrip("/")


def positive_int_env(name: str, default: int) -> int:
    """Read a positive integer env var while keeping a safe default."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        parsed_value = int(raw_value)
    except ValueError:
        return default

    return parsed_value if parsed_value > 0 else default


class StartLoadRequest(BaseModel):
    """Operator request to start one bounded or unbounded loadgen run."""

    rate_per_second: float = Field(gt=0)
    max_orders: int | None = Field(default=None, gt=0)
    restaurant_ref: str = Field(default="restaurant-1", min_length=1)
    customer_ref_prefix: str = Field(default="loadgen-customer", min_length=1)


class UpdateRateRequest(BaseModel):
    """Operator request to change an active run's target send rate."""

    rate_per_second: float = Field(gt=0)


@dataclass
class LoadgenRunConfig:
    """Immutable per-run values used to generate deterministic order payloads."""

    run_id: str
    restaurant_ref: str
    customer_ref_prefix: str
    max_orders: int | None


class LoadGenerator:
    """In-memory controller for one persistent, single-replica load generator."""

    def __init__(self) -> None:
        # This is a high safety limit for the load generator itself, not a way
        # to protect the API. If the API slows down during a demo, that should
        # remain visible while preventing unbounded local task/memory growth.
        self._max_inflight = positive_int_env("LOADGEN_MAX_INFLIGHT", 1000)
        self._inflight_semaphore = asyncio.Semaphore(self._max_inflight)
        # The lock protects run state shared by FastAPI handlers, the producer
        # loop, and request completion callbacks.
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._producer_task: asyncio.Task[None] | None = None
        self._inflight_tasks: set[asyncio.Task[None]] = set()
        self._client: httpx.AsyncClient | None = None
        self._running = False
        self._run_config: LoadgenRunConfig | None = None
        self._rate_per_second = 0.0
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None
        self._attempted_count = 0
        self._successful_count = 0
        self._created_count = 0
        self._reused_count = 0
        self._failed_count = 0
        self._last_error: str | None = None

    async def open(self) -> None:
        """Create the reusable async HTTP client during service startup."""
        self._client = httpx.AsyncClient(base_url=api_base_url(), timeout=5.0)

    async def close(self) -> None:
        """Stop load generation and close HTTP resources during shutdown."""
        await self.stop()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def start(self, request: StartLoadRequest) -> dict[str, Any]:
        """Start one new run, rejecting the request if a run is already active."""
        async with self._lock:
            if self._running or self._inflight_tasks:
                conflict_message = (
                    "load generation is already running"
                    if self._running
                    else "previous load generation run is still draining"
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "message": conflict_message,
                        "status": self._status_unlocked(),
                    },
                )

            # A fresh run_id per successful start prevents idempotency key reuse
            # after stop/start cycles in the same container.
            self._run_config = LoadgenRunConfig(
                run_id=uuid4().hex,
                restaurant_ref=request.restaurant_ref,
                customer_ref_prefix=request.customer_ref_prefix,
                max_orders=request.max_orders,
            )
            self._rate_per_second = request.rate_per_second
            self._started_at = utc_now()
            self._stopped_at = None
            self._attempted_count = 0
            self._successful_count = 0
            self._created_count = 0
            self._reused_count = 0
            self._failed_count = 0
            self._last_error = None
            self._running = True
            self._wake_event = asyncio.Event()
            self._producer_task = asyncio.create_task(self._producer_loop())
            return self._status_unlocked()

    async def stop(self) -> dict[str, Any]:
        """Stop the active producer loop and keep final counters visible."""
        producer_task: asyncio.Task[None] | None = None
        inflight_tasks: set[asyncio.Task[None]] = set()
        async with self._lock:
            if self._running:
                self._running = False
                self._stopped_at = utc_now()
                self._wake_event.set()
                producer_task = self._producer_task

        # Await outside the lock so the producer can take the lock while exiting.
        if producer_task is not None:
            await producer_task

        async with self._lock:
            # Requests already sent before stop are allowed to finish so final
            # counters are stable before the operator starts another run.
            inflight_tasks = set(self._inflight_tasks)

        if inflight_tasks:
            await asyncio.gather(*inflight_tasks, return_exceptions=True)

        async with self._lock:
            return self._status_unlocked()

    async def update_rate(self, request: UpdateRateRequest) -> dict[str, Any]:
        """Apply a new rate to the active run and wake the producer promptly."""
        async with self._lock:
            if not self._running:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="load generation is not running",
                )

            self._rate_per_second = request.rate_per_second
            # Waking avoids stale sleeps when the operator dials traffic up/down.
            self._wake_event.set()
            return self._status_unlocked()

    async def status(self) -> dict[str, Any]:
        """Return the current or most recent run state for operators."""
        async with self._lock:
            return self._status_unlocked()

    def _status_unlocked(self) -> dict[str, Any]:
        """Build status while the caller already holds the state lock."""
        inflight_count = len(self._inflight_tasks)
        return {
            "service": os.getenv("SERVICE_NAME", "loadgen"),
            "running": self._running,
            "run_id": self._run_config.run_id if self._run_config else None,
            "rate_per_second": self._rate_per_second,
            "max_orders": self._run_config.max_orders if self._run_config else None,
            "started_at": format_timestamp(self._started_at),
            "stopped_at": format_timestamp(self._stopped_at),
            "attempted_count": self._attempted_count,
            "successful_count": self._successful_count,
            "created_count": self._created_count,
            "reused_count": self._reused_count,
            "failed_count": self._failed_count,
            "inflight_count": inflight_count,
            "max_inflight": self._max_inflight,
            "last_error": self._last_error,
        }

    async def _producer_loop(self) -> None:
        """Launch order-create requests until stopped or max_orders is reached."""
        next_send_at = asyncio.get_running_loop().time()

        while True:
            async with self._lock:
                if not self._running or self._run_config is None:
                    return

                if (
                    self._run_config.max_orders is not None
                    and self._attempted_count >= self._run_config.max_orders
                ):
                    self._running = False
                    self._stopped_at = utc_now()
                    return

                rate_per_second = self._rate_per_second

            wait_seconds = max(0.0, next_send_at - asyncio.get_running_loop().time())
            if wait_seconds > 0:
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=wait_seconds)
                    self._wake_event.clear()
                    next_send_at = asyncio.get_running_loop().time()
                    continue
                except TimeoutError:
                    pass

            await self._launch_order_request()
            next_send_at = asyncio.get_running_loop().time() + (1.0 / rate_per_second)

    async def _launch_order_request(self) -> None:
        """Reserve the next sequence number and launch one async order request."""
        await self._inflight_semaphore.acquire()
        async with self._lock:
            if not self._running or self._run_config is None:
                self._inflight_semaphore.release()
                return

            self._attempted_count += 1
            sequence = self._attempted_count
            run_config = self._run_config

        try:
            task = asyncio.create_task(self._send_order(run_config, sequence))
        except Exception:
            self._inflight_semaphore.release()
            raise

        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _send_order(self, run_config: LoadgenRunConfig, sequence: int) -> None:
        """Send one order request and record whether the API accepted or reused it."""
        try:
            await self._send_order_with_reserved_slot(run_config, sequence)
        finally:
            self._inflight_semaphore.release()

    async def _send_order_with_reserved_slot(
        self, run_config: LoadgenRunConfig, sequence: int
    ) -> None:
        """Send one order request after the caller has reserved capacity."""
        if self._client is None:
            async with self._lock:
                self._failed_count += 1
                self._last_error = "HTTP client is not initialized"
            return

        idempotency_key = f"loadgen-{run_config.run_id}-{sequence}"
        payload = {
            "restaurant_ref": run_config.restaurant_ref,
            "customer_ref": f"{run_config.customer_ref_prefix}-{sequence}",
        }

        try:
            response = await self._client.post(
                "/orders",
                headers={"Idempotency-Key": idempotency_key},
                json=payload,
            )
        except httpx.HTTPError as exc:
            async with self._lock:
                self._failed_count += 1
                self._last_error = str(exc)
            return

        async with self._lock:
            if response.status_code == status.HTTP_201_CREATED:
                self._successful_count += 1
                self._created_count += 1
                return

            if response.status_code == status.HTTP_200_OK:
                self._successful_count += 1
                self._reused_count += 1
                return

            self._failed_count += 1
            self._last_error = f"{response.status_code}: {response.text[:200]}"


async def probe_api_dependency() -> None:
    """Verify the load generator can reach the API it will send orders to."""
    async with httpx.AsyncClient(timeout=2.0) as probe_client:
        response = await probe_client.get(f"{api_base_url()}/healthz")
        response.raise_for_status()


load_generator = LoadGenerator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open loadgen HTTP resources once and clean them up on shutdown."""
    await load_generator.open()
    try:
        yield
    finally:
        await load_generator.close()


app = FastAPI(title="Load Generator", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the load generator process is alive."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "loadgen")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report readiness only when the configured API dependency is reachable."""
    try:
        await probe_api_dependency()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"API dependency is not reachable: {exc}",
        ) from exc

    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "loadgen"),
        "api_base_url": api_base_url(),
        "dependencies": {"api": "ok"},
    }


@app.get("/status")
async def get_status() -> dict[str, Any]:
    """Return current run state and counters for demo operators."""
    return await load_generator.status()


@app.post("/load/start", status_code=status.HTTP_201_CREATED)
async def start_load(request: StartLoadRequest) -> dict[str, Any]:
    """Start a new load run unless one is already active."""
    return await load_generator.start(request)


@app.post("/load/stop")
async def stop_load() -> dict[str, Any]:
    """Stop the active run and return final in-memory counters."""
    return await load_generator.stop()


@app.patch("/load/rate")
async def update_rate(request: UpdateRateRequest) -> dict[str, Any]:
    """Change the active run's target request rate."""
    return await load_generator.update_rate(request)


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple response for humans hitting the loadgen root."""
    return {"message": "Load generator control API"}
