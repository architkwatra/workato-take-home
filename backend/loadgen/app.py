import os

from fastapi import FastAPI


app = FastAPI(title="Load Generator")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the load generator process is alive."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "loadgen")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report load generator readiness and its current idle scaffold mode."""
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "loadgen"),
        "mode": "idle",
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the loadgen root."""
    return {"message": "Load generator scaffold"}
