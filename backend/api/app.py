import os

from fastapi import FastAPI


app = FastAPI(title="Order Pipeline API")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the API process is alive and able to serve requests."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "api")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report API readiness; dependency checks will be filled in as services grow."""
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "api"),
        "dependencies": {"postgres": "not_checked"},
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a simple scaffold response for humans hitting the API root."""
    return {"message": "Order Pipeline API scaffold"}
