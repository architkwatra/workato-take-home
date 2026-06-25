import os

from fastapi import FastAPI


app = FastAPI(title="Load Generator")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "loadgen")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "loadgen"),
        "mode": "idle",
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Load generator scaffold"}

