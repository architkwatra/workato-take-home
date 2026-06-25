import os

from fastapi import FastAPI


app = FastAPI(title="Downstream Simulator")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "downstream-sim")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "downstream-sim"),
        "simulator": {"restaurant": "idle", "courier": "idle"},
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Downstream simulator scaffold"}

