import os

from fastapi import FastAPI


app = FastAPI(title="Order Pipeline API")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "api")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    return {
        "status": "ok",
        "service": os.getenv("SERVICE_NAME", "api"),
        "dependencies": {"postgres": "not_checked"},
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Order Pipeline API scaffold"}

