import os

import psycopg
from fastapi import FastAPI, HTTPException

from common.db import DatabaseConfigError, check_database_ready


app = FastAPI(title="Order Pipeline API")


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
