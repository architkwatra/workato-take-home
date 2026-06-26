import os

import psycopg
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Order Pipeline API")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the API process is alive and able to serve requests."""
    return {"status": "ok", "service": os.getenv("SERVICE_NAME", "api")}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
    """Report API readiness by checking the Postgres connection."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")

    try:
        with psycopg.connect(database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
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
