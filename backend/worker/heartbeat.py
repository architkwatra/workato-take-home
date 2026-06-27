import os
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from common.db import open_db_connection


@dataclass(frozen=True)
class WorkerIdentity:
    """Stable identity values for one running worker process."""

    worker_id: str
    hostname: str
    started_at: datetime
    metadata: dict[str, object]


def utc_now() -> datetime:
    """Return one timezone-aware timestamp for worker DB writes."""
    return datetime.now(timezone.utc)


def create_worker_identity() -> WorkerIdentity:
    """Create a unique worker identity for this process lifetime."""
    started_at = utc_now()
    hostname = socket.gethostname()
    return WorkerIdentity(
        worker_id=f"worker-{uuid4().hex}",
        hostname=hostname,
        started_at=started_at,
        metadata={
            "service": os.getenv("SERVICE_NAME", "worker"),
            "pid": os.getpid(),
        },
    )


def record_worker_heartbeat(identity: WorkerIdentity) -> None:
    """Upsert the worker heartbeat row that dashboard health checks will read."""
    seen_at = utc_now()
    with open_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into workers (
                    worker_id,
                    hostname,
                    started_at,
                    last_seen_at,
                    metadata
                )
                values (%s, %s, %s, %s, %s)
                on conflict (worker_id) do update
                set
                    -- Preserve started_at as the process-start timestamp; only
                    -- refresh liveness/debug fields on each heartbeat.
                    hostname = excluded.hostname,
                    last_seen_at = excluded.last_seen_at,
                    metadata = excluded.metadata
                """,
                (
                    identity.worker_id,
                    identity.hostname,
                    identity.started_at,
                    seen_at,
                    Jsonb(identity.metadata),
                ),
            )
