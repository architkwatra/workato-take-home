import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool


class DatabaseConfigError(RuntimeError):
    """Raised when a service cannot safely configure its Postgres dependency."""


# One pool is kept per service process. Sharing it avoids creating a new TCP
# connection for every order request during bursty dinner-rush traffic.
_db_pool: ConnectionPool | None = None


def get_database_url() -> str:
    """Return the configured Postgres connection string for this service."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise DatabaseConfigError("DATABASE_URL is not configured")
    return database_url


def _read_pool_size(env_name: str, default: int) -> int:
    """Read a positive integer pool size from the environment."""
    raw_value = os.getenv(env_name)
    if raw_value is None:
        return default

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise DatabaseConfigError(f"{env_name} must be an integer") from exc

    if value <= 0:
        raise DatabaseConfigError(f"{env_name} must be greater than zero")

    return value


def configure_db_pool(*, connect_timeout: int = 2, pool_wait_timeout: int = 30) -> None:
    """Create the process-wide Postgres connection pool for service DB traffic."""
    global _db_pool
    if _db_pool is not None:
        return

    # Keep the default pool intentionally modest for a single-machine demo.
    # It gives the API concurrency without consuming most of Postgres'
    # connection budget before workers and other services are added.
    min_size = _read_pool_size("DATABASE_POOL_MIN_SIZE", 1)
    max_size = _read_pool_size("DATABASE_POOL_MAX_SIZE", 10)
    if min_size > max_size:
        raise DatabaseConfigError(
            "DATABASE_POOL_MIN_SIZE cannot exceed DATABASE_POOL_MAX_SIZE"
        )

    pool = ConnectionPool(
        conninfo=get_database_url(),
        min_size=min_size,
        max_size=max_size,
        kwargs={"connect_timeout": connect_timeout},
        open=False,
    )
    pool.open()
    # Connection timeout is per TCP attempt. Pool wait timeout is the total
    # startup window for the pool to prepare its minimum connections while other
    # Compose services may also be connecting to Postgres.
    pool.wait(timeout=pool_wait_timeout)
    _db_pool = pool


def close_db_pool() -> None:
    """Close the process-wide Postgres connection pool during service shutdown."""
    global _db_pool
    if _db_pool is None:
        return

    _db_pool.close()
    _db_pool = None


@contextmanager
def open_db_connection(*, connect_timeout: int = 2) -> Iterator[psycopg.Connection]:
    """Yield a Postgres connection from the pool, falling back to direct connect."""
    if _db_pool is not None:
        with _db_pool.connection() as conn:
            yield conn
        return

    # The fallback keeps one-off scripts, migrations, and tests usable before a
    # web service lifespan has configured the process-wide pool.
    with psycopg.connect(get_database_url(), connect_timeout=connect_timeout) as conn:
        yield conn


def check_database_ready() -> None:
    """Run the smallest DB query needed to prove Postgres is reachable."""
    with open_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
