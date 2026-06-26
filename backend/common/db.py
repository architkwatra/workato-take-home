import os

import psycopg


class DatabaseConfigError(RuntimeError):
    """Raised when a service needs Postgres but DATABASE_URL is missing."""


def get_database_url() -> str:
    """Return the configured Postgres connection string for this service."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise DatabaseConfigError("DATABASE_URL is not configured")
    return database_url


def open_db_connection(*, connect_timeout: int = 2):
    """Open a short-lived psycopg connection using the service DATABASE_URL."""
    return psycopg.connect(get_database_url(), connect_timeout=connect_timeout)


def check_database_ready() -> None:
    """Run the smallest DB query needed to prove Postgres is reachable."""
    with open_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            cur.fetchone()
