import asyncio
import logging
import os
import signal

from common.db import close_db_pool, configure_db_pool
from worker.heartbeat import create_worker_identity, record_worker_heartbeat
from worker.tasks import claim_and_process_one_task


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("worker")

HEARTBEAT_INTERVAL_SECONDS = 10
TASK_POLL_INITIAL_IDLE_SECONDS = 0.1
TASK_POLL_MAX_IDLE_SECONDS = 1.0


async def run_heartbeat_loop(identity, stop_event: asyncio.Event) -> None:
    """Record worker liveness until shutdown."""
    while not stop_event.is_set():
        try:
            # The DB helper is synchronous. Running it in a thread keeps the
            # async signal/shutdown loop responsive while Postgres is slow.
            await asyncio.to_thread(record_worker_heartbeat, identity)
            logger.info("worker heartbeat", extra={"worker_id": identity.worker_id})
        except Exception:
            # A missed heartbeat should be visible but should not kill the
            # worker. The next loop iteration will retry the upsert.
            logger.exception(
                "worker heartbeat failed",
                extra={"worker_id": identity.worker_id},
            )

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=HEARTBEAT_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            continue


async def run_task_poll_loop(identity, stop_event: asyncio.Event) -> None:
    """Poll for claimable tasks and process supported work."""
    idle_delay = TASK_POLL_INITIAL_IDLE_SECONDS

    while not stop_event.is_set():
        try:
            found_work = await asyncio.to_thread(
                claim_and_process_one_task,
                worker_id=identity.worker_id,
            )
        except Exception:
            logger.exception(
                "worker task polling failed",
                extra={"worker_id": identity.worker_id},
            )
            found_work = False

        if found_work:
            idle_delay = TASK_POLL_INITIAL_IDLE_SECONDS
            continue

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=idle_delay)
        except asyncio.TimeoutError:
            idle_delay = min(idle_delay * 2, TASK_POLL_MAX_IDLE_SECONDS)


async def run_worker() -> None:
    """Run the worker heartbeat and task polling loops until shutdown."""
    identity = create_worker_identity()
    stop_event = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []

    def request_stop() -> None:
        """Ask the async worker loop to exit after SIGINT or SIGTERM."""
        logger.info("shutdown requested", extra={"worker_id": identity.worker_id})
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)

    try:
        # Pool setup is synchronous and can wait while Postgres is busy. Run it
        # off the event loop so SIGTERM/SIGINT can still be handled promptly.
        await asyncio.to_thread(configure_db_pool)
        logger.info("worker started", extra={"worker_id": identity.worker_id})

        worker_tasks = [
            asyncio.create_task(run_heartbeat_loop(identity, stop_event)),
            asyncio.create_task(run_task_poll_loop(identity, stop_event)),
        ]
        await stop_event.wait()
    finally:
        stop_event.set()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        await asyncio.to_thread(close_db_pool)
        logger.info("worker stopped", extra={"worker_id": identity.worker_id})


def main() -> None:
    """Start the worker scaffold from the module entrypoint."""
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
