import asyncio
import logging
import os
import signal
import socket


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("worker")


async def run_worker() -> None:
    worker_id = os.getenv("WORKER_ID", socket.gethostname())
    stop_event = asyncio.Event()

    def request_stop() -> None:
        logger.info("shutdown requested", extra={"worker_id": worker_id})
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)

    logger.info("worker scaffold started", extra={"worker_id": worker_id})

    while not stop_event.is_set():
        logger.info("worker idle", extra={"worker_id": worker_id})
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            continue

    logger.info("worker scaffold stopped", extra={"worker_id": worker_id})


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()

