"""
Standalone order-persistence worker.

Runs the Redis Streams consumer as its own process (the `worker` service in docker-compose), so
database writes never share an event loop or CPU with the HTTP API replicas. Consumers join the
same consumer group, so adding more worker containers scales persistence horizontally.
"""
import asyncio
import logging
import signal

from app.database import init_db
from app.redis_client import redis_manager
from app.worker import worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("flash_sale.worker")


async def main():
    await init_db()  # idempotent; serialized across processes by an advisory lock
    await redis_manager.connect()

    task = asyncio.create_task(worker.start())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass

    await stop.wait()
    logger.info("Shutdown requested: finishing the current batch, then exiting.")
    worker.stop()
    try:
        await asyncio.wait_for(task, timeout=10)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        task.cancel()
    await redis_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
