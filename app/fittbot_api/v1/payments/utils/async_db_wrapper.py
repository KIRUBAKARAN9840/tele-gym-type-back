import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable
import logging

logger = logging.getLogger("payments.async_db")

# Thread pool configuration tuned for expected payment load
_db_executor = ThreadPoolExecutor(
    max_workers=50,
    thread_name_prefix="db_worker"
)


async def run_sync_db_operation(func: Callable, *args, **kwargs) -> Any:
    """Execute a blocking DB operation in the shared thread pool."""
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(
            _db_executor,
            partial(func, *args, **kwargs)
        )
    except Exception as exc:
        logger.error("DB operation failed: %s", exc, exc_info=True)
        raise


async def shutdown_db_executor() -> None:
    """Shutdown the database executor to release worker threads."""
    _db_executor.shutdown(wait=True)
    logger.info("DB executor shutdown complete")


def get_thread_pool_stats() -> dict:
    """Return basic runtime metrics for monitoring thread usage."""
    return {
        "max_workers": _db_executor._max_workers,
        "active_threads": len([t for t in _db_executor._threads if t.is_alive()]),
        "queue_size": _db_executor._work_queue.qsize(),
    }
