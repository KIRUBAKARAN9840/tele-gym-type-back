"""
Utilities for Celery workers to share a single asyncio event loop per process.

Celery forks worker processes; each process runs tasks serially in the main
thread. Reusing a single event loop per process keeps async DB engines tied to
the same loop and avoids creating/destroying loops for every task invocation.
"""
import asyncio
from threading import Lock

_loop = None
_loop_lock = Lock()


def get_worker_loop() -> asyncio.AbstractEventLoop:
    """
    Return a per-process event loop for Celery workers.

    Lazily creates the loop the first time and keeps it alive for the life of
    the worker process so async DB pools are bound to a stable loop.
    """
    global _loop
    if _loop and not _loop.is_closed():
        return _loop

    with _loop_lock:
        if _loop and not _loop.is_closed():
            return _loop
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop


def run_in_worker_loop(coro):
    """
    Run a coroutine on the shared worker event loop.

    This replaces asyncio.run() inside Celery tasks to avoid per-task loop
    creation while keeping execution synchronous for Celery.
    """
    loop = get_worker_loop()
    return loop.run_until_complete(coro)


def close_worker_loop() -> None:
    """
    Close the shared worker loop. Safe to call multiple times.
    """
    global _loop
    with _loop_lock:
        if _loop and not _loop.is_closed():
            _loop.close()
        _loop = None
