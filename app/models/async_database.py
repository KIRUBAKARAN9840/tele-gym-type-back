import logging
from functools import lru_cache
from threading import Lock
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker

from app.config.settings import settings

logger = logging.getLogger(__name__)

_celery_async_engine = None
_celery_sessionmaker = None
_celery_lock = Lock()


def _build_async_url() -> str:

    url = settings.database_url
    if "+pymysql" in url:
        return url.replace("+pymysql", "+aiomysql")
    if url.startswith("mysql://"):
        return "mysql+aiomysql://" + url[len("mysql://") :]
    return url


@lru_cache(maxsize=1)
def get_async_engine():
    async_url = _build_async_url()
    logger.info("Creating async engine %s", async_url)
    engine = create_async_engine(
        async_url,
        # Async engines cannot use QueuePool directly; default async pool adapts pooling.
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        pool_pre_ping=settings.db_pool_pre_ping,
        pool_reset_on_return="rollback",
        echo=settings.db_pool_echo,
        future=True,
    )
    return engine


@lru_cache(maxsize=1)
def get_async_sessionmaker():
    engine = get_async_engine()
    return sessionmaker(
        engine,
        class_=AsyncSession,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


async def get_async_db():
    """
    FastAPI dependency that yields an AsyncSession.
    """
    SessionLocal = get_async_sessionmaker()
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


def create_celery_async_sessionmaker():

    from app.utils.celery_asyncio import get_worker_loop

    global _celery_async_engine, _celery_sessionmaker
    _ = get_worker_loop()
    with _celery_lock:
        if _celery_sessionmaker and _celery_async_engine:
            return _celery_sessionmaker

        async_url = _build_async_url()
        logger.info("Creating Celery async engine %s", async_url)
        _celery_async_engine = create_async_engine(
            async_url,
            # Use Celery-specific pool settings (smaller pools since prefetch_multiplier=1)
            pool_size=settings.celery_db_pool_size,
            max_overflow=settings.celery_db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=settings.db_pool_pre_ping,
            pool_reset_on_return="rollback",
            echo=settings.db_pool_echo,
            future=True,
        )
        _celery_sessionmaker = sessionmaker(
            _celery_async_engine,
            class_=AsyncSession,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        return _celery_sessionmaker


def init_celery_async_db() -> None:
    """
    Explicitly initialise the Celery async engine/sessionmaker.

    Called from Celery worker init so the pool is ready before tasks run.
    Safe to call multiple times.
    """
    create_celery_async_sessionmaker()


async def dispose_celery_async_engine() -> None:
    """
    Dispose the Celery async engine and reset the cached sessionmaker.
    """
    global _celery_async_engine, _celery_sessionmaker
    engine: Optional[AsyncEngine] = None
    with _celery_lock:
        engine = _celery_async_engine
        _celery_async_engine = None
        _celery_sessionmaker = None

    if engine:
        await engine.dispose()
