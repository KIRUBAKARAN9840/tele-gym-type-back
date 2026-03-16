
import logging
import os
from typing import Optional, Dict, Any

from redis.asyncio import Redis as AsyncRedis, ConnectionPool as AsyncConnectionPool
from redis import Redis as SyncRedis, ConnectionPool as SyncConnectionPool
from redis.exceptions import ConnectionError, TimeoutError
from dotenv import load_dotenv

_log = logging.getLogger("app.utils.redis_config")

load_dotenv()

# Global connection pool - reused across all requests
_redis_pool: Optional[AsyncConnectionPool] = None
_redis_client: Optional[AsyncRedis] = None
_redis_sync_pool: Optional[SyncConnectionPool] = None
_redis_sync_client: Optional[SyncRedis] = None


def _get_redis_target() -> Dict[str, Any]:
    """Determine Redis endpoint/connection sizing from env."""

    # Get environment from env variable, default to local for safety
    environment = os.getenv("ENVIRONMENT", "production").lower()

    if environment == "production":
        target = {
            "host": "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 200,
        }
        _log.debug("redis-config ENV=production target=%s", target)
        return target

    if environment == "staging":
        target = {
            "host": "staging-redis.azdytp.ng.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 150,
        }
        _log.debug("redis-config ENV=staging target=%s", target)
        return target

    target = {"host": "localhost", "port": 6379, "max_connections": 100}
    _log.debug("redis-config ENV=%s default target=%s", environment, target)
    return target


def _get_async_connection_kwargs() -> Dict[str, Any]:
    """Socket tuning for asyncio redis pools."""
    return dict(
        decode_responses=True,
        socket_keepalive=True,
        socket_keepalive_options={},
        retry_on_timeout=True,
        retry_on_error=[ConnectionError, TimeoutError],
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def _get_sync_connection_kwargs() -> Dict[str, Any]:
    """Socket tuning for sync redis pools."""
    kwargs = _get_async_connection_kwargs().copy()
    kwargs.pop("retry_on_error", None)
    return kwargs


def create_redis_pool() -> AsyncConnectionPool:
    """Create Redis connection pool for enterprise connection management."""
    target = _get_redis_target()
    connection_kwargs = _get_async_connection_kwargs()

    if "url" in target:
        return AsyncConnectionPool.from_url(
            target["url"],
            max_connections=target["max_connections"],
            **connection_kwargs,
        )

    return AsyncConnectionPool(
        host=target["host"],
        port=target["port"],
        max_connections=target["max_connections"],
        **connection_kwargs,
    )


def _create_sync_pool() -> SyncConnectionPool:
    """Create sync Redis pool for Celery/worker contexts."""
    target = _get_redis_target()
    connection_kwargs = _get_sync_connection_kwargs()

    if "url" in target:
        return SyncConnectionPool.from_url(
            target["url"],
            max_connections=target["max_connections"],
            **connection_kwargs,
        )

    return SyncConnectionPool(
        host=target["host"],
        port=target["port"],
        max_connections=target["max_connections"],
        **connection_kwargs,
    )


async def get_redis() -> AsyncRedis:
    """Get Redis client with enterprise connection pooling"""
    global _redis_pool, _redis_client

    if _redis_client is None:
        if _redis_pool is None:
            _redis_pool = create_redis_pool()

        _redis_client = AsyncRedis(connection_pool=_redis_pool)

        # Test connection
        try:
            await _redis_client.ping()
        except Exception as e:
            print(f"Redis connection failed: {e}")
            # Reset and retry once
            _redis_client = None
            _redis_pool = None
            if _redis_pool is None:
                _redis_pool = create_redis_pool()
            _redis_client = AsyncRedis(connection_pool=_redis_pool)

    return _redis_client


def get_redis_sync() -> SyncRedis:
   
    global _redis_sync_pool, _redis_sync_client

    if _redis_sync_client is None:
        if _redis_sync_pool is None:
            _redis_sync_pool = _create_sync_pool()

        _redis_sync_client = SyncRedis(connection_pool=_redis_sync_pool)

        try:
            _redis_sync_client.ping()
        except Exception as e:
            print(f"Redis sync connection failed: {e}")
            _redis_sync_client = None
            _redis_sync_pool = None
            raise

    return _redis_sync_client

async def get_redis_pool_info() -> dict:
    """Get Redis connection pool information for monitoring"""
    global _redis_pool
    if _redis_pool:
        return {
            "pool_created_connections": getattr(_redis_pool, "created_connections", "unknown"),
            "pool_available_connections": len(getattr(_redis_pool, "_available_connections", [])),
            "pool_in_use_connections": len(getattr(_redis_pool, "_in_use_connections", [])),
            "max_connections": _redis_pool.max_connections if hasattr(_redis_pool, "max_connections") else "unknown"
        }
    return {"status": "no_pool"}

async def close_redis():
    """Close Redis connections gracefully"""
    global _redis_pool, _redis_client
    
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
    
    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None

_log.debug(
    "redis-config loaded from %s, has get_redis_sync=%s",
    __file__, "get_redis_sync" in globals(),
)
