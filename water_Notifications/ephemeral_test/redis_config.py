import os
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ConnectionError, TimeoutError

load_dotenv()

_redis_pool: Optional[ConnectionPool] = None
_redis_client: Optional[Redis] = None


def _get_redis_target() -> Dict[str, Any]:
    """Determine Redis endpoint/connection sizing from env (matches main app)."""
    environment = os.getenv("ENVIRONMENT", "production").lower()

    if environment == "production":
        target = {
            "host": "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 200,
        }
        print(f"[redis-config-debug] ENV=production target={target}")
        return target

    if environment == "staging":
        target = {
            "host": "staging-redis.azdytp.ng.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 150,
        }
        print(f"[redis-config-debug] ENV=staging target={target}")
        return target

    target = {"host": "localhost", "port": 6379, "max_connections": 100}
    print(f"[redis-config-debug] ENV={environment} default target={target}")
    return target


def _connection_kwargs() -> dict:
    """Shared connection options mirroring the main service defaults."""
    return {
        "decode_responses": True,
        "socket_keepalive": True,
        "socket_keepalive_options": {},
        "retry_on_timeout": True,
        "retry_on_error": [ConnectionError, TimeoutError],
        "health_check_interval": 30,
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
    }


def create_redis_pool() -> ConnectionPool:
    """Create Redis connection pool (matches main app approach)."""
    target = _get_redis_target()
    connection_kwargs = _connection_kwargs()

    return ConnectionPool(
        host=target["host"],
        port=target["port"],
        max_connections=target["max_connections"],
        **connection_kwargs,
    )


async def get_redis() -> Redis:
    """Get Redis client with enterprise connection pooling."""
    global _redis_pool, _redis_client

    if _redis_client is None:
        if _redis_pool is None:
            _redis_pool = create_redis_pool()

        _redis_client = Redis(connection_pool=_redis_pool)

        try:
            await _redis_client.ping()
        except Exception as e:
            print(f"Redis connection failed: {e}")
            # Reset and retry once with a fresh pool
            _redis_client = None
            _redis_pool = None
            if _redis_pool is None:
                _redis_pool = create_redis_pool()
            _redis_client = Redis(connection_pool=_redis_pool)

    return _redis_client


async def close_redis():
    """Close Redis connections gracefully (parity with main service)."""
    global _redis_pool, _redis_client

    if _redis_client:
        await _redis_client.close()
        _redis_client = None

    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None


print(
    f"[redis-config-debug] loaded from {__file__}"
)
