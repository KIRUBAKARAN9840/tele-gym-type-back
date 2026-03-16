"""
Instrumented Redis Client
Automatically tracks all Redis operations for Prometheus metrics.
"""

import time
import functools
from typing import Optional, Any, Callable
from redis.asyncio import Redis as AsyncRedis

from app.utils.metrics import (
    REDIS_OPERATIONS,
    REDIS_LATENCY,
    REDIS_ERRORS,
    REDIS_SLOW_COMMANDS,
    REDIS_CACHE_HITS,
    REDIS_CACHE_MISSES,
    get_redis_command_type,
)


class InstrumentedRedis:
    """
    Wrapper around Redis client that automatically tracks metrics for all operations.

    Usage:
        redis = await get_redis()
        instrumented = InstrumentedRedis(redis)
        value = await instrumented.get("key")  # Automatically tracked!
    """

    # Operations that return None on miss (for cache hit/miss tracking)
    CACHEABLE_OPS = {"get", "hget", "hgetall", "lrange", "smembers", "zrange"}

    def __init__(self, redis_client: AsyncRedis, cache_type: str = "default"):
        self._redis = redis_client
        self._cache_type = cache_type

    def __getattr__(self, name: str) -> Callable:
        """Intercept all Redis method calls and wrap them with metrics."""
        attr = getattr(self._redis, name)

        if not callable(attr):
            return attr

        @functools.wraps(attr)
        async def instrumented_method(*args, **kwargs) -> Any:
            start = time.perf_counter()
            status = "success"
            command_type = get_redis_command_type(name)
            result = None

            try:
                result = await attr(*args, **kwargs)

                # Track cache hits/misses for cacheable operations
                if name.lower() in self.CACHEABLE_OPS:
                    if result is None or result == {} or result == []:
                        REDIS_CACHE_MISSES.labels(cache_type=self._cache_type).inc()
                    else:
                        REDIS_CACHE_HITS.labels(cache_type=self._cache_type).inc()

                return result

            except TimeoutError:
                status = "timeout"
                REDIS_ERRORS.labels(error_type="timeout").inc()
                raise
            except ConnectionError:
                status = "connection_error"
                REDIS_ERRORS.labels(error_type="connection").inc()
                raise
            except Exception:
                status = "error"
                REDIS_ERRORS.labels(error_type="command_error").inc()
                raise
            finally:
                duration = time.perf_counter() - start

                REDIS_OPERATIONS.labels(operation=name, status=status).inc()
                REDIS_LATENCY.labels(operation=name, command_type=command_type).observe(duration)

                # Track slow commands (>10ms)
                if duration > 0.01:
                    REDIS_SLOW_COMMANDS.labels(operation=name).inc()

        return instrumented_method

    @property
    def client(self) -> AsyncRedis:
        """Access the underlying Redis client directly."""
        return self._redis

    async def pipeline(self):
        """Return instrumented pipeline."""
        pipe = self._redis.pipeline()
        return InstrumentedPipeline(pipe, self._cache_type)


class InstrumentedPipeline:
    """Instrumented Redis pipeline for batch operations."""

    def __init__(self, pipeline, cache_type: str = "default"):
        self._pipeline = pipeline
        self._cache_type = cache_type
        self._commands = []

    def __getattr__(self, name: str) -> Callable:
        """Track pipeline commands."""
        attr = getattr(self._pipeline, name)

        if not callable(attr):
            return attr

        @functools.wraps(attr)
        def wrapped(*args, **kwargs):
            self._commands.append(name)
            return attr(*args, **kwargs)

        return wrapped

    async def execute(self):
        """Execute pipeline and track all commands."""
        start = time.perf_counter()
        status = "success"

        try:
            result = await self._pipeline.execute()
            return result
        except Exception:
            status = "error"
            REDIS_ERRORS.labels(error_type="pipeline_error").inc()
            raise
        finally:
            duration = time.perf_counter() - start

            # Track each command in the pipeline
            for cmd in self._commands:
                command_type = get_redis_command_type(cmd)
                REDIS_OPERATIONS.labels(operation=cmd, status=status).inc()
                # Distribute duration across commands (approximate)
                per_cmd_duration = duration / len(self._commands) if self._commands else duration
                REDIS_LATENCY.labels(operation=cmd, command_type=command_type).observe(per_cmd_duration)

            # Track as pipeline operation too
            REDIS_OPERATIONS.labels(operation="pipeline", status=status).inc()
            REDIS_LATENCY.labels(operation="pipeline", command_type="batch").observe(duration)

            if duration > 0.05:  # 50ms for pipelines
                REDIS_SLOW_COMMANDS.labels(operation="pipeline").inc()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_instrumented_client: Optional[InstrumentedRedis] = None


async def get_instrumented_redis(cache_type: str = "default") -> InstrumentedRedis:
    """
    Get instrumented Redis client with automatic metrics tracking.

    Args:
        cache_type: Label for cache hit/miss tracking (e.g., "session", "user_data", "eligibility")

    Usage:
        redis = await get_instrumented_redis("user_data")
        user = await redis.get(f"user:{user_id}")  # Automatically tracked!
    """
    from app.utils.redis_config import get_redis

    redis_client = await get_redis()
    return InstrumentedRedis(redis_client, cache_type)


def instrument_redis(redis_client: AsyncRedis, cache_type: str = "default") -> InstrumentedRedis:
    """
    Wrap an existing Redis client with instrumentation.

    Usage:
        redis = await get_redis()
        instrumented = instrument_redis(redis, "otp")
        await instrumented.setex("otp:123", 300, "456789")
    """
    return InstrumentedRedis(redis_client, cache_type)
