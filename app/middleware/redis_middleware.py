"""
Redis Connection Management Middleware
Ensures proper connection lifecycle management
"""

import logging
from fastapi import Request, Response
from app.utils.redis_config import get_redis_pool_info

logger = logging.getLogger(__name__)

async def redis_connection_middleware(request: Request, call_next):
    """
    Middleware to monitor Redis connection usage and ensure proper cleanup
    """
    # Log pool info before request (for debugging)
    if request.url.path == "/health" or request.url.path.startswith("/metrics"):
        # Skip logging for health checks to reduce noise
        pass
    else:
        try:
            pool_info = await get_redis_pool_info()
            if pool_info.get("pool_in_use_connections", 0) > 50:  # Alert if too many connections in use
                logger.warning(f"High Redis connection usage: {pool_info}")
        except Exception as e:
            logger.error(f"Failed to get Redis pool info: {e}")

    # Process request
    response = await call_next(request)

    # Log pool info after request if there were issues
    try:
        pool_info = await get_redis_pool_info()
        in_use = pool_info.get("pool_in_use_connections", 0)

        # Alert if connections are leaking
        if in_use > 80:  # More than 80% of max connections in use
            logger.error(f"Potential Redis connection leak detected: {pool_info}")

    except Exception as e:
        logger.error(f"Failed to monitor Redis connections: {e}")

    return response