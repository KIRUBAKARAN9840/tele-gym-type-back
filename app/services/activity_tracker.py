

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from app.utils.redis_config import get_redis

logger = logging.getLogger("activity_tracker")


EVENTS_QUEUE_KEY = "activity:events:queue"
VIEWS_KEY_PREFIX = "activity:views"
CHECKOUT_KEY_PREFIX = "activity:checkout"
WA_SENT_KEY_PREFIX = "activity:wa_sent"

# Index sets for O(1) lookup instead of SCAN
ACTIVE_CHECKOUTS_SET = "activity:checkout:active_clients"
ACTIVE_VIEWS_SET = "activity:views:active_clients"

VIEWS_TTL = 86400
CHECKOUT_TTL = 7200
WA_RATE_LIMIT_TTL = 86400


async def track_event(
    client_id: int,
    event_type: str,
    gym_id: Optional[int] = None,
    product_type: Optional[str] = None,
    product_details: Optional[Dict[str, Any]] = None,
    source: Optional[str] = None,
    command_id: Optional[str] = None,
) -> None:

    try:
        redis = await get_redis()

        event = {
            "client_id": client_id,
            "event_type": event_type,
            "gym_id": gym_id,
            "product_type": product_type,
            "product_details": product_details,
            "source": source,
            "command_id": command_id,
            "created_at": datetime.now().isoformat(),
        }

        # Push event to processing queue
        await redis.rpush(EVENTS_QUEUE_KEY, json.dumps(event))

        # Update real-time tracking hashes based on event type
        if event_type in ("gym_viewed", "dailypass_viewed", "session_viewed", "membership_viewed"):
            await _track_view(redis, client_id, gym_id)
        elif event_type == "checkout_initiated" and command_id:
            await _track_checkout(redis, client_id, command_id, gym_id, product_type)
        elif event_type == "checkout_completed" and command_id:
            await _clear_checkout(redis, client_id, command_id)

    except Exception:
        
        logger.exception("Failed to track activity event for client %s", client_id)


async def _track_view(redis, client_id: int, gym_id: Optional[int]) -> None:
    """Increment view count for a client-gym pair in Redis."""
    if gym_id is None:
        return
    key = f"{VIEWS_KEY_PREFIX}:{client_id}"
    await redis.hincrby(key, str(gym_id), 1)
    await redis.expire(key, VIEWS_TTL)
    await redis.sadd(ACTIVE_VIEWS_SET, str(client_id))


async def _track_checkout(
    redis, client_id: int, command_id: str,
    gym_id: Optional[int], product_type: Optional[str],
) -> None:
    """Record a pending checkout for abandoned-checkout detection."""
    key = f"{CHECKOUT_KEY_PREFIX}:{client_id}"
    checkout_data = json.dumps({
        "gym_id": gym_id,
        "product_type": product_type,
        "initiated_at": datetime.now().isoformat(),
    })
    await redis.hset(key, command_id, checkout_data)
    await redis.expire(key, CHECKOUT_TTL)
    await redis.sadd(ACTIVE_CHECKOUTS_SET, str(client_id))


async def _clear_checkout(redis, client_id: int, command_id: str) -> None:
    """Remove a completed checkout from the pending set."""
    key = f"{CHECKOUT_KEY_PREFIX}:{client_id}"
    await redis.hdel(key, command_id)
