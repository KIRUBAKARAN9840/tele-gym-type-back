# app/routers/websocket_feed.py
from __future__ import annotations

import os
import json
import asyncio
import time
from collections import defaultdict
from contextlib import suppress
from typing import Dict, List

from fastapi import APIRouter, Header, Depends
from fastapi import HTTPException, status
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from redis.asyncio import Redis

from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis

router = APIRouter(prefix="/websocket_feed", tags=["websocket_feed"])

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────
REDIS_DSN = os.getenv(
    "WEBSOCKET_REDIS_DSN",
    "redis://fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com:6379/0",
)
PING_SEC = 25  # heartbeat

# Globals (lazily initialized)
redis_pool: Redis | None = None
hub: RoomHub | None = None


async def create_redis() -> Redis:
    return Redis.from_url(REDIS_DSN, decode_responses=True)


async def ensure_hub() -> "RoomHub":
    """Ensure Redis connection and RoomHub are ready and started."""
    global redis_pool, hub
    if redis_pool is None:
        redis_pool = await create_redis()
    if hub is None:
        hub = RoomHub(redis_pool)
        await hub.start()
    return hub


# ──────────────────────────────────────────────────────────────
# RoomHub
# ──────────────────────────────────────────────────────────────
class RoomHub:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._conns: Dict[int, List[WebSocket]] = defaultdict(list)
        self._rx_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the fan-in task that relays Redis pubsub messages to clients."""
        if self._rx_task and not self._rx_task.done():
            return
        pubsub = self._redis.pubsub()
        await pubsub.psubscribe("gym:*")
        self._rx_task = asyncio.create_task(self._fan_in(pubsub))

    async def _fan_in(self, pubsub) -> None:
        """Read from Redis pattern channel and fan out to room subscribers."""
        async for msg in pubsub.listen():
            if msg.get("type") != "pmessage":
                continue
            channel = msg.get("channel")
            payload = msg.get("data")
            if not isinstance(channel, str):
                continue
            try:
                # channel pattern: "gym:{id}"
                gym_id = int(channel.split(":")[1])
            except Exception:
                # Ignore malformed channels
                continue
            await self._fan_out(gym_id, payload)

    async def _fan_out(self, gym_id: int, payload: str) -> None:
        """Send a payload to all active connections in a room. Drop dead ones."""
        dead: List[WebSocket] = []
        for ws in list(self._conns[gym_id]):
            try:
                if ws.application_state == WebSocketState.CONNECTED:
                    await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self._drop(gym_id, ws)

    async def join(self, gym_id: int, ws: WebSocket) -> None:
        """Add a client WS to a room."""
        if ws not in self._conns[gym_id]:
            self._conns[gym_id].append(ws)

    async def publish(self, gym_id: int, obj: dict) -> None:
        """Publish a message to a room via Redis (so all app instances get it)."""
        await self._redis.publish(f"gym:{gym_id}", json.dumps(obj))

    async def _drop(self, gym_id: int, ws: WebSocket) -> None:
        """Remove a client from the room."""
        with suppress(ValueError):
            self._conns[gym_id].remove(ws)


# ──────────────────────────────────────────────────────────────
# HTTP endpoint to notify clients about new posts
# (used by internal producers like Lambdas / workers)
# ──────────────────────────────────────────────────────────────
@router.post("/internal/new_post", status_code=status.HTTP_202_ACCEPTED)
async def internal_new_post(
    payload: dict,
    x_api_key: str = Header(..., alias="x-api-key"),
):
    # Simple auth: shared header
    expected = os.getenv("LAMBDA_HEADER", "lambda_header_feed_not_out")
    if x_api_key != expected:
        raise FittbotHTTPException(
            status_code=401,
            detail="Invalid API key",
            error_code="WEBSOCKET_INVALID_API_KEY",
        )

    # Validate payload
    try:
        gym_id = int(payload["gym_id"])
        post_id = int(payload["post_id"])
    except Exception:
        raise FittbotHTTPException(
            status_code=422,
            detail="Invalid payload format (gym_id/post_id required and must be integers)",
            error_code="WEBSOCKET_INVALID_PAYLOAD",
        )

    # Ensure hub and publish
    try:
        _hub = await ensure_hub()
        await _hub.publish(
            gym_id, {"action": "new_post", "gym_id": gym_id, "post_id": post_id}
        )
        return {"ok": True}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error during post notification",
            error_code="WEBSOCKET_POST_NOTIFICATION_ERROR",
            log_data={"error": repr(e)},
        )


@router.post("/internal/invalidate_cache", status_code=status.HTTP_200_OK)
async def internal_invalidate_cache(
    payload: dict,
    x_api_key: str = Header(..., alias="x-api-key"),
    redis: Redis = Depends(get_redis),
):
    """
    Lambda endpoint to invalidate Redis cache after S3 upload completes.
    This prevents race condition where cache has local paths instead of S3 URLs.
    """
    # Simple auth: shared header
    expected = os.getenv("LAMBDA_HEADER", "lambda_header_feed_not_out")
    if x_api_key != expected:
        raise FittbotHTTPException(
            status_code=401,
            detail="Invalid API key",
            error_code="CACHE_INVALIDATION_INVALID_API_KEY",
        )

    # Validate payload
    try:
        gym_id = int(payload["gym_id"])
        post_id = int(payload["post_id"])
    except Exception:
        raise FittbotHTTPException(
            status_code=422,
            detail="Invalid payload format (gym_id/post_id required and must be integers)",
            error_code="CACHE_INVALIDATION_INVALID_PAYLOAD",
        )

    # Delete cache keys
    try:
        media_cache_key = f"post:{post_id}:media"
        gym_cache_key = f"gym:{gym_id}:posts"

        deleted_media = await redis.delete(media_cache_key)
        deleted_gym = await redis.delete(gym_cache_key)

        return {
            "ok": True,
            "deleted": {
                "media_cache": deleted_media,
                "gym_cache": deleted_gym
            }
        }
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error during cache invalidation",
            error_code="CACHE_INVALIDATION_ERROR",
            log_data={"error": repr(e)},
        )


# ──────────────────────────────────────────────────────────────
# WebSocket endpoint for clients to receive feed updates
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/posts/{gym_id}")
async def posts_ws(ws: WebSocket, gym_id: int):
    _hub = await ensure_hub()

    await ws.accept()
    await _hub.join(gym_id, ws)
    await ws.send_json({"action": "probe", "msg": "hello"})

    async def heartbeat():
        while ws.application_state == WebSocketState.CONNECTED:
            await asyncio.sleep(PING_SEC)
            try:
                await ws.send_json({"type": "ping"})
            except Exception:
                break

    hb_task = asyncio.create_task(heartbeat())
    try:
        while True:
            # We don't expect messages from client; just keep the socket open.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hb_task.cancel()
        await _hub._drop(gym_id, ws)
