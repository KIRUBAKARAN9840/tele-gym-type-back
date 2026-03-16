"""WebSocket routes: real-time feed, live gym buddy, general websocket."""

from fastapi import APIRouter

from app.fittbot_api.v1.websockets.websocket import router as websocket_router
from app.fittbot_api.v1.websockets.websocket_feed import router as websocket_feed_router
from app.fittbot_api.v1.websockets.websocket_live_gb import router as websocket_live_gb_router

router = APIRouter()

router.include_router(websocket_router)
router.include_router(websocket_feed_router)
router.include_router(websocket_live_gb_router)
