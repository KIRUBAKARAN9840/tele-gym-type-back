"""Miscellaneous routes: notifications, telecaller."""

from fastapi import APIRouter

from app.fittbot_api.v1.notifications.send_rich_notification import router as rich_notification_router
from app.telecaller.router import router as telecaller_router

# Side-effect import: registers telecaller status handlers at module load
from app.telecaller.status import gym_registration_status  # noqa: F401

router = APIRouter()

router.include_router(rich_notification_router)
router.include_router(telecaller_router)
