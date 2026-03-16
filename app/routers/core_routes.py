"""Core infrastructure routes: health, monitoring, auth, app version, load testing."""

from fastapi import APIRouter

from app.fittbot_api.v1.health import router as health_router
from app.fittbot_api.v1.monitoring import router as monitoring_router
from app.fittbot_api.v1.load_test.load_test_router import router as load_test_router
from app.fittbot_api.v1.app_version import router as app_version_router
from app.fittbot_api.v1.auth.auth import router as auth_router

router = APIRouter()

router.include_router(health_router)
router.include_router(monitoring_router)
router.include_router(load_test_router, prefix="/api/v1")
router.include_router(app_version_router)
router.include_router(auth_router)
