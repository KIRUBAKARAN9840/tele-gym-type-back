"""FITTBOT API – Enterprise-grade fitness and gym management platform."""

from fastapi import FastAPI

from app.config.settings import settings
from app.utils.logging_config import setup_logging

setup_logging()

# ── Application ─────────────────────────────────────────────────────
_is_prod = settings.environment == "production"

app = FastAPI(
    title="Fymble API",
    description="Enterprise-grade fitness and gym management API",
    version="1.0.0",
    openapi_url=None if _is_prod else "/openapi.json",
    docs_url=None if _is_prod else "/docs",
    redoc_url=None if _is_prod else "/redoc",
)

# ── Middleware & Exception Handlers ─────────────────────────────────
from app.startup import (
    configure_middleware,
    register_exception_handlers,
    register_http_middleware,
    register_health_endpoints,
    register_lifecycle_events,
)

configure_middleware(app)
register_exception_handlers(app)


from app.routers.core_routes import router as core_router
from app.routers.payment_routes import router as payment_router
from app.routers.client_routes import router as client_router
from app.routers.owner_routes import router as owner_router
from app.routers.ai_routes import router as ai_router
from app.routers.ws_routes import router as ws_router
from app.routers.marketing_routes import router as marketing_router
from app.routers.admin_routes import router as admin_router
from app.routers.misc_routes import router as misc_router

for r in (
    core_router,
    payment_router,
    client_router,
    owner_router,
    ai_router,
    ws_router,
    marketing_router,
    admin_router,
    misc_router,
):
    app.include_router(r)

# ── HTTP Middleware (order-dependent: rate_limit then prometheus) ────
register_http_middleware(app)

# ── Health, Metrics & Root Endpoints ────────────────────────────────
register_health_endpoints(app)

# ── Lifecycle (startup / shutdown / Razorpay async client) ──────────
register_lifecycle_events(app)
