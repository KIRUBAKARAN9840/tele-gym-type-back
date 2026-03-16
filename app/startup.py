"""
Application bootstrap: middleware stack, HTTP middleware, health endpoints,
Prometheus metrics, and startup / shutdown lifecycle.

Every function receives the FastAPI `app` instance so main.py stays minimal.
"""

import os
import time
import asyncio
import logging

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.gzip import GZipMiddleware
from prometheus_client import Histogram, Counter

from app.config.settings import settings
from app.middleware.auth_middleware import AuthMiddleware
from app.middleware.app_key_middleware import AppKeyMiddleware
from app.middleware.rate_limit_middleware import (
    IPRateLimitMiddleware,
    EndpointSpecificRateLimit,
    get_real_client_ip,
)
from app.middleware.explicit_cors_origin import ExplicitCorsOriginMiddleware
from app.middleware.log_context import LogContextMiddleware
from app.utils.redis_config import get_redis
from app.utils.exception_handlers import install_exception_handlers
from app.utils.metrics import (
    get_metrics,
    get_metrics_content_type,
    set_app_info,
    collect_process_metrics,
    collect_db_pool_metrics,
    collect_celery_queue_metrics,
    HTTP_REQUEST_LATENCY,
    HTTP_REQUEST_TOTAL,
    HTTP_REQUESTS_IN_PROGRESS,
    SLOW_REQUESTS,
    normalize_endpoint,
    get_user_type,
)

# Security headers – optional dependency
try:
    from app.middleware.security_headers import (
        SecurityHeadersMiddleware,
        SecurityHeadersConfig,
        swagger_csp,
    )
    _HAS_SEC_HEADERS = True
except Exception:
    _HAS_SEC_HEADERS = False

log = logging.getLogger("app")

# ── Prometheus Metrics ──────────────────────────────────────────────
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Latency of HTTP requests in seconds",
    ["endpoint"],
    buckets=[0.05, 0.1, 0.3, 0.5, 1, 2, 5],
)
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["endpoint", "method", "status_code"],
)
RATE_LIMIT_BLOCKS = Counter(
    "http_rate_limit_blocks_total",
    "Requests blocked by rate limit",
    ["type", "endpoint"],
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. CLASS-BASED MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def configure_middleware(app: FastAPI) -> None:
    """
    Register class-based middleware.
    Starlette uses insert(0, …) so LAST added = outermost.

    Final stack (outer → inner):
      ExplicitCORS → CORS → Auth → SecurityHeaders → TrustedHost → GZip → LogContext
    """
    app.add_middleware(LogContextMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allowed_hosts)

    if _HAS_SEC_HEADERS:
        security_config = SecurityHeadersConfig(
            enable_hsts=settings.environment == "production",
            hsts_max_age=31536000,
            hsts_include_subdomains=True,
            hsts_preload=False,
            x_frame_options="DENY",
            referrer_policy="no-referrer",
            x_content_type_options="nosniff",
            permissions_policy="geolocation=(), microphone=(), camera=(), payment=(), usb=()",
            cross_origin_opener_policy="same-origin",
            cross_origin_embedder_policy=None,
            cross_origin_resource_policy="same-origin",
            x_permitted_cross_domain_policies="none",
            csp="default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none';",
            remove_server_header=True,
        )
        path_overrides = {
            "/docs": {"csp": swagger_csp()},
            "/redoc": {"csp": swagger_csp()},
            "/openapi.json": {"csp": swagger_csp()},
        }
        app.add_middleware(
            SecurityHeadersMiddleware,
            config=security_config,
            path_overrides=path_overrides,
        )

    app.add_middleware(AuthMiddleware)
    app.add_middleware(AppKeyMiddleware, api_key=settings.app_api_key)

    # CORS – resolve origins & handle wildcard fallback
    resolved_cors_origins = list(settings.cors_origins_resolved)
    cors_origin_regex = settings.cors_origin_regex

    if any(origin.strip() == "*" for origin in resolved_cors_origins):
        log.warning(
            "CORS_ORIGINS contains '*'. Wildcards cannot be used with cookies, "
            "so falling back to regex-based origin matching.",
        )
        resolved_cors_origins = [o for o in resolved_cors_origins if o.strip() != "*"]
        if not cors_origin_regex:
            cors_origin_regex = r"https?://.*"

    cors_kwargs: dict = {
        "allow_origins": resolved_cors_origins,
        "allow_credentials": True,
        "allow_methods": ["*"],
        "allow_headers": ["*"],
    }
    if cors_origin_regex:
        cors_kwargs["allow_origin_regex"] = cors_origin_regex

    app.add_middleware(CORSMiddleware, **cors_kwargs)
    app.add_middleware(
        ExplicitCorsOriginMiddleware,
        allowed_origins=resolved_cors_origins,
        origin_regex=cors_origin_regex,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. EXCEPTION HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def register_exception_handlers(app: FastAPI) -> None:
    """Install structured exception handlers + global catch-all."""
    install_exception_handlers(app)

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        from fastapi.exceptions import HTTPException

        if isinstance(exc, HTTPException):
            raise exc

        request_id = getattr(getattr(request, "state", None), "request_id", "unknown")
        log.exception("Unhandled exception %s", request_id)

        if settings.environment == "production":
            return JSONResponse(
                status_code=500,
                content={"error": "Internal server error", "request_id": request_id},
            )
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "request_id": request_id,
                "detail": str(exc),
            },
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. HTTP MIDDLEWARE (order-dependent)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def register_http_middleware(app: FastAPI) -> None:
    """
    Register @app.middleware("http") handlers.

    Registration order matters – first registered = innermost.
    Result: prometheus (outer) → rate_limit (inner) → class middleware → router
    """

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        """Enterprise-level rate limiting with JWT token extraction."""
        if request.scope.get("type") == "websocket":
            return await call_next(request)

        path = request.url.path
        if path in ("/health", "/health/ready", "/metrics", "/"):
            return await call_next(request)

        if path.startswith("/telecaller/"):
            return await call_next(request)

        is_admin_request = path.startswith("/api/admin/") or path.startswith("/marketing/")
        client_ip = get_real_client_ip(request)

        user_id = None
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                from jose import jwt
                from app.utils.security import SECRET_KEY, ALGORITHM

                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                user_id = payload.get("sub")
            except Exception:
                pass

        user_limiter = None
        if user_id:
            limiter_attr = (
                "admin_user_rate_limiter"
                if is_admin_request and hasattr(app.state, "admin_user_rate_limiter")
                else "user_rate_limiter"
            )
            if hasattr(app.state, limiter_attr):
                user_limiter = getattr(app.state, limiter_attr)

        if user_id and user_limiter:
            subject = f"user:{user_id}"
            user_blocked, user_info = await user_limiter.is_subject_limited(subject=subject)

            if user_blocked:
                RATE_LIMIT_BLOCKS.labels(type="user", endpoint=path).inc()
                retry_after = str(user_info.get("retry_after", 60))
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": "User rate limit exceeded",
                        "message": f"Too many requests for user {user_id}. Rate limit persists across app restarts.",
                        "subject": subject,
                        "rate_limit_info": user_info,
                        "type": "user_limit",
                        "user_id": user_id,
                    },
                    headers={
                        "Retry-After": retry_after,
                        "X-RateLimit-Type": "user",
                        "X-RateLimit-User-ID": str(user_id),
                        "X-RateLimit-Remaining": str(
                            max(0, user_limiter.requests_per_minute - user_info.get("minute_count", 0))
                        ),
                    },
                )
        else:
            limiter_attr = (
                "admin_ip_rate_limiter"
                if is_admin_request and hasattr(app.state, "admin_ip_rate_limiter")
                else "ip_rate_limiter"
            )
            if hasattr(app.state, limiter_attr):
                ip_limiter = getattr(app.state, limiter_attr)
                ip_blocked, ip_info = await ip_limiter.is_subject_limited(subject=client_ip)

                if ip_blocked:
                    RATE_LIMIT_BLOCKS.labels(type="ip", endpoint=path).inc()
                    retry_after = str(ip_info.get("retry_after", 60))
                    return JSONResponse(
                        status_code=429,
                        content={
                            "error": "IP rate limit exceeded",
                            "message": "Too many requests from your IP. Please authenticate for higher limits.",
                            "subject": client_ip,
                            "rate_limit_info": ip_info,
                            "type": "ip_limit",
                        },
                        headers={
                            "Retry-After": retry_after,
                            "X-RateLimit-Type": "ip",
                            "X-RateLimit-Limit-Minute": str(ip_limiter.requests_per_minute),
                            "X-RateLimit-Limit-Hour": str(ip_limiter.requests_per_hour),
                            "X-RateLimit-Remaining": str(
                                max(0, ip_limiter.requests_per_minute - ip_info.get("minute_count", 0))
                            ),
                        },
                    )

        if (not is_admin_request) and hasattr(app.state, "endpoint_rate_limiter"):
            ep_limiter = app.state.endpoint_rate_limiter
            blocked, detail = await ep_limiter.check(path, client_ip)
            if blocked:
                RATE_LIMIT_BLOCKS.labels(type="endpoint", endpoint=detail["pattern"]).inc()
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": "Endpoint rate limit exceeded",
                        "message": f"Too many requests to {detail['pattern']} endpoint",
                        "endpoint": detail["pattern"],
                        "limits": detail["limits"],
                        "rate_limit_info": detail["info"],
                        "type": "endpoint_limit",
                    },
                    headers={"Retry-After": str(detail["info"]["retry_after"])},
                )

        return await call_next(request)

    @app.middleware("http")
    async def prometheus_middleware(request: Request, call_next):
        """Enhanced metrics middleware with detailed tracking."""
        if request.url.path in ("/health", "/health/ready", "/metrics", "/"):
            return await call_next(request)

        start = time.perf_counter()
        path = request.url.path
        method = request.method
        endpoint = normalize_endpoint(path)

        HTTP_REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).inc()

        status_code = 500
        try:
            resp = await call_next(request)
            status_code = resp.status_code
            return resp
        finally:
            duration = time.perf_counter() - start
            duration_ms = int(duration * 1000)

            HTTP_REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()

            HTTP_REQUEST_LATENCY.labels(
                method=method, endpoint=endpoint, status_code=str(status_code)
            ).observe(duration)

            user_type = get_user_type(request)
            HTTP_REQUEST_TOTAL.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code),
                user_type=user_type,
            ).inc()

            if duration_ms >= 1000:
                SLOW_REQUESTS.labels(
                    method=method, endpoint=endpoint, threshold_ms="1000"
                ).inc()

            # Legacy metrics for backward compatibility
            REQUEST_LATENCY.labels(endpoint=path).observe(duration)
            try:
                REQUEST_COUNT.labels(
                    endpoint=path, method=method, status_code=status_code
                ).inc()
            except Exception:
                pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. HEALTH / READINESS / ROOT / METRICS ENDPOINTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def register_health_endpoints(app: FastAPI) -> None:
    """Register operational endpoints that live outside domain routers."""

    @app.get("/health")
    async def simple_health_check():
        """Simple health check for ECS (no trailing slash, no dependencies)"""
        return {"status": "ok", "version": "1.0.0"}

    @app.get("/health/ready")
    async def readiness_check():
        """Readiness check for Kubernetes"""
        try:
            redis_client = await get_redis()
            await redis_client.ping()
            return {
                "status": "ready",
                "version": "1.0.0",
                "checks": {"redis": "ok", "database": "ok"},
            }
        except Exception as e:
            log.error(f"Readiness check failed: {e}")
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "error": str(e)},
            )

    @app.get("/")
    async def root():
        """API root endpoint"""
        return {
            "message": "Welcome to the Fymble API",
            "version": "1.0.0",
            "docs_url": "/docs" if settings.environment != "production" else None,
        }

    @app.get("/metrics")
    async def metrics():
        """Prometheus metrics endpoint with comprehensive monitoring"""
        collect_process_metrics()

        try:
            from app.models.database import get_engine

            engine = get_engine()
            if engine:
                collect_db_pool_metrics(engine)
        except Exception:
            pass

        try:
            from app.utils.redis_config import get_redis as _get_redis, _redis_pool
            from app.utils.metrics import collect_redis_metrics

            redis_client = await _get_redis()
            await collect_redis_metrics(redis_client, _redis_pool)
        except Exception:
            pass

        return Response(get_metrics(), media_type=get_metrics_content_type())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. LIFECYCLE (startup / shutdown / Razorpay client)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def register_lifecycle_events(app: FastAPI) -> None:
    """Wire up startup, shutdown, and the Razorpay async-client lifecycle."""

    from app.fittbot_api.v1.payments.razorpay_async_gateway import (
        init_client as init_rzp_client,
        close_client as close_rzp_client,
    )
    from app.fittbot_api.v1.websockets.websocket_feed import RoomHub
    import app.fittbot_api.v1.websockets.websocket_feed as ws_feed
    import app.fittbot_api.v1.websockets.websocket_live_gb as ws_live
    from app.fittbot_api.v1.websockets.websocket_live_gb import PatternHub
    from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.kb_store import KB

    # ── Razorpay async client ───────────────────────────────────────
    @app.on_event("startup")
    async def _startup_rzp_client():
        try:
            await init_rzp_client()
        except Exception as exc:
            log.error("Failed to init Razorpay async client", extra={"error": repr(exc)})

    @app.on_event("shutdown")
    async def _shutdown_rzp_client():
        try:
            await close_rzp_client()
        except Exception as exc:
            log.error("Failed to close Razorpay async client", extra={"error": repr(exc)})

    # ── Main startup ────────────────────────────────────────────────
    async def startup():
        try:
            # Distributed tracing (OpenTelemetry)
            try:
                from app.utils.tracing import init_tracing

                init_tracing(
                    service_name="fittbot-api",
                    environment=settings.environment,
                    otlp_endpoint=os.getenv("OTLP_ENDPOINT"),
                    sample_rate=0.1 if settings.environment == "production" else 1.0,
                )
            except Exception as e:
                log.warning(f"Tracing initialization skipped: {e}")

            # Sentry error tracking
            try:
                from app.utils.sentry_config import init_sentry

                init_sentry(
                    environment=settings.environment,
                    traces_sample_rate=0.1 if settings.environment == "production" else 0.5,
                    profiles_sample_rate=0.1,
                )
            except Exception as e:
                log.warning(f"Sentry initialization skipped: {e}")

            # Application info for metrics
            set_app_info(
                version="1.0.0",
                environment=settings.environment,
                commit_sha=os.getenv("GIT_COMMIT", "unknown"),
            )

            # Redis & FastAPILimiter
            redis_client = await get_redis()

            from fastapi_limiter import FastAPILimiter

            async def async_get_real_client_ip(request):
                return get_real_client_ip(request)

            await FastAPILimiter.init(redis_client, identifier=async_get_real_client_ip)

            # Rate limiters – IP tier
            app.state.ip_rate_limiter = IPRateLimitMiddleware(
                redis_client=redis_client,
                requests_per_minute=settings.rate_limit_requests_per_minute,
                requests_per_hour=settings.rate_limit_requests_per_hour,
                requests_per_day=settings.rate_limit_requests_per_day,
                burst_limit=settings.rate_limit_burst_limit,
                burst_window=settings.rate_limit_burst_window,
                whitelist_subjects=settings.whitelist_ips_list,
            )

            # Rate limiters – user tier
            app.state.user_rate_limiter = IPRateLimitMiddleware(
                redis_client=redis_client,
                requests_per_minute=settings.user_limit_requests_per_minute,
                requests_per_hour=settings.user_limit_requests_per_hour,
                requests_per_day=settings.user_limit_requests_per_day,
                burst_limit=settings.user_limit_burst_limit,
                burst_window=settings.user_limit_burst_window,
                whitelist_subjects=[],
            )

            # Rate limiters – admin IP tier
            app.state.admin_ip_rate_limiter = IPRateLimitMiddleware(
                redis_client=redis_client,
                requests_per_minute=settings.admin_rate_limit_requests_per_minute,
                requests_per_hour=settings.admin_rate_limit_requests_per_hour,
                requests_per_day=settings.admin_rate_limit_requests_per_day,
                burst_limit=settings.admin_rate_limit_burst_limit,
                burst_window=settings.admin_rate_limit_burst_window,
                whitelist_subjects=settings.whitelist_ips_list,
            )

            # Rate limiters – admin user tier
            app.state.admin_user_rate_limiter = IPRateLimitMiddleware(
                redis_client=redis_client,
                requests_per_minute=settings.admin_user_limit_requests_per_minute,
                requests_per_hour=settings.admin_user_limit_requests_per_hour,
                requests_per_day=settings.admin_user_limit_requests_per_day,
                burst_limit=settings.admin_user_limit_burst_limit,
                burst_window=settings.admin_user_limit_burst_window,
                whitelist_subjects=[],
            )

            # Rate limiters – endpoint-specific
            app.state.endpoint_rate_limiter = EndpointSpecificRateLimit(
                redis_client=redis_client,
            )

            # WebSocket hubs
            legacy_hub = RoomHub(redis_client)
            await legacy_hub.start()
            ws_feed.hub = legacy_hub

            session_hub = PatternHub(redis_client, "sessions:")
            live_hub = PatternHub(redis_client, "live:")
            chat_hub = PatternHub(redis_client, "chat:")
            await asyncio.gather(session_hub.start(), live_hub.start(), chat_hub.start())

            ws_live.session_hub = session_hub
            ws_live.live_hub = live_hub
            ws_live.chat_hub = chat_hub

            app.state.session_hub = session_hub
            app.state.live_hub = live_hub
            app.state.chat_hub = chat_hub

            # Core state
            app.state.rds = redis_client
            app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0))

            from app.utils.openai_pool import get_openai_client

            app.state.oai = get_openai_client()  # Weighted pool (Tier3: 83%, Tier1: 8.5% each)
            KB.bind_oai(app.state.oai)

            # Database initialization
            from app.models.database import _ensure_engine, engine

            _ensure_engine()

            try:
                from app.middleware.metrics_middleware import DatabaseMetricsMiddleware

                DatabaseMetricsMiddleware(engine)
                log.info("Database metrics collection enabled")
            except Exception as e:
                log.warning(f"Database metrics initialization skipped: {e}")

            # Background metrics collection
            asyncio.create_task(_collect_metrics_periodically(redis_client))

        except Exception as e:
            log.error(f"Failed to start application: {e}")
            raise

    # ── Main shutdown ───────────────────────────────────────────────
    async def shutdown():
        try:
            if hasattr(app.state, "http"):
                await app.state.http.aclose()

            if hasattr(app.state, "rds"):
                await app.state.rds.close()

            from app.utils.redis_config import close_redis

            await close_redis()

            from app.utils.leegality_client import get_leegality_client

            try:
                leegality_client = get_leegality_client()
                await leegality_client.aclose()
            except Exception as e:
                log.warning(f"Error closing Leegality client: {e}")

        except Exception as e:
            log.error(f"Error during shutdown: {e}")

    app.add_event_handler("startup", startup)
    app.add_event_handler("shutdown", shutdown)


async def _collect_metrics_periodically(redis_client):
    """Background task to collect metrics every 30 seconds."""
    while True:
        try:
            await asyncio.sleep(30)
            collect_process_metrics()
            await collect_celery_queue_metrics(redis_client)

            try:
                from app.utils.metrics import collect_celery_worker_metrics

                collect_celery_worker_metrics()
            except Exception:
                pass

            try:
                from app.models.database import engine

                collect_db_pool_metrics(engine)
            except Exception:
                pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.debug(f"Metrics collection error: {e}")
