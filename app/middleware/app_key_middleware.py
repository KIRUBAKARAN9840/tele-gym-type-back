import logging
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("app_key_middleware")

# Paths that should never require the app key (infra / webhooks)
_SKIP_PATHS = frozenset({
    "/health", "/health/ready", "/metrics", "/", "/docs", "/redoc", "/openapi.json",
})

_SKIP_PREFIXES = (
    "/razorpay_payments/webhooks",
    "/revenuecat/webhooks",
    "/revenuecat_v2/webhooks",
    "/razorpay_payments_v2/webhook",
    "/webhooks/",
    "/whatsapp/dlr",
)


class AppKeyMiddleware(BaseHTTPMiddleware):
    """
    Validates a shared secret sent by the mobile app in the ``X-App-Key``
    header.  Requests without a valid key receive a 403.

    Set the ``APP_API_KEY`` env-var (or add it to ``.env``) to enable.
    When the key is *not* configured the middleware is a transparent pass-through
    so existing environments are unaffected.
    """

    def __init__(self, app, *, api_key: Optional[str] = None):
        super().__init__(app)
        self.api_key = api_key

    async def dispatch(self, request: Request, call_next):
        # Disabled when no key is configured
        if not self.api_key:
            return await call_next(request)

        # Let preflight through
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path

        # Skip infra & webhook paths
        if path in _SKIP_PATHS or any(path.startswith(p) for p in _SKIP_PREFIXES):
            return await call_next(request)

        # Validate
        client_key = request.headers.get("X-App-Key")
        if client_key != self.api_key:
            logger.warning(
                "[app-key] Rejected request – missing or invalid X-App-Key",
                extra={"path": path, "method": request.method},
            )
            return JSONResponse(
                status_code=403,
                content={"detail": "Forbidden – invalid app key"},
            )

        return await call_next(request)
