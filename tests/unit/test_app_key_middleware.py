"""
Unit tests for the AppKeyMiddleware.

Tests key validation, skip paths, OPTIONS bypass, and disabled mode.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from starlette.responses import JSONResponse

from app.middleware.app_key_middleware import AppKeyMiddleware, _SKIP_PATHS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request(path="/api/v1/test", method="GET", app_key=None):
    req = MagicMock()
    req.url.path = path
    req.method = method
    headers = {}
    if app_key is not None:
        headers["X-App-Key"] = app_key
    req.headers = MagicMock()
    req.headers.get = lambda key, default=None: headers.get(key, default)
    return req


async def _dispatch(middleware, request):
    """Simulate dispatch by calling the middleware's dispatch method."""
    call_next = AsyncMock(return_value=JSONResponse(status_code=200, content={"ok": True}))
    response = await middleware.dispatch(request, call_next)
    return response, call_next


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAppKeyMiddleware:
    @pytest.mark.asyncio
    async def test_valid_key_passes(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(app_key="secret-123")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200
        call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalid_key_rejected(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(app_key="wrong-key")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 403
        call_next.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_key_rejected(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(app_key=None)
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_disabled_mode_passes_all(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key=None)
        req = _make_request()
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200
        call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_options_bypass(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(method="OPTIONS")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200
        call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skip_health_path(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(path="/health")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200
        call_next.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skip_metrics_path(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(path="/metrics")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_skip_webhook_prefix(self):
        mw = AppKeyMiddleware(app=MagicMock(), api_key="secret-123")
        req = _make_request(path="/razorpay_payments/webhooks/razorpay")
        resp, call_next = await _dispatch(mw, req)
        assert resp.status_code == 200
