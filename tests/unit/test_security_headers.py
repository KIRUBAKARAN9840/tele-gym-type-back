"""
Unit tests for the SecurityHeadersMiddleware.

Tests config presets, header injection, path overrides, and CSP variants.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock

from app.middleware.security_headers import (
    SecurityHeadersConfig,
    SecurityHeadersMiddleware,
    swagger_csp,
    api_csp,
    production_config,
    development_config,
    auto_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request(path="/api/v1/test", scheme="https", forwarded_proto=None, content_type=None):
    """Build a minimal mock Request."""
    req = MagicMock()
    req.url.path = path
    req.url.scheme = scheme
    headers = {}
    if forwarded_proto:
        headers["x-forwarded-proto"] = forwarded_proto
    req.headers = MagicMock()
    req.headers.get = lambda key, default=None: headers.get(key, default)
    return req


def _make_response(content_type="application/json"):
    """Build a minimal mock Response with real headers dict."""
    resp = MagicMock()
    resp.headers = {"content-type": content_type}
    return resp


# ---------------------------------------------------------------------------
# Config preset tests
# ---------------------------------------------------------------------------

class TestConfigPresets:
    def test_production_config_enables_hsts(self):
        cfg = production_config()
        assert cfg.enable_hsts is True
        assert cfg.hsts_max_age == 31536000
        assert cfg.hsts_preload is True

    def test_production_config_strict_csp(self):
        cfg = production_config()
        assert "default-src 'none'" in cfg.csp

    def test_development_config_disables_hsts(self):
        cfg = development_config()
        assert cfg.enable_hsts is False

    def test_development_config_permissive_csp(self):
        cfg = development_config()
        assert "'unsafe-inline'" in cfg.csp

    def test_auto_config_development(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "development")
        cfg = auto_config()
        assert cfg.enable_hsts is False

    def test_auto_config_production(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "production")
        cfg = auto_config()
        assert cfg.enable_hsts is True


# ---------------------------------------------------------------------------
# CSP function tests
# ---------------------------------------------------------------------------

class TestCSPFunctions:
    def test_swagger_csp_allows_cdn(self):
        csp = swagger_csp()
        assert "cdn.jsdelivr.net" in csp
        assert "unsafe-eval" in csp

    def test_api_csp_restrictive(self):
        csp = api_csp()
        assert "default-src 'none'" in csp
        assert "frame-ancestors 'none'" in csp


# ---------------------------------------------------------------------------
# Header injection tests
# ---------------------------------------------------------------------------

class TestSecurityHeadersMiddleware:
    def _apply_headers(self, config, path="/api/test", scheme="https", overrides=None, content_type="application/json"):
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=config, path_overrides=overrides)
        req = _make_request(path=path, scheme=scheme)
        resp = _make_response(content_type=content_type)
        mw._add_security_headers(resp, config, req)
        return resp.headers

    def test_hsts_header_on_https(self):
        cfg = SecurityHeadersConfig(enable_hsts=True, hsts_max_age=31536000, hsts_include_subdomains=True)
        headers = self._apply_headers(cfg)
        assert "Strict-Transport-Security" in headers
        assert "max-age=31536000" in headers["Strict-Transport-Security"]
        assert "includeSubDomains" in headers["Strict-Transport-Security"]

    def test_hsts_not_added_on_http(self):
        cfg = SecurityHeadersConfig(enable_hsts=True)
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=cfg)
        req = _make_request(scheme="http")
        resp = _make_response()
        mw._add_security_headers(resp, cfg, req)
        assert "Strict-Transport-Security" not in resp.headers

    def test_hsts_with_forwarded_proto(self):
        cfg = SecurityHeadersConfig(enable_hsts=True, hsts_max_age=31536000)
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=cfg)
        req = _make_request(scheme="http", forwarded_proto="https")
        resp = _make_response()
        mw._add_security_headers(resp, cfg, req)
        assert "Strict-Transport-Security" in resp.headers

    def test_xframe_options_deny(self):
        cfg = SecurityHeadersConfig(x_frame_options="DENY")
        headers = self._apply_headers(cfg)
        assert headers["X-Frame-Options"] == "DENY"

    def test_content_type_nosniff(self):
        cfg = SecurityHeadersConfig(x_content_type_options="nosniff")
        headers = self._apply_headers(cfg)
        assert headers["X-Content-Type-Options"] == "nosniff"

    def test_csp_set(self):
        cfg = SecurityHeadersConfig(csp="default-src 'self'")
        headers = self._apply_headers(cfg)
        assert headers["Content-Security-Policy"] == "default-src 'self'"

    def test_cache_control_for_api_routes(self):
        cfg = SecurityHeadersConfig()
        headers = self._apply_headers(cfg, path="/api/v1/clients")
        assert headers["Cache-Control"] == "no-cache, no-store, must-revalidate"

    def test_cache_control_for_json_content(self):
        cfg = SecurityHeadersConfig()
        headers = self._apply_headers(cfg, path="/something", content_type="application/json")
        assert headers["Cache-Control"] == "no-cache, no-store, must-revalidate"

    def test_remove_server_header(self):
        cfg = SecurityHeadersConfig(remove_server_header=True)
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=cfg)
        req = _make_request()
        resp = _make_response()
        resp.headers["server"] = "uvicorn"
        mw._add_security_headers(resp, cfg, req)
        assert "server" not in resp.headers

    def test_custom_headers_added(self):
        cfg = SecurityHeadersConfig(custom_headers={"X-Custom": "value123"})
        headers = self._apply_headers(cfg)
        assert headers["X-Custom"] == "value123"


# ---------------------------------------------------------------------------
# Path override tests
# ---------------------------------------------------------------------------

class TestPathOverrides:
    def test_exact_path_override(self):
        base_cfg = SecurityHeadersConfig(x_frame_options="DENY")
        overrides = {"/docs": {"x_frame_options": "SAMEORIGIN"}}
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=base_cfg, path_overrides=overrides)
        resolved = mw._get_config_for_path("/docs")
        assert resolved.x_frame_options == "SAMEORIGIN"

    def test_prefix_path_override(self):
        base_cfg = SecurityHeadersConfig(csp=api_csp())
        overrides = {"/docs*": {"csp": swagger_csp()}}
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=base_cfg, path_overrides=overrides)
        resolved = mw._get_config_for_path("/docs/swagger")
        assert "cdn.jsdelivr.net" in resolved.csp

    def test_no_override_returns_base(self):
        base_cfg = SecurityHeadersConfig(x_frame_options="DENY")
        app = MagicMock()
        mw = SecurityHeadersMiddleware(app, config=base_cfg, path_overrides={})
        resolved = mw._get_config_for_path("/api/v1/test")
        assert resolved.x_frame_options == "DENY"
