"""
Integration tests for the auth middleware.

Tests the full request flow through AuthMiddleware using a minimal
FastAPI app with the real middleware attached.
"""

import pytest
from datetime import datetime, timedelta
from jose import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.testclient import TestClient

from app.middleware.auth_middleware import AuthMiddleware

# Use test-safe constants
SECRET_KEY = "test-secret-key-for-testing-only"
ALGORITHM = "HS256"


# ---------------------------------------------------------------------------
# Minimal test app
# ---------------------------------------------------------------------------

def _create_test_app():
    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/")
    async def root():
        return {"root": True}

    @app.get("/api/v1/protected")
    async def protected(request: Request):
        return {"user": request.state.user, "role": request.state.role}

    @app.get("/docs")
    async def docs():
        return {"docs": True}

    return app


@pytest.fixture
def client():
    """TestClient with auth middleware."""
    import unittest.mock as mock
    with mock.patch("app.middleware.auth_middleware.SECRET_KEY", SECRET_KEY), \
         mock.patch("app.middleware.auth_middleware.ALGORITHM", ALGORITHM):
        app = _create_test_app()
        yield TestClient(app)


def _make_token(sub="42", role="client", expired=False, extra=None):
    """Generate a JWT for testing."""
    exp = datetime.utcnow() + (timedelta(hours=-1) if expired else timedelta(hours=1))
    payload = {"sub": sub, "role": role, "exp": exp}
    if extra:
        payload.update(extra)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAuthMiddleware:
    def test_public_path_no_auth(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_root_public(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_docs_public(self, client):
        resp = client.get("/docs")
        assert resp.status_code == 200

    def test_protected_path_no_token(self, client):
        resp = client.get("/api/v1/protected")
        assert resp.status_code == 401
        assert "Missing authentication token" in resp.json()["detail"]

    def test_valid_bearer_token(self, client):
        token = _make_token(sub="100", role="client")
        resp = client.get("/api/v1/protected", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["user"] == "100"
        assert data["role"] == "client"

    def test_valid_cookie_token(self, client):
        token = _make_token(sub="200", role="owner")
        resp = client.get("/api/v1/protected", cookies={"access_token": token})
        assert resp.status_code == 200
        data = resp.json()
        assert data["user"] == "200"

    def test_expired_token(self, client):
        token = _make_token(expired=True)
        resp = client.get("/api/v1/protected", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401
        assert "expired" in resp.json()["detail"].lower() or "Session expired" in resp.json()["detail"]

    def test_invalid_token(self, client):
        resp = client.get("/api/v1/protected", headers={"Authorization": "Bearer not.a.valid.jwt"})
        assert resp.status_code == 401
        assert "Invalid token" in resp.json()["detail"]

    def test_invalid_header_format(self, client):
        resp = client.get("/api/v1/protected", headers={"Authorization": "Basic abc123"})
        assert resp.status_code == 401
        assert "Invalid authorization header format" in resp.json()["detail"]

    def test_options_bypass(self, client):
        resp = client.options("/api/v1/protected")
        assert resp.status_code in (200, 405)  # depends on endpoint method
