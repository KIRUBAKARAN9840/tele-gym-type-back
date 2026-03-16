"""
Integration tests for health/readiness endpoints.

Uses a minimal FastAPI app that mirrors the real health route patterns.
"""

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient


def _create_health_app():
    app = FastAPI()

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.get("/")
    async def root():
        return {"service": "Fittbot API", "status": "running"}

    return app


@pytest.fixture
def client():
    app = _create_health_app()
    return TestClient(app)


class TestHealthEndpoints:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"

    def test_root_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "status" in resp.json()

    def test_nonexistent_returns_404(self, client):
        resp = client.get("/does-not-exist")
        assert resp.status_code == 404
