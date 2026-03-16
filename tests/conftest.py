"""
Shared test fixtures for the Fittbot test suite.

Provides fake Redis, mock DB sessions, and test-safe configuration
so tests never touch real infrastructure.
"""

import os

# Set test environment BEFORE any app imports
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")
os.environ.setdefault("ALGORITHM", "HS256")

import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis

from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Fake Redis (async, in-memory)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def fake_redis():
    """Provide a fresh fakeredis instance, flushed after each test."""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield redis
    await redis.flushall()
    await redis.close()


# ---------------------------------------------------------------------------
# Mock async DB session
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    """Provide an AsyncMock that behaves like sqlalchemy AsyncSession."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.execute = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# Test JWT constants
# ---------------------------------------------------------------------------

TEST_SECRET_KEY = "test-secret-key-for-testing-only"
TEST_ALGORITHM = "HS256"
