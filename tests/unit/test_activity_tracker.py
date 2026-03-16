"""
Unit tests for the activity tracker service.

All tests use fakeredis so no real Redis instance is needed.
"""

import json
import pytest
from unittest.mock import patch, AsyncMock

from app.services.activity_tracker import (
    track_event,
    _track_view,
    _track_checkout,
    _clear_checkout,
    EVENTS_QUEUE_KEY,
    VIEWS_KEY_PREFIX,
    CHECKOUT_KEY_PREFIX,
    ACTIVE_VIEWS_SET,
    ACTIVE_CHECKOUTS_SET,
    VIEWS_TTL,
    CHECKOUT_TTL,
)


# ---------------------------------------------------------------------------
# View tracking
# ---------------------------------------------------------------------------

class TestTrackView:
    @pytest.mark.asyncio
    async def test_track_view_increments_hash(self, fake_redis):
        await _track_view(fake_redis, client_id=42, gym_id=10)
        val = await fake_redis.hget(f"{VIEWS_KEY_PREFIX}:42", "10")
        assert val == "1"

    @pytest.mark.asyncio
    async def test_track_view_increments_twice(self, fake_redis):
        await _track_view(fake_redis, client_id=42, gym_id=10)
        await _track_view(fake_redis, client_id=42, gym_id=10)
        val = await fake_redis.hget(f"{VIEWS_KEY_PREFIX}:42", "10")
        assert val == "2"

    @pytest.mark.asyncio
    async def test_track_view_no_gym_id_skips(self, fake_redis):
        await _track_view(fake_redis, client_id=42, gym_id=None)
        exists = await fake_redis.exists(f"{VIEWS_KEY_PREFIX}:42")
        assert exists == 0

    @pytest.mark.asyncio
    async def test_track_view_sets_ttl(self, fake_redis):
        await _track_view(fake_redis, client_id=42, gym_id=10)
        ttl = await fake_redis.ttl(f"{VIEWS_KEY_PREFIX}:42")
        assert 0 < ttl <= VIEWS_TTL

    @pytest.mark.asyncio
    async def test_track_view_adds_to_active_set(self, fake_redis):
        await _track_view(fake_redis, client_id=42, gym_id=10)
        is_member = await fake_redis.sismember(ACTIVE_VIEWS_SET, "42")
        assert is_member


# ---------------------------------------------------------------------------
# Checkout tracking
# ---------------------------------------------------------------------------

class TestTrackCheckout:
    @pytest.mark.asyncio
    async def test_track_checkout_stores_data(self, fake_redis):
        await _track_checkout(fake_redis, client_id=7, command_id="cmd-1", gym_id=5, product_type="dailypass")
        raw = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:7", "cmd-1")
        data = json.loads(raw)
        assert data["gym_id"] == 5
        assert data["product_type"] == "dailypass"
        assert "initiated_at" in data

    @pytest.mark.asyncio
    async def test_track_checkout_sets_ttl(self, fake_redis):
        await _track_checkout(fake_redis, client_id=7, command_id="cmd-1", gym_id=5, product_type="dailypass")
        ttl = await fake_redis.ttl(f"{CHECKOUT_KEY_PREFIX}:7")
        assert 0 < ttl <= CHECKOUT_TTL

    @pytest.mark.asyncio
    async def test_track_checkout_adds_to_active_set(self, fake_redis):
        await _track_checkout(fake_redis, client_id=7, command_id="cmd-1", gym_id=5, product_type="dailypass")
        is_member = await fake_redis.sismember(ACTIVE_CHECKOUTS_SET, "7")
        assert is_member


# ---------------------------------------------------------------------------
# Clear checkout
# ---------------------------------------------------------------------------

class TestClearCheckout:
    @pytest.mark.asyncio
    async def test_clear_checkout_removes_field(self, fake_redis):
        await _track_checkout(fake_redis, client_id=7, command_id="cmd-1", gym_id=5, product_type="dailypass")
        await _clear_checkout(fake_redis, client_id=7, command_id="cmd-1")
        val = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:7", "cmd-1")
        assert val is None


# ---------------------------------------------------------------------------
# Full track_event function
# ---------------------------------------------------------------------------

class TestTrackEvent:
    @pytest.mark.asyncio
    async def test_track_gym_viewed_pushes_to_queue(self, fake_redis):
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(client_id=1, event_type="gym_viewed", gym_id=10)
        raw = await fake_redis.lpop(EVENTS_QUEUE_KEY)
        event = json.loads(raw)
        assert event["client_id"] == 1
        assert event["event_type"] == "gym_viewed"
        assert event["gym_id"] == 10
        assert "created_at" in event

    @pytest.mark.asyncio
    async def test_track_checkout_initiated(self, fake_redis):
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(
                client_id=1,
                event_type="checkout_initiated",
                gym_id=10,
                product_type="membership",
                command_id="cmd-99",
            )
        # Should have queued event AND created checkout hash
        queue_len = await fake_redis.llen(EVENTS_QUEUE_KEY)
        assert queue_len == 1
        checkout_data = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:1", "cmd-99")
        assert checkout_data is not None

    @pytest.mark.asyncio
    async def test_track_checkout_completed_clears(self, fake_redis):
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(client_id=1, event_type="checkout_initiated", gym_id=10, command_id="cmd-99", product_type="membership")
            await track_event(client_id=1, event_type="checkout_completed", command_id="cmd-99")
        checkout_data = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:1", "cmd-99")
        assert checkout_data is None

    @pytest.mark.asyncio
    async def test_track_checkout_no_command_id_skips(self, fake_redis):
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(client_id=1, event_type="checkout_initiated", gym_id=10)
        # Should still queue the event
        queue_len = await fake_redis.llen(EVENTS_QUEUE_KEY)
        assert queue_len == 1
        # But should NOT create checkout hash (no command_id)
        exists = await fake_redis.exists(f"{CHECKOUT_KEY_PREFIX}:1")
        assert exists == 0

    @pytest.mark.asyncio
    async def test_redis_error_is_caught(self):
        """Redis failures should be logged, not raised."""
        broken_redis = AsyncMock()
        broken_redis.rpush = AsyncMock(side_effect=ConnectionError("Redis down"))
        with patch("app.services.activity_tracker.get_redis", return_value=broken_redis):
            # Should not raise
            await track_event(client_id=1, event_type="gym_viewed", gym_id=10)

    @pytest.mark.asyncio
    async def test_event_json_has_all_fields(self, fake_redis):
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(
                client_id=42,
                event_type="dailypass_viewed",
                gym_id=5,
                product_type="dailypass",
                product_details={"price": 49},
                source="app",
                command_id="c-1",
            )
        raw = await fake_redis.lpop(EVENTS_QUEUE_KEY)
        event = json.loads(raw)
        assert event["client_id"] == 42
        assert event["event_type"] == "dailypass_viewed"
        assert event["gym_id"] == 5
        assert event["product_type"] == "dailypass"
        assert event["product_details"] == {"price": 49}
        assert event["source"] == "app"
        assert event["command_id"] == "c-1"
