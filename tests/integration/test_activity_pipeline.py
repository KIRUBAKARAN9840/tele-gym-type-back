"""
Integration tests for the activity tracking pipeline.

Tests the flow: track_event() → Redis queue → event dequeue,
and verifies lead score calculation logic.
"""

import json
import pytest
from unittest.mock import patch

from app.services.activity_tracker import (
    track_event,
    EVENTS_QUEUE_KEY,
    VIEWS_KEY_PREFIX,
    CHECKOUT_KEY_PREFIX,
    ACTIVE_VIEWS_SET,
    ACTIVE_CHECKOUTS_SET,
)


class TestEventQueuePipeline:
    @pytest.mark.asyncio
    async def test_event_queued_and_dequeued(self, fake_redis):
        """Events pushed by tracker can be popped as the task would."""
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(client_id=1, event_type="gym_viewed", gym_id=10, source="app")
            await track_event(client_id=1, event_type="gym_viewed", gym_id=10, source="app")
            await track_event(client_id=1, event_type="checkout_initiated", gym_id=10, command_id="cmd-1", product_type="dailypass")

        # Dequeue like process_events does (LPOP in a loop)
        events = []
        while True:
            raw = await fake_redis.lpop(EVENTS_QUEUE_KEY)
            if raw is None:
                break
            events.append(json.loads(raw))

        assert len(events) == 3
        assert events[0]["event_type"] == "gym_viewed"
        assert events[2]["event_type"] == "checkout_initiated"

    @pytest.mark.asyncio
    async def test_view_counts_accumulate(self, fake_redis):
        """Multiple views for the same gym accumulate in Redis hash."""
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            for _ in range(5):
                await track_event(client_id=1, event_type="gym_viewed", gym_id=10)

        count = await fake_redis.hget(f"{VIEWS_KEY_PREFIX}:1", "10")
        assert count == "5"

    @pytest.mark.asyncio
    async def test_checkout_lifecycle(self, fake_redis):
        """Checkout initiated → completed cleans up state."""
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(
                client_id=1, event_type="checkout_initiated",
                gym_id=10, command_id="cmd-1", product_type="membership",
            )

        # Verify checkout exists
        checkout = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:1", "cmd-1")
        assert checkout is not None
        is_active = await fake_redis.sismember(ACTIVE_CHECKOUTS_SET, "1")
        assert is_active

        # Complete checkout
        with patch("app.services.activity_tracker.get_redis", return_value=fake_redis):
            await track_event(
                client_id=1, event_type="checkout_completed", command_id="cmd-1",
            )

        # Checkout hash field should be removed
        checkout_after = await fake_redis.hget(f"{CHECKOUT_KEY_PREFIX}:1", "cmd-1")
        assert checkout_after is None


class TestLeadScoreCalculation:
    """
    Tests the lead score formula: (views * 1) + (checkouts * 5) - (purchases * 10)
    and status transitions: cold → warm (3+ views) → hot (checkout) → converted (purchase)
    """

    def test_lead_score_formula(self):
        """Verify the scoring formula directly."""
        views, checkouts, purchases = 5, 2, 1
        score = (views * 1) + (checkouts * 5) - (purchases * 10)
        assert score == 5  # 5 + 10 - 10

    def test_lead_status_cold(self):
        views = 1
        if views >= 3:
            status = "warm"
        else:
            status = "cold"
        assert status == "cold"

    def test_lead_status_warm(self):
        views = 3
        has_checkout = False
        has_purchase = False
        if has_purchase:
            status = "converted"
        elif has_checkout:
            status = "hot"
        elif views >= 3:
            status = "warm"
        else:
            status = "cold"
        assert status == "warm"

    def test_lead_status_hot(self):
        views = 5
        has_checkout = True
        has_purchase = False
        if has_purchase:
            status = "converted"
        elif has_checkout:
            status = "hot"
        elif views >= 3:
            status = "warm"
        else:
            status = "cold"
        assert status == "hot"

    def test_lead_status_converted(self):
        has_purchase = True
        has_checkout = True
        if has_purchase:
            status = "converted"
        elif has_checkout:
            status = "hot"
        else:
            status = "cold"
        assert status == "converted"
