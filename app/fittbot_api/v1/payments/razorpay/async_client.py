"""Asynchronous Razorpay client built on top of the shared async HTTP utilities."""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from ..config.settings import get_payment_settings
from ..utils.async_http_client import get_async_http_client
from .crypto import auth_header

RZP_API_BASE = "https://api.razorpay.com/v1"
RZP_CLIENT_KEY = "razorpay"


async def _get_client():
    return await get_async_http_client(
        RZP_CLIENT_KEY,
        base_url=RZP_API_BASE,
        max_connections=300,
        max_keepalive_connections=80,
        max_retries=4,
        backoff_factor=0.5,
        backoff_max=8.0,
    )


def _headers() -> Dict[str, str]:
    settings = get_payment_settings()
    return {
        "Content-Type": "application/json",
        **auth_header(settings.razorpay_key_id, settings.razorpay_key_secret),
    }


async def get_plan(plan_id: str) -> Dict[str, Any]:
    client = await _get_client()
    response = await client.get(f"/plans/{plan_id}", headers=_headers())
    response.raise_for_status()
    return response.json()


async def create_subscription(
    plan_id: str,
    notes: Dict[str, Any],
    *,
    total_count: Optional[int] = None,
    customer_notify: int = 1,
) -> Dict[str, Any]:
    client = await _get_client()
    payload = {
        "plan_id": plan_id,
        "customer_notify": customer_notify,
        "notes": notes,
        "total_count": total_count or 12,
    }
    response = await client.post("/subscriptions", headers=_headers(), json=payload)
    response.raise_for_status()
    return response.json()


async def get_subscription(sub_id: str) -> Dict[str, Any]:
    client = await _get_client()
    response = await client.get(f"/subscriptions/{sub_id}", headers=_headers())
    response.raise_for_status()
    return response.json()


async def get_payment(payment_id: str) -> Dict[str, Any]:
    client = await _get_client()
    response = await client.get(f"/payments/{payment_id}", headers=_headers())
    response.raise_for_status()
    return response.json()


async def cancel_subscription(
    provider_subscription_id: str,
    *,
    cancel_at_cycle_end: bool = True,
) -> httpx.Response:
    """
    Cancel a subscription at Razorpay.

    Returns the httpx.Response object so callers can inspect status/json.
    """
    client = await _get_client()
    payload = {"cancel_at_cycle_end": 1 if cancel_at_cycle_end else 0}
    response = await client.post(
        f"/subscriptions/{provider_subscription_id}/cancel",
        headers=_headers(),
        json=payload,
    )
    return response
