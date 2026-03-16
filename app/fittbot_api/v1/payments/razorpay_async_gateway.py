from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Sequence

import base64
import httpx
from fastapi import HTTPException, status as http_status

from app.fittbot_api.v1.payments.config.settings import get_payment_settings

_client: Optional[httpx.AsyncClient] = None
_lock = asyncio.Lock()


def _headers(settings) -> Dict[str, str]:
    auth = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}"
    encoded = base64.b64encode(auth.encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client:
        return _client
    async with _lock:
        if _client:
            return _client
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        return _client


async def init_client() -> None:
    """Warm up the shared client (idempotent)."""
    await _get_client()


async def _request(
    method: str,
    path: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    retry_on: Sequence[int] = (),
) -> Dict[str, Any]:
    settings = get_payment_settings()
    url = f"https://api.razorpay.com/v1{path}"
    client = await _get_client()

    attempts = 3 if retry_on else 1
    for attempt in range(1, attempts + 1):
        try:
            resp = await client.request(method, url, headers=_headers(settings), json=json_body)
            if resp.status_code in retry_on and attempt < attempts:
                await asyncio.sleep(0.25 * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in retry_on and attempt < attempts:
                await asyncio.sleep(0.25 * attempt)
                continue
            raise HTTPException(
                status_code=http_status.HTTP_502_BAD_GATEWAY,
                detail="Failed to reach payment provider",
            )
        except Exception:
            if attempt >= attempts:
                raise HTTPException(
                    status_code=http_status.HTTP_502_BAD_GATEWAY,
                    detail="Failed to reach payment provider",
                )
            await asyncio.sleep(0.25 * attempt)


async def create_order(
    *,
    amount_minor: int,
    currency: str,
    receipt: str,
    notes: Dict[str, Any],
    offers: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "amount": amount_minor,
        "currency": currency,
        "receipt": receipt,
        "payment_capture": 1,
        "notes": notes,
    }
    if offers:
        payload["offers"] = list(offers)
    return await _request("POST", "/orders", json_body=payload)


async def get_payment(payment_id: str) -> Dict[str, Any]:
    return await _request("GET", f"/payments/{payment_id}", retry_on=(429, 500, 502, 503, 504))


async def get_order(order_id: str) -> Dict[str, Any]:
    return await _request("GET", f"/orders/{order_id}", retry_on=(429, 500, 502, 503, 504))


async def close_client():
    global _client
    if _client:
        await _client.aclose()
        _client = None
