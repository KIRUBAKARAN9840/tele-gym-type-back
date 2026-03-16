from __future__ import annotations

import base64
import json
from typing import Dict, Any, Optional

import httpx
import requests

RZP_API = "https://api.razorpay.com/v1"


def _auth_headers(settings) -> Dict[str, str]:
    auth = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}".encode("utf-8")
    b64 = base64.b64encode(auth).decode("utf-8")
    return {"Authorization": f"Basic {b64}", "Content-Type": "application/json"}


def create_order(amount_minor: int, currency: str, receipt: str, notes: Dict[str, Any], settings) -> Dict[str, Any]:
    resp = requests.post(
        f"{RZP_API}/orders",
        headers=_auth_headers(settings),
        data=json.dumps({
            "amount": amount_minor,
            "currency": currency,
            "receipt": receipt,
            "payment_capture": 1,
            "notes": notes,
        }),
        timeout=(5, 20),
    )
    resp.raise_for_status()
    return resp.json()


def get_payment(payment_id: str, settings) -> Dict[str, Any]:
    resp = requests.get(
        f"{RZP_API}/payments/{payment_id}",
        headers=_auth_headers(settings),
        timeout=(5, 20),
    )
    resp.raise_for_status()
    return resp.json()


def create_subscription(plan_id: str, customer_notify: int, notes: Dict[str, Any], settings, **kwargs) -> Dict[str, Any]:
    payload = {"plan_id": plan_id, "customer_notify": customer_notify, "notes": notes}

    # Set default total_count if not provided - required by Razorpay API
    if "total_count" not in kwargs:
        payload["total_count"] = 12  # Default for monthly subscriptions

    payload.update(kwargs)
    resp = requests.post(
        f"{RZP_API}/subscriptions",
        headers=_auth_headers(settings),
        data=json.dumps(payload),
        timeout=(5, 20),
    )
    resp.raise_for_status()
    return resp.json()


def get_order(order_id: str, settings) -> Dict[str, Any]:
    """Fetch Razorpay order details by order_id"""
    resp = requests.get(
        f"{RZP_API}/orders/{order_id}",
        headers=_auth_headers(settings),
        timeout=(5, 20),
    )
    resp.raise_for_status()
    return resp.json()


def get_settlement_recon(year: int, month: int, day: int, settings) -> Dict[str, Any]:
    """Fetch Razorpay Settlement Recon Combined for a given date.
    API: GET /v1/settlements/recon/combined?year=YYYY&month=MM&day=DD
    Returns JSON with list of transactions (payments/refunds/transfers/adjustments).
    """
    url = f"{RZP_API}/settlements/recon/combined?year={year}&month={month}&day={day}"
    resp = requests.get(url, headers=_auth_headers(settings), timeout=(5, 30))
    resp.raise_for_status()
    return resp.json()


# ---------- Async helpers (non-blocking) ----------
_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=20.0, write=5.0, pool=5.0)


async def _request_async(
    method: str,
    path: str,
    settings,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    url = f"{RZP_API}{path}"
    headers = _auth_headers(settings)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(method, url, headers=headers, json=json_body)
        resp.raise_for_status()
        return resp.json()


async def async_create_order(amount_minor: int, currency: str, receipt: str, notes: Dict[str, Any], settings) -> Dict[str, Any]:
    payload = {
        "amount": amount_minor,
        "currency": currency,
        "receipt": receipt,
        "payment_capture": 1,
        "notes": notes,
    }
    return await _request_async("POST", "/orders", settings, json_body=payload)


async def async_get_payment(payment_id: str, settings) -> Dict[str, Any]:
    return await _request_async("GET", f"/payments/{payment_id}", settings)


async def async_create_subscription(plan_id: str, customer_notify: int, notes: Dict[str, Any], settings, **kwargs) -> Dict[str, Any]:
    payload = {"plan_id": plan_id, "customer_notify": customer_notify, "notes": notes}
    if "total_count" not in kwargs:
        payload["total_count"] = 12
    payload.update(kwargs)
    return await _request_async("POST", "/subscriptions", settings, json_body=payload)


async def async_get_order(order_id: str, settings) -> Dict[str, Any]:
    return await _request_async("GET", f"/orders/{order_id}", settings)
