"""
Async RazorpayX client for settlements API and payouts.

Handles:
- Fetching settlements from Razorpay
- Fetching settlement recon items (which payments are in a settlement)
- Creating RazorpayX contacts, fund accounts, and payouts
"""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any, Dict, List, Optional

import httpx

from app.fittbot_api.v1.payments.config.settings import get_payment_settings

logger = logging.getLogger("auto_settlements.razorpayx")

RAZORPAY_BASE = "https://api.razorpay.com/v1"

_client: Optional[httpx.AsyncClient] = None
_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client and not _client.is_closed:
        return _client
    async with _lock:
        if _client and not _client.is_closed:
            return _client
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=10),
        )
        return _client


async def close_client() -> None:
    global _client
    if _client:
        await _client.aclose()
        _client = None


def _auth_headers() -> Dict[str, str]:
    settings = get_payment_settings()
    auth = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}"
    encoded = base64.b64encode(auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


def _razorpayx_auth_headers() -> Dict[str, str]:
    """RazorpayX uses the same credentials (or separate ones if configured)."""
    settings = get_payment_settings()
    auth = f"{settings.razorpayx_key_id}:{settings.razorpayx_key_secret}"
    encoded = base64.b64encode(auth.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


async def _request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
) -> Dict[str, Any]:
    client = await _get_client()
    hdrs = headers or _auth_headers()

    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.request(
                method, url, headers=hdrs, json=json_body, params=params
            )
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                wait = 0.5 * (2 ** (attempt - 1))
                logger.warning(
                    "Razorpay %s %s returned %s, retrying in %.1fs (attempt %d/%d)",
                    method, url, resp.status_code, wait, attempt, max_retries,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as exc:
            if attempt >= max_retries:
                logger.error("Razorpay request failed after %d attempts: %s", max_retries, exc)
                raise
            await asyncio.sleep(0.5 * attempt)

    return {}


# ─── Settlements API ─────────────────────────────────────────────────────────


async def fetch_settlements(
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    count: int = 100,
    skip: int = 0,
) -> Dict[str, Any]:
    """
    GET /v1/settlements
    Fetches list of settlements from Razorpay.
    from_ts/to_ts are Unix timestamps.
    """
    params: Dict[str, Any] = {"count": count, "skip": skip}
    if from_ts:
        params["from"] = from_ts
    if to_ts:
        params["to"] = to_ts

    return await _request("GET", f"{RAZORPAY_BASE}/settlements", params=params)


async def fetch_settlement_recon(
    year: int,
    month: int,
    day: Optional[int] = None,
    count: int = 100,
    skip: int = 0,
) -> Dict[str, Any]:
    """
    GET /v1/settlements/recon/combined
    Fetches settlement reconciliation items.
    Each item contains: entity_id (payment_id), amount, fee, tax, settlement_id, etc.
    """
    params: Dict[str, Any] = {"year": year, "month": month, "count": count, "skip": skip}
    if day:
        params["day"] = day

    return await _request(
        "GET", f"{RAZORPAY_BASE}/settlements/recon/combined", params=params
    )


async def fetch_all_settlement_recon_items(
    year: int, month: int, day: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Paginate through all recon items for a given date."""
    all_items: List[Dict[str, Any]] = []
    skip = 0
    page_size = 100

    while True:
        resp = await fetch_settlement_recon(
            year=year, month=month, day=day, count=page_size, skip=skip
        )
        items = resp.get("items", [])
        all_items.extend(items)
        if len(items) < page_size:
            break
        skip += page_size

    return all_items


async def fetch_settlement_by_id(settlement_id: str) -> Dict[str, Any]:
    """GET /v1/settlements/:id"""
    return await _request("GET", f"{RAZORPAY_BASE}/settlements/{settlement_id}")


async def fetch_payment(payment_id: str) -> Dict[str, Any]:
    """
    GET /v1/payments/:id
    Fetch a single payment from Razorpay.
    Used during reconciliation to check offer_id for no-cost EMI detection.
    """
    return await _request("GET", f"{RAZORPAY_BASE}/payments/{payment_id}")


# ─── RazorpayX Contacts API ──────────────────────────────────────────────────


async def create_contact(
    name: str,
    contact_number: Optional[str] = None,
    email: Optional[str] = None,
    contact_type: str = "vendor",
    reference_id: Optional[str] = None,
    notes: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:

    payload: Dict[str, Any] = {
        "name": name,
        "type": contact_type,
    }
    if contact_number:
        payload["contact"] = contact_number
    if email:
        payload["email"] = email
    if reference_id:
        payload["reference_id"] = reference_id
    if notes:
        payload["notes"] = notes

    return await _request(
        "POST", f"{RAZORPAY_BASE}/contacts",
        headers=_razorpayx_auth_headers(), json_body=payload,
    )


async def create_fund_account_bank(
    contact_id: str,
    account_holder_name: str,
    account_number: str,
    ifsc: str,
) -> Dict[str, Any]:
    """
    POST /v1/fund_accounts
    Creates a bank account fund account under a contact.
    """
    payload = {
        "contact_id": contact_id,
        "account_type": "bank_account",
        "bank_account": {
            "name": account_holder_name,
            "ifsc": ifsc,
            "account_number": account_number,
        },
    }
    return await _request(
        "POST", f"{RAZORPAY_BASE}/fund_accounts",
        headers=_razorpayx_auth_headers(), json_body=payload,
    )


async def create_fund_account_upi(
    contact_id: str,
    upi_address: str,
) -> Dict[str, Any]:

    payload = {
        "contact_id": contact_id,
        "account_type": "vpa",
        "vpa": {"address": upi_address},
    }
    return await _request(
        "POST", f"{RAZORPAY_BASE}/fund_accounts",
        headers=_razorpayx_auth_headers(), json_body=payload,
    )




async def create_payout(
    fund_account_id: str,
    amount_paise: int,
    currency: str = "INR",
    mode: str = "NEFT",
    purpose: str = "payout",
    reference_id: Optional[str] = None,
    narration: Optional[str] = None,
    queue_if_low_balance: bool = True,
    notes: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    POST /v1/payouts
    Initiates a payout to the fund account.
    """
    settings = get_payment_settings()
    payload: Dict[str, Any] = {
        "account_number": settings.razorpay_payout_account_number,
        "fund_account_id": fund_account_id,
        "amount": amount_paise,
        "currency": currency,
        "mode": mode,
        "purpose": purpose,
        "queue_if_low_balance": queue_if_low_balance,
    }
    if reference_id:
        payload["reference_id"] = reference_id
    if narration:
        payload["narration"] = narration
    if notes:
        payload["notes"] = notes

    return await _request(
        "POST", f"{RAZORPAY_BASE}/payouts",
        headers=_razorpayx_auth_headers(), json_body=payload,
    )


async def get_payout(payout_id: str) -> Dict[str, Any]:
    """GET /v1/payouts/:id"""
    return await _request(
        "GET", f"{RAZORPAY_BASE}/payouts/{payout_id}",
        headers=_razorpayx_auth_headers(),
    )
