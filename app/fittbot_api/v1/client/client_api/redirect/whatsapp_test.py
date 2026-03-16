# app/fittbot_api/v1/client/client_api/redirect/whatsapp_test.py

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal, Dict, Any, List
import os
import time
import uuid
import json
import logging
import httpx
from datetime import datetime

from app.config.settings import settings

router = APIRouter(prefix="/whatsapp", tags=["WhatsApp (Infinito)"])
logger = logging.getLogger("whatsapp_debug")

# ─── In-memory DLR store for debugging ───
_dlr_reports: List[Dict[str, Any]] = []


class WhatsAppSendRequest(BaseModel):
    to: str

    # 1=TRANS, 2=PROMO, 3=TRANS+MEDIA, 4=PROMO+MEDIA
    msgtype: Literal[1, 2, 3, 4] = 3

    # template info format: TEMPLATEID~var1~var2...
    templateinfo: str

    # keep non-empty (some gateways reject empty string)
    text: str = " "

    # media (URL mode)
    media_type: Literal["image", "video", "document"] = "image"
    content_type: str = "image/png"
    media_url: str

    # optional
    tag: str = ""


class DLRDebugRequest(BaseModel):
    """Send your exact working payload to multiple numbers with DLR tracking."""
    numbers: List[str]
    dlr_base_url: str  # Your ngrok/public URL e.g. https://abc.ngrok-free.app


def _normalize_phone(phone: str) -> str:
    p = phone.strip().replace(" ", "").replace("-", "")
    if p.startswith("+"):
        p = p[1:]
    if len(p) == 10 and not p.startswith("91"):
        p = "91" + p
    return p


def _unique_seq_int() -> str:
    # must be integer, unique per message
    return str(int(time.time() * 1000))


def _base_url() -> str:
    return settings.whatsapp_base_url.rstrip("/")


def _build_headers() -> Dict[str, str]:
    """
    Authorization token only.
    If env already has 'Bearer ...' keep it; else prefix Bearer.
    """
    token = (settings.whatsapp_authorization or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"

    return {
        "Authorization": token,
        "Content-Type": "application/json",
    }


def _build_dlr_url(base_url: str) -> str:
    """
    Build DLR callback URL with all Infinito placeholders from their docs.
    These get replaced by Infinito with actual delivery values.
    """
    base = base_url.rstrip("/")
    params = (
        "TO=%p"
        "&FROM=%P"
        "&TIME=%t"
        "&MESSAGE_STATUS=%d"
        "&REASON_CODE=%2"
        "&DELIVERED_DATE=%3"
        "&STATUS_ERROR=%4"
        "&GUID=%5"
        "&SEQ_NUMBER=%6"
        "&MESSAGE_ID=%7"
        "&CIRCLE=%8"
        "&OPERATOR=%9"
        "&TEXT_STATUS=%13"
        "&SUBMIT_DATE=%14"
        "&MSG_STATUS=%16"
        "&TAG=%TAG"
    )
    return f"{base}/whatsapp/dlr?{params}"


async def _check_media_url(media_url: str) -> Dict[str, Any]:
    """
    Preflight check to ensure URL is publicly reachable (200 OK) and not huge.
    Many providers fail delivery if they cannot fetch the URL.
    """
    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        # Try HEAD first (fast)
        try:
            r = await client.head(media_url)
        except Exception:
            r = None

        if r is None or r.status_code >= 400:
            # Fallback to GET with range to avoid downloading full file
            r = await client.get(media_url, headers={"Range": "bytes=0-10240"})

        info = {
            "status_code": r.status_code,
            "content_type": r.headers.get("content-type", ""),
            "content_length": r.headers.get("content-length", ""),
            "final_url": str(r.url),
        }

        if r.status_code != 200 and r.status_code != 206:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "media_url_not_publicly_accessible",
                    "media_url": media_url,
                    "probe": info,
                },
            )

        return info


def _build_payload(req: WhatsAppSendRequest) -> Dict[str, Any]:
    message_id = uuid.uuid4().hex[:30]

    payload = {
        "apiver": "1.0",
        "whatsapp": {
            "ver": "2.0",
            "dlr": {"url": settings.whatsapp_dlr_url.strip()},
            "messages": [
                {
                    "coding": 1,
                    "id": message_id,
                    "msgtype": int(req.msgtype),
                    "type": req.media_type,
                    "contenttype": req.content_type,
                    "mediadata": req.media_url,
                    "text": req.text or " ",
                    "templateinfo": req.templateinfo,
                    "addresses": [
                        {
                            "seq": _unique_seq_int(),
                            "to": _normalize_phone(req.to),
                            "from": settings.whatsapp_from_number.strip(),
                            "tag": req.tag or "",
                        }
                    ],
                }
            ],
        },
    }
    return payload


@router.post("/send")
async def send_whatsapp(req: WhatsAppSendRequest):
    """
    Sends WhatsApp template message with media URL (msgtype=3/4).

    Env:
      WHATSAPP_AUTHORIZATION   -> token (with or without 'Bearer ')
      WHATSAPP_FROM_NUMBER     -> sender
      WHATSAPP_DLR_URL         -> dlr callback
      WHATSAPP_BASE_URL        -> (optional) https://103.229.250.150 or https://api.goinfinito.com
    """
    # Preflight ensure URL is reachable publicly
    media_probe = await _check_media_url(req.media_url)

    url = f"{_base_url()}/unified/v2/send"
    headers = _build_headers()
    payload = _build_payload(req)

    print("\n" + "=" * 70)
    print("[WHATSAPP SEND -> INFINITO]")
    print("=" * 70)
    print(f"URL: {url}")
    print(f"Authorization present: {'YES' if bool(headers.get('Authorization')) else 'NO'}")
    print(f"MEDIA PROBE: {json.dumps(media_probe, ensure_ascii=False)}")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("=" * 70 + "\n")

    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        resp = await client.post(url, headers=headers, json=payload)

    raw_text = resp.text or ""
    try:
        parsed = resp.json()
    except Exception:
        parsed = None

    return {
        "http_status": resp.status_code,
        "request_url": url,
        "payload_sent": payload,
        "media_probe": media_probe,
        "response_headers": dict(resp.headers),
        "api_response_json": parsed,
        "api_response_raw": raw_text,
    }


# ═══════════════════════════════════════════════════════════════
# DLR DEBUG: Send your EXACT working payload with DLR tracking
# ═══════════════════════════════════════════════════════════════

@router.post("/dlr-debug")
async def dlr_debug_send(req: DLRDebugRequest):
    """
    Send your EXACT working payload to multiple numbers with DLR callback.

    This uses the same payload structure as your working curl command.
    The DLR URL will capture delivery reports so you can see WHY
    messages fail for specific numbers.

    Example POST body:
    {
        "numbers": ["919486987082", "918667458723", "918667427956"],
        "dlr_base_url": "https://your-ngrok-url.ngrok-free.app"
    }
    """
    dlr_url = _build_dlr_url(req.dlr_base_url)
    from_number = settings.whatsapp_from_number.strip()
    template_info = os.getenv("WHATSAPP_TEMPLATE_INFO", "1701810").strip()
    media_url = os.getenv(
        "WHATSAPP_MEDIA_URL",
        "https://image.shutterstock.com/image-photo/large-beautiful-drops-transparent-rain-600w-668593321.jpg"
    ).strip()
    user_id = int(os.getenv("WHATSAPP_USER_ID", "3"))

    url = f"{_base_url()}/unified/v2/send"
    headers = _build_headers()
    results = []

    for number in req.numbers:
        phone = _normalize_phone(number)
        msg_id = f"dlr_{phone}_{int(time.time())}"
        seq_id = str(int(time.time() * 1000))

        # EXACT same payload structure as the working curl
        payload = {
            "apiver": "1.0",
            "user": {"userid": user_id},
            "whatsapp": {
                "ver": "2.0",
                "dlr": {"url": dlr_url},
                "messages": [
                    {
                        "coding": 1,
                        "templateinfo": template_info,
                        "id": msg_id,
                        "msgtype": 3,
                        "type": "image",
                        "contenttype": "image/png",
                        "mediadata": media_url,
                        "addresses": [
                            {
                                "seq": seq_id,
                                "to": phone,
                                "from": from_number,
                                "tag": ""
                            }
                        ]
                    }
                ]
            }
        }

        print(f"\n[DLR-DEBUG] Sending to {phone} | id={msg_id} | seq={seq_id}")

        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                resp = await client.post(url, headers=headers, json=payload)

            try:
                api_resp = resp.json()
            except Exception:
                api_resp = {"raw": resp.text}

            guid = None
            if api_resp.get("status") == "Success":
                guids = api_resp.get("messageack", {}).get("guids", [])
                if guids:
                    guid = guids[0].get("guid")

            result = {
                "number": phone,
                "msg_id": msg_id,
                "seq_id": seq_id,
                "http_status": resp.status_code,
                "api_status": api_resp.get("status"),
                "guid": guid,
                "api_response": api_resp,
            }
            print(f"[DLR-DEBUG] {phone} -> {api_resp.get('status')} | GUID: {guid}")

        except Exception as e:
            result = {
                "number": phone,
                "msg_id": msg_id,
                "seq_id": seq_id,
                "http_status": 0,
                "api_status": "Error",
                "guid": None,
                "error": str(e),
            }
            print(f"[DLR-DEBUG] {phone} -> ERROR: {e}")

        results.append(result)

    return {
        "dlr_url_configured": dlr_url,
        "from_number": from_number,
        "template_info": template_info,
        "total_sent": len(results),
        "results": results,
        "note": "Check /whatsapp/dlr-reports to see delivery reports as they arrive",
    }


# ═══════════════════════════════════════════════════════════════
# DLR CALLBACK: Receives delivery reports from Infinito
# ═══════════════════════════════════════════════════════════════

@router.api_route("/dlr", methods=["GET", "POST"])
async def whatsapp_dlr(request: Request):
    """
    Receives DLR (Delivery Reports) from Infinito.
    Infinito replaces the %p, %d etc. placeholders with actual values.
    """
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8", errors="ignore") if body_bytes else ""
    query_params = dict(request.query_params)

    # Parse the DLR data from query params (Infinito sends via GET params)
    dlr_entry = {
        "received_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "method": request.method,
        "to": query_params.get("TO", ""),
        "from": query_params.get("FROM", ""),
        "message_status": query_params.get("MESSAGE_STATUS", ""),
        "msg_status": query_params.get("MSG_STATUS", ""),
        "text_status": query_params.get("TEXT_STATUS", ""),
        "reason_code": query_params.get("REASON_CODE", ""),
        "status_error": query_params.get("STATUS_ERROR", ""),
        "guid": query_params.get("GUID", ""),
        "message_id": query_params.get("MESSAGE_ID", ""),
        "seq_number": query_params.get("SEQ_NUMBER", ""),
        "circle": query_params.get("CIRCLE", ""),
        "operator": query_params.get("OPERATOR", ""),
        "delivered_date": query_params.get("DELIVERED_DATE", ""),
        "submit_date": query_params.get("SUBMIT_DATE", ""),
        "tag": query_params.get("TAG", ""),
        "raw_query": query_params,
        "raw_body": body_text,
    }

    # Store in memory
    _dlr_reports.append(dlr_entry)

    # Log with clear formatting
    print("\n" + "=" * 70)
    print("  DLR RECEIVED")
    print("=" * 70)
    print(f"  TO:             {dlr_entry['to']}")
    print(f"  FROM:           {dlr_entry['from']}")
    print(f"  MSG_STATUS:     {dlr_entry['msg_status']}")
    print(f"  TEXT_STATUS:    {dlr_entry['text_status']}")
    print(f"  MESSAGE_STATUS: {dlr_entry['message_status']}")
    print(f"  REASON_CODE:    {dlr_entry['reason_code']}")
    print(f"  STATUS_ERROR:   {dlr_entry['status_error']}")
    print(f"  OPERATOR:       {dlr_entry['operator']}")
    print(f"  CIRCLE:         {dlr_entry['circle']}")
    print(f"  GUID:           {dlr_entry['guid']}")
    print(f"  DELIVERED_DATE: {dlr_entry['delivered_date']}")
    print(f"  SUBMIT_DATE:    {dlr_entry['submit_date']}")
    print("=" * 70 + "\n")

    logger.info(f"DLR: to={dlr_entry['to']} status={dlr_entry['msg_status']} "
                f"reason={dlr_entry['reason_code']} error={dlr_entry['status_error']}")

    return {"ok": True}


@router.get("/dlr-reports")
async def get_dlr_reports():
    """
    View all received DLR reports.
    Use this to compare delivery status across numbers.
    """
    return {
        "total_reports": len(_dlr_reports),
        "reports": _dlr_reports,
    }


@router.delete("/dlr-reports")
async def clear_dlr_reports():
    """Clear all stored DLR reports."""
    _dlr_reports.clear()
    return {"ok": True, "message": "DLR reports cleared"}




