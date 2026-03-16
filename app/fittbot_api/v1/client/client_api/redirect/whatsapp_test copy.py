# app/fittbot_api/v1/client/client_api/redirect/whatsapp_test.py
# API endpoint to test WhatsApp template messaging

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List

from app.utils.whatsapp.whatsapp_client import (
    get_whatsapp_client,
    send_template,
    send_daily_pass,
    WhatsAppResponse
)

router = APIRouter(prefix="/whatsapp", tags=["WhatsApp Test"])


class TemplateRequest(BaseModel):
    to: str
    template_name: str
    variables: Optional[List[str]] = None


class DailyPassRequest(BaseModel):
    to: str
    gym_name: str
    price: str


@router.post("/test/template")
async def test_template_message(request: TemplateRequest):
    """
    Test sending a WhatsApp template message

    Example:
    {
        "to": "919876543210",
        "template_name": "daily_pass_promotion",
        "variables": ["FitZone Gym", "199"]
    }
    """
    try:
        response = await send_template(
            to=request.to,
            template_name=request.template_name,
            variables=request.variables
        )

        return {
            "status": 200 if response.success else 400,
            "success": response.success,
            "message_id": response.message_id,
            "guid": response.guid,
            "submit_date": response.submit_date,
            "api_status": response.status_text,
            "error": response.error
        }

    except Exception as e:
        return {
            "status": 500,
            "success": False,
            "error": str(e)
        }


@router.post("/test/daily-pass")
async def test_daily_pass_message(request: DailyPassRequest):
    """
    Test sending Daily Pass promo template

    Example:
    {
        "to": "919876543210",
        "gym_name": "FitZone Gym",
        "price": "199"
    }
    """
    try:
        response = await send_daily_pass(
            to=request.to,
            gym_name=request.gym_name,
            price=request.price
        )

        return {
            "status": 200 if response.success else 400,
            "success": response.success,
            "message_id": response.message_id,
            "guid": response.guid,
            "submit_date": response.submit_date,
            "api_status": response.status_text,
            "error": response.error
        }

    except Exception as e:
        return {
            "status": 500,
            "success": False,
            "error": str(e)
        }


@router.get("/test/config")
async def check_whatsapp_config():
    """Check if WhatsApp is configured correctly"""
    import os

    return {
        "status": 200,
        "config": {
            "base_url": os.getenv("WHATSAPP_BASE_URL", "NOT SET"),
            "client_id": "***" + os.getenv("WHATSAPP_CLIENT_ID", "NOT SET")[-4:] if os.getenv("WHATSAPP_CLIENT_ID") else "NOT SET",
            "from_number": os.getenv("WHATSAPP_FROM_NUMBER", "NOT SET"),
            "password_set": bool(os.getenv("WHATSAPP_CLIENT_PASSWORD"))
        }
    }


class RawTemplateRequest(BaseModel):
    to: str
    template_id: str
    variables: Optional[List[str]] = None
    msgtype: int = 2  # 2=PROMO (for templates)
    var_format: str = "bracketed"  # "bracketed" = {{1}}=val, "plain" = just val


@router.post("/test/raw")
async def test_raw_template(request: RawTemplateRequest):

    import uuid
    from datetime import datetime
    import os
    import httpx

    message_id = f"{uuid.uuid4().hex}{datetime.now().strftime('%Y%m%d%H%M%S')}"
    seq_id = f"{uuid.uuid4().hex[:24]}-{datetime.now().strftime('%Y%m%d')}"

    # Format phone
    phone = request.to.strip().replace(" ", "").replace("-", "")
    if phone.startswith("+"):
        phone = phone[1:]
    if not phone.startswith("91"):
        phone = f"91{phone}"

    # Build variables string based on format
    var_string = ""
    if request.variables:
        if request.var_format == "plain":
            var_string = ",".join(request.variables)
        elif request.var_format == "pipe":
            var_string = "|".join(request.variables)
        else:  # bracketed (default)
            var_parts = [f"{{{{{i+1}}}}}={v}" for i, v in enumerate(request.variables)]
            var_string = ",".join(var_parts)

    payload = {
        "apiver": "1.0",
        "whatsapp": {
            "ver": "2.0",
            "dlr": {"url": ""},
            "messages": [{
                "coding": 1,
                "id": message_id,
                "msgtype": request.msgtype,
                "templateid": request.template_id,
                "text": var_string,
                "addresses": [{
                    "seq": seq_id,
                    "to": phone,
                    "from": os.getenv("WHATSAPP_FROM_NUMBER", ""),
                    "tag": request.template_id
                }]
            }]
        }
    }

    # Send the request
    try:
        base_url = os.getenv("WHATSAPP_BASE_URL", "https://103.229.250.150").rstrip("/")
        url = f"{base_url}/unified/v2/send"

        headers = {
            "x-client-id": os.getenv("WHATSAPP_CLIENT_ID", ""),
            "x-client-password": os.getenv("WHATSAPP_CLIENT_PASSWORD", ""),
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
            response = await client.post(url, headers=headers, json=payload)

            # Parse response
            api_response = response.json() if response.text else {}

            # Extract detailed metadata
            result = {
                "http_status": response.status_code,
                "msgtype_used": request.msgtype,
                "var_format_used": request.var_format,
                "payload_sent": payload,
                "api_response": api_response,
            }

            # Extract GUID details if available
            if api_response.get("messageack", {}).get("guids"):
                guids = api_response["messageack"]["guids"]
                for guid_info in guids:
                    result["guid"] = guid_info.get("guid")
                    result["submit_date"] = guid_info.get("submitdate")
                    result["message_id"] = guid_info.get("id")

                    # Check for errors in the response
                    if guid_info.get("errors"):
                        result["errors"] = guid_info["errors"]
                        for err in guid_info["errors"]:
                            print(f"[WhatsApp ERROR] Code: {err.get('errorcode')}, Text: {err.get('errortext')}, Seq: {err.get('seq')}")

            # Print full metadata to console
            print("\n" + "="*60)
            print("[WhatsApp API Request/Response Metadata]")
            print("="*60)
            print(f"URL: {url}")
            print(f"Template ID: {request.template_id}")
            print(f"To: {phone}")
            print(f"From: {os.getenv('WHATSAPP_FROM_NUMBER', '')}")
            print(f"MsgType: {request.msgtype}")
            print(f"Variables: {request.variables}")
            print(f"Text sent: {var_string}")
            print("-"*60)
            print(f"HTTP Status: {response.status_code}")
            print(f"API Status: {api_response.get('status')}")
            print(f"Status Code: {api_response.get('statuscode')}")
            print(f"Status Text: {api_response.get('statustext')}")
            if result.get("guid"):
                print(f"GUID: {result.get('guid')}")
            if result.get("errors"):
                print(f"ERRORS: {result.get('errors')}")
            print("="*60 + "\n")

            return result

    except Exception as e:
        print(f"[WhatsApp] Exception: {str(e)}")
        return {
            "status": 500,
            "error": str(e),
            "payload_sent": payload
        }
