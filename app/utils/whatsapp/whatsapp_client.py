# app/utils/whatsapp/whatsapp_client.py

import httpx
import uuid
import os
import logging
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from app.utils.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.config.settings import settings

logger = logging.getLogger("whatsapp")

# ── Environment Config ────────────────────────────────────────────────────────

WHATSAPP_BASE_URL = settings.whatsapp_base_url
WHATSAPP_BEARER_TOKEN = settings.whatsapp_bearer_token or ""
WHATSAPP_FROM_NUMBER = settings.whatsapp_from_number
WHATSAPP_USER_ID = os.getenv("WHATSAPP_USER_ID", "3")

# Template IDs (from WhatsApp Business API approval)
WHATSAPP_TEMPLATE_ABANDONED = os.getenv("WHATSAPP_TEMPLATE_ABANDONED", "1708982")
WHATSAPP_TEMPLATE_BOOKED_DAILYPASS = os.getenv("WHATSAPP_TEMPLATE_BOOKED_DAILYPASS", "1708894")
WHATSAPP_TEMPLATE_BOOKED_SESSION = os.getenv("WHATSAPP_TEMPLATE_BOOKED_SESSION", "1708912")
WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP = os.getenv("WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP", "1708926")
WHATSAPP_TEMPLATE_BOOKED_SUBSCRIPTION = os.getenv("WHATSAPP_TEMPLATE_BOOKED_SUBSCRIPTION", WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP)
WHATSAPP_TEMPLATE_BROWSING = os.getenv("WHATSAPP_TEMPLATE_BROWSING", "")

# Image URLs for abandoned checkout messages
WHATSAPP_IMAGE_DAILYPASS_ABANDONED = os.getenv(
    "WHATSAPP_IMAGE_DAILYPASS_ABANDONED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/dailypass_abandoned.png",
)
WHATSAPP_IMAGE_MEMBERSHIP_ABANDONED = os.getenv(
    "WHATSAPP_IMAGE_MEMBERSHIP_ABANDONED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/membership_abandoned.png",
)
WHATSAPP_IMAGE_SESSION_ABANDONED = os.getenv(
    "WHATSAPP_IMAGE_SESSION_ABANDONED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/Sessions_abandoned.png",
)

# Image URLs for booking confirmation messages
WHATSAPP_IMAGE_DAILYPASS_BOOKED = os.getenv(
    "WHATSAPP_IMAGE_DAILYPASS_BOOKED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/Dailypass_booked.png",
)
WHATSAPP_IMAGE_MEMBERSHIP_BOOKED = os.getenv(
    "WHATSAPP_IMAGE_MEMBERSHIP_BOOKED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/Membership_booked.png",
)
WHATSAPP_IMAGE_SESSION_BOOKED = os.getenv(
    "WHATSAPP_IMAGE_SESSION_BOOKED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/Session_booked.png",
)

# Image URLs for subscription messages
WHATSAPP_IMAGE_SUBSCRIPTION_ABANDONED = os.getenv(
    "WHATSAPP_IMAGE_SUBSCRIPTION_ABANDONED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/subscription_abandoned.png",
)
WHATSAPP_IMAGE_SUBSCRIPTION_BOOKED = os.getenv(
    "WHATSAPP_IMAGE_SUBSCRIPTION_BOOKED",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/subscription_taken.png",
)

# Image URL for browsing follow-up
WHATSAPP_IMAGE_BROWSING = os.getenv(
    "WHATSAPP_IMAGE_BROWSING",
    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/whatsapp_messaging/browsing_followup.png",
)


# ── Circuit Breaker ───────────────────────────────────────────────────────────

whatsapp_circuit_breaker = CircuitBreaker(
    name="whatsapp",
    failure_threshold=5,
    recovery_timeout=60.0,
    half_open_max_calls=3,
    success_threshold=2,
)


# ── Response Model ────────────────────────────────────────────────────────────

class WhatsAppResponse(BaseModel):
    success: bool
    status_code: int
    status_text: str
    message_id: Optional[str] = None
    guid: Optional[str] = None
    submit_date: Optional[str] = None
    error: Optional[str] = None


# ── Client ────────────────────────────────────────────────────────────────────

class WhatsAppClient:

    def __init__(
        self,
        bearer_token: Optional[str] = None,
        from_number: Optional[str] = None,
        user_id: Optional[str] = None,
        base_url: Optional[str] = None,
        dlr_url: Optional[str] = "",
    ):
        self.base_url = base_url or WHATSAPP_BASE_URL
        self.bearer_token = bearer_token or WHATSAPP_BEARER_TOKEN
        self.from_number = from_number or WHATSAPP_FROM_NUMBER
        self.user_id = user_id or WHATSAPP_USER_ID
        self.dlr_url = dlr_url

        if not self.bearer_token:
            raise ValueError("WHATSAPP_BEARER_TOKEN is required")
        if not self.from_number:
            raise ValueError("WHATSAPP_FROM_NUMBER is required")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }

    def _generate_message_id(self) -> str:
        """Simple numeric-ish message ID (like '122', '123', etc.)."""
        import random
        return str(random.randint(100, 999999))

    def _generate_seq_id(self) -> str:
        """Simple sequential ID (like '1', '2', etc.)."""
        return "1"

    def _format_phone(self, phone: str) -> str:
        phone = phone.strip().replace(" ", "").replace("-", "")
        if phone.startswith("+"):
            phone = phone[1:]
        if not phone.startswith("91"):
            phone = f"91{phone}"
        return phone

    def _build_templateinfo(self, template_id: str, variables: List[str]) -> str:
        """Build templateinfo string: template_id~var1~var2~var3"""
        parts = [template_id] + variables
        return "~".join(parts)

    # ── Payload Builders ──────────────────────────────────────────────────────

    def _build_text_message(
        self,
        to: str,
        template_id: str,
        variables: List[str],
        tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a text-only template message (msgtype 2)."""
        message_id = self._generate_message_id()
        seq_id = self._generate_seq_id()

        return {
            "coding": 1,
            "id": message_id,
            "msgtype": 2,
            "templateinfo": self._build_templateinfo(template_id, variables),
            "addresses": [
                {
                    "seq": seq_id,
                    "to": self._format_phone(to),
                    "from": self.from_number,
                    "tag": tag or template_id,
                }
            ],
        }

    def _build_image_message(
        self,
        to: str,
        template_id: str,
        variables: List[str],
        image_url: str,
        tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build an image template message (msgtype 3)."""
        message_id = self._generate_message_id()
        seq_id = self._generate_seq_id()

        return {
            "coding": 1,
            "id": message_id,
            "msgtype": 3,
            "templateinfo": self._build_templateinfo(template_id, variables),
            "type": "image",
            "contenttype": "image/png",
            "mediadata": image_url,
            "addresses": [
                {
                    "seq": seq_id,
                    "to": self._format_phone(to),
                    "from": self.from_number,
                    "tag": tag or template_id,
                }
            ],
        }

    def _build_request_payload(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build the full API request payload."""
        # userid must be an integer for the API
        try:
            userid_val = int(self.user_id)
        except (ValueError, TypeError):
            userid_val = self.user_id

        return {
            "apiver": "1.0",
            "user": {"userid": userid_val},
            "whatsapp": {
                "ver": "2.0",
                "dlr": {"url": self.dlr_url},
                "messages": messages,
            },
        }

    # ── Response Parser ───────────────────────────────────────────────────────

    def _parse_response(self, response_data: Dict[str, Any], message_id: str) -> WhatsAppResponse:
        status = response_data.get("status", "Error")
        status_code = response_data.get("statuscode", 500)
        status_text = response_data.get("statustext", "Unknown error")

        if status == "Success":
            guids = response_data.get("messageack", {}).get("guids", [])
            guid_info = next((g for g in guids if g.get("id") == message_id), None)

            return WhatsAppResponse(
                success=True,
                status_code=status_code,
                status_text=status_text,
                message_id=message_id,
                guid=guid_info.get("guid") if guid_info else None,
                submit_date=guid_info.get("submitdate") if guid_info else None,
            )

        return WhatsAppResponse(
            success=False,
            status_code=status_code,
            status_text=status_text,
            message_id=message_id,
            error=status_text,
        )

    # ── HTTP with Circuit Breaker + Retry ─────────────────────────────────────

    async def _send_request(
        self,
        payload: Dict[str, Any],
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> Dict[str, Any]:
        base = self.base_url.rstrip("/")
        url = f"{base}/unified/v2/send"

        # Circuit breaker check
        try:
            whatsapp_circuit_breaker._before_call()
        except CircuitOpenError as e:
            logger.warning(f"WhatsApp circuit OPEN: {e.remaining_seconds:.1f}s until retry")
            return {
                "status": "Error",
                "statuscode": 503,
                "statustext": f"Service temporarily unavailable. Retry in {e.remaining_seconds:.1f}s",
            }

        last_error = None

        # Debug: log the full payload being sent
        import json as _json
        logger.info(f"[WA_DEBUG] URL: {url}")
        logger.info(f"[WA_DEBUG] Headers: {self._get_headers()}")
        logger.info(f"[WA_DEBUG] Full payload:\n{_json.dumps(payload, indent=2)}")

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"WhatsApp API call attempt {attempt}/{max_retries}")

                async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                    response = await client.post(
                        url,
                        headers=self._get_headers(),
                        json=payload,
                    )

                    logger.info(f"[WA_DEBUG] Response status={response.status_code} body={response.text}")

                    if response.status_code == 200 and response.text:
                        whatsapp_circuit_breaker.record_success()
                        return response.json()

                    if response.status_code >= 500:
                        last_error = f"HTTP {response.status_code}"
                        logger.warning(
                            f"WhatsApp server error {response.status_code}, "
                            f"attempt {attempt}/{max_retries}"
                        )
                        if attempt < max_retries:
                            delay = base_delay * (2 ** (attempt - 1))
                            await asyncio.sleep(delay)
                            continue

                    if response.status_code != 200:
                        whatsapp_circuit_breaker.record_failure(
                            Exception(f"HTTP {response.status_code}")
                        )
                        return {
                            "status": "Error",
                            "statuscode": response.status_code,
                            "statustext": f"HTTP Error: {response.text or 'Empty response'}",
                        }

                    if not response.text:
                        whatsapp_circuit_breaker.record_failure(Exception("Empty response"))
                        return {
                            "status": "Error",
                            "statuscode": 500,
                            "statustext": "Empty response from WhatsApp API",
                        }

            except httpx.TimeoutException as e:
                last_error = f"Timeout: {e}"
                logger.warning(f"WhatsApp timeout, attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue

            except httpx.ConnectError as e:
                last_error = f"Connection error: {e}"
                logger.warning(f"WhatsApp connection error, attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue

            except Exception as e:
                last_error = str(e)
                logger.error(f"WhatsApp unexpected error: {e}")
                break

        # All retries exhausted
        whatsapp_circuit_breaker.record_failure(Exception(last_error or "Unknown error"))
        logger.error(f"WhatsApp API failed after {max_retries} attempts: {last_error}")

        return {
            "status": "Error",
            "statuscode": 500,
            "statustext": f"Request failed after {max_retries} attempts: {last_error}",
        }

    # ── Generic Send Methods ──────────────────────────────────────────────────

    async def send_template(
        self,
        to: str,
        template_id: str,
        variables: List[str],
        image_url: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> WhatsAppResponse:
        """
        Send a template message (text or image).

        If image_url is provided → msgtype 3 (image template).
        Otherwise → msgtype 2 (text-only template).
        """
        if image_url:
            message = self._build_image_message(
                to=to, template_id=template_id, variables=variables,
                image_url=image_url, tag=tag,
            )
        else:
            message = self._build_text_message(
                to=to, template_id=template_id, variables=variables, tag=tag,
            )

        payload = self._build_request_payload([message])
        response_data = await self._send_request(payload)
        return self._parse_response(response_data, message["id"])

    async def send_template_bulk(
        self,
        template_id: str,
        recipients: List[Dict[str, Any]],
        image_url: Optional[str] = None,
    ) -> List[WhatsAppResponse]:
        """
        Send template to multiple recipients in one API call.

        recipients: [{"to": "phone", "variables": ["var1", "var2"], "tag": "..."}]
        """
        built_messages = []
        message_ids = []

        for recipient in recipients:
            if image_url:
                msg = self._build_image_message(
                    to=recipient["to"], template_id=template_id,
                    variables=recipient.get("variables", []),
                    image_url=image_url, tag=recipient.get("tag"),
                )
            else:
                msg = self._build_text_message(
                    to=recipient["to"], template_id=template_id,
                    variables=recipient.get("variables", []),
                    tag=recipient.get("tag"),
                )
            built_messages.append(msg)
            message_ids.append(msg["id"])

        payload = self._build_request_payload(built_messages)
        response_data = await self._send_request(payload)
        return [self._parse_response(response_data, msg_id) for msg_id in message_ids]

    # ══════════════════════════════════════════════════════════════════════════
    # PRE-BUILT: Abandoned Checkout Messages
    # Template: "Hi {{1}}, your {{2}} at {{3}} was not completed because the
    #            payment was not finished. Please complete the payment to
    #            confirm your booking. For any help, contact support@fymble.app."
    # Variables: {{1}}=client_name, {{2}}=product_label, {{3}}=gym_name
    # ══════════════════════════════════════════════════════════════════════════

    async def send_abandoned_dailypass(
        self, to: str, client_name: str, gym_name: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_ABANDONED,
            variables=[client_name, "Daily Pass", gym_name],
            image_url=WHATSAPP_IMAGE_DAILYPASS_ABANDONED,
            tag="abandoned_dailypass",
        )

    async def send_abandoned_membership(
        self, to: str, client_name: str, gym_name: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_ABANDONED,
            variables=[client_name, "Membership", gym_name],
            image_url=WHATSAPP_IMAGE_MEMBERSHIP_ABANDONED,
            tag="abandoned_membership",
        )

    async def send_abandoned_session(
        self, to: str, client_name: str, gym_name: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_ABANDONED,
            variables=[client_name, "Session", gym_name],
            image_url=WHATSAPP_IMAGE_SESSION_ABANDONED,
            tag="abandoned_session",
        )

    async def send_abandoned_subscription(
        self, to: str, client_name: str, plan_name: str = "Fymble",
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_ABANDONED,
            variables=[client_name, "Subscription", plan_name],
            image_url=WHATSAPP_IMAGE_SUBSCRIPTION_ABANDONED,
            tag="abandoned_subscription",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # PRE-BUILT: Booking Confirmation Messages
    #
    # Daily Pass (ID 1708894): {{1}}=name, {{2}}=pass_name, {{3}}=gym, {{4}}=days
    # Session   (ID 1708912): {{1}}=name, {{2}}=session_count, {{3}}=gym
    # Membership(ID 1708926): {{1}}=name, {{2}}=gym
    # ══════════════════════════════════════════════════════════════════════════

    async def send_booked_dailypass(
        self, to: str, client_name: str, pass_name: str,
        gym_name: str, days: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_BOOKED_DAILYPASS,
            variables=[client_name, pass_name, gym_name, days],
            image_url=WHATSAPP_IMAGE_DAILYPASS_BOOKED,
            tag="booked_dailypass",
        )

    async def send_booked_session(
        self, to: str, client_name: str, session_count: str,
        gym_name: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_BOOKED_SESSION,
            variables=[client_name, session_count, gym_name],
            image_url=WHATSAPP_IMAGE_SESSION_BOOKED,
            tag="booked_session",
        )

    async def send_booked_membership(
        self, to: str, client_name: str, gym_name: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP,
            variables=[client_name, gym_name],
            image_url=WHATSAPP_IMAGE_MEMBERSHIP_BOOKED,
            tag="booked_membership",
        )

    async def send_booked_subscription(
        self, to: str, client_name: str, plan_name: str = "Fymble",
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_BOOKED_SUBSCRIPTION,
            variables=[client_name, plan_name],
            image_url=WHATSAPP_IMAGE_SUBSCRIPTION_BOOKED,
            tag="booked_subscription",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # PRE-BUILT: Browsing Follow-up
    # (Template ID + image URL to be set in env when template is approved)
    # ══════════════════════════════════════════════════════════════════════════

    async def send_browsing_followup(
        self, to: str, client_name: str, gym_name: str, lowest_price: str,
    ) -> WhatsAppResponse:
        return await self.send_template(
            to=to,
            template_id=WHATSAPP_TEMPLATE_BROWSING,
            variables=[client_name, gym_name, lowest_price],
            image_url=WHATSAPP_IMAGE_BROWSING,
            tag="browsing_followup",
        )


# ── Singleton ─────────────────────────────────────────────────────────────────

_whatsapp_client: Optional[WhatsAppClient] = None


def get_whatsapp_client() -> WhatsAppClient:
    """Get or create WhatsApp client singleton."""
    global _whatsapp_client
    if _whatsapp_client is None:
        _whatsapp_client = WhatsAppClient()
    return _whatsapp_client


# ── Quick Functions ───────────────────────────────────────────────────────────

async def send_template(
    to: str,
    template_id: str,
    variables: List[str],
    image_url: Optional[str] = None,
    tag: Optional[str] = None,
) -> WhatsAppResponse:
    return await get_whatsapp_client().send_template(
        to, template_id, variables, image_url, tag,
    )


def get_whatsapp_circuit_status() -> Dict[str, Any]:
    return whatsapp_circuit_breaker.get_status()
