

import os
import random
import logging
import asyncio
import time
import requests
import httpx
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any, Optional
from urllib.parse import quote as url_quote

from app.utils.logging_utils import FittbotHTTPException
from app.utils.http_retry import http_get_with_retry, calculate_http_backoff_seconds
from app.utils.circuit_breaker import CircuitBreaker, CircuitOpenError

logger = logging.getLogger("sms")

router = APIRouter(tags=["Verification/SMS"])

# Circuit breaker for SMS provider
sms_circuit_breaker = CircuitBreaker(
    name="sms-pwtpl",
    failure_threshold=5,      # Open after 5 consecutive failures
    recovery_timeout=60.0,    # Wait 60s before testing recovery
    half_open_max_calls=3,    # Allow 3 test calls
    success_threshold=2,      # Need 2 successes to close
)


# ---- Unchanged helper logic ----
def generate_otp():
    return str(random.randint(100000, 999999))


def _send_sms_bhashsms(phone_number, message):
    """Primary SMS provider via BhashSMS."""
    logger.info(f"Attempting SMS via BhashSMS (primary)")
    bhashsms_user = os.getenv("BHASHSMS_USER")
    bhashsms_pass = os.getenv("BHASHSMS_PASS")

    if not bhashsms_user or not bhashsms_pass:
        logger.warning("BhashSMS credentials not configured")
        return False

    url = (
        f"http://bhashsms.com/api/sendmsg.php?"
        f"user={bhashsms_user}&pass={bhashsms_pass}"
        f"&sender=Fymble&phone={phone_number}"
        f"&text={requests.utils.quote(message)}"
        f"&priority=ndnd&stype=normal"
    )

    try:
        response = http_get_with_retry(
            url=url,
            max_attempts=2,
            timeout=10,
            service_name="bhashsms-fallback"
        )

        if response.status_code == 200:
            response_text = response.text.strip().lower()
            if "fail" not in response_text and "error" not in response_text:
                logger.info(f"SMS sent successfully via BhashSMS (primary)")
                return True
            else:
                logger.warning(f"BhashSMS (primary): API returned error: {response.text}")
                return False
        else:
            logger.warning(f"BhashSMS (primary): HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"BhashSMS (primary) error: {e}")
        return False


def _send_sms_pwtpl(phone_number, message, template_id):
    """Fallback SMS provider via PWTPL."""
    logger.info(f"Attempting SMS via PWTPL (fallback)")
    api_key = os.getenv("OTHER_API_KEY")

    if not api_key:
        logger.warning("PWTPL API key not configured, fallback unavailable")
        return False

    sender_id = "Fymble"
    entity_id = "1701174022473316577"

    url = (
        f"http://pwtpl.com/sms/V1/send-sms-api.php?"
        f"apikey={api_key}&senderid={sender_id}&templateid={template_id}"
        f"&entityid={entity_id}&number={phone_number}&message={message}&format=json"
    )

    try:
        response = http_get_with_retry(
            url=url,
            max_attempts=2,
            timeout=10,
            service_name="pwtpl-fallback"
        )

        if response.status_code == 200:
            try:
                json_response = response.json()
                if json_response.get('status') == 'OK':
                    logger.info("SMS sent successfully via PWTPL (fallback)")
                    return True
            except ValueError:
                pass

        logger.warning(f"PWTPL (fallback): API returned error")
        return False
    except Exception as e:
        logger.error(f"PWTPL (fallback) error: {e}")
        return False


def send_verification_sms(phone_number, otp):

    encoded_message = f"Your OTP for the Verification is {otp}. Please Do not share this code with anyone - Fymble"

    # Try BhashSMS first (primary provider)
    result = _send_sms_bhashsms(phone_number, encoded_message)

    if result:
        return True

    # BhashSMS failed — try PWTPL fallback
    logger.warning("BhashSMS failed for OTP, trying PWTPL fallback")
    return _send_sms_pwtpl(phone_number, encoded_message, "1707177070041081926")


def send_ios_premium_sms(phone_number, client_name):

    # Check circuit breaker first
    try:
        sms_circuit_breaker._before_call()
    except CircuitOpenError as e:
        logger.warning(f"SMS circuit OPEN: {e.remaining_seconds:.1f}s until retry")
        return False

    encoded_message = (
        f"Hi {client_name} , Welcome Aboard. To Access our Premium Features, Please complete your Subscription via this secure link: payments.fymble.app. Enjoy the full experience! - Fymble"
    )

    masked_phone = f"****{phone_number[-4:]}" if len(phone_number) >= 4 else "****"
    logger.debug(f"Sending iOS Premium SMS to {masked_phone}")

    # Try BhashSMS first (primary provider)
    result = _send_sms_bhashsms(phone_number, encoded_message)
   
    if result:
        sms_circuit_breaker.record_success()
        logger.info(f"iOS Premium SMS sent successfully to {masked_phone}")
        return True

    # BhashSMS failed — try PWTPL fallback
    sms_circuit_breaker.record_failure(Exception("BhashSMS primary failed"))
    logger.warning(f"BhashSMS failed for iOS Premium SMS to {masked_phone}, trying PWTPL fallback")
    return _send_sms_pwtpl(phone_number, encoded_message, "1707177070023443847")


def send_password_reset_sms(phone_number, otp):
    """Send password reset OTP SMS (different template from verification OTP)."""
    encoded_message = f"Your OTP for Reset Password is {otp}. Please Do not share this code with anyone.- Fymble"

    # Try BhashSMS first (primary provider)
    result = _send_sms_bhashsms(phone_number, encoded_message)
    
    if result:
        return True

    # BhashSMS failed — try PWTPL fallback
    logger.warning("BhashSMS failed for password reset, trying PWTPL fallback")
    return _send_sms_pwtpl(phone_number, encoded_message, "1707177070045447745")


def get_sms_circuit_status() -> Dict[str, Any]:
    """Get SMS circuit breaker status for monitoring."""
    return sms_circuit_breaker.get_status()


# ---- Async helper: httpx GET with retry (non-blocking) ----

async def _async_http_get_with_retry(
    url: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    timeout: float = 10.0,
    service_name: str = "async-http-get",
) -> httpx.Response:
    """
    Async HTTP GET with exponential backoff using httpx.
    Mirrors the sync http_get_with_retry but fully non-blocking.
    """
    last_exception: Optional[Exception] = None

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                start = time.time()
                response = await client.get(url)
                duration_ms = (time.time() - start) * 1000

                if response.status_code < 400:
                    if attempt > 1:
                        logger.info(
                            f"✓ {service_name} succeeded on attempt {attempt}/{max_attempts} "
                            f"(status={response.status_code}, {duration_ms:.0f}ms)"
                        )
                    return response

                # Non-retryable client errors (400-499 except 429)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    logger.warning(
                        f"✗ {service_name} non-retryable error: HTTP {response.status_code} "
                        f"({duration_ms:.0f}ms)"
                    )
                    return response

                # Last attempt — return whatever we got
                if attempt >= max_attempts:
                    logger.error(
                        f"✗ {service_name} failed after {max_attempts} attempts: "
                        f"HTTP {response.status_code} ({duration_ms:.0f}ms)"
                    )
                    return response

                # Exponential backoff (non-blocking)
                delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)
                logger.warning(
                    f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: "
                    f"HTTP {response.status_code}. Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)

            except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPError) as exc:
                last_exception = exc
                duration_ms = (time.time() - start) * 1000

                if attempt >= max_attempts:
                    logger.error(
                        f"✗ {service_name} failed after {max_attempts} attempts: {exc} "
                        f"({duration_ms:.0f}ms)"
                    )
                    raise exc

                delay = calculate_http_backoff_seconds(attempt, base_delay, max_delay, jitter=True)
                logger.warning(
                    f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)

    if last_exception:
        raise last_exception
    raise Exception(f"{service_name} failed without explicit error")


# ---- Async BhashSMS fallback ----

async def _async_send_sms_bhashsms(phone_number: str, message: str) -> bool:
    """Async primary SMS provider via BhashSMS using httpx."""
    logger.info(f"Attempting SMS via BhashSMS (primary) [async]")
    bhashsms_user = os.getenv("BHASHSMS_USER")
    bhashsms_pass = os.getenv("BHASHSMS_PASS")

    if not bhashsms_user or not bhashsms_pass:
        logger.warning("BhashSMS credentials not configured, fallback unavailable")
        return False

    url = (
        f"http://bhashsms.com/api/sendmsg.php?"
        f"user={bhashsms_user}&pass={bhashsms_pass}"
        f"&sender=Fymble&phone={phone_number}"
        f"&text={url_quote(message)}"
        f"&priority=ndnd&stype=normal"
    )

    try:
        response = await _async_http_get_with_retry(
            url=url,
            max_attempts=2,
            timeout=10,
            service_name="async-bhashsms-fallback",
        )

        if response.status_code == 200:
            response_text = response.text.strip().lower()
            if "fail" not in response_text and "error" not in response_text:
                logger.info("SMS sent successfully via BhashSMS (primary) [async]")
                return True
            else:
                logger.warning(f"BhashSMS (primary) [async]: API returned error: {response.text}")
                return False
        else:
            logger.warning(f"BhashSMS (primary) [async]: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"BhashSMS (primary) [async] error: {e}")
        return False



async def _async_send_sms_pwtpl(phone_number: str, message: str, template_id: str) -> bool:
    """Async fallback SMS provider via PWTPL using httpx."""
    logger.info(f"Attempting SMS via PWTPL (fallback) [async]")
    api_key = os.getenv("OTHER_API_KEY")

    if not api_key:
        logger.warning("PWTPL API key not configured, fallback unavailable")
        return False

    sender_id = "Fymble"
    entity_id = "1701174022473316577"

    url = (
        f"http://pwtpl.com/sms/V1/send-sms-api.php?"
        f"apikey={api_key}&senderid={sender_id}&templateid={template_id}"
        f"&entityid={entity_id}&number={phone_number}&message={message}&format=json"
    )

    try:
        response = await _async_http_get_with_retry(
            url=url,
            max_attempts=2,
            timeout=10,
            service_name="async-pwtpl-fallback",
        )

        if response.status_code == 200:
            try:
                json_response = response.json()
                if json_response.get("status") == "OK":
                    logger.info("SMS sent successfully via PWTPL (fallback) [async]")
                    return True
            except ValueError:
                pass

        logger.warning("PWTPL (fallback) [async]: API returned error")
        return False
    except Exception as e:
        logger.error(f"PWTPL (fallback) [async] error: {e}")
        return False


async def async_send_verification_sms(phone_number: str, otp: str) -> bool:
    """Async version of send_verification_sms using httpx."""
    encoded_message = f"Your OTP for the Verification is {otp}. Please Do not share this code with anyone - Fymble"

    # Try BhashSMS first (primary provider)
    result = await _async_send_sms_bhashsms(phone_number, encoded_message)
    
    if result:
        return True

    # BhashSMS failed — try PWTPL fallback
    logger.warning("BhashSMS async failed for OTP, trying PWTPL fallback")
    return await _async_send_sms_pwtpl(phone_number, encoded_message, "1707177070041081926")


async def async_send_ios_premium_sms(phone_number: str, client_name: str) -> bool:
    """Async version of send_ios_premium_sms using httpx."""
    # Check circuit breaker first
    try:
        sms_circuit_breaker._before_call()
    except CircuitOpenError as e:
        logger.warning(f"SMS circuit OPEN: {e.remaining_seconds:.1f}s until retry")
        return False

    encoded_message = (
        f"Hi {client_name} , Welcome Aboard. To Access our Premium Features, "
        f"Please complete your Subscription via this secure link: payments.fymble.app. "
        f"Enjoy the full experience! - Fymble"
    )

    masked_phone = f"****{phone_number[-4:]}" if len(phone_number) >= 4 else "****"
    logger.debug(f"Sending async iOS Premium SMS to {masked_phone}")

    # Try BhashSMS first (primary provider)
    result = await _async_send_sms_bhashsms(phone_number, encoded_message)
    
    if result:
        sms_circuit_breaker.record_success()
        logger.info(f"iOS Premium SMS sent successfully (async) to {masked_phone}")
        return True

    # BhashSMS failed — try PWTPL fallback
    sms_circuit_breaker.record_failure(Exception("BhashSMS primary failed"))
    logger.warning(f"BhashSMS async failed for iOS Premium SMS to {masked_phone}, trying PWTPL fallback")
    return await _async_send_sms_pwtpl(phone_number, encoded_message, "1707177070023443847")


async def async_send_password_reset_sms(phone_number: str, otp: str) -> bool:
    """Async version of send_password_reset_sms using httpx."""
    encoded_message = f"Your OTP for Reset Password is {otp}. Please Do not share this code with anyone.- Fymble"

    # Try BhashSMS first (primary provider)
    result = await _async_send_sms_bhashsms(phone_number, encoded_message)

    if result:
        return True

    # BhashSMS failed — try PWTPL fallback
    logger.warning("BhashSMS async failed for password reset, trying PWTPL fallback")
    return await _async_send_sms_pwtpl(phone_number, encoded_message, "1707177070045447745")
