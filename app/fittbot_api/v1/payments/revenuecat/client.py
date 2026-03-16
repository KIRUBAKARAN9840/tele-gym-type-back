"""
RevenueCat REST API Client with Circuit Breaker and Retry Logic

Features:
- Circuit breaker to prevent cascading failures
- Exponential backoff retry for transient errors
- Proper logging for subscription verification monitoring
"""

import logging
import time
import requests
from typing import Dict, Any, Optional
from requests.exceptions import RequestException, Timeout, ConnectionError

from app.utils.circuit_breaker import CircuitBreaker, CircuitOpenError

logger = logging.getLogger("payments.revenuecat.client")

# RevenueCat API Base URL
RC_API_BASE = "https://api.revenuecat.com/v1"

# Circuit breaker for RevenueCat API
revenuecat_circuit_breaker = CircuitBreaker(
    name="revenuecat",
    failure_threshold=5,      # Open after 5 consecutive failures
    recovery_timeout=45.0,    # Wait 45s before testing recovery
    half_open_max_calls=3,    # Allow 3 test calls
    success_threshold=2,      # Need 2 successes to close
)


class RevenueCatAPIError(Exception):
    """RevenueCat API error"""
    pass


def _is_retryable_error(status_code: Optional[int] = None) -> bool:
    """Check if error is retryable (5xx or rate limit)."""
    if status_code and status_code in [429, 500, 502, 503, 504]:
        return True
    return False


def get_subscriber(
    app_user_id: str,
    api_key: str,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Dict[str, Any]:
    """
    Get subscriber information from RevenueCat API with circuit breaker and retry.

    Args:
        app_user_id: The app user ID (customer_id)
        api_key: RevenueCat API key (V1 public API key)
        max_retries: Maximum retry attempts (default 3)
        base_delay: Base delay for exponential backoff (default 1.0s)

    Returns:
        Subscriber data including subscriptions and entitlements

    Raises:
        RevenueCatAPIError: If API request fails after all retries
    """
    # Check circuit breaker first
    try:
        revenuecat_circuit_breaker._before_call()
    except CircuitOpenError as e:
        logger.warning(f"RevenueCat circuit OPEN: {e.remaining_seconds:.1f}s until retry")
        raise RevenueCatAPIError(f"Service temporarily unavailable. Retry in {e.remaining_seconds:.1f}s")

    url = f"{RC_API_BASE}/subscribers/{app_user_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Platform": "android"
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"RevenueCat API call attempt {attempt}/{max_retries} for {app_user_id}")
            start_time = time.time()

            response = requests.get(url, headers=headers, timeout=10)
            duration_ms = (time.time() - start_time) * 1000

            # Success
            if response.status_code == 200:
                revenuecat_circuit_breaker.record_success()
                data = response.json()
                if attempt > 1:
                    logger.info(f"RevenueCat succeeded on attempt {attempt} ({duration_ms:.0f}ms)")
                else:
                    logger.info(f"Successfully fetched subscriber data for {app_user_id}")
                return data

            # Retryable server error
            if _is_retryable_error(response.status_code):
                last_error = f"HTTP {response.status_code}"
                logger.warning(
                    f"RevenueCat {response.status_code} error, attempt {attempt}/{max_retries}"
                )
                if attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))
                    time.sleep(delay)
                    continue

            # Non-retryable client errors
            if response.status_code == 404:
                # 404 is not a circuit failure - subscriber just doesn't exist
                raise RevenueCatAPIError(f"Subscriber {app_user_id} not found")
            elif response.status_code == 401:
                # Auth error - not a circuit failure
                raise RevenueCatAPIError("Invalid RevenueCat API key")
            else:
                response.raise_for_status()

        except (Timeout, ConnectionError) as e:
            last_error = str(e)
            logger.warning(f"RevenueCat connection error, attempt {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                time.sleep(delay)
                continue

        except RevenueCatAPIError:
            # Re-raise our custom errors (404, 401) without tripping circuit
            raise

        except requests.exceptions.HTTPError as e:
            # Other HTTP errors
            last_error = str(e)
            logger.error(f"RevenueCat HTTP error: {e}")
            break

        except Exception as e:
            last_error = str(e)
            logger.error(f"RevenueCat unexpected error: {e}")
            break

    # All retries exhausted - record circuit failure
    revenuecat_circuit_breaker.record_failure(Exception(last_error or "Unknown error"))
    logger.error(f"RevenueCat API failed after {max_retries} attempts: {last_error}")
    raise RevenueCatAPIError(f"RevenueCat failed after {max_retries} attempts: {last_error}")


def verify_purchase(
    app_user_id: str,
    api_key: str
) -> tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Verify if user has any active subscription with RevenueCat API

    Args:
        app_user_id: The app user ID (customer_id)
        api_key: RevenueCat API key

    Returns:
        Tuple of (has_active_subscription, subscription_data, error_message)
        subscription_data contains the FIRST active subscription found
    """
    try:
        subscriber_data = get_subscriber(app_user_id, api_key)

        # Check subscriber object
        subscriber = subscriber_data.get("subscriber", {})
        subscriptions = subscriber.get("subscriptions", {})
        entitlements = subscriber.get("entitlements", {})

        logger.info(f"🔍 Checking subscriptions for user: {app_user_id}")
        logger.info(f"   - Subscriptions found: {list(subscriptions.keys())}")
        logger.info(f"   - Entitlements found: {list(entitlements.keys())}")

        # Check if ANY active subscription exists
        for product_id, subscription_info in subscriptions.items():
            original_purchase_date = subscription_info.get("original_purchase_date")
            expires_date = subscription_info.get("expires_date")
            unsubscribe_detected_at = subscription_info.get("unsubscribe_detected_at")

            # Check if subscription is active (has expiration date in future and not unsubscribed)
            is_active = expires_date and not unsubscribe_detected_at

            logger.info(f"   📝 Subscription '{product_id}':")
            logger.info(f"      - original_purchase_date: {original_purchase_date}")
            logger.info(f"      - expires_date: {expires_date}")
            logger.info(f"      - unsubscribe_detected_at: {unsubscribe_detected_at}")
            logger.info(f"      - is_active: {is_active}")

            if is_active:
                logger.info(f"✅ Active subscription found: {product_id}")
                # Add product_id to subscription data
                subscription_info["product_identifier"] = product_id
                return True, subscription_info, None

        # Check if any entitlement is active (as fallback)
        for ent_id, ent_data in entitlements.items():
            expires_date = ent_data.get("expires_date")
            if expires_date:
                logger.info(f"✅ Active entitlement found: {ent_id}")
                return True, ent_data, None

        error_msg = f"No active subscriptions or entitlements found for user {app_user_id}"
        logger.warning(f"⚠️ {error_msg}")
        return False, None, error_msg

    except RevenueCatAPIError as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        return False, None, str(e)
    except Exception as e:
        logger.error(f"❌ Unexpected verification error: {str(e)}")
        return False, None, str(e)


def get_revenuecat_circuit_status() -> Dict[str, Any]:
    """Get RevenueCat circuit breaker status for monitoring."""
    return revenuecat_circuit_breaker.get_status()
