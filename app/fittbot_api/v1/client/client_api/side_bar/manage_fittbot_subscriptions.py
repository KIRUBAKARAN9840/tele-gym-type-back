# app/fittbot_api/v1/client/client_api/side_bar/manage_fittbot_subscriptions.py

import base64
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.models.database import get_db
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.enums import SubscriptionStatus
from app.fittbot_api.v1.payments.config.settings import get_payment_settings
from app.models.fittbot_models import FreeTrial
from app.utils.check_subscriptions import get_client_tier

import requests

router = APIRouter(prefix="/manage_subscriptions", tags=["Manage Fittbot Subscriptions"])
logger = logging.getLogger("payments.manage_subscriptions")

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)


# Product ID to plan name mapping
PRODUCT_PLAN_MAPPING = {
    # 1 month plans
    "premium_monthly:plan-monthly": "Gold Plan",
    "one_month_plan:one-month-premium:rp": "Gold Plan",

    # 6 month plans
    "premium_monthly:plan-half-yearly": "Platinum Plan",
    "six_month_plan:six-month-premium:rp": "Platinum Plan",

    # 12 month plans
    "premium_monthly:plan-yearly": "Diamond Plan",
    "twelve_month_plan:twelve-month-premium:rp": "Diamond Plan",
}


def get_plan_name_from_product_id(product_id: str) -> str:

    if not product_id:
        return "Unknown Plan"

    # Check exact match first (case-sensitive for SKUs)
    if product_id in PRODUCT_PLAN_MAPPING:
        return PRODUCT_PLAN_MAPPING[product_id]

    # Fallback: check if product_id contains plan identifiers
    product_id_lower = product_id.lower()
    if "one_month_plan" in product_id_lower or "one-month-premium" in product_id_lower:
        return "Gold Plan"
    elif "six_month_plan" in product_id_lower or "six-month-premium" in product_id_lower:
        return "Platinum Plan"
    elif "twelve_month_plan" in product_id_lower or "twelve-month-premium" in product_id_lower:
        return "Diamond Plan"

    return "Premium Plan"


def parse_complementary_duration(rc_original_txn_id: str) -> Optional[str]:
    """
    Parse the complementary plan duration from rc_original_txn_id.
    Expected format: something containing duration info like "1month", "3months", etc.
    """
    if not rc_original_txn_id:
        return None

    txn_id_lower = rc_original_txn_id.lower()

    # Try to extract duration patterns
    if "1month" in txn_id_lower :
        return "1 Month"
    elif "3month" in txn_id_lower :
        return "3 Months"
    elif "6month" in txn_id_lower:
        return "6 Months"
    elif "12month" in txn_id_lower or "12_month" in txn_id_lower or "1year" in txn_id_lower:
        return "12 Months"

    # If we can't parse, return the txn_id itself as a fallback
    return rc_original_txn_id


@router.get("/get")
async def get_active_subscriptions(client_id: int, db: Session = Depends(get_db)):

    try:
        current_time_aware = now_ist()
        current_time = current_time_aware.replace(tzinfo=None)

        # Get all subscriptions where active_until is in the future
        active_subscriptions = (
            db.query(Subscription)
            .filter(
                Subscription.customer_id == str(client_id),
                Subscription.active_until > current_time
            )
            .all()
        )

        #logger.info(f"Found {len(active_subscriptions)} subscriptions with active access for client {client_id}")

        paid_subscriptions = []
        complementary_plans = []

        for sub in active_subscriptions:
            
            is_cancelled_with_access = (
                sub.status == SubscriptionStatus.canceled.value and
                sub.active_until and
                sub.active_until > current_time
            )

            subscription_data = {
                "id": sub.id,
                "product_id": sub.product_id,
                "plan_name": get_plan_name_from_product_id(sub.product_id),
                "provider": sub.provider,
                "status": sub.status,
                "active_from": sub.active_from.isoformat() if sub.active_from else None,
                "active_until": sub.active_until.isoformat() if sub.active_until else None,
                "expires_at": sub.active_until.isoformat() if sub.active_until else None,
                "auto_renew": sub.auto_renew if sub.auto_renew is not None else False,
                "is_cancelled": is_cancelled_with_access,
            }

            if is_cancelled_with_access:
                subscription_data["note"] = "Subscription cancelled but access remains until expiry"


            if sub.provider == "internal_manual":
                # This is a complementary plan
                complementary_plan = {
                    "id": sub.id,
                    "duration": sub.rc_original_txn_id,
                    "mode": sub.product_id,
                    "status": sub.status,
                    "expires_at": sub.active_until.isoformat() if sub.active_until else None,
                    "granted_by": "internal_manual",
                    "is_cancelled": is_cancelled_with_access,
                }
                if is_cancelled_with_access:
                    complementary_plan["note"] = "Cancelled but access remains until expiry"
                complementary_plans.append(complementary_plan)
            elif sub.provider in ["razorpay_pg", "google_play", "revenuecat"]:
                # This is a paid subscription
                paid_subscriptions.append(subscription_data)

        #logger.info(f"Returning {len(paid_subscriptions)} paid and {len(complementary_plans)} complementary subscriptions")

        # Determine plan: prioritize paid subscriptions over free_trial
        plan = None

        # If client has active paid subscriptions, show subscription tier instead of free_trial
        if paid_subscriptions or complementary_plans:
            client_tier = get_client_tier(db, client_id)
            plan = client_tier
        else:
            # Check free_trial table only if no paid subscriptions
            free_trial_entry = db.query(FreeTrial).filter(
                FreeTrial.client_id == client_id,
                FreeTrial.status == "active"
            ).first()

            if free_trial_entry:
                plan = "free_trial"
            else:
                # Get client tier from check_subscriptions utility
                client_tier = get_client_tier(db, client_id)
                plan = client_tier

        
        response={
            "status": 200,
            "message": "Subscriptions retrieved successfully",
            "data": {
                "plan": plan,
                "paid_subscriptions": paid_subscriptions,
                "complementary_plans": complementary_plans,
            }
        }

        print("responseeeeeeeeee is",response)
        return {
            "status": 200,
            "message": "Subscriptions retrieved successfully",
            "data": {
                "plan": plan,
                "paid_subscriptions": paid_subscriptions,
                "complementary_plans": complementary_plans,
            }
        }

    except Exception as e:
        logger.error(f"Error fetching subscriptions for client {client_id}: {str(e)}", exc_info=True)
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while fetching subscriptions: {str(e)}",
            error_code="GET_SUBSCRIPTIONS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )

from pydantic import BaseModel

class PostRequest(BaseModel):
    client_id: int
    subscription_id: str

@router.post("/cancel_subscription")
async def cancel_subscription(
    data:PostRequest,
    db: Session = Depends(get_db)
):
  
    try:
        client_id=data.client_id
        subscription_id=data.subscription_id


        # Get the subscription from database
        subscription = (
            db.query(Subscription)
            .filter(
                Subscription.id == subscription_id,
                Subscription.customer_id == str(client_id)
            )
            .first()
        )

        if not subscription:
            logger.warning(
                f"[CANCEL_FAILED] Subscription not found: {subscription_id}",
                extra={"client_id": client_id, "subscription_id": subscription_id}
            )
            raise FittbotHTTPException(
                status_code=404,
                detail="Subscription not found or does not belong to this user",
                error_code="SUBSCRIPTION_NOT_FOUND",
                log_data={"subscription_id": subscription_id, "client_id": client_id},
            )

        # Check if already cancelled/expired
        if subscription.status in [SubscriptionStatus.canceled.value, SubscriptionStatus.expired.value]:

            return {
                "status": 200,
                "message": f"Subscription already {subscription.status}",
                "data": {
                    "subscription_id": subscription.id,
                    "status": subscription.status,
                    "provider": subscription.provider,
                    "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                }
            }

        provider = subscription.provider


        # Handle cancellation based on provider
        if provider == "google_play":
            # RevenueCat handles Google Play subscriptions
            result = await cancel_revenuecat_subscription(subscription, db)
        elif provider == "razorpay_pg":
            # Razorpay subscription cancellation
            result = await cancel_razorpay_subscription(subscription, db)
        else:
            logger.error(
                f"[CANCEL_UNSUPPORTED] Unsupported provider: {provider}",
                extra={"subscription_id": subscription_id, "provider": provider}
            )
            raise FittbotHTTPException(
                status_code=400,
                detail=f"Cancellation not supported for provider: {provider}. Supported providers: revenuecat, razorpay_pg",
                error_code="UNSUPPORTED_PROVIDER",
                log_data={"subscription_id": subscription_id, "provider": provider},
            )


        return result

    except FittbotHTTPException:
        raise
    except Exception as e:
        logger.error(
            f"[CANCEL_ERROR] Unexpected error: {str(e)}",
            exc_info=True,
            extra={"subscription_id": subscription_id, "client_id": client_id}
        )
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while cancelling subscription: {str(e)}",
            error_code="CANCEL_SUBSCRIPTION_ERROR",
            log_data={"subscription_id": subscription_id, "client_id": client_id, "error": str(e)},
        )


async def cancel_revenuecat_subscription(subscription: Subscription, db: Session) -> Dict[str, Any]:


    settings = get_payment_settings()
    api_key = settings.revenuecat_api_key

    if not api_key:
        logger.error("[REVENUECAT_CANCEL_NO_KEY] RevenueCat API key not configured")
        raise FittbotHTTPException(
            status_code=500,
            detail="RevenueCat API key not configured",
            error_code="REVENUECAT_API_KEY_MISSING",
            log_data={"subscription_id": subscription.id}
        )

    app_user_id = subscription.customer_id

    try:
        # Call RevenueCat API to revoke promotional entitlement or delete subscriber
        RC_API_BASE = "https://api.revenuecat.com/v1"

        # First try to revoke promotional entitlement
        revoke_url = f"{RC_API_BASE}/subscribers/{app_user_id}/entitlements/{subscription.product_id}/revoke_promotional"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-Platform": "android"
        }


        revoke_response = requests.post(revoke_url, headers=headers, timeout=15)

        if revoke_response.status_code == 200:
            logger.info(
                f"[REVENUECAT_CANCEL_API_SUCCESS] Promotional entitlement revoked successfully",
                extra={"subscription_id": subscription.id, "app_user_id": app_user_id}
            )
        elif revoke_response.status_code in [404, 400]:
            # Not a promotional entitlement, this is a store-managed subscription
            # RevenueCat will automatically detect cancellation from the store
            logger.info(
                f"[REVENUECAT_CANCEL_STORE_MANAGED] Store-managed subscription (not promotional)",
                extra={"subscription_id": subscription.id, "status_code": revoke_response.status_code}
            )
        else:
            logger.warning(
                f"[REVENUECAT_CANCEL_API_WARNING] RevenueCat API returned {revoke_response.status_code}",
                extra={
                    "status_code": revoke_response.status_code,
                    "response": revoke_response.text[:500]
                }
            )

        # Successfully called API, now update database
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = "user_requested"

        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        logger.info(
            f"[REVENUECAT_CANCEL_SUCCESS] Subscription {subscription.id} cancelled successfully",
            extra={
                "subscription_id": subscription.id,
                "new_status": subscription.status,
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None
            }
        )

        return {
            "status": 200,
            "message": "Subscription cancelled successfully via RevenueCat API. For store subscriptions, please also cancel through your app store.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "google_play",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "access_until": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "instructions": {
                    "android": "For Google Play subscriptions, cancel via: play.google.com/store/account/subscriptions",
                    "ios": "For iOS subscriptions, cancel via: Settings > [Your Name] > Subscriptions"
                },
                "note": "RevenueCat has been notified. You will have access until your current billing period ends."
            }
        }

    except requests.exceptions.HTTPError as e:
        response_text = e.response.text if hasattr(e, 'response') else str(e)
        status_code = e.response.status_code if hasattr(e, 'response') else None

        logger.error(
            f"[REVENUECAT_CANCEL_API_ERROR] RevenueCat API error {status_code}: {response_text}",
            extra={
                "subscription_id": subscription.id,
                "app_user_id": app_user_id,
                "status_code": status_code,
                "error_response": response_text[:500]
            }
        )

        # Mark as cancelled locally despite API failure
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = f"user_requested_api_error_{status_code}"
        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        return {
            "status": 200,
            "message": "Subscription marked as cancelled locally. RevenueCat API call failed. Please cancel through app store.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "google_play",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "warning": "RevenueCat API error - local cancellation applied",
                "api_error": f"HTTP {status_code}" if status_code else "Connection error"
            }
        }

    except requests.exceptions.RequestException as e:
        logger.error(
            f"[REVENUECAT_CANCEL_NETWORK_ERROR] Network error: {str(e)}",
            exc_info=True,
            extra={"subscription_id": subscription.id, "app_user_id": app_user_id}
        )

        # Mark as cancelled locally
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = "user_requested_network_error"
        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        return {
            "status": 200,
            "message": "Subscription marked as cancelled locally due to network error. Please cancel through app store.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "google_play",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "warning": "Network error - local cancellation applied"
            }
        }

    except Exception as e:
        logger.error(
            f"[REVENUECAT_CANCEL_UNEXPECTED_ERROR] Unexpected error: {str(e)}",
            exc_info=True,
            extra={"subscription_id": subscription.id}
        )
        db.rollback()
        raise


async def cancel_razorpay_subscription(subscription: Subscription, db: Session) -> Dict[str, Any]:

    logger.info(
        f"[RAZORPAY_CANCEL] Processing cancellation for subscription {subscription.id}",
        extra={
            "subscription_id": subscription.id,
            "customer_id": subscription.customer_id,
            "product_id": subscription.product_id,
            "active_until": subscription.active_until.isoformat() if subscription.active_until else None,
            "cancel_reason": subscription.cancel_reason,
            "rc_original_txn_id": subscription.rc_original_txn_id
        }
    )

    settings = get_payment_settings()

    # Get the Razorpay subscription ID
    # Priority 1: Extract from cancel_reason field (format: "provider_subscription_id:sub_XXXXX")
    # Priority 2: Fall back to rc_original_txn_id
    razorpay_sub_id = None

    if subscription.cancel_reason and "provider_subscription_id:" in subscription.cancel_reason:
        # Parse: "provider_subscription_id:sub_XXXXX" or "provider_subscription_id:sub_XXXXX reason:..."
        try:
            parts = subscription.cancel_reason.split("provider_subscription_id:")
            if len(parts) > 1:
                # Get the subscription ID part (before any space or other text)
                sub_id_part = parts[1].strip()
                # Handle cases like "sub_XXX reason:YYY" or just "sub_XXX"
                razorpay_sub_id = sub_id_part.split()[0] if ' ' in sub_id_part else sub_id_part
                logger.info(f"[RAZORPAY_CANCEL] Extracted subscription ID from cancel_reason: {razorpay_sub_id}")
        except Exception as e:
            logger.warning(f"[RAZORPAY_CANCEL] Failed to parse cancel_reason: {e}")

    # Fallback to rc_original_txn_id if not found in cancel_reason
    if not razorpay_sub_id and subscription.rc_original_txn_id:
        razorpay_sub_id = subscription.rc_original_txn_id
        logger.info(f"[RAZORPAY_CANCEL] Using rc_original_txn_id as subscription ID: {razorpay_sub_id}")

    if not razorpay_sub_id:
        logger.warning(
            f"[RAZORPAY_CANCEL_NO_ID] No Razorpay subscription ID found for {subscription.id}",
            extra={"subscription_id": subscription.id, "rc_original_txn_id": subscription.rc_original_txn_id}
        )

        # Fallback: mark as cancelled locally
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = "user_requested_no_provider_id"
        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        return {
            "status": 200,
            "message": "Subscription cancelled locally (Razorpay subscription ID not found). Auto-renewal disabled.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "razorpay_pg",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "warning": "Provider-level cancellation unavailable"
            }
        }

    try:
        # Call Razorpay API to cancel subscription
        RZP_API = "https://api.razorpay.com/v1"

        auth_string = f"{settings.razorpay_key_id}:{settings.razorpay_key_secret}"
        auth_bytes = base64.b64encode(auth_string.encode("utf-8"))
        headers = {
            "Authorization": f"Basic {auth_bytes.decode('utf-8')}",
            "Content-Type": "application/json"
        }

        # STEP 1: First fetch the subscription status from Razorpay
        fetch_url = f"{RZP_API}/subscriptions/{razorpay_sub_id}"
        logger.info(
            f"[RAZORPAY_CANCEL_FETCH] Fetching subscription status from Razorpay: {razorpay_sub_id}",
            extra={"razorpay_subscription_id": razorpay_sub_id, "fetch_url": fetch_url}
        )

        try:
            fetch_response = requests.get(fetch_url, headers=headers, timeout=15)
            fetch_response.raise_for_status()
            subscription_info = fetch_response.json()

            razorpay_status = subscription_info.get("status")
            razorpay_paid_count = subscription_info.get("paid_count")
            razorpay_total_count = subscription_info.get("total_count")
            razorpay_current_end = subscription_info.get("current_end")

            logger.info(
                f"[RAZORPAY_CANCEL_FETCH_SUCCESS] Razorpay subscription info fetched",
                extra={
                    "razorpay_subscription_id": razorpay_sub_id,
                    "razorpay_status": razorpay_status,
                    "paid_count": razorpay_paid_count,
                    "total_count": razorpay_total_count,
                    "current_end": razorpay_current_end,
                    "full_response": subscription_info
                }
            )

            # Check if subscription can be cancelled
            if razorpay_status in ["completed", "cancelled", "expired"]:
                logger.warning(
                    f"[RAZORPAY_CANCEL_SKIP] Subscription already {razorpay_status} in Razorpay - cannot cancel",
                    extra={
                        "razorpay_subscription_id": razorpay_sub_id,
                        "razorpay_status": razorpay_status
                    }
                )

                # Update local status to match Razorpay
                if razorpay_status == "completed":
                    subscription.status = "completed"
                elif razorpay_status == "cancelled":
                    subscription.status = SubscriptionStatus.canceled.value
                elif razorpay_status == "expired":
                    subscription.status = "expired"

                subscription.auto_renew = False
                db.add(subscription)
                db.commit()
                db.refresh(subscription)

                return {
                    "status": 200,
                    "message": f"Subscription is already '{razorpay_status}' in Razorpay. No further charges will occur.",
                    "data": {
                        "subscription_id": subscription.id,
                        "status": razorpay_status,
                        "provider": "razorpay_pg",
                        "razorpay_status": razorpay_status,
                        "razorpay_subscription_id": razorpay_sub_id,
                        "paid_count": razorpay_paid_count,
                        "total_count": razorpay_total_count,
                        "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                        "auto_renew": False,
                        "note": f"Subscription already {razorpay_status}. No cancellation needed."
                    }
                }

        except requests.exceptions.RequestException as fetch_err:
            logger.warning(
                f"[RAZORPAY_CANCEL_FETCH_ERROR] Could not fetch subscription status: {fetch_err}",
                extra={"razorpay_subscription_id": razorpay_sub_id, "error": str(fetch_err)}
            )

        cancel_url = f"{RZP_API}/subscriptions/{razorpay_sub_id}/cancel"
        cancel_payload = {"cancel_at_cycle_end": False}

        logger.info(
            f"[RAZORPAY_CANCEL_API] Calling Razorpay API to cancel subscription {razorpay_sub_id}",
            extra={
                "razorpay_subscription_id": razorpay_sub_id,
                "subscription_id": subscription.id,
                "cancel_url": cancel_url,
                "payload": cancel_payload
            }
        )

        # Try with JSON body first (standard approach)
        cancel_response = requests.post(
            cancel_url,
            headers=headers,
            json=cancel_payload,  # Use json= instead of data=json.dumps()
            timeout=15,
        )

        # Log FULL raw response before raising (need to see has_scheduled_changes field)
        logger.info(
            f"[RAZORPAY_CANCEL_API_RESPONSE] Raw response: status={cancel_response.status_code}, body={cancel_response.text}",
            extra={
                "razorpay_subscription_id": razorpay_sub_id,
                "status_code": cancel_response.status_code,
                "response_body": cancel_response.text
            }
        )

        cancel_response.raise_for_status()
        razorpay_data = cancel_response.json()

        # Check if cancellation is scheduled for end of billing cycle
        # When cancel_at_cycle_end=1, Razorpay sets:
        # - has_scheduled_changes: true
        # - schedule_change_at: "cycle_end"
        # - status remains "active" until billing cycle ends
        has_scheduled_changes = razorpay_data.get("has_scheduled_changes", False)
        schedule_change_at = razorpay_data.get("schedule_change_at")
        razorpay_status = razorpay_data.get("status")
        current_end = razorpay_data.get("current_end")  # Unix timestamp of cycle end
        ended_at = razorpay_data.get("ended_at")

        logger.info(
            f"[RAZORPAY_CANCEL_API_SUCCESS] Razorpay API call successful",
            extra={
                "razorpay_subscription_id": razorpay_sub_id,
                "subscription_id": subscription.id,
                "razorpay_status": razorpay_status,
                "has_scheduled_changes": has_scheduled_changes,
                "schedule_change_at": schedule_change_at,
                "current_end": current_end,
                "ended_at": ended_at,
                "razorpay_response": razorpay_data
            }
        )

        # Determine if cancellation is scheduled or immediate
        is_scheduled_cancellation = (
            has_scheduled_changes and
            schedule_change_at == "cycle_end" and
            razorpay_status == "active"
        )
        is_immediate_cancellation = razorpay_status == "cancelled"

        if is_scheduled_cancellation:
            logger.info(
                f"[RAZORPAY_CANCEL_SCHEDULED] Cancellation scheduled for end of billing cycle",
                extra={
                    "razorpay_subscription_id": razorpay_sub_id,
                    "current_end": current_end,
                    "has_scheduled_changes": has_scheduled_changes
                }
            )
            # Keep local status as "active" but disable auto_renew since it's scheduled
            subscription.auto_renew = False
            subscription.cancel_reason = f"scheduled_cancellation:provider_subscription_id:{razorpay_sub_id}"

            # Convert current_end timestamp to datetime for display
            cycle_end_display = None
            if current_end:
                from datetime import datetime as dt
                cycle_end_dt = dt.fromtimestamp(current_end)
                cycle_end_display = cycle_end_dt.strftime('%Y-%m-%d %H:%M')
                # Update active_until to match Razorpay's cycle end
                subscription.active_until = cycle_end_dt
        else:
            # Immediate cancellation or already cancelled
            subscription.status = SubscriptionStatus.canceled.value
            subscription.auto_renew = False
            subscription.cancel_reason = f"user_requested:provider_subscription_id:{razorpay_sub_id}"
            cycle_end_display = subscription.active_until.strftime('%Y-%m-%d %H:%M') if subscription.active_until else None

        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        logger.info(
            f"[RAZORPAY_CANCEL_SUCCESS] Subscription {subscription.id} cancellation processed",
            extra={
                "subscription_id": subscription.id,
                "razorpay_subscription_id": razorpay_sub_id,
                "local_status": subscription.status,
                "is_scheduled_cancellation": is_scheduled_cancellation,
                "is_immediate_cancellation": is_immediate_cancellation,
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None
            }
        )

        if is_scheduled_cancellation:
            return {
                "status": 200,
                "message": f"Subscription scheduled for cancellation. You will have full access until {cycle_end_display or 'end of billing cycle'}. No more charges will occur.",
                "data": {
                    "subscription_id": subscription.id,
                    "status": "scheduled_cancellation",
                    "provider": "razorpay_pg",
                    "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                    "access_until": subscription.active_until.isoformat() if subscription.active_until else None,
                    "auto_renew": False,
                    "razorpay_status": razorpay_status,
                    "has_scheduled_changes": has_scheduled_changes,
                    "schedule_change_at": schedule_change_at,
                    "razorpay_subscription_id": razorpay_sub_id,
                    "note": "Cancellation scheduled for end of billing cycle. Full access maintained until then."
                }
            }

        return {
            "status": 200,
            "message": f"Subscription cancelled successfully. You will have access until {cycle_end_display or 'now'}.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "razorpay_pg",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "access_until": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "razorpay_status": razorpay_status,
                "razorpay_subscription_id": razorpay_sub_id,
                "note": "Subscription cancelled immediately."
            }
        }

    except requests.exceptions.HTTPError as e:
        response_text = e.response.text if hasattr(e, 'response') else str(e)
        status_code = e.response.status_code if hasattr(e, 'response') else None

        logger.error(
            f"[RAZORPAY_CANCEL_API_ERROR] Razorpay API returned error {status_code}: {response_text}",
            extra={
                "subscription_id": subscription.id,
                "razorpay_subscription_id": razorpay_sub_id,
                "status_code": status_code,
                "error_response": response_text
            }
        )

        # Mark as cancelled locally despite API failure
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = f"user_requested_api_error_{status_code}"
        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        return {
            "status": 200,
            "message": "Subscription cancelled locally. Auto-renewal disabled. Please contact support if billing continues.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "razorpay_pg",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "warning": "Razorpay API error - local cancellation applied",
                "api_error": f"HTTP {status_code}" if status_code else "Connection error"
            }
        }

    except requests.exceptions.RequestException as e:
        logger.error(
            f"[RAZORPAY_CANCEL_NETWORK_ERROR] Network error while cancelling: {str(e)}",
            exc_info=True,
            extra={"subscription_id": subscription.id, "razorpay_subscription_id": razorpay_sub_id}
        )

        # Mark as cancelled locally despite network failure
        subscription.status = SubscriptionStatus.canceled.value
        subscription.auto_renew = False
        subscription.cancel_reason = "user_requested_network_error"
        db.add(subscription)
        db.commit()
        db.refresh(subscription)

        return {
            "status": 200,
            "message": "Subscription cancelled locally due to network error. Auto-renewal disabled.",
            "data": {
                "subscription_id": subscription.id,
                "status": "canceled",
                "provider": "razorpay_pg",
                "expires_at": subscription.active_until.isoformat() if subscription.active_until else None,
                "auto_renew": False,
                "warning": "Network error - local cancellation applied. Please verify in Razorpay dashboard."
            }
        }

    except Exception as e:
        logger.error(
            f"[RAZORPAY_CANCEL_UNEXPECTED_ERROR] Unexpected error: {str(e)}",
            exc_info=True,
            extra={"subscription_id": subscription.id}
        )
        db.rollback()
        raise

