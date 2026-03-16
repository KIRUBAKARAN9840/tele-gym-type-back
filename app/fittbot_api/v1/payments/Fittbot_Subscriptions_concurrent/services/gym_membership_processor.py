import asyncio
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, Optional, Tuple, Set

from dateutil.relativedelta import relativedelta
from fastapi import HTTPException, status as http_status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.fittbot_api.v1.payments.routes import gym_membership as gm_routes
from app.fittbot_api.v1.payments.config.settings import get_payment_settings
from app.fittbot_api.v1.payments.models.enums import (
    ItemType,
    StatusOrder,
    StatusPayment,
    SubscriptionStatus,
    EntType,
    StatusEnt,
    StatusPayoutLine,
)
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.entitlements import Entitlement
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.payouts import PayoutLine
from app.fittbot_api.v1.payments.services.entitlement_service import EntitlementService
from app.fittbot_api.v1.payments.razorpay_async_gateway import (
    create_order as rzp_create_order,
    get_payment as rzp_get_payment,
    get_order as rzp_get_order,
)
from app.fittbot_api.v1.payments.Fittbot_Subscriptions.razorpay import process_razorpay_webhook_payload
from app.models.fittbot_models import FittbotGymMembership, ReferralFittbotCash, NoCostEmi, GymPlans, GymFees
from app.models.fittbot_plans_model import FittbotPlan
from app.models.async_database import create_celery_async_sessionmaker
from app.models.fittbot_payments_models import Payment as FittbotPayment
from redis import Redis
from app.fittbot_api.v1.client.client_api.nutrition.nutrition_eligibility_service import (
    grant_nutrition_eligibility_async,
)
from app.config.settings import settings
from app.tasks.notification_tasks import queue_membership_notification
from app.fittbot_api.v1.client.client_api.reward_program.reward_service import add_gym_membership_entry

from ...config.database import PaymentDatabase
from app.config.pricing import get_markup_multiplier
from .payment_event_logger import PaymentEventLogger

logger = logging.getLogger("payments.gym_membership.v2.processor")
pel = PaymentEventLogger("razorpay", "gym_membership")

UTC = timezone.utc

# Import legacy helpers (for non-DB operations)
UnifiedMembershipRequest = gm_routes.UnifiedMembershipRequest
_new_id = gm_routes._new_id
_mask = gm_routes._mask
_verify_checkout_sig = gm_routes._verify_checkout_sig


def _mask_sensitive(s: Optional[str]) -> str:

    if not s:
        return ""
    return f"{s[:8]}...{s[-4:]}" if len(s) > 12 else "***"


def smart_round_price(price: float) -> int:

    price_int = int(round(price))
    last_two_digits = price_int % 100

    if last_two_digits == 0:
        return price_int - 1
    elif last_two_digits <= 50:
        return (price_int // 100) * 100 + 49
    else:
        return ((price_int // 100) + 1) * 100 - 1


def round_per_month_price(price: float) -> int:
    """
    NEW rounding logic for LOWEST plan only:
    Rounds to the nearest ceiling number ending in 9.
    e.g., 700 -> 709, 710 -> 719, 721 -> 729, 751 -> 759
    """
    price_int = int(round(price))
    # Round to nearest ceiling 9: (price // 10) * 10 + 9
    return (price_int // 10) * 10 + 9


# Redis key for promo plan IDs per gym (same as gym_studios.py)
PROMO_PLANS_KEY = "promo_plans:{gym_id}"


async def get_promo_plan_ids_from_redis(redis: Redis, gym_id: int) -> set:
    """Get all cached promo plan IDs for a gym from Redis.
    Handles both sync and async Redis clients (Celery tasks use sync Redis).
    """
    try:
        key = PROMO_PLANS_KEY.format(gym_id=gym_id)
        # sync Redis returns set directly; async Redis returns coroutine
        result = redis.smembers(key)
        if asyncio.iscoroutine(result):
            members = await result
        else:
            members = result
        if members:
            return {int(m.decode() if isinstance(m, bytes) else m) for m in members}
    except Exception as e:
        logger.warning(f"Error getting promo plans from Redis for gym {gym_id}: {e}")
    return set()


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

async def _get_plan_details_async(db: AsyncSession, gym_id: int, plan_id: int, redis: Optional[Redis] = None) -> Tuple[int, int, bool, int]:
    """Async version - Get plan details including whether it's a personal training service
    Returns: (amount_with_markup_minor, duration, is_personal_training, base_amount)

    Uses promo pricing (raw × markup ÷ duration → ceil 9 → × duration) for promo plans,
    49/99 smart_round_price for all other plans.
    """
    result = await db.execute(
        select(GymPlans).where(GymPlans.id == plan_id, GymPlans.gym_id == gym_id)
    )
    plan = result.scalars().first()
    if not plan:
        raise HTTPException(404, "Plan not found for gym")

    base_amount = int(plan.amount)  # Gym owner's actual base price
    duration = int(plan.duration or 1)
    is_personal_training = bool(plan.personal_training)

    if base_amount <= 0 or duration <= 0:
        raise HTTPException(409, "Invalid plan config")

    # Check if this plan is a promo plan for this gym (from Redis)
    is_promo_plan = False
    if redis and duration > 1:
        try:
            promo_ids = await get_promo_plan_ids_from_redis(redis, gym_id)
            if plan_id in promo_ids:
                is_promo_plan = True
        except Exception as e:
            logger.warning(f"Error checking promo plans from Redis: {e}")

    if is_promo_plan:
        # Promo plan pricing: raw × markup ÷ duration (no bonus, no smart_round) → ceil 9 → × duration
        raw_per_month = (base_amount * get_markup_multiplier()) / duration
        rounded_per_month = round_per_month_price(raw_per_month)
        amount_with_markup = rounded_per_month * duration
    else:
        # Regular plan pricing: 49/99 smart_round_price
        amount_with_markup = smart_round_price(base_amount * get_markup_multiplier())

    return amount_with_markup * 100, duration, is_personal_training, base_amount


async def _get_plan_by_id_async(db: AsyncSession, plan_id: str) -> Optional[FittbotPlan]:
    """Async version of get_plan_by_id"""
    return (
        await db.execute(
            select(FittbotPlan).where(FittbotPlan.id == plan_id)
        )
    ).scalars().first()


async def _find_active_subscription_async(db: AsyncSession, customer_id: str) -> Optional[Dict[str, Any]]:
    """Async version - Find active subscription for customer"""
    try:
        active_sub = (
            await db.execute(
                select(Subscription).where(
                    Subscription.customer_id == customer_id,
                    Subscription.status == SubscriptionStatus.active,
                    Subscription.active_until > datetime.now(UTC),
                )
            )
        ).scalars().first()
        if active_sub:
            return {
                "id": active_sub.id,
                "provider": active_sub.provider,
                "product_id": active_sub.product_id,
                "active_until": active_sub.active_until,
                "rc_original_txn_id": active_sub.rc_original_txn_id,
            }
    except Exception as e:
        logger.error(f"Error finding active subscription: {e}")
    return None


async def _deduct_reward_async(db: AsyncSession, customer_id: str, reward_amount: int):
    """Async version - Deduct reward from fittbot_cash"""
    client_id = int(customer_id)
    fittbot_cash_entry = (
        await db.execute(
            select(ReferralFittbotCash).where(ReferralFittbotCash.client_id == client_id)
        )
    ).scalars().first()

    if fittbot_cash_entry:
        reward_rupees = reward_amount / 100
        if fittbot_cash_entry.fittbot_cash >= reward_rupees:
            fittbot_cash_entry.fittbot_cash -= reward_rupees
            db.add(fittbot_cash_entry)
            await db.flush()
            logger.info(f"[GM_REWARD_DEDUCTED] Deducted {reward_rupees}₹ from fittbot_cash for client {client_id}")
        else:
            logger.warning(f"[GM_REWARD_INSUFFICIENT] Insufficient fittbot_cash for client {client_id}")


async def _upsert_gym_fees_async(db: AsyncSession, client_id: str, start_date: date, end_date: date):
    """Async version - Mirrors to gym_fees table"""
    stmt = mysql_insert(GymFees).values(
        client_id=client_id, start_date=start_date, end_date=end_date
    )
    stmt = stmt.on_duplicate_key_update(
        end_date=func.greatest(GymFees.end_date, stmt.inserted.end_date)
    )
    await db.execute(stmt)


async def _extend_subscription_validity_async(db: AsyncSession, subscription_id: str, plan_id: int = None, duration_months: int = None) -> bool:
    """Async version - Extend subscription validity by plan duration or explicit months"""
    try:
        subscription = (
            await db.execute(
                select(Subscription).where(Subscription.id == subscription_id)
            )
        ).scalars().first()
        if subscription:
            # Use explicit duration_months if provided, otherwise get from plan
            if duration_months:
                months_to_add = duration_months
            elif plan_id:
                plan = await _get_plan_by_id_async(db, str(plan_id))
                if not plan:
                    logger.error(f"Plan {plan_id} not found for subscription extension")
                    return False
                months_to_add = int(plan.duration)
            else:
                logger.error("Either plan_id or duration_months required for extension")
                return False

            current_end = subscription.active_until or datetime.now(UTC)
            new_end = current_end + relativedelta(months=months_to_add)

            subscription.active_until = new_end
            db.add(subscription)
            logger.info(f"Extended subscription {subscription_id} by {months_to_add} months from {current_end} to {new_end}")
            return True
    except Exception as e:
        logger.error(f"Error extending subscription validity: {e}")
    return False


async def _process_app_subscription_activation_async(
    db: AsyncSession,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    """Async version - Process app subscription activation for unified flow"""
    settings = get_payment_settings()

    meta = order_item.item_metadata or {}
    plan_id = meta.get("plan_id")
    # Support both duration_months (new) and duration_days (legacy, actually months)
    duration_months = int(meta.get("duration_months") or meta.get("duration_days", 1))
    is_free_with_membership = meta.get("is_free_with_membership", False)
    product_id = f"fittbot_free_{duration_months}m" if is_free_with_membership else (f"fittbot_plan_{plan_id}" if plan_id else "fittbot_plan")
    is_existing = meta.get("is_existing", False)
    existing_subscription_id = meta.get("existing_subscription_id")
    existing_subscription_provider = meta.get("existing_subscription_provider")

    nowu = datetime.now(UTC)
    paused_existing = False
    extended_existing = False

    # Handle existing subscription logic
    if is_existing and existing_subscription_id:
        logger.info(f"Processing existing subscription logic for {existing_subscription_id}")

        # Try to pause existing subscription if it's from Razorpay
        if existing_subscription_provider == "razorpay":
            # Pause Razorpay subscription - run in thread
            try:
                paused_existing = await asyncio.to_thread(
                    gm_routes._pause_razorpay_subscription, existing_subscription_id, settings
                )
            except Exception as e:
                logger.error(f"Failed to pause subscription: {e}")
                paused_existing = False

        # Extend existing subscription validity
        if paused_existing or existing_subscription_provider != "razorpay":
            extended_existing = await _extend_subscription_validity_async(
                db, existing_subscription_id, plan_id=plan_id, duration_months=duration_months
            )
            if extended_existing:
                existing_sub = (
                    await db.execute(
                        select(Subscription).where(Subscription.id == existing_subscription_id)
                    )
                ).scalars().first()
                if existing_sub:
                    return {
                        "subscription_id": existing_sub.id,
                        "plan_id": plan_id,
                        "active_from": existing_sub.active_from.isoformat(),
                        "active_until": existing_sub.active_until.isoformat(),
                        "status": "extended",
                        "provider": existing_sub.provider,
                        "was_paused": paused_existing,
                        "was_extended": True,
                        "extension_months": duration_months
                    }

    # Create new subscription (default behavior or if extension failed)
    months_to_add = duration_months

    sub = Subscription(
        id=_new_id("sub_"),
        customer_id=customer_id,
        provider="internal_manual",
        product_id=str(product_id),
        status=SubscriptionStatus.active,
        rc_original_txn_id=None,
        latest_txn_id=payment_id,
        active_from=nowu,
        active_until=(nowu + relativedelta(months=months_to_add)),
        auto_renew=False,
    )
    db.add(sub)
    await db.flush()

    # Create entitlements for subscription
    order = (
        await db.execute(
            select(Order).where(Order.id == order_item.order_id)
        )
    ).scalars().first()

    if order:
        ents = (
            await db.execute(
                select(Entitlement)
                .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                .where(OrderItem.order_id == order.id)
            )
        ).scalars().all()

        if not ents:
            # EntitlementService uses sync operations - use a sync session
            from app.models.database import get_db_sync
            order_id_copy = order.id
            def _create_entitlements_sync():
                sync_session = next(get_db_sync())
                try:
                    sync_order = sync_session.query(Order).filter(Order.id == order_id_copy).first()
                    if sync_order:
                        EntitlementService(sync_session).create_entitlements_from_order(sync_order)
                        sync_session.commit()
                finally:
                    sync_session.close()
            await asyncio.to_thread(_create_entitlements_sync)
            # Re-fetch entitlements
            ents = (
                await db.execute(
                    select(Entitlement)
                    .join(OrderItem, Entitlement.order_item_id == OrderItem.id)
                    .where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

        for e in ents:
            if e.order_item_id == order_item.id:
                e.entitlement_type = EntType.app
                e.active_from = sub.active_from
                e.active_until = sub.active_until
                e.status = StatusEnt.active
                db.add(e)

    return {
        "subscription_id": sub.id,
        "plan_id": plan_id,
        "active_from": sub.active_from.isoformat(),
        "active_until": sub.active_until.isoformat(),
        "status": "active",
        "provider": "internal_manual",
        "existing_subscription_handled": is_existing and existing_subscription_id is not None,
        "existing_subscription_paused": paused_existing,
    }


async def _process_gym_membership_item_async(
    db: AsyncSession, item: OrderItem, order: Order, payment: Payment
) -> Dict[str, Any]:
    """Async version - Process gym membership item activation"""
    meta = item.item_metadata or {}
    duration = int(meta.get("duration_months", 1))
    start_on = meta.get("start_on")
    amount_major = meta.get("amount")
    plan_id = meta.get("plan_id")

    # Get amount from order metadata if plan_id exists, otherwise use amount_major
    final_amount = amount_major
    if plan_id and order.order_metadata:
        try:
            service_base_rupees = order.order_metadata.get("payment_summary", {}).get("step_1_base_amounts", {}).get("service_base_rupees")
            if service_base_rupees:
                final_amount = float(service_base_rupees)
                logger.info(f"[GM_AMOUNT_FROM_ORDER_META] Using amount {final_amount} from order metadata for plan_id={plan_id}")
        except Exception as e:
            logger.warning(f"[GM_AMOUNT_FALLBACK] Failed to get amount from order metadata: {e}, using amount_major={amount_major}")

    # Determine start date
    if start_on:
        start_dt = datetime.fromisoformat(start_on).replace(tzinfo=UTC)
    else:
        start_dt = datetime.now(UTC)

    end_dt = start_dt + relativedelta(months=duration)

    # Create entitlement
    ent = Entitlement(
        id=_new_id("ent_"),
        order_item_id=item.id,
        customer_id=order.customer_id,
        gym_id=item.gym_id,
        entitlement_type=EntType.membership,
        active_from=start_dt,
        active_until=end_dt,
        status=StatusEnt.active,
    )
    db.add(ent)
    await db.flush()

    # Create payout line
    gross = item.unit_price_minor * item.qty
    pl = PayoutLine(
        id=_new_id("pl_"),
        entitlement_id=ent.id,
        gym_id=item.gym_id,
        gross_amount_minor=gross,
        commission_amount_minor=0,
        net_amount_minor=gross,
        applied_commission_pct=0.0,
        applied_commission_fixed_minor=0,
        scheduled_for=date.today() + timedelta(days=7),
        status=StatusPayoutLine.pending,
    )
    db.add(pl)

    # Legacy table mirror
    await _upsert_gym_fees_async(db, client_id=order.customer_id, start_date=start_dt.date(), end_date=end_dt.date())

    # Insert into fittbot_gym_membership table
    try:
        base_amount_value = item.item_metadata.get("base_amount") if item.item_metadata else final_amount
        fittbot_membership = FittbotGymMembership(
            gym_id=str(item.gym_id),
            client_id=order.customer_id,
            plan_id=item.item_metadata.get("plan_id") if item.item_metadata else None,
            amount=base_amount_value,
            type="gym_membership",
            entitlement_id=ent.id,
            purchased_at=datetime.now(UTC),
            status="upcoming",
        )
        db.add(fittbot_membership)
        logger.info(f"[GM_MEMBERSHIP_RECORD] Created fittbot_gym_membership: entitlement={ent.id}, amount={base_amount_value}")
    except Exception as e:
        logger.warning(f"Failed to create fittbot_gym_membership record: {e}")

    return {
        "entitlement_id": ent.id,
        "gym_id": item.gym_id,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
        "status": "active",
    }


async def _process_personal_training_item_async(
    db: AsyncSession, item: OrderItem, order: Order, payment: Payment
) -> Dict[str, Any]:
    """Async version - Process personal training item activation"""
    meta = item.item_metadata or {}
    sessions = int(meta.get("sessions", 1))
    start_on = meta.get("start_on")
    amount_major = meta.get("amount")
    plan_id = meta.get("plan_id")

    # Get amount from order metadata if plan_id exists, otherwise use amount_major
    final_amount = amount_major
    if plan_id and order.order_metadata:
        try:
            service_base_rupees = order.order_metadata.get("payment_summary", {}).get("step_1_base_amounts", {}).get("service_base_rupees")
            if service_base_rupees:
                final_amount = float(service_base_rupees)
                logger.info(f"[PT_AMOUNT_FROM_ORDER_META] Using amount {final_amount} from order metadata for plan_id={plan_id}")
        except Exception as e:
            logger.warning(f"[PT_AMOUNT_FALLBACK] Failed to get amount from order metadata: {e}, using amount_major={amount_major}")

    # Determine start date
    if start_on:
        start_dt = datetime.fromisoformat(start_on).replace(tzinfo=UTC)
    else:
        start_dt = datetime.now(UTC)

    # PT sessions valid for 6 months
    end_dt = start_dt + timedelta(days=180)

    # Create entitlement
    ent = Entitlement(
        id=_new_id("ent_"),
        order_item_id=item.id,
        customer_id=order.customer_id,
        gym_id=item.gym_id,
        entitlement_type=EntType.session,
        active_from=start_dt,
        active_until=end_dt,
        status=StatusEnt.active,
    )
    db.add(ent)
    await db.flush()

    # Create payout line
    gross = item.unit_price_minor * item.qty
    
    pl = PayoutLine(
        id=_new_id("pl_"),
        entitlement_id=ent.id,
        gym_id=item.gym_id,
        gross_amount_minor=gross,
        commission_amount_minor=0,
        net_amount_minor=gross,
        applied_commission_pct=0.0,
        applied_commission_fixed_minor=0,
        scheduled_for=date.today() + timedelta(days=7),
        status=StatusPayoutLine.pending,
    )
    db.add(pl)

    # Insert into fittbot_gym_membership table
    try:
        base_amount_value = item.item_metadata.get("base_amount") if item.item_metadata else final_amount
        fittbot_membership = FittbotGymMembership(
            gym_id=str(item.gym_id),
            client_id=order.customer_id,
            plan_id=item.item_metadata.get("plan_id") if item.item_metadata else None,
            type="personal_training",
            amount=base_amount_value,
            entitlement_id=ent.id,
            purchased_at=datetime.now(UTC),
            status="upcoming",
        )
        db.add(fittbot_membership)
        logger.info(f"[GM_PT_RECORD] Created fittbot_gym_membership: entitlement={ent.id}, amount={base_amount_value}")
    except Exception as e:
        logger.warning(f"Failed to create fittbot_gym_membership record: {e}")

    return {
        "entitlement_id": ent.id,
        "gym_id": item.gym_id,
        "sessions": sessions,
        "active_from": start_dt.isoformat(),
        "active_until": end_dt.isoformat(),
        "status": "active",
        "service_type": "personal_training",
    }


class GymMembershipVerificationResponse:
    def __init__(
        self,
        verified: bool,
        captured: bool,
        order_id: str,
        payment_id: str,
        service_activated: bool = False,
        service_details: Optional[Dict[str, Any]] = None,
        subscription_activated: bool = False,
        subscription_details: Optional[Dict[str, Any]] = None,
        total_amount: int = 0,
        currency: str = "INR",
        message: str = "",
        purchased_at: Optional[datetime] = None,
    ):
        self.verified = verified
        self.captured = captured
        self.order_id = order_id
        self.payment_id = payment_id
        self.service_activated = service_activated
        self.service_details = service_details
        self.subscription_activated = subscription_activated
        self.subscription_details = subscription_details
        self.total_amount = total_amount
        self.currency = currency
        self.message = message
        self.purchased_at = purchased_at or datetime.now(UTC)

    def dict(self) -> Dict[str, Any]:
        return {
            "verified": self.verified,
            "captured": self.captured,
            "order_id": self.order_id,
            "payment_id": self.payment_id,
            "service_activated": self.service_activated,
            "service_details": self.service_details,
            "subscription_activated": self.subscription_activated,
            "subscription_details": self.subscription_details,
            "total_amount": self.total_amount,
            "currency": self.currency,
            "message": self.message,
            "purchased_at": self.purchased_at.isoformat() if isinstance(self.purchased_at, datetime) else str(self.purchased_at),
        }


class GymMembershipProcessor:
    """Runs heavy Gym Membership checkout + verification work in Celery workers."""

    def __init__(
        self,
        config,
        payment_db: PaymentDatabase,
        *,
        redis: Optional[Redis] = None,
    ):
        self.config = config
        self.payment_db = payment_db
        self.settings = get_payment_settings()
        self.redis = redis

    async def _invalidate_home_booking_cache(self, gym_ids: Set[int]) -> None:
        """Clear home booking counts cache for impacted gyms."""
        if not self.redis or not gym_ids:
            return
        for gid in gym_ids:
            if gid is None:
                continue
            key = f"gym:{gid}:home_booking_counts:v2"
            try:
                await asyncio.to_thread(self.redis.delete, key)
            except Exception:
                logger.warning("HOME_BOOKING_CACHE_DELETE_FAILED", extra={"gym_id": gid, "key": key})

    async def process_checkout(self, command_id: str, store) -> None:
        record = await store.mark_processing(command_id)
        payload = UnifiedMembershipRequest(**record.payload)
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, client_id=str(payload.client_id),
                             gym_id=payload.gym_id, plan_id=payload.plan_id)
        try:
            result = await self._execute_checkout(payload)
        except Exception as exc:
            pel.checkout_failed(command_id=command_id, client_id=str(payload.client_id),
                                error_code=type(exc).__name__, error_detail=str(exc),
                                duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("Gym membership checkout failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        pel.checkout_completed(command_id=command_id, client_id=str(payload.client_id),
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               gym_id=payload.gym_id, plan_id=payload.plan_id)
        pel.order_created(command_id=command_id, client_id=str(payload.client_id),
                          gym_id=payload.gym_id)
        await store.mark_completed(command_id, result)

    async def process_verify(self, command_id: str, store) -> None:
        record = await store.mark_processing(command_id)
        payload_dict = record.payload
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id,
                           razorpay_payment_id=payload_dict.get("razorpay_payment_id"),
                           razorpay_order_id=payload_dict.get("razorpay_order_id"))
        try:
            result = await self._execute_verify(payload_dict)
        except Exception as exc:
            pel.verify_failed(command_id=command_id,
                              error_code=type(exc).__name__, error_detail=str(exc),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("Gym membership verification failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        _dur = int((time.perf_counter() - _start) * 1000)
        if result.get("verified"):
            pel.verify_completed(command_id=command_id, verify_path="gym_membership",
                                 duration_ms=_dur)
            pel.payment_captured(command_id=command_id,
                                 razorpay_payment_id=payload_dict.get("razorpay_payment_id"))
        else:
            pel.verify_failed(command_id=command_id,
                              error_code="verify_unsuccessful", duration_ms=_dur)
        await store.mark_completed(command_id, result)

    async def _execute_checkout(self, payload: UnifiedMembershipRequest) -> Dict[str, Any]:
        """Execute checkout using async DB session."""
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            return await self._checkout_async(db, payload)

    async def _execute_verify(self, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute verification using async DB session."""
        razorpay_payment_id = payload_dict.get("razorpay_payment_id")
        capture_marker = await self._capture_marker_snapshot(razorpay_payment_id)
        if capture_marker:
            logger.info(
                "GYM_MEMBERSHIP_VERIFY_CAPTURE_CACHE_HIT",
                extra={
                    "payment_id": _mask_sensitive(razorpay_payment_id),
                    "order_id": capture_marker.get("order_id"),
                },
            )
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            return await self._verify_async(db, payload_dict, capture_marker)

    async def _checkout_async(self, db: AsyncSession, payload: UnifiedMembershipRequest) -> Dict[str, Any]:

        user_id = payload.client_id
        settings = self.settings
        gym_id = payload.gym_id
        plan_id = payload.plan_id

        logger.info(f"[GM_CHECKOUT_START] client={user_id}, gym={gym_id}, plan={plan_id}, include_sub={payload.includeSubscription}")


        service_amount_minor, duration_value, is_personal_training, base_amount = await _get_plan_details_async(
            db, gym_id, plan_id, self.redis
        )

        if is_personal_training:
            service_label = "Personal Training"
            service_type = "personal_training"
        else:
            service_label = "Gym Membership"
            service_type = "gym_membership"

        logger.info(f"[GM_SERVICE_TYPE] type={service_type}, amount={service_amount_minor/100}rs, duration={duration_value}")

        sub_total = 0  # Always free
        sub_duration_months = getattr(payload, 'fittbotDuration', None) or duration_value  # Use fittbotDuration from frontend or default to gym plan duration
        existing_subscription = None

        # Check for existing subscription to extend
        if payload.is_existing:
            existing_subscription = await _find_active_subscription_async(db, user_id)
            if existing_subscription:
                logger.info(f"[GM_EXISTING_SUB] Found subscription: {existing_subscription['id']}")

        logger.info(f"[GM_FREE_FITTBOT] All memberships get FREE Fittbot subscription, duration={sub_duration_months} months")

        # NOTE: service_amount_minor already includes platform markup + smart rounding from _get_plan_details_async
        gross_before_rewards = service_amount_minor + sub_total
        logger.info(f"[GM_PRICING] service={service_amount_minor/100}rs, sub={sub_total/100}rs, gross={gross_before_rewards/100}rs")

        # 3) Calculate reward if enabled - async query
        reward_amount = 0
        reward_calculation_details = {}

        if payload.reward:
            # 10% of service amount, capped at 100 rupees (10000 paisa)
            ten_percent_minor = int(service_amount_minor * 0.10)
            capped_reward_minor = min(ten_percent_minor, 10000)  # Max 100 rupees

            fittbot_cash_entry = (
                await db.execute(
                    select(ReferralFittbotCash).where(ReferralFittbotCash.client_id == int(user_id))
                )
            ).scalars().first()

            available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0
            available_fittbot_cash_minor = available_fittbot_cash_rupees * 100

            reward_amount = round(min(available_fittbot_cash_minor, capped_reward_minor) / 100) * 100
            logger.debug("reward_amount: %s", reward_amount)

            reward_calculation_details = {
                "reward_applied": True,
                "reward_amount_minor": reward_amount,
                "reward_amount_rupees": reward_amount / 100,
                "ten_percent_cap_minor": ten_percent_minor,
                "available_fittbot_cash_minor": available_fittbot_cash_minor,
                "available_fittbot_cash_rupees": available_fittbot_cash_rupees,
                "calculation_base": "service_amount",
                "max_reward_cap": 100,
            }

            logger.info(f"[GM_REWARD] calculated={reward_amount/100}rs, available_cash={available_fittbot_cash_rupees}rs")

        grand_total = gross_before_rewards - reward_amount
        grand_total=round(grand_total)
        logger.info(f"[GM_GRAND_TOTAL] {grand_total/100}rs (reward={reward_amount/100}rs)")

      
        order_metadata = {
            "order_info": {
                "order_type": f"unified_{service_type}_with_free_fittbot",
                "customer_id": user_id,
                "created_at": datetime.now(UTC).isoformat(),
                "currency": "INR",
                "flow": f"unified_{service_type}_with_free_fittbot",
            },
            "order_composition": {
                "includes_gym_service": True,
                "service_type": service_type,
                "service_label": service_label,
                "includes_subscription": True,  # Always includes free Fittbot
                "free_fittbot_duration_months": sub_duration_months,
                "items_count": 2,  # Service + Free Fittbot
            },
            "payment_summary": {
                "step_1_base_amounts": {
                    "service_base_minor": service_amount_minor,
                    "service_base_rupees": service_amount_minor / 100,
                    "subscription_base_minor": sub_total,
                    "subscription_base_rupees": sub_total / 100,
                    "total_base_minor": gross_before_rewards,
                    "total_base_rupees": gross_before_rewards / 100,
                },
                "step_2_reward_deduction": {
                    "reward_requested": payload.reward,
                    "reward_applied": reward_amount > 0,
                    "reward_amount_minor": reward_amount,
                    "reward_amount_rupees": reward_amount / 100,
                    "reward_source": "fittbot_cash",
                    "max_reward_cap_rupees": 100,
                },
                "step_3_final_amount": {
                    "final_amount_minor": grand_total,
                    "final_amount_rupees": grand_total / 100,
                    "amount_saved_minor": gross_before_rewards - grand_total,
                    "amount_saved_rupees": (gross_before_rewards - grand_total) / 100,
                },
            },
        }

        # 5) Create internal order + items
        order = Order(
            id=_new_id("ord_"),
            customer_id=user_id,
            provider="razorpay_pg",
            currency="INR",
            gross_amount_minor=grand_total,
            status=StatusOrder.pending,
            order_metadata=order_metadata,
        )
        db.add(order)
        await db.flush()

        logger.info(f"[GM_ORDER_CREATED] order_id={order.id}, grand_total={grand_total/100}rs")

        if is_personal_training:
            item_type = ItemType.pt_session
            metadata = {
                "plan_id": plan_id,
                "sessions": duration_value,
                "service_type": "personal_training",
                "amount": service_amount_minor / 100,  # Client paid (WITH platform markup)
                "base_amount": base_amount,  # Gym owner's base price (WITHOUT markup)
            }
        else:
            item_type = ItemType.gym_membership
            metadata = {
                "plan_id": plan_id,
                "duration_months": duration_value,
                "service_type": "gym_membership",
                "amount": service_amount_minor / 100,  # Client paid (WITH platform markup)
                "base_amount": base_amount,  # Gym owner's base price (WITHOUT markup)
            }

        service_item = OrderItem(
            id=_new_id("itm_"),
            order_id=order.id,
            item_type=item_type,
            gym_id=str(gym_id),
            unit_price_minor=service_amount_minor,
            qty=1,
            item_metadata=metadata,
        )
        db.add(service_item)

        # App subscription item - ALWAYS included (FREE Fittbot for all memberships)
        sub_item = OrderItem(
            id=_new_id("itm_"),
            order_id=order.id,
            item_type=ItemType.app_subscription,
            unit_price_minor=0,  # Always free
            qty=1,
            item_metadata={
                "plan_id": None,  # No specific plan - duration based
                "duration_days": sub_duration_months,  # Actually months, used as duration
                "duration_months": sub_duration_months,  # Explicit months field
                "provider": "internal_manual",
                "is_free_with_membership": True,  # Flag to indicate this is free with gym membership
                "is_existing": payload.is_existing,
                "existing_subscription_id": existing_subscription["id"] if existing_subscription else None,
                "existing_subscription_provider": existing_subscription["provider"] if existing_subscription else None,
                "existing_subscription_active_until": existing_subscription["active_until"].isoformat() if existing_subscription else None,
            },
        )
        db.add(sub_item)

        await db.flush()

        # 6) Create Razorpay order - include no-cost EMI offers when enabled
        offers = None

        if service_amount_minor >= 400000:
            no_cost = (
                await db.execute(select(NoCostEmi).where(NoCostEmi.gym_id == gym_id))
            ).scalars().first()
            
            if no_cost and no_cost.no_cost_emi:
     
                if settings.environment.lower() == "production":
     
                    offers = [
                        "offer_SNXYDGpMAJwXsK",
                        "offer_SNXXHNuKbnyQJQ",
                        "offer_SNXVCzmtF8FAOp",
                        "offer_SNXTlaiqz2J41t",
                        "offer_SNXSqBiKSFo6Ma",
                        "offer_SNXRuWFuYrBBRg",
                        "offer_SNXQptrhrlay7I",
                        "offer_SNXPMXUV4tgj4W",
                        "offer_SNXOIEtX83Y8Ir",
                        "offer_SNXNK5ESSB9mtg",
                        "offer_SNXMBVm0VaiSVz",
                        "offer_SNXKm07yTiOAjD",
                    ]
                else:

                    offers = ["offer_RqKnTuCiT9ji63", "offer_RqJnG19pLSCvhU"]


        pel.provider_call_started(command_id=order.id, provider_endpoint="create_order")
        _prov_start = time.perf_counter()
        try:
            rzp_order = await rzp_create_order(
                amount_minor=grand_total,
                currency="INR",
                receipt=order.id,
                notes={
                    "order_id": order.id,
                    "user_id": user_id,
                    "gym_id": gym_id,
                    "plan_id": plan_id,
                    "flow": f"unified_{service_type}_with_free_fittbot",
                    "service_type": service_type,
                    "free_fittbot_months": str(sub_duration_months),
                    "reward_applied": str(reward_amount),
                    "final_amount": str(grand_total),
                },
                offers=offers,
            )
            pel.provider_call_completed(command_id=order.id, provider_endpoint="create_order",
                                        duration_ms=int((time.perf_counter() - _prov_start) * 1000))
        except Exception as prov_exc:
            pel.provider_call_failed(command_id=order.id, provider_endpoint="create_order",
                                     error_code=type(prov_exc).__name__,
                                     duration_ms=int((time.perf_counter() - _prov_start) * 1000))
            raise

        order.provider_order_id = rzp_order["id"]
        db.add(order)
        await db.commit()

        logger.info(f"[GM_CHECKOUT_SUCCESS] order={order.id}, rzp_order={_mask(rzp_order['id'])}")

        response = {
            "razorpay_order_id": rzp_order["id"],
            "razorpay_key_id": settings.razorpay_key_id,
            "order_id": order.id,
            "amount_minor": grand_total,
            "currency": "INR",
            "service_amount": service_amount_minor,
            "service_type": service_type,
            "subscription_amount": 0,  # Always free
            "total_amount": grand_total,
            "reward_applied": reward_amount,
            "reward_enabled": payload.reward,
            "includes_subscription": True,  # Always includes free Fittbot
            "free_fittbot_duration_months": sub_duration_months,
            "subscription_is_existing": payload.is_existing,
            "display_title": f"{service_label} + Free {sub_duration_months}M Fittbot Subscription",
        }

        if existing_subscription:
            response["existing_subscription"] = {
                "id": existing_subscription["id"],
                "provider": existing_subscription["provider"],
                "active_until": existing_subscription["active_until"].isoformat(),
                "will_be_paused": True,
            }

        return response

    async def _verify_async(
        self,
        db: AsyncSession,
        payload_dict: Dict[str, Any],
        capture_marker: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Async Business Logic for Gym Membership Verification - preserves ALL legacy logic
        """
        settings = self.settings
        reward = payload_dict.get("reward")
        reward_amount = payload_dict.get("reward_applied") if reward else 0

        pid = payload_dict.get("razorpay_payment_id")
        oid = payload_dict.get("razorpay_order_id")
        sig = payload_dict.get("razorpay_signature")

        logger.info(f"[GM_VERIFY_START] payment_id={_mask_sensitive(pid)}, order_id={oid}")

        if not all([pid, oid, sig]):
            raise HTTPException(400, "Missing required fields")

        if not _verify_checkout_sig(settings.razorpay_key_secret, oid, pid, sig):
            pel.verify_signature_invalid(command_id=oid, razorpay_payment_id=pid)
            logger.error(f"[GM_VERIFY_ERROR] Invalid signature for order {oid}")
            raise HTTPException(403, "Invalid signature")

        # Async query for order
        order = (
            await db.execute(
                select(Order).where(Order.provider_order_id == oid)
            )
        ).scalars().first()
        if not order:
            logger.error(f"[GM_VERIFY_ERROR] Order not found: {oid}")
            raise HTTPException(404, "Order not found")

        logger.info(f"[GM_ORDER_FOUND] order_id={order.id}, customer_id={order.customer_id}")

        # Get payment data from capture_marker or Razorpay API
        if capture_marker:
            payment_data = capture_marker.copy()
            payment_data.setdefault("amount", capture_marker.get("amount"))
            payment_data.setdefault("currency", capture_marker.get("currency"))
            payment_data.setdefault("method", capture_marker.get("method"))
            payment_data.setdefault("order_id", capture_marker.get("order_id"))
            payment_data.setdefault("status", "captured")
        else:
            capture_marker = await self._await_capture_marker(pid)
            if capture_marker:
                payment_data = capture_marker.copy()
                payment_data.setdefault("amount", capture_marker.get("amount"))
                payment_data.setdefault("currency", capture_marker.get("currency"))
                payment_data.setdefault("method", capture_marker.get("method"))
                payment_data.setdefault("order_id", capture_marker.get("order_id"))
                payment_data.setdefault("status", "captured")
            else:
                # Razorpay API call - async
                logger.info(
                    "[GM_VERIFY_PROVIDER_FALLBACK]",
                    extra={
                        "payment_id": _mask_sensitive(pid),
                        "order_id": oid,
                        "redis_prefix": self.config.redis_prefix,
                    },
                )
                pel.provider_call_started(command_id=oid, provider_endpoint="get_payment")
                _prov_start = time.perf_counter()
                try:
                    payment_data = await rzp_get_payment(pid)
                    pel.provider_call_completed(command_id=oid, provider_endpoint="get_payment",
                                                duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                except Exception as prov_exc:
                    pel.provider_call_failed(command_id=oid, provider_endpoint="get_payment",
                                             error_code=type(prov_exc).__name__,
                                             duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                    raise

        if payment_data.get("status") != "captured":
            logger.error(f"[GM_VERIFY_ERROR] Payment not captured: {payment_data.get('status')}")
            raise HTTPException(400, f"Payment not captured (status={payment_data.get('status')})")

        # Check for existing payment (idempotency) - async query
        existing_payment = (
            await db.execute(
                select(Payment).where(
                    Payment.provider_payment_id == pid,
                    Payment.status == StatusPayment.captured,
                )
            )
        ).scalars().first()

        if existing_payment:
            logger.info(f"[GM_EXISTING_PAYMENT] Payment {pid} already exists, checking if service was activated...")

            # Check if service/subscription was actually activated - async query
            items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            service_ok = False
            sub_ok = False
            service_details = None
            sub_details = None

            for it in items:
                if it.item_type in (ItemType.gym_membership, ItemType.pt_session):
                    # Check if entitlement exists - async query
                    existing_ent = (
                        await db.execute(
                            select(Entitlement).where(Entitlement.order_item_id == it.id)
                        )
                    ).scalars().first()

                    if existing_ent:
                        logger.info(f"[GM_EXISTING_SERVICE] Entitlement {existing_ent.id} already exists")
                        service_ok = True
                        service_details = {
                            "entitlement_id": existing_ent.id,
                            "status": "active",
                        }
                    else:
                        # Service payment exists but service not activated - CREATE IT NOW!
                        logger.warning(f"[GM_MISSING_SERVICE] Payment exists but service missing! Creating now for order {order.id}")

                        # Deduct reward if applicable
                        if reward and reward_amount:
                            await _deduct_reward_async(db, order.customer_id, reward_amount)

                        if it.item_type == ItemType.gym_membership:
                            service_details = await _process_gym_membership_item_async(db, it, order, existing_payment)
                            source_type = "gym_membership"
                        else:
                            service_details = await _process_personal_training_item_async(db, it, order, existing_payment)
                            source_type = "personal_training"

                        # Create FittbotPayment for recovery case
                        entitlement_id = service_details.get("entitlement_id")
                        if entitlement_id:
                            gym_id = int(it.gym_id) if it.gym_id else 0
                            client_id = int(order.customer_id)
                            client_paid = it.item_metadata.get("amount", it.unit_price_minor / 100) if it.item_metadata else it.unit_price_minor / 100
                            base_amount_from_metadata = it.item_metadata.get("base_amount") if it.item_metadata else None
                            # Fallback: divide by markup if base_amount not in metadata (for old payments)
                            gym_owner_gets = base_amount_from_metadata if base_amount_from_metadata else (client_paid / get_markup_multiplier())

                            # Detect no-cost EMI from Razorpay: method=emi + offer_id present
                            recovery_method = payment_data.get("method", "")
                            recovery_offer_id = payment_data.get("offer_id")
                            recovery_is_nce = (recovery_method == "emi" and recovery_offer_id is not None)

                            fittbot_payment = FittbotPayment(
                                gym_id=gym_id,
                                client_id=client_id,
                                entitlement_id=entitlement_id,
                                source_type=source_type,
                                amount_gross=client_paid,      # What client paid (WITH platform markup)
                                amount_net=gym_owner_gets,     # What gym owner gets (WITHOUT markup)
                                currency="INR",
                                gateway="razorpay",
                                gateway_payment_id=pid,
                                payment_method=recovery_method or None,
                                is_no_cost_emi=recovery_is_nce,
                                status="paid",
                                paid_at=datetime.now(UTC),
                            )
                            db.add(fittbot_payment)
                            logger.info(f"[FITTBOT_PAYMENTS_RECOVERY_CREATED] source_type={source_type}, entitlement_id={entitlement_id}, method={recovery_method}, is_no_cost_emi={recovery_is_nce}")

                        await db.commit()
                        logger.info(f"[GM_CREATED_MISSING_SERVICE] Created service: {service_details.get('entitlement_id')}")
                        service_ok = True

                elif it.item_type == ItemType.app_subscription:
                    # Check if subscription exists - async query
                    existing_sub = (
                        await db.execute(
                            select(Subscription).where(Subscription.latest_txn_id == pid)
                        )
                    ).scalars().first()

                    if existing_sub:
                        logger.info(f"[GM_EXISTING_SUBSCRIPTION] Subscription {existing_sub.id} already exists")
                        sub_ok = True
                        sub_details = {"subscription_id": existing_sub.id}
                    else:
                        logger.warning(f"[GM_MISSING_SUBSCRIPTION] Creating subscription for order {order.id}")

                        # Deduct reward if applicable (only once)
                        if reward and reward_amount and not service_ok:
                            await _deduct_reward_async(db, order.customer_id, reward_amount)

                        sub_details = await _process_app_subscription_activation_async(
                            db, it, order.customer_id, pid
                        )
                        await db.commit()
                        logger.info(f"[GM_CREATED_MISSING_SUBSCRIPTION] Created subscription")
                        sub_ok = True

            logger.info(f"[GM_EXISTING_PAYMENT_COMPLETE] service_ok={service_ok}, sub_ok={sub_ok}")

            response = GymMembershipVerificationResponse(
                verified=True,
                captured=True,
                order_id=order.id,
                payment_id=pid,
                service_activated=service_ok,
                service_details=service_details,
                subscription_activated=sub_ok,
                subscription_details=sub_details,
                total_amount=order.gross_amount_minor,
                currency="INR",
                message="Payment already processed",
            )
            return response.dict()

        # New payment - process everything
        try:
            # Early idempotency check: if Entitlement already exists for any order item,
            # the payment was already fully processed (by a retry or webhook).
            # Return immediately without creating duplicate Payment/FittbotPayment/reward.
            early_items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            for ei in early_items:
                if ei.item_type in (ItemType.gym_membership, ItemType.pt_session):
                    existing_ent_early = (
                        await db.execute(
                            select(Entitlement).where(Entitlement.order_item_id == ei.id)
                        )
                    ).scalars().first()
                    if existing_ent_early:
                        logger.info(
                            f"[GM_ALREADY_FULFILLED] Entitlement {existing_ent_early.id} already exists "
                            f"for item {ei.id}, returning early"
                        )
                        await self._invalidate_home_booking_cache(gym_ids)
                        return GymMembershipVerificationResponse(
                            verified=True,
                            captured=True,
                            order_id=order.id,
                            payment_id=pid,
                            service_activated=True,
                            service_details={"entitlement_id": existing_ent_early.id, "status": "active"},
                            subscription_activated=False,
                            subscription_details=None,
                            total_amount=order.gross_amount_minor,
                            currency="INR",
                            message="Payment already processed",
                        ).dict()

            # Create payment record
            pay = Payment(
                id=_new_id("pay_"),
                order_id=order.id,
                customer_id=order.customer_id,
                provider="razorpay_pg",
                provider_payment_id=pid,
                amount_minor=int(payment_data.get("amount") or order.gross_amount_minor),
                currency=payment_data.get("currency", "INR"),
                status=StatusPayment.captured,
                captured_at=datetime.now(UTC),
                payment_metadata={
                    "method": payment_data.get("method"),
                    "source": "unified_gym_verify",
                    "razorpay_order_id": oid,
                },
            )
            db.add(pay)
            order.status = StatusOrder.paid
            db.add(order)

            logger.info(f"[GM_PAYMENT_RECORDED] payment_id={pay.id}, amount={pay.amount_minor/100}rs")

            # ═══════════════════════════════════════════════════════════════════
            # Create fittbot_payments.Payment record right after payments.Payment
            # - entitlement_id = order.id (used in owner.py to lookup and create Payout)
            # ═══════════════════════════════════════════════════════════════════
            # Determine source_type from order items
            order_items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            source_type = "gym_membership"  # default
            gym_id = 0
            base_amount_from_metadata = None
            for oi in order_items:
                if oi.item_type == ItemType.pt_session:
                    source_type = "personal_training"
                if oi.gym_id:
                    gym_id = int(oi.gym_id)
                # Extract base_amount from metadata
                if oi.item_metadata:
                    base_amount_from_metadata = oi.item_metadata.get("base_amount")
                break  # Use first item for source_type

            client_paid = pay.amount_minor / 100
            # Fallback: divide by markup if base_amount not in metadata (for old payments)
            gym_owner_gets = base_amount_from_metadata if base_amount_from_metadata else (client_paid / get_markup_multiplier())

            # Detect no-cost EMI from Razorpay payment data:
            # - method="emi" means client chose EMI at checkout
            # - offer_id != null means a no-cost EMI offer was applied
            #   (Razorpay sets this when our offer IDs match at payment time)
            # - Regular EMI has offer_id=null (client bears interest themselves)
            rp_method = payment_data.get("method", "")
            rp_offer_id = payment_data.get("offer_id")
            is_no_cost_emi = (rp_method == "emi" and rp_offer_id is not None)

            if is_no_cost_emi:
                logger.info(
                    f"[GM_NO_COST_EMI_DETECTED] gym={gym_id}, offer_id={rp_offer_id}, "
                    f"order_amount={order.gross_amount_minor}, payment_amount={pay.amount_minor}"
                )

            fittbot_payment = FittbotPayment(
                gym_id=gym_id,
                client_id=int(order.customer_id),
                entitlement_id=order.id,  # Using order.id as entitlement_id
                source_type=source_type,
                amount_gross=client_paid,      # What client paid (WITH platform markup)
                amount_net=gym_owner_gets,     # What gym owner gets (WITHOUT markup)
                currency="INR",
                gateway="razorpay",
                gateway_payment_id=pid,
                payment_method=rp_method or None,  # Actual Razorpay method: card/upi/emi/netbanking
                is_no_cost_emi=is_no_cost_emi,     # True when offer_id present + method=emi
                status="paid",
                paid_at=datetime.now(UTC),
            )
            db.add(fittbot_payment)
            logger.info(f"[FITTBOT_PAYMENTS_CREATED] entitlement_id={order.id}, gym_id={gym_id}, client_paid={client_paid}, gym_owner_gets={gym_owner_gets}, source_type={source_type}, method={rp_method}, is_no_cost_emi={is_no_cost_emi}")

            # Deduct reward if applicable
            if reward and reward_amount:
                await _deduct_reward_async(db, order.customer_id, reward_amount)

            # Process each order item - async query
            service_details = None
            sub_details = None
            service_ok = False
            sub_ok = False

            items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            logger.info(f"[GM_ITEMS] Found {len(items)} order items for order {order.id}")

            gym_ids: Set[int] = set()
            for it in items:
                try:
                    if it.gym_id is not None:
                        gym_ids.add(int(it.gym_id))
                except Exception:
                    logger.warning("GM_GYM_ID_PARSE_FAILED", extra={"gym_id": it.gym_id, "item_id": it.id})

                logger.info(f"[GM_ITEM] Processing item {it.id}, type={it.item_type}")

                if it.item_type == ItemType.gym_membership:
                    logger.info(f"[GM_MEMBERSHIP] Activating gym membership for item {it.id}")
                    service_details = await _process_gym_membership_item_async(db, it, order, pay)
                    service_ok = True
                    logger.info(f"[GM_MEMBERSHIP_DONE] Created entitlement: {service_details.get('entitlement_id')}")

                    # Grant nutrition eligibility for online gym membership (3+ months)
                    try:
                        duration_months = int(it.item_metadata.get("duration_months", 0)) if it.item_metadata else 0
                        if duration_months >= 1:
                            await grant_nutrition_eligibility_async(
                                db=db,
                                client_id=int(order.customer_id),
                                source_type="gym_membership",
                                source_id=service_details.get("entitlement_id", order.id),
                                plan_name=f"Gym Membership {duration_months}M",
                                duration_months=duration_months,
                                gym_id=int(it.gym_id) if it.gym_id else None,
                            )
                            pel.side_effect_success(command_id=oid, side_effect="nutrition",
                                                    client_id=str(order.customer_id))
                        else:
                            pel.side_effect_skipped(command_id=oid, side_effect="nutrition",
                                                    reason="duration_lt_3m", client_id=str(order.customer_id))
                    except Exception as nutr_exc:
                        pel.side_effect_failed(command_id=oid, side_effect="nutrition",
                                               error_detail=str(nutr_exc), client_id=str(order.customer_id))
                        logger.warning(f"[GM_NUTRITION_ELIGIBILITY_ERROR] {nutr_exc}")

                    # Reward program entry for gym membership
                    try:
                        reward_ok, entries_added, reward_msg = await add_gym_membership_entry(
                            db=db,
                            client_id=int(order.customer_id),
                            duration_months=duration_months,
                            source_id=service_details.get("entitlement_id", order.id),
                        )
                        if reward_ok:
                            logger.info(f"[REWARD_ENTRY_ADDED] Gym membership reward entry: {entries_added} entries for {duration_months}mo, client_id={order.customer_id}")
                        else:
                            logger.info(f"[REWARD_ENTRY_SKIPPED] {reward_msg}, client_id={order.customer_id}")
                    except Exception as reward_exc:
                        logger.warning(f"[REWARD_ENTRY_FAILED] Gym membership reward entry error: {reward_exc}")

                elif it.item_type == ItemType.pt_session:
                    logger.info(f"[GM_PT] Activating personal training for item {it.id}")
                    service_details = await _process_personal_training_item_async(db, it, order, pay)
                    service_ok = True
                    logger.info(f"[GM_PT_DONE] Created entitlement: {service_details.get('entitlement_id')}")

                    # Grant nutrition eligibility for online personal training (3+ months)
                    try:
                        sessions_count = int(it.item_metadata.get("sessions", 0)) if it.item_metadata else 0
                        duration_months = max(1, sessions_count // 4)
                        if duration_months >= 1:
                            await grant_nutrition_eligibility_async(
                                db=db,
                                client_id=int(order.customer_id),
                                source_type="personal_training",
                                source_id=service_details.get("entitlement_id", order.id),
                                plan_name=f"Personal Training {sessions_count} Sessions",
                                duration_months=duration_months,
                                gym_id=int(it.gym_id) if it.gym_id else None,
                            )
                            pel.side_effect_success(command_id=oid, side_effect="nutrition",
                                                    client_id=str(order.customer_id))
                        else:
                            pel.side_effect_skipped(command_id=oid, side_effect="nutrition",
                                                    reason="pt_duration_lt_3m", client_id=str(order.customer_id))
                    except Exception as nutr_exc:
                        pel.side_effect_failed(command_id=oid, side_effect="nutrition",
                                               error_detail=str(nutr_exc), client_id=str(order.customer_id))
                        logger.warning(f"[GM_NUTRITION_ELIGIBILITY_ERROR] {nutr_exc}")

                elif it.item_type == ItemType.app_subscription:
                    logger.info(f"[GM_SUBSCRIPTION] Activating app subscription for item {it.id}")
                    sub_details = await _process_app_subscription_activation_async(db, it, order.customer_id, pid)
                    sub_ok = True
                    logger.info(f"[GM_SUBSCRIPTION_DONE] Created subscription: {sub_details.get('subscription_id')}")

            logger.info(f"[GM_COMMITTING] Committing transaction for order {order.id}")
            try:
                await db.commit()
            except IntegrityError as e:
                # Race condition: another request already created the records
                if "Duplicate entry" in str(e):
                    await db.rollback()
                    logger.info(f"[GM_DUPLICATE_DETECTED] Duplicate entry detected for order {order.id}, returning already_processed")
                    return GymMembershipVerificationResponse(
                        verified=True,
                        captured=True,
                        order_id=order.id,
                        payment_id=pid,
                        service_activated=service_ok,
                        service_details=service_details,
                        subscription_activated=sub_ok,
                        subscription_details=sub_details,
                        total_amount=order.gross_amount_minor,
                        currency="INR",
                        message="Payment already processed",
                    ).dict()
                await db.rollback()
                raise

            logger.info(f"[GM_COMMIT_SUCCESS] Transaction committed successfully")
            await self._invalidate_home_booking_cache(gym_ids)

            # Queue owner notification (fire-and-forget, never blocks payment flow)
            try:
                for it in items:
                    if it.item_type in (ItemType.gym_membership, ItemType.pt_session):
                        queue_membership_notification(
                            gym_id=int(it.gym_id) if it.gym_id else 0,
                            client_id=int(order.customer_id),
                            amount=pay.amount_minor / 100,
                            duration_months=it.item_metadata.get("duration_months", 1) if it.item_metadata else 1,
                            is_personal_training=(it.item_type == ItemType.pt_session),
                        )
                        break  # Only send one notification per order
            except Exception as e:
                logger.warning(f"[GM_NOTIFICATION_ERROR] Failed to queue owner notification: {e}")

            response = GymMembershipVerificationResponse(
                verified=True,
                captured=True,
                order_id=order.id,
                payment_id=pid,
                service_activated=service_ok,
                service_details=service_details,
                subscription_activated=sub_ok,
                subscription_details=sub_details,
                total_amount=order.gross_amount_minor,
                currency="INR",
                message="Payment verified and services activated",
            )

            return response.dict()

        except Exception:
            await db.rollback()
            raise

    async def _capture_marker_snapshot(self, payment_id: str) -> Optional[Dict[str, Any]]:
        """Get payment capture data from webhook cache"""
        if not self.redis or not payment_id:
            return None
        key = f"{self.config.redis_prefix}:capture:{payment_id}"
        raw = await asyncio.to_thread(self.redis.get, key)
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            return None
        return data

    async def _await_capture_marker(self, payment_id: str) -> Optional[Dict[str, Any]]:
        """Poll Redis briefly for a capture marker before hitting Razorpay."""
        delay = max(0.2, self.config.verify_db_poll_base_delay_ms / 1000)
        max_delay = max(delay, self.config.verify_db_poll_max_delay_ms / 1000)
        deadline = time.monotonic() + max(1, self.config.verify_db_poll_total_timeout_seconds)
        attempts = max(1, self.config.verify_db_poll_attempts)

        for attempt in range(1, attempts + 1):
            marker = await self._capture_marker_snapshot(payment_id)
            if marker:
                logger.info(
                    "[GM_VERIFY_CAPTURE_CACHE_HIT]",
                    extra={
                        "payment_id": _mask_sensitive(payment_id),
                        "attempt": attempt,
                        "redis_prefix": self.config.redis_prefix,
                    },
                )
                return marker
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 1.5)
        return None

    async def fulfill_from_webhook(
        self,
        razorpay_order_id: str,
        payment_id: str,
        payment_data: Dict[str, Any],
    ) -> None:
        """Full webhook fulfillment — mirrors _verify_async business logic.

        Called from _persist_webhook on payment.captured events.
        Creates Payment, FittbotPayment, Entitlement, PayoutLine,
        Subscription, gym_fees, FittbotGymMembership, reward deduction,
        nutrition eligibility, cache invalidation, and owner notification.
        """
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            try:
                await self._fulfill_from_webhook_async(
                    db, razorpay_order_id, payment_id, payment_data
                )
            except IntegrityError as e:
                await db.rollback()
                if "Duplicate entry" in str(e):
                    logger.info(
                        "GM_WEBHOOK_FULFILL_ALREADY_DONE",
                        extra={
                            "razorpay_order_id": razorpay_order_id,
                            "payment_id": _mask_sensitive(payment_id),
                        },
                    )
                    return
                raise
            except Exception:
                await db.rollback()
                raise

    async def _fulfill_from_webhook_async(
        self,
        db: AsyncSession,
        razorpay_order_id: str,
        payment_id: str,
        payment_data: Dict[str, Any],
    ) -> None:
        """Inner webhook fulfillment with full business logic."""
        # 1) Find order
        order = (
            await db.execute(
                select(Order).where(Order.provider_order_id == razorpay_order_id)
            )
        ).scalars().first()
        if not order:
            logger.warning(
                "GM_WEBHOOK_FULFILL_ORDER_NOT_FOUND",
                extra={"razorpay_order_id": razorpay_order_id},
            )
            return

        # 2) Idempotency — if Payment already exists, skip
        existing_payment = (
            await db.execute(
                select(Payment).where(
                    Payment.provider_payment_id == payment_id,
                    Payment.status == StatusPayment.captured,
                )
            )
        ).scalars().first()
        if existing_payment:
            logger.info(
                "GM_WEBHOOK_FULFILL_ALREADY_PROCESSED",
                extra={
                    "razorpay_order_id": razorpay_order_id,
                    "payment_id": _mask_sensitive(payment_id),
                    "existing_payment_id": existing_payment.id,
                },
            )
            return

        # 2b) Early idempotency — if Entitlement already exists, skip entirely
        webhook_items = (
            await db.execute(
                select(OrderItem).where(OrderItem.order_id == order.id)
            )
        ).scalars().all()
        for wi in webhook_items:
            if wi.item_type in (ItemType.gym_membership, ItemType.pt_session):
                existing_ent_wh = (
                    await db.execute(
                        select(Entitlement).where(Entitlement.order_item_id == wi.id)
                    )
                ).scalars().first()
                if existing_ent_wh:
                    logger.info(
                        f"[GM_WEBHOOK_ALREADY_FULFILLED] Entitlement {existing_ent_wh.id} already exists "
                        f"for item {wi.id}, returning early"
                    )
                    return

        # 3) Create Payment record
        pay = Payment(
            id=_new_id("pay_"),
            order_id=order.id,
            customer_id=order.customer_id,
            provider="razorpay_pg",
            provider_payment_id=payment_id,
            amount_minor=int(payment_data.get("amount") or order.gross_amount_minor),
            currency=payment_data.get("currency", "INR"),
            status=StatusPayment.captured,
            captured_at=datetime.now(UTC),
            payment_metadata={
                "method": payment_data.get("method"),
                "source": "webhook_fulfillment_gym_membership",
                "razorpay_order_id": razorpay_order_id,
            },
        )
        db.add(pay)
        order.status = StatusOrder.paid
        db.add(order)

        logger.info(
            "GM_WEBHOOK_FULFILL_PAYMENT_CREATED",
            extra={
                "payment_id": _mask_sensitive(payment_id),
                "order_id": order.id,
                "amount": pay.amount_minor,
            },
        )

        # 4) Get order items
        items = (
            await db.execute(
                select(OrderItem).where(OrderItem.order_id == order.id)
            )
        ).scalars().all()

        # 5) Create FittbotPayment
        source_type = "gym_membership"
        gym_id = 0
        base_amount_from_metadata = None
        for oi in items:
            if oi.item_type == ItemType.pt_session:
                source_type = "personal_training"
            if oi.gym_id:
                gym_id = int(oi.gym_id)
            if oi.item_metadata:
                base_amount_from_metadata = oi.item_metadata.get("base_amount")
            break

        client_paid = pay.amount_minor / 100
        gym_owner_gets = base_amount_from_metadata if base_amount_from_metadata else (client_paid / get_markup_multiplier())

        rp_method = payment_data.get("method", "")
        rp_offer_id = payment_data.get("offer_id")
        is_no_cost_emi = (rp_method == "emi" and rp_offer_id is not None)

        fittbot_payment = FittbotPayment(
            gym_id=gym_id,
            client_id=int(order.customer_id),
            entitlement_id=order.id,
            source_type=source_type,
            amount_gross=client_paid,
            amount_net=gym_owner_gets,
            currency="INR",
            gateway="razorpay",
            gateway_payment_id=payment_id,
            payment_method=rp_method or None,
            is_no_cost_emi=is_no_cost_emi,
            status="paid",
            paid_at=datetime.now(UTC),
        )
        db.add(fittbot_payment)

        # 6) Deduct reward from order_metadata
        reward_meta = (order.order_metadata or {}).get("payment_summary", {}).get(
            "step_2_reward_deduction", {}
        )
        if reward_meta.get("reward_applied") and reward_meta.get("reward_amount_minor"):
            reward_amount = int(reward_meta["reward_amount_minor"])
            await _deduct_reward_async(db, order.customer_id, reward_amount)

        # 7) Process each order item
        gym_ids: Set[int] = set()
        service_item_for_notification = None

        for it in items:
            try:
                if it.gym_id is not None:
                    gym_ids.add(int(it.gym_id))
            except Exception:
                pass

            if it.item_type == ItemType.gym_membership:
                await _process_gym_membership_item_async(db, it, order, pay)
                service_item_for_notification = it
                logger.info(
                    "GM_WEBHOOK_FULFILL_MEMBERSHIP_CREATED",
                    extra={"order_id": order.id, "item_id": it.id},
                )

                # Nutrition eligibility
                try:
                    duration_months = int(it.item_metadata.get("duration_months", 0)) if it.item_metadata else 0
                    if duration_months >= 1:
                        await grant_nutrition_eligibility_async(
                            db=db,
                            client_id=int(order.customer_id),
                            source_type="gym_membership",
                            source_id=order.id,
                            plan_name=f"Gym Membership {duration_months}M",
                            duration_months=duration_months,
                            gym_id=int(it.gym_id) if it.gym_id else None,
                        )
                except Exception as nutr_exc:
                    logger.warning(f"[GM_WEBHOOK_NUTRITION_ERROR] {nutr_exc}")

                # Reward program entry for gym membership
                try:
                    reward_ok, entries_added, reward_msg = await add_gym_membership_entry(
                        db=db,
                        client_id=int(order.customer_id),
                        duration_months=duration_months,
                        source_id=order.id,
                    )
                    if reward_ok:
                        logger.info(f"[REWARD_ENTRY_ADDED] Webhook gym membership reward: {entries_added} entries for {duration_months}mo, client_id={order.customer_id}")
                    else:
                        logger.info(f"[REWARD_ENTRY_SKIPPED] {reward_msg}, client_id={order.customer_id}")
                except Exception as reward_exc:
                    logger.warning(f"[REWARD_ENTRY_FAILED] Webhook gym membership reward error: {reward_exc}")

            elif it.item_type == ItemType.pt_session:
                await _process_personal_training_item_async(db, it, order, pay)
                service_item_for_notification = it
                logger.info(
                    "GM_WEBHOOK_FULFILL_PT_CREATED",
                    extra={"order_id": order.id, "item_id": it.id},
                )

                # Nutrition eligibility
                try:
                    sessions_count = int(it.item_metadata.get("sessions", 0)) if it.item_metadata else 0
                    duration_months = max(1, sessions_count // 4)
                    if duration_months >= 1:
                        await grant_nutrition_eligibility_async(
                            db=db,
                            client_id=int(order.customer_id),
                            source_type="personal_training",
                            source_id=order.id,
                            plan_name=f"Personal Training {sessions_count} Sessions",
                            duration_months=duration_months,
                            gym_id=int(it.gym_id) if it.gym_id else None,
                        )
                except Exception as nutr_exc:
                    logger.warning(f"[GM_WEBHOOK_NUTRITION_ERROR] {nutr_exc}")

            elif it.item_type == ItemType.app_subscription:
                await _process_app_subscription_activation_async(
                    db, it, order.customer_id, payment_id
                )
                logger.info(
                    "GM_WEBHOOK_FULFILL_SUBSCRIPTION_CREATED",
                    extra={"order_id": order.id, "item_id": it.id},
                )

        # 8) Commit
        await db.commit()

        logger.info(
            "GM_WEBHOOK_FULFILL_SUCCESS",
            extra={
                "order_id": order.id,
                "razorpay_order_id": razorpay_order_id,
                "payment_id": _mask_sensitive(payment_id),
                "source_type": source_type,
                "is_no_cost_emi": is_no_cost_emi,
            },
        )

        # 9) Post-commit side effects (best-effort)
        await self._invalidate_home_booking_cache(gym_ids)

        # Owner notification
        try:
            if service_item_for_notification:
                it = service_item_for_notification
                queue_membership_notification(
                    gym_id=int(it.gym_id) if it.gym_id else 0,
                    client_id=int(order.customer_id),
                    amount=pay.amount_minor / 100,
                    duration_months=it.item_metadata.get("duration_months", 1) if it.item_metadata else 1,
                    is_personal_training=(it.item_type == ItemType.pt_session),
                )
        except Exception as e:
            logger.warning(f"[GM_WEBHOOK_NOTIFICATION_ERROR] {e}")

    async def process_webhook(self, command_id: str, store) -> None:
        """Process Razorpay webhook for gym membership payments."""
        record = await store.mark_processing(command_id)
        payload = record.payload
        try:
            await self._persist_webhook(payload)
        except Exception as exc:
            logger.exception("Gym membership webhook failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        await store.mark_completed(
            command_id,
            {"event": payload.get("event"), "webhook_id": payload.get("webhook_id")},
        )

    async def _persist_webhook(self, body: Dict) -> None:
        raw = body.get("raw_body")
        signature = body.get("signature")
        if raw is None or signature is None:
            raise ValueError("webhook_signature_missing")
        raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")

        with self._session_scope() as session:
            await process_razorpay_webhook_payload(raw_bytes, signature, session)
        await self._record_capture_marker(body)

        # For payment.captured events, do full fulfillment
        if body.get("event") == "payment.captured":
            await self._try_webhook_fulfillment(body)

    async def _try_webhook_fulfillment(self, body: Dict) -> None:
        """Best-effort full fulfillment from webhook payload."""
        try:
            pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
            razorpay_order_id = pay_entity.get("order_id")
            payment_id = pay_entity.get("id")
            if not razorpay_order_id or not payment_id:
                return

            logger.info(
                "GM_WEBHOOK_FULFILLMENT_TRIGGERED",
                extra={
                    "razorpay_order_id": razorpay_order_id,
                    "payment_id": f"****{payment_id[-4:]}" if len(payment_id) > 4 else payment_id,
                },
            )

            payment_data = {
                "amount": pay_entity.get("amount"),
                "currency": pay_entity.get("currency"),
                "method": pay_entity.get("method"),
                "offer_id": pay_entity.get("offer_id"),
                "status": "captured",
            }

            await self.fulfill_from_webhook(razorpay_order_id, payment_id, payment_data)

        except Exception:
            logger.exception(
                "GM_WEBHOOK_FULFILLMENT_ERROR",
                extra={
                    "razorpay_order_id": body.get("payload", {}).get("payment", {}).get("entity", {}).get("order_id"),
                },
            )

    @contextmanager
    def _session_scope(self):
        with self.payment_db.get_session() as session:
            yield session

    async def _record_capture_marker(self, body: Dict[str, Any]) -> None:
        """Store capture marker in Redis for faster verify."""
        if not self.redis:
            return
        if body.get("event") != "payment.captured":
            return
        pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id = pay_entity.get("id")
        if not payment_id:
            return
        marker = {
            "amount": pay_entity.get("amount"),
            "currency": pay_entity.get("currency"),
            "method": pay_entity.get("method"),
            "order_id": pay_entity.get("order_id"),
            "captured_at": pay_entity.get("created_at") or int(time.time()),
        }
        key = f"{self.config.redis_prefix}:capture:{payment_id}"
        try:
            await asyncio.to_thread(
                self.redis.set,
                key,
                json.dumps(marker),
                ex=self.config.verify_capture_cache_ttl_seconds,
            )
            masked_payment = f"****{payment_id[-4:]}" if isinstance(payment_id, str) and len(payment_id) > 4 else payment_id
            logger.info(
                "GM_WEBHOOK_CAPTURE_CACHE_SET",
                extra={
                    "payment_id": masked_payment,
                    "order_id": marker.get("order_id"),
                    "redis_prefix": self.config.redis_prefix,
                    "ttl_seconds": self.config.verify_capture_cache_ttl_seconds,
                },
            )
        except Exception:
            logger.exception("Failed to set capture cache marker", extra={"payment_id": payment_id})
