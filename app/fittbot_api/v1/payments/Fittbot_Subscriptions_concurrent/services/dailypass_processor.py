import asyncio
import json
import logging
import time
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Optional, Set

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.fittbot_api.v1.payments.dailypass import routes as dailypass_routes
from app.fittbot_api.v1.payments.razorpay_async_gateway import (
    create_order as rzp_create_order,
    get_payment as rzp_get_payment,
    get_order as rzp_get_order,
)
from app.fittbot_api.v1.payments.models.enums import (
    ItemType,
    StatusOrder,
    StatusPayment,
    SubscriptionStatus,
    StatusEnt,
    EntType,
)
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.entitlements import Entitlement
from app.fittbot_api.v1.payments.services.entitlement_service import EntitlementService
from app.models.fittbot_models import ReferralFittbotCash
from app.models.fittbot_plans_model import FittbotPlan
from app.models.dailypass_models import (
    DailyPass,
    DailyPassDay,
    DailyPassAudit,
    DailyPassPricing,
    LedgerAllocation,
)
from app.models.fittbot_payments_models import Payment as FittbotPayment
from app.models.async_database import create_celery_async_sessionmaker
from app.fittbot_api.v1.client.client_api.reward_program.reward_service import (
    add_dailypass_entry,
    add_subscription_entry,
)
from app.models.fittbot_models import NewOffer, Gym
from app.tasks.notification_tasks import queue_dailypass_notification
from sqlalchemy import func

from ...config.database import PaymentDatabase
from ...config.settings import get_payment_settings
from redis import Redis
from app.config.pricing import get_markup_multiplier
from .payment_event_logger import PaymentEventLogger

logger = logging.getLogger("payments.dailypass.v2.processor")
pel = PaymentEventLogger("razorpay", "dailypass")

UnifiedCheckoutRequest = dailypass_routes.UnifiedCheckoutRequest
UnifiedCheckoutResponse = dailypass_routes.UnifiedCheckoutResponse
UnifiedVerificationRequest = dailypass_routes.UnifiedVerificationRequest
UnifiedVerificationResponse = dailypass_routes.UnifiedVerificationResponse
_validate_date_range = dailypass_routes._validate_date_range
_new_id = dailypass_routes._new_id
_verify_checkout_signature = dailypass_routes._verify_checkout_signature
_mask_sensitive = dailypass_routes._mask



async def _get_price_for_gym_async(db: AsyncSession, gym_id: int, is_offer_eligible: bool = False) -> int:

    if is_offer_eligible:
        logger.info(
            "DAILYPASS_OFFER_APPLIED",
            extra={
                "gym_id": gym_id,
                "offer_price_paise": 4900,
                "offer_price_rupees": 49,
            }
        )
        return 4900  # Return ₹49 in paise without markup

    rec = (
        await db.execute(
            select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
        )
    ).scalars().first()
    if not rec:
        raise ValueError("daily pass price not configured for gym")
    price_paise = int(rec.discount_price)

    logger.warning(
        f"[PRICING_DEBUG] gym_id={gym_id}, is_offer_eligible={is_offer_eligible}, "
        f"discount_price={price_paise} paise (₹{price_paise/100})"
    )

    # Skip 30% markup if discount_price is exactly 4900 paisa (49 rupees) - gym-configured offer
    if price_paise == 4900:
        logger.warning(f"[PRICING_DEBUG] Gym has ₹49 configured, no markup applied")
        return 4900  # Return 49 rupees in paise without markup
    # Convert paise to rupees, apply 30% markup, round to nearest rupee
    price_rupees = price_paise / 100
    price_with_markup_rupees = round(price_rupees * get_markup_multiplier())
    price_with_markup_paise = int(price_with_markup_rupees * 100)

    logger.warning(
        f"[PRICING_DEBUG] base=₹{price_rupees}, with_markup=₹{price_with_markup_rupees}, "
        f"final={price_with_markup_paise} paise"
    )

    return price_with_markup_paise


async def _get_actual_price_for_gym_async(db: AsyncSession, gym_id: int, is_offer_eligible: bool = False) -> int:

    # If client is eligible for offer, force ₹49 pricing (what gym owner gets)
    if is_offer_eligible:
        return 4900  # Return ₹49 in paise

    rec = (
        await db.execute(
            select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
        )
    ).scalars().first()
    if not rec:
        raise ValueError("daily pass price not configured for gym")
    return int(rec.discount_price)


async def _check_dailypass_offer_eligibility(db: AsyncSession, client_id: int, gym_id: int) -> bool:

    try:
       
        user_dp_count_stmt = (
            select(func.count())
            .select_from(DailyPassDay)
            .join(DailyPass, DailyPass.id == DailyPassDay.pass_id)
            .where(
                DailyPass.client_id == str(client_id),
                DailyPass.status != "canceled",
            )
        )
        user_dp_result = await db.execute(user_dp_count_stmt)
        user_dp_count = user_dp_result.scalar() or 0

        if user_dp_count >= 3:
            logger.info(
                "DAILYPASS_OFFER_INELIGIBLE_USER_LIMIT",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "user_booking_count": user_dp_count,
                    "reason": "User has >= 3 daily pass bookings"
                }
            )
            return False

        # 2. Check gym has dailypass feature enabled (matches gym_studios.py line 1053)
        gym_stmt = select(Gym).where(Gym.gym_id == gym_id)
        gym_result = await db.execute(gym_stmt)
        gym = gym_result.scalars().first()

        if not gym or not gym.dailypass:
            logger.info(
                "DAILYPASS_OFFER_INELIGIBLE_GYM_NO_FEATURE",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "reason": "Gym does not have dailypass feature enabled"
                }
            )
            return False

        # 3. Check gym offer flags: Gym must have opted into the offer
        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.dailypass:
            logger.info(
                "DAILYPASS_OFFER_INELIGIBLE_GYM_NOT_OPTED_IN",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "reason": "Gym has not opted into the offer"
                }
            )
            return False

        # 4. Check gym cap: Gym must have < 50 unique users who booked at ₹49
        gym_promo_count_stmt = (
            select(func.count(func.distinct(DailyPass.client_id)))
            .select_from(DailyPass)
            .join(DailyPassDay, DailyPassDay.pass_id == DailyPass.id)
            .where(
                DailyPass.gym_id == str(gym_id),
                DailyPass.status != "canceled",
                DailyPassDay.dailypass_price == 49,  # ₹49 in rupees
            )
        )
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:
            logger.info(
                "DAILYPASS_OFFER_INELIGIBLE_GYM_CAP_REACHED",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "gym_promo_user_count": gym_promo_count,
                    "reason": "Gym has >= 50 users who used the ₹49 offer"
                }
            )
            return False

        # All conditions met - user is eligible!
        # Note: For dailypass, users CAN book ₹49 at the same gym multiple times
        logger.info(
            "DAILYPASS_OFFER_ELIGIBLE",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "user_booking_count": user_dp_count,
                "gym_promo_user_count": gym_promo_count,
                "slots_remaining": 50 - gym_promo_count
            }
        )
        return True

    except Exception as e:
        logger.error(
            "DAILYPASS_OFFER_CHECK_ERROR",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "error": repr(e)
            }
        )
        # On error, default to not eligible (safer)
        return False


async def _get_plan_by_duration_async(db: AsyncSession, duration: int) -> Optional[FittbotPlan]:
    """Async version of get_plan_by_duration."""
    return (
        await db.execute(
            select(FittbotPlan).where(FittbotPlan.duration == duration)
        )
    ).scalars().first()


async def _process_daily_pass_activation_async(
    db: AsyncSession,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    """Async version of _process_daily_pass_activation - creates DailyPass and DailyPassDay records."""
    logger.info(f"[DAILYPASS_ACTIVATION_START] order_item_id={order_item.id}, customer_id={customer_id}, payment_id={payment_id}")

    metadata = order_item.item_metadata or {}
    gym_id = int(order_item.gym_id)
    dates = [datetime.fromisoformat(d).date() for d in metadata.get("dates", [])]
    selected_time = metadata.get("selected_time")

    logger.info(f"[DAILYPASS_METADATA] gym_id={gym_id}, dates_count={len(dates)}, selected_time={selected_time}")

    if not dates:
        logger.error(f"[DAILYPASS_ERROR] Missing dates in metadata: {metadata}")
        raise HTTPException(500, "Daily pass metadata missing dates")

    # Get actual_price (gym owner's base price WITHOUT markup) from item_metadata
    daily_pass_pricing = metadata.get("daily_pass_pricing", {})
    actual_price_minor = daily_pass_pricing.get("actual_price_minor", order_item.unit_price_minor)
    dailypass_price_rupees = actual_price_minor // 100

    # Calculate amount_paid: start with subtotal, then subtract rewards if applied
    pricing_breakdown = metadata.get("pricing_breakdown", {})
    reward_details = metadata.get("reward_details", {})

    # Use subtotal from pricing breakdown (after multi-day discount)
    amount_before_rewards = pricing_breakdown.get("subtotal_minor", order_item.unit_price_minor * order_item.qty)

    # Subtract reward amount if applied
    reward_amount = reward_details.get("reward_amount_minor", 0) if reward_details else 0
    actual_amount_paid = amount_before_rewards - reward_amount

    logger.info(f"Daily pass amount calculation: subtotal={amount_before_rewards}, reward={reward_amount}, final={actual_amount_paid}")

    daily_pass = DailyPass(
        client_id=customer_id,
        gym_id=gym_id,
        start_date=dates[0],
        end_date=dates[-1],
        days_total=len(dates),
        amount_paid=actual_amount_paid,
        payment_id=payment_id,
        status="active",
        selected_time=selected_time,
        purchase_timestamp=datetime.now(IST),
    )

    logger.info(f"[DAILYPASS_CREATING] Creating DailyPass: client_id={customer_id}, gym_id={gym_id}, days={len(dates)}, amount={actual_amount_paid}")
    db.add(daily_pass)
    logger.info(f"[DAILYPASS_ADDED] DailyPass added to session, flushing...")
    await db.flush()
    logger.info(f"[DAILYPASS_FLUSHED] DailyPass ID: {daily_pass.id}")

    logger.info(f"[DAILYPASS_DAYS] Creating {len(dates)} DailyPassDay records for pass_id={daily_pass.id}")
    day_records = []
    for i, d in enumerate(dates):
        rec = DailyPassDay(
            daily_pass_id=daily_pass.id,
            date=d,
            status="available",
            gym_id=gym_id,
            client_id=customer_id,
            dailypass_price=dailypass_price_rupees,
        )
        db.add(rec)
        await db.flush()
        day_records.append(rec)
        if i == 0 or i == len(dates) - 1:
            logger.info(f"[DAILYPASS_DAY] Created day {i+1}/{len(dates)}: id={rec.id}, date={d}")
    logger.info(f"[DAILYPASS_DAYS_COMPLETE] Created {len(day_records)} day records")

    db.add(
        DailyPassAudit(
            daily_pass_id=daily_pass.id,
            action="purchase",
            details=f"Daily pass purchased for {len(dates)} days",
            timestamp=datetime.now(IST),
            client_id=customer_id,
            actor="system",
        )
    )

    total_minor = int(daily_pass.amount_paid)
    n = max(1, len(day_records))
    base, rem = divmod(total_minor, n)
    for i, dr in enumerate(day_records):
        amt = base + (1 if i < rem else 0)
        db.add(
            LedgerAllocation(
                daily_pass_id=daily_pass.id,
                pass_day_id=dr.id,
                gym_id=gym_id,
                client_id=customer_id,
                payment_id=payment_id,
                order_id=order_item.order_id,
                amount=amt,
                amount_net_minor=amt,
                allocation_date=datetime.now(IST).date(),
                status="allocated",
            )
        )

    # Don't commit here - let the caller handle the transaction
    logger.info(f"[DAILYPASS_ACTIVATION_COMPLETE] Successfully created pass {daily_pass.id} with {len(day_records)} days, NOT committing yet (caller will commit)")
    return {
        "daily_pass_id": daily_pass.id,
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "days_total": len(dates),
        "status": "active",
    }


async def _process_local_subscription_activation_async(
    db: AsyncSession,
    order_item: OrderItem,
    customer_id: str,
    payment_id: str,
) -> Dict[str, Any]:
    """Async version of _process_local_subscription_activation - creates Subscription record."""
    meta = order_item.item_metadata or {}
    plan_id = meta.get("plan_id")
    duration_months = int(meta.get("duration_months") or 1)
    product_id = f"fittbot_plan_{plan_id}" if plan_id is not None else "fittbot_plan"

    now_ist_time = datetime.now(IST)
    sub = Subscription(
        id=_new_id("sub_"),
        customer_id=customer_id,
        provider="internal_manual",
        product_id=str(product_id),
        status=SubscriptionStatus.active,
        rc_original_txn_id=None,
        latest_txn_id=payment_id,
        active_from=now_ist_time,
        active_until=(now_ist_time + timedelta(days=30*duration_months)),
        auto_renew=False,
    )
    db.add(sub)
    logger.debug(f"Created subscription {sub.id} for customer {customer_id}, plan {plan_id}, duration {duration_months} months")

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
            # EntitlementService uses sync operations - use a sync session for this
            from app.models.database import get_db_sync
            order_id_copy = order.id  # Capture order ID for sync context
            def _create_entitlements_sync():
                sync_session = next(get_db_sync())
                try:
                    # Re-fetch order in sync session
                    sync_order = sync_session.query(Order).filter(Order.id == order_id_copy).first()
                    if sync_order:
                        EntitlementService(sync_session).create_entitlements_from_order(sync_order)
                        sync_session.commit()
                finally:
                    sync_session.close()
            await asyncio.to_thread(_create_entitlements_sync)
            logger.debug(f"Created entitlements for order {order.id}")
            # Re-fetch entitlements in async session
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
                logger.debug(f"Updated entitlement {e.id} for subscription {sub.id}")

    # Don't commit here - let the caller handle the transaction
    return {
        "subscription_id": sub.id,
        "plan_id": plan_id,
        "active_from": sub.active_from.isoformat(),
        "active_until": sub.active_until.isoformat(),
        "status": "active",
        "provider": "internal_manual",
    }


class DailyPassProcessor:
    """Runs heavy DailyPass checkout + verification work in Celery workers."""

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
        payload = UnifiedCheckoutRequest(**record.payload)
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, client_id=str(payload.clientId),
                             gym_id=payload.gymId, days_total=payload.daysTotal)
        try:
            result = await self._execute_checkout(payload)
        except Exception as exc:  # pragma: no cover - logged for observability
            pel.checkout_failed(command_id=command_id, client_id=str(payload.clientId),
                                error_code=type(exc).__name__, error_detail=str(exc),
                                duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("DailyPass checkout failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        pel.checkout_completed(command_id=command_id, client_id=str(payload.clientId),
                               duration_ms=int((time.perf_counter() - _start) * 1000),
                               gym_id=payload.gymId, days_total=payload.daysTotal)
        pel.order_created(command_id=command_id, client_id=str(payload.clientId),
                          gym_id=payload.gymId)
        await store.mark_completed(command_id, result)

    async def process_verify(self, command_id: str, store) -> None:
        record = await store.mark_processing(command_id)
        payload = UnifiedVerificationRequest(**record.payload)
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id,
                           razorpay_payment_id=payload.razorpay_payment_id,
                           razorpay_order_id=payload.razorpay_order_id)
        try:
            result = await self._execute_verify(payload)
        except Exception as exc:  # pragma: no cover
            pel.verify_failed(command_id=command_id,
                              error_code=type(exc).__name__, error_detail=str(exc),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception("DailyPass verification failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        _dur = int((time.perf_counter() - _start) * 1000)
        if result.get("success"):
            pel.verify_completed(command_id=command_id, verify_path="dailypass",
                                 duration_ms=_dur)
            pel.payment_captured(command_id=command_id,
                                 razorpay_payment_id=payload.razorpay_payment_id)
        else:
            pel.verify_failed(command_id=command_id,
                              error_code="verify_unsuccessful", duration_ms=_dur)
        await store.mark_completed(command_id, result)

    async def _execute_checkout(self, payload: UnifiedCheckoutRequest) -> Dict[str, Any]:
        """Execute checkout using async DB session."""
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            return await self._checkout_async(db, payload)

    async def _execute_verify(self, payload: UnifiedVerificationRequest) -> Dict[str, Any]:
        """Execute verification using async DB session.

        First polls local DB to check if webhook already fulfilled (like RevenueCat).
        If found, returns immediately. Otherwise falls back to normal verify flow.
        """
        # Poll local DB first — webhook may have already fulfilled this order
        local_result = await self._poll_local_fulfillment(payload.razorpay_order_id, payload.razorpay_payment_id)
        if local_result:
            logger.info(
                "DAILYPASS_VERIFY_ALREADY_FULFILLED_BY_WEBHOOK",
                extra={
                    "payment_id": _mask_sensitive(payload.razorpay_payment_id),
                    "order_id": local_result.get("order_id"),
                },
            )
            return local_result

        capture_marker = await self._capture_marker_snapshot(payload.razorpay_payment_id)
        if capture_marker:
            logger.info(
                "DAILYPASS_VERIFY_CAPTURE_CACHE_HIT",
                extra={
                    "payment_id": _mask_sensitive(payload.razorpay_payment_id),
                    "order_id": capture_marker.get("order_id"),
                },
            )
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            return await self._verify_async(db, payload, capture_marker)

    async def _poll_local_fulfillment(
        self, razorpay_order_id: str, razorpay_payment_id: str
    ) -> Optional[Dict[str, Any]]:
        """Check if webhook already fulfilled this order (Payment + DailyPass exist)."""
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            # Find order by razorpay order id
            order = (
                await db.execute(
                    select(Order).where(Order.provider_order_id == razorpay_order_id)
                )
            ).scalars().first()
            if not order or order.status != StatusOrder.paid:
                return None

            # Check payment exists and is captured
            existing_payment = (
                await db.execute(
                    select(Payment).where(
                        Payment.provider_payment_id == razorpay_payment_id,
                        Payment.status == StatusPayment.captured,
                    )
                )
            ).scalars().first()
            if not existing_payment:
                return None

            # Check dailypass exists
            existing_dp = (
                await db.execute(
                    select(DailyPass).where(DailyPass.payment_id == razorpay_payment_id)
                )
            ).scalars().first()
            if not existing_dp:
                return None

            # Check subscription if order had one
            items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            sub_ok = False
            sub_details = None
            for it in items:
                if it.item_type == ItemType.app_subscription:
                    existing_sub = (
                        await db.execute(
                            select(Subscription).where(
                                Subscription.latest_txn_id == razorpay_payment_id
                            )
                        )
                    ).scalars().first()
                    if existing_sub:
                        sub_ok = True
                        sub_details = {"subscription_id": existing_sub.id}
                    else:
                        # Subscription missing — let normal verify handle it
                        return None

            logger.info(
                "DAILYPASS_LOCAL_FULFILLMENT_FOUND",
                extra={
                    "order_id": order.id,
                    "daily_pass_id": existing_dp.id,
                    "payment_id": _mask_sensitive(razorpay_payment_id),
                },
            )
            response = UnifiedVerificationResponse(
                success=True,
                payment_captured=True,
                order_id=order.id,
                payment_id=razorpay_payment_id,
                daily_pass_activated=True,
                daily_pass_details={
                    "daily_pass_id": existing_dp.id,
                    "status": "active",
                },
                subscription_activated=sub_ok,
                subscription_details=sub_details,
                total_amount=order.gross_amount_minor,
                currency="INR",
                message="Payment already verified via webhook",
            )
            return response.dict()

    async def fulfill_from_webhook(
        self, razorpay_order_id: str, payment_id: str, payment_data: Dict[str, Any]
    ) -> None:
        """Called by WebhookProcessor on payment.captured for dailypass orders.

        Performs the same fulfillment as _verify_async but without needing
        client-provided signature (webhook already verified by Razorpay).
        """
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            # Find order
            order = (
                await db.execute(
                    select(Order).where(Order.provider_order_id == razorpay_order_id)
                )
            ).scalars().first()
            if not order:
                logger.warning(
                    "DAILYPASS_WEBHOOK_FULFILL_ORDER_NOT_FOUND",
                    extra={"razorpay_order_id": razorpay_order_id, "payment_id": _mask_sensitive(payment_id)},
                )
                return

            # Check if already fulfilled (idempotent)
            existing_payment = (
                await db.execute(
                    select(Payment).where(
                        Payment.provider_payment_id == payment_id,
                        Payment.status == StatusPayment.captured,
                    )
                )
            ).scalars().first()
            if existing_payment:
                # Payment already exists — check if DailyPass also exists
                existing_dp = (
                    await db.execute(
                        select(DailyPass).where(DailyPass.payment_id == payment_id)
                    )
                ).scalars().first()
                if existing_dp:
                    logger.info(
                        "DAILYPASS_WEBHOOK_FULFILL_ALREADY_DONE",
                        extra={"order_id": order.id, "daily_pass_id": existing_dp.id},
                    )
                    return
                # Payment exists but no DailyPass — fall through to create it
                logger.warning(
                    "DAILYPASS_WEBHOOK_FULFILL_PAYMENT_EXISTS_NO_PASS",
                    extra={"order_id": order.id, "payment_id": _mask_sensitive(payment_id)},
                )

            gym_ids: set = set()

            try:
                # Create Payment record if not exists
                if not existing_payment:
                    pay = Payment(
                        id=_new_id("pay_"),
                        order_id=order.id,
                        customer_id=order.customer_id,
                        provider="razorpay_pg",
                        provider_payment_id=payment_id,
                        amount_minor=int(payment_data.get("amount") or order.gross_amount_minor),
                        currency=payment_data.get("currency", "INR"),
                        status=StatusPayment.captured,
                        captured_at=datetime.now(IST),
                        payment_metadata={
                            "method": payment_data.get("method"),
                            "source": "webhook_fulfillment",
                            "razorpay_order_id": razorpay_order_id,
                        },
                    )
                    db.add(pay)
                    order.status = StatusOrder.paid
                    db.add(order)

                # Deduct rewards if applicable (from order metadata)
                order_meta = order.order_metadata or {}
                reward_info = order_meta.get("payment_summary", {}).get("step_3_reward_deduction", {})
                reward_amount_minor = reward_info.get("reward_amount_minor", 0)
                if reward_amount_minor and reward_amount_minor > 0:
                    client_id = int(order.customer_id)
                    reward_to_deduct = int(round(reward_amount_minor / 100))
                    fittbot_cash_entry = (
                        await db.execute(
                            select(ReferralFittbotCash)
                            .where(ReferralFittbotCash.client_id == client_id)
                        )
                    ).scalars().first()
                    if fittbot_cash_entry:
                        old_balance = int(fittbot_cash_entry.fittbot_cash) if fittbot_cash_entry.fittbot_cash else 0
                        new_balance = max(old_balance - reward_to_deduct, 0)
                        fittbot_cash_entry.fittbot_cash = new_balance
                        db.add(fittbot_cash_entry)
                        await db.flush()
                        logger.info(
                            "DAILYPASS_WEBHOOK_REWARD_DEDUCTED",
                            extra={
                                "client_id": client_id,
                                "old_balance": old_balance,
                                "deducted": reward_to_deduct,
                                "new_balance": new_balance,
                            },
                        )

                dp_details = None
                sub_details = None
                dp_ok = False
                sub_ok = False

                items = (
                    await db.execute(
                        select(OrderItem).where(OrderItem.order_id == order.id)
                    )
                ).scalars().all()

                for it in items:
                    try:
                        if it.gym_id is not None:
                            gym_ids.add(int(it.gym_id))
                    except Exception:
                        pass

                    if it.item_type == ItemType.daily_pass:
                        # Check if DailyPass already exists (idempotent)
                        existing_dp = (
                            await db.execute(
                                select(DailyPass).where(DailyPass.payment_id == payment_id)
                            )
                        ).scalars().first()
                        if existing_dp:
                            dp_ok = True
                            dp_details = {"daily_pass_id": existing_dp.id, "status": "active"}
                            continue

                        dp_details = await _process_daily_pass_activation_async(
                            db=db,
                            order_item=it,
                            customer_id=order.customer_id,
                            payment_id=payment_id,
                        )
                        dp_ok = True

                        # Create FittbotPayment records (same logic as verify)
                        daily_pass_id = dp_details.get("daily_pass_id")
                        if daily_pass_id:
                            day_records = (
                                await db.execute(
                                    select(DailyPassDay).where(DailyPassDay.pass_id == daily_pass_id)
                                )
                            ).scalars().all()

                            gym_id = int(it.gym_id)
                            client_id_int = int(order.customer_id)

                            item_metadata = it.item_metadata or {}
                            daily_pass_pricing = item_metadata.get("daily_pass_pricing", {})
                            pricing_breakdown = item_metadata.get("pricing_breakdown", {})
                            reward_details_meta = item_metadata.get("reward_details", {})

                            per_day_with_markup_minor = daily_pass_pricing.get("per_day_minor", it.unit_price_minor)
                            per_day_base_minor = daily_pass_pricing.get("actual_price_minor", per_day_with_markup_minor)

                            subtotal_minor = pricing_breakdown.get("subtotal_minor", per_day_with_markup_minor * it.qty)
                            reward_amount_meta = reward_details_meta.get("reward_amount_minor", 0)
                            client_paid_total_minor = subtotal_minor - reward_amount_meta

                            num_days = len(day_records) if day_records else 1
                            client_paid_per_day_minor = client_paid_total_minor / num_days if num_days > 0 else 0
                            client_paid_per_day = int(round(client_paid_per_day_minor / 100))
                            gym_owner_per_day = int(round(per_day_base_minor / 100))

                            for day in day_records:
                                fittbot_payment = FittbotPayment(
                                    source_type="daily_pass",
                                    source_id=str(daily_pass_id),
                                    entitlement_id=str(day.id),
                                    gym_id=gym_id,
                                    client_id=client_id_int,
                                    amount_gross=client_paid_per_day,
                                    amount_net=gym_owner_per_day,
                                    currency="INR",
                                    gateway="razorpay",
                                    gateway_payment_id=payment_id,
                                    status="paid",
                                    paid_at=datetime.now(IST),
                                )
                                db.add(fittbot_payment)

                            logger.info(
                                "DAILYPASS_WEBHOOK_FITTBOT_PAYMENTS_CREATED",
                                extra={
                                    "daily_pass_id": daily_pass_id,
                                    "days_count": len(day_records),
                                    "client_paid_per_day": client_paid_per_day,
                                    "gym_owner_per_day": gym_owner_per_day,
                                },
                            )

                    elif it.item_type == ItemType.app_subscription:
                        existing_sub = (
                            await db.execute(
                                select(Subscription).where(
                                    Subscription.latest_txn_id == payment_id
                                )
                            )
                        ).scalars().first()
                        if existing_sub:
                            sub_ok = True
                            sub_details = {"subscription_id": existing_sub.id}
                            continue

                        sub_details = await _process_local_subscription_activation_async(
                            db=db,
                            order_item=it,
                            customer_id=order.customer_id,
                            payment_id=payment_id,
                        )
                        sub_ok = True

                # Reward program entries (best-effort)
                try:
                    reward_client_id = int(order.customer_id) if order.customer_id is not None else None
                    if reward_client_id is not None:
                        if dp_ok:
                            days_count = dp_details.get("days_total", 1) if dp_details else 1
                            await add_dailypass_entry(
                                db,
                                client_id=reward_client_id,
                                source_id=payment_id,
                                days_count=days_count,
                            )
                        if sub_ok:
                            await add_subscription_entry(
                                db,
                                client_id=reward_client_id,
                                source_id=payment_id,
                            )
                except Exception as reward_exc:
                    logger.warning(
                        "DAILYPASS_WEBHOOK_REWARD_ENTRY_FAILED",
                        extra={"error": repr(reward_exc), "order_id": order.id},
                    )

                await db.commit()
                logger.info(
                    "DAILYPASS_WEBHOOK_FULFILL_SUCCESS",
                    extra={
                        "order_id": order.id,
                        "payment_id": _mask_sensitive(payment_id),
                        "dp_ok": dp_ok,
                        "sub_ok": sub_ok,
                    },
                )
                await self._invalidate_home_booking_cache(gym_ids)

                # Queue owner notification (fire-and-forget)
                try:
                    if dp_ok and dp_details:
                        starting_date = None
                        if dp_details.get("start_date"):
                            starting_date = date.fromisoformat(dp_details["start_date"])
                        for it in items:
                            if it.item_type == ItemType.daily_pass:
                                queue_dailypass_notification(
                                    gym_id=int(it.gym_id) if it.gym_id else 0,
                                    client_id=int(order.customer_id),
                                    amount=order.gross_amount_minor / 100,
                                    days_count=dp_details.get("days_total", 1),
                                    starting_date=starting_date,
                                )
                                break
                except Exception as e:
                    logger.warning(f"[DAILYPASS_WEBHOOK_NOTIFICATION_ERROR] {e}")

            except IntegrityError as e:
                if "Duplicate entry" in str(e) or "uq_pass_day_unique_date" in str(e):
                    await db.rollback()
                    logger.info(
                        "DAILYPASS_WEBHOOK_FULFILL_DUPLICATE",
                        extra={"order_id": order.id, "payment_id": _mask_sensitive(payment_id)},
                    )
                    return
                await db.rollback()
                raise
            except Exception:
                await db.rollback()
                raise

    async def _checkout_async(self, db: AsyncSession, payload: UnifiedCheckoutRequest) -> Dict[str, Any]:
        """Async checkout implementation - creates order in DB and Razorpay."""
        cid = payload.clientId
        settings = self.settings

        start_date = datetime.fromisoformat(f"{payload.startDate}T00:00:00").date()
        dp_dates = _validate_date_range(start_date, payload.daysTotal)

        # Calculate offer eligibility based on client_id and gym_id (don't trust client input)
        try:
            client_id_int = int(cid)
        except (ValueError, TypeError):
            client_id_int = None

        is_offer_eligible = False
        if client_id_int is not None:
            is_offer_eligible = await _check_dailypass_offer_eligibility(db, client_id_int, payload.gymId)
            logger.info(
                "DAILYPASS_CHECKOUT_ELIGIBILITY_CHECK",
                extra={
                    "client_id": client_id_int,
                    "gym_id": payload.gymId,
                    "is_offer_eligible": is_offer_eligible,
                }
            )
        else:
            logger.warning(
                "DAILYPASS_CHECKOUT_INVALID_CLIENT_ID",
                extra={
                    "client_id": cid,
                    "gym_id": payload.gymId,
                }
            )

        # Async DB queries for pricing - use calculated is_offer_eligible to force ₹49 if client is eligible
        per_day_minor = await _get_price_for_gym_async(db, payload.gymId, is_offer_eligible)
        actual_price = await _get_actual_price_for_gym_async(db, payload.gymId, is_offer_eligible)
        dp_gross = per_day_minor * payload.daysTotal

        logger.info("DAILYPASS_PRICING", extra={
            "per_day_minor": per_day_minor, "actual_price": actual_price,
            "dp_gross": dp_gross, "days_total": payload.daysTotal,
        })

        dp_discount = 0
        dp_total = dp_gross

        sub_total = 0
        plan_duration_months = None
        if payload.includeSubscription:
            if not payload.selectedPlan:
                raise HTTPException(400, "selectedPlan is required when includeSubscription=true")
            plan = await _get_plan_by_duration_async(db, payload.selectedPlan)
            if not plan:
                raise HTTPException(404, "Plan not found")
            sub_total = int(getattr(plan, "price", 0) or 0)
            if sub_total and sub_total < 100:
                sub_total *= 100
            plan_duration_months = int(getattr(plan, "duration", 1))
            if sub_total <= 0 or plan_duration_months <= 0:
                raise HTTPException(409, "Invalid plan configuration")

        gross_total_before_rewards = dp_total + sub_total

        reward_amount = 0
        reward_calculation_details: Dict[str, Any] = {}
        if payload.reward:
 
            ten_percent_minor = dp_total // 10
            capped_reward_minor = ten_percent_minor
          
            fittbot_cash_entry = (
                await db.execute(
                    select(ReferralFittbotCash)
                    .where(ReferralFittbotCash.client_id == payload.clientId)
                )
            ).scalars().first()
            available_fittbot_cash_rupees = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0
            available_fittbot_cash_minor = int(available_fittbot_cash_rupees * 100)
            reward_amount = min(available_fittbot_cash_minor, capped_reward_minor)
            # Round reward to nearest rupee (in minor)
            reward_amount = int(round(reward_amount / 100) * 100)
            reward_calculation_details = {
                "reward_applied": True,
                "reward_amount_minor": reward_amount,
                "reward_amount_rupees": reward_amount // 100,
                "ten_percent_cap_minor": ten_percent_minor,
                "available_fittbot_cash_minor": available_fittbot_cash_minor,
                "available_fittbot_cash_rupees": available_fittbot_cash_rupees,
                "calculation_base": "daily_pass_total",
            }

        grand_total = gross_total_before_rewards - reward_amount

        order_metadata = {
            "order_info": {
                "order_type": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
                "customer_id": cid,
                "created_at": datetime.now(IST).isoformat(),
                "currency": "INR",
                "flow": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
            },
            "order_composition": {
                "includes_daily_pass": True,
                "includes_subscription": payload.includeSubscription,
                "items_count": 2 if payload.includeSubscription else 1,
            },
            "payment_summary": {
                "step_1_base_amounts": {
                    "daily_pass_base_minor": dp_gross,
                    "daily_pass_base_rupees": dp_gross / 100,
                    "subscription_base_minor": sub_total,
                    "subscription_base_rupees": sub_total / 100,
                    "total_base_minor": dp_gross + sub_total,
                    "total_base_rupees": (dp_gross + sub_total) / 100,
                },
                "step_2_multi_day_discount": {
                    "discount_applicable": False,
                    "discount_percentage": 0,
                    "discount_reason": "No discount",
                    "discount_amount_minor": 0,
                    "discount_amount_rupees": 0,
                    "subtotal_after_discount_minor": gross_total_before_rewards,
                    "subtotal_after_discount_rupees": gross_total_before_rewards / 100,
                },
                "step_3_reward_deduction": {
                    "reward_requested": payload.reward,
                    "reward_applied": reward_amount > 0,
                    "reward_amount_minor": reward_amount,
                    "reward_amount_rupees": reward_amount / 100,
                    "reward_source": "fittbot_cash",
                    "available_fittbot_cash_minor": reward_calculation_details.get("available_fittbot_cash_minor", 0)
                    if payload.reward
                    else 0,
                    "available_fittbot_cash_rupees": reward_calculation_details.get("available_fittbot_cash_rupees", 0)
                    if payload.reward
                    else 0,
                    "ten_percent_cap_minor": reward_calculation_details.get("ten_percent_cap_minor", 0)
                    if payload.reward
                    else 0,
                    "reward_calculation": (
                        f"min(10% of {dp_total/100}rs, available cash "
                        f"{reward_calculation_details.get('available_fittbot_cash_rupees', 0)}rs) = {reward_amount/100}rs"
                    )
                    if payload.reward
                    else "No reward applied",
                },
                "step_4_final_amount": {
                    "final_amount_minor": grand_total,
                    "final_amount_rupees": grand_total / 100,
                    "amount_saved_minor": (dp_gross + sub_total) - grand_total,
                    "amount_saved_rupees": ((dp_gross + sub_total) - grand_total) / 100,
                    "savings_percentage": round(
                        ((dp_gross + sub_total - grand_total) / (dp_gross + sub_total)) * 100, 2
                    )
                    if (dp_gross + sub_total) > 0
                    else 0,
                },
                "calculation_formula": f"({(dp_gross + sub_total)/100}rs base - {dp_discount/100}rs discount - "
                f"{reward_amount/100}rs reward) = {grand_total/100}rs paid",
                "one_line_summary": f"Paid {grand_total/100}rs (saved {((dp_gross + sub_total) - grand_total)/100}rs from "
                f"{(dp_gross + sub_total)/100}rs)",
            },
        }

        order = Order(
            id=_new_id("ord_"),
            customer_id=cid,
            currency="INR",
            provider="razorpay_pg",
            status=StatusOrder.pending,
            gross_amount_minor=grand_total,
            order_metadata=order_metadata,
        )
        db.add(order)
        await db.flush()

        dp_item_metadata = {
            "dates": [d.isoformat() for d in dp_dates],
            "selected_time": payload.selectedTime,
            "gym_id": payload.gymId,
            "daily_pass_pricing": {
                "per_day_minor": per_day_minor,
                "per_day_rupees": per_day_minor / 100,
                "actual_price_minor": actual_price,
                "actual_price_rupees": actual_price / 100,
                "gross_minor": dp_gross,
                "discount_minor": dp_discount,
                "subtotal_minor": dp_total,
            },
            "pricing_breakdown": {
                "subtotal_minor": dp_total,
                "subtotal_rupees": dp_total / 100,
                "discount_amount_minor": dp_discount,
                "discount_amount_rupees": dp_discount / 100,
            },
            "reward_details": reward_calculation_details,
        }

        db.add(
            OrderItem(
                id=_new_id("itm_"),
                order_id=order.id,
                item_type=ItemType.daily_pass,
                gym_id=str(payload.gymId),
                unit_price_minor=per_day_minor,
                qty=payload.daysTotal,
                item_metadata=dp_item_metadata,
            )
        )

        if payload.includeSubscription:
            sub_item_metadata = {
                "plan_id": payload.selectedPlan,
                "duration_months": plan_duration_months,
                "provider": "internal_manual",
                "pricing_breakdown": {
                    "plan_price_minor": sub_total,
                    "plan_price_rupees": sub_total / 100,
                    "duration_months": plan_duration_months,
                },
            }
            db.add(
                OrderItem(
                    id=_new_id("itm_"),
                    order_id=order.id,
                    item_type=ItemType.app_subscription,
                    unit_price_minor=sub_total,
                    qty=1,
                    item_metadata=sub_item_metadata,
                )
            )

        await db.flush()

        grand_total = round(grand_total / 100) * 100


        pel.provider_call_started(command_id=order.id, provider_endpoint="create_order")
        _prov_start = time.perf_counter()
        try:
            rzp_order = await rzp_create_order(
                amount_minor=grand_total,
                currency="INR",
                receipt=order.id,
                notes={
                    "order_id": order.id,
                    "customer_id": cid,
                    "gym_id": payload.gymId,
                    "flow": "unified_dailypass_local_sub" if payload.includeSubscription else "dailypass_only",
                    "gross_before_rewards": str(gross_total_before_rewards),
                    "daily_pass_subtotal": str(dp_total),
                    "subscription_subtotal": str(sub_total),
                    "reward_applied": str(reward_amount),
                    "final_amount": str(grand_total),
                    "includes_subscription": str(payload.includeSubscription),
                    "reward_used": str(payload.reward and reward_amount > 0),
                },
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

        response = UnifiedCheckoutResponse(
            success=True,
            orderId=order.id,
            razorpayOrderId=rzp_order["id"],
            razorpayKeyId=settings.razorpay_key_id,
            amount=grand_total,
            currency="INR",
            dailyPassAmount=dp_total,
            subscriptionAmount=sub_total,
            finalAmount=grand_total,
            gymId=payload.gymId,
            daysTotal=payload.daysTotal,
            startDate=payload.startDate,
            includesSubscription=payload.includeSubscription,
            displayTitle=f"{payload.daysTotal} Day Pass" + (" + Membership" if payload.includeSubscription else ""),
            description="Daily pass" + (" with app membership" if payload.includeSubscription else ""),
            reward_applied=reward_amount,
        )
        return response.dict()

    async def _verify_async(
        self,
        db: AsyncSession,
        request: UnifiedVerificationRequest,
        capture_marker: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Async verification implementation - verifies payment and creates dailypass/subscription."""
        settings = self.settings

        logger.info(f"[PROCESSOR_VERIFY_START] payment_id={_mask_sensitive(request.razorpay_payment_id)}, order_id={request.razorpay_order_id}")

        if not _verify_checkout_signature(
            settings.razorpay_key_secret,
            request.razorpay_order_id,
            request.razorpay_payment_id,
            request.razorpay_signature,
        ):
            pel.verify_signature_invalid(command_id=request.razorpay_order_id,
                                          razorpay_payment_id=request.razorpay_payment_id)
            logger.error(f"[PROCESSOR_VERIFY_ERROR] Invalid signature for order {request.razorpay_order_id}")
            raise HTTPException(403, "Invalid payment signature")

        # Async query for order
        order = (
            await db.execute(
                select(Order).where(Order.provider_order_id == request.razorpay_order_id)
            )
        ).scalars().first()
        if not order:
            logger.error(f"[PROCESSOR_VERIFY_ERROR] Order not found: {request.razorpay_order_id}")
            raise HTTPException(404, "Order not found")

        logger.info(f"[PROCESSOR_ORDER_FOUND] order_id={order.id}, customer_id={order.customer_id}")
        gym_ids: Set[int] = set()

        if not capture_marker:
            capture_marker = await self._await_capture_marker(request.razorpay_payment_id)

        if capture_marker:
            payment_data = capture_marker.copy()
            payment_data.setdefault("amount", capture_marker.get("amount"))
            payment_data.setdefault("currency", capture_marker.get("currency"))
            payment_data.setdefault("method", capture_marker.get("method"))
            payment_data.setdefault("order_id", capture_marker.get("order_id"))
            payment_data.setdefault("status", "captured")
        else:
            logger.info(
                "[PROCESSOR_VERIFY_PROVIDER_FALLBACK]",
                extra={
                    "payment_id": _mask_sensitive(request.razorpay_payment_id),
                    "order_id": request.razorpay_order_id,
                },
            )
            pel.provider_call_started(command_id=request.razorpay_order_id,
                                       provider_endpoint="get_payment")
            _prov_start = time.perf_counter()
            try:
                payment_data = await rzp_get_payment(request.razorpay_payment_id)
                pel.provider_call_completed(command_id=request.razorpay_order_id,
                                            provider_endpoint="get_payment",
                                            duration_ms=int((time.perf_counter() - _prov_start) * 1000))
            except Exception as prov_exc:
                pel.provider_call_failed(command_id=request.razorpay_order_id,
                                         provider_endpoint="get_payment",
                                         error_code=type(prov_exc).__name__,
                                         duration_ms=int((time.perf_counter() - _prov_start) * 1000))
                raise

        if payment_data.get("status") != "captured":
            raise HTTPException(
                400, f"Payment not captured (status={payment_data.get('status')})"
            )

        # Async query to check for existing payment
        existing_payment = (
            await db.execute(
                select(Payment).where(
                    Payment.provider_payment_id == request.razorpay_payment_id,
                    Payment.status == StatusPayment.captured,
                )
            )
        ).scalars().first()

        if existing_payment:
            logger.info(f"[PROCESSOR_EXISTING_PAYMENT] Payment {request.razorpay_payment_id} already exists, checking if dailypass was created...")

            # Check if dailypass/subscription was actually created
            items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            dp_ok = False
            sub_ok = False
            dp_details = None
            sub_details = None

            for it in items:
                try:
                    if it.gym_id is not None:
                        gym_ids.add(int(it.gym_id))
                except Exception:
                    logger.warning("DAILYPASS_GYM_ID_PARSE_FAILED", extra={"gym_id": it.gym_id, "item_id": it.id})

                if it.item_type == ItemType.daily_pass:
                    # Check if dailypass exists for this payment
                    existing_dp = (
                        await db.execute(
                            select(DailyPass).where(DailyPass.payment_id == request.razorpay_payment_id)
                        )
                    ).scalars().first()

                    if existing_dp:
                        logger.info(f"[PROCESSOR_EXISTING_DAILYPASS] DailyPass {existing_dp.id} already exists for payment {request.razorpay_payment_id}")
                        dp_ok = True
                        dp_details = {
                            "daily_pass_id": existing_dp.id,
                            "status": "active",
                        }
                    else:
                        # Payment exists but dailypass doesn't - CREATE IT NOW!
                        logger.warning(f"[PROCESSOR_MISSING_DAILYPASS] Payment exists but dailypass missing! Creating now for order {order.id}")

                        # IMPORTANT: Deduct reward if it was used (and not already deducted)
                        if request.reward and request.reward_applied:
                            client_id = int(order.customer_id)
                            # Cast to int to ensure exact deduction (no floating point)
                            reward_to_deduct = int(round(request.reward_applied / 100))
                            fittbot_cash_entry = (
                                await db.execute(
                                    select(ReferralFittbotCash)
                                    .where(ReferralFittbotCash.client_id == client_id)
                                )
                            ).scalars().first()
                            if fittbot_cash_entry:
                                old_balance = int(fittbot_cash_entry.fittbot_cash) if fittbot_cash_entry.fittbot_cash else 0
                                new_balance = max(old_balance - reward_to_deduct, 0)
                                fittbot_cash_entry.fittbot_cash = new_balance
                                db.add(fittbot_cash_entry)
                                await db.flush()
                                logger.info(f"[DAILYPASS_REWARD_DEDUCTED_RECOVERY] old={old_balance}, deducted={reward_to_deduct}, new={new_balance} for client {client_id}")

                        dp_details = await _process_daily_pass_activation_async(
                            db=db,
                            order_item=it,
                            customer_id=order.customer_id,
                            payment_id=request.razorpay_payment_id,
                        )
                        try:
                            await db.commit()
                        except IntegrityError as e:
                            # Race condition: another request already created the records
                            if "Duplicate entry" in str(e) or "uq_pass_day_unique_date" in str(e):
                                await db.rollback()
                                logger.info(f"[PROCESSOR_RECOVERY_DUPLICATE] Duplicate detected during dailypass recovery for order {order.id}")
                                dp_ok = True
                                # Re-fetch the existing dailypass
                                existing_dp = (
                                    await db.execute(
                                        select(DailyPass).where(DailyPass.payment_id == request.razorpay_payment_id)
                                    )
                                ).scalars().first()
                                if existing_dp:
                                    dp_details = {"daily_pass_id": existing_dp.id, "status": "active"}
                            else:
                                await db.rollback()
                                raise
                        else:
                            logger.info(f"[PROCESSOR_CREATED_MISSING_DAILYPASS] Created dailypass {dp_details.get('daily_pass_id')}")
                            dp_ok = True

                elif it.item_type == ItemType.app_subscription:
                    # Similar check for subscriptions
                    existing_sub = (
                        await db.execute(
                            select(Subscription).where(Subscription.latest_txn_id == request.razorpay_payment_id)
                        )
                    ).scalars().first()

                    if existing_sub:
                        logger.info(f"[PROCESSOR_EXISTING_SUBSCRIPTION] Subscription {existing_sub.id} already exists")
                        sub_ok = True
                        sub_details = {"subscription_id": existing_sub.id}
                    else:
                        logger.warning(f"[PROCESSOR_MISSING_SUBSCRIPTION] Creating subscription for order {order.id}")
                        sub_details = await _process_local_subscription_activation_async(
                            db=db,
                            order_item=it,
                            customer_id=order.customer_id,
                            payment_id=request.razorpay_payment_id,
                        )
                        try:
                            await db.commit()
                        except IntegrityError as e:
                            # Race condition: another request already created the records
                            if "Duplicate entry" in str(e):
                                await db.rollback()
                                logger.info(f"[PROCESSOR_RECOVERY_DUPLICATE] Duplicate detected during subscription recovery for order {order.id}")
                                sub_ok = True
                                # Re-fetch the existing subscription
                                existing_sub = (
                                    await db.execute(
                                        select(Subscription).where(Subscription.latest_txn_id == request.razorpay_payment_id)
                                    )
                                ).scalars().first()
                                if existing_sub:
                                    sub_details = {"subscription_id": existing_sub.id}
                            else:
                                await db.rollback()
                                raise
                        else:
                            logger.info(f"[PROCESSOR_CREATED_MISSING_SUBSCRIPTION] Created subscription")
                            sub_ok = True

            logger.info(f"[PROCESSOR_EXISTING_PAYMENT_COMPLETE] Returning dp_ok={dp_ok}, sub_ok={sub_ok}")
            await self._invalidate_home_booking_cache(gym_ids)
            response = UnifiedVerificationResponse(
                success=True,
                payment_captured=True,
                order_id=order.id,
                payment_id=request.razorpay_payment_id,
                daily_pass_activated=dp_ok,
                daily_pass_details=dp_details,
                subscription_activated=sub_ok,
                subscription_details=sub_details,
                total_amount=order.gross_amount_minor,
                currency="INR",
                message="Payment already processed",
            )
            return response.dict()

        try:
            # Early idempotency check: if DailyPass already exists for this payment,
            # the payment was already fully processed (by a retry or webhook).
            # Return immediately without creating duplicate Payment/reward/notification.
            existing_dp_early = (
                await db.execute(
                    select(DailyPass).where(DailyPass.payment_id == request.razorpay_payment_id)
                )
            ).scalars().first()
            if existing_dp_early:
                logger.info(
                    f"[PROCESSOR_ALREADY_FULFILLED] DailyPass {existing_dp_early.id} already exists "
                    f"for payment {request.razorpay_payment_id}, returning early"
                )
                await self._invalidate_home_booking_cache(gym_ids)
                return UnifiedVerificationResponse(
                    success=True,
                    payment_captured=True,
                    order_id=order.id,
                    payment_id=request.razorpay_payment_id,
                    daily_pass_activated=True,
                    daily_pass_details={
                        "daily_pass_id": existing_dp_early.id,
                        "days_total": existing_dp_early.days_total,
                        "status": "active",
                    },
                    subscription_activated=False,
                    subscription_details=None,
                    total_amount=order.gross_amount_minor,
                    currency="INR",
                    message="Payment already processed",
                ).dict()

            pay = Payment(
                id=_new_id("pay_"),
                order_id=order.id,
                customer_id=order.customer_id,
                provider="razorpay_pg",
                provider_payment_id=request.razorpay_payment_id,
                amount_minor=int(payment_data.get("amount") or order.gross_amount_minor),
                currency=payment_data.get("currency", "INR"),
                status=StatusPayment.captured,
                captured_at=datetime.now(IST),
                payment_metadata={
                    "method": payment_data.get("method"),
                    "source": "unified_verify",
                    "razorpay_order_id": request.razorpay_order_id,
                },
            )
            db.add(pay)
            order.status = StatusOrder.paid
            db.add(order)

            if request.reward and request.reward_applied:
                client_id = int(order.customer_id)
                # Cast to int to ensure exact deduction (no floating point)
                reward_to_deduct = int(round(request.reward_applied / 100))
                fittbot_cash_entry = (
                    await db.execute(
                        select(ReferralFittbotCash)
                        .where(ReferralFittbotCash.client_id == client_id)
                    )
                ).scalars().first()
                if fittbot_cash_entry:
                    old_balance = int(fittbot_cash_entry.fittbot_cash) if fittbot_cash_entry.fittbot_cash else 0
                    new_balance = max(old_balance - reward_to_deduct, 0)
                    fittbot_cash_entry.fittbot_cash = new_balance
                    db.add(fittbot_cash_entry)
                    await db.flush()
                    logger.info(f"[DAILYPASS_REWARD_DEDUCTED] old={old_balance}, deducted={reward_to_deduct}, new={new_balance} for client {client_id}")

            dp_details: Optional[Dict[str, Any]] = None
            sub_details: Optional[Dict[str, Any]] = None
            dp_ok = False
            sub_ok = False

            items = (
                await db.execute(
                    select(OrderItem).where(OrderItem.order_id == order.id)
                )
            ).scalars().all()

            logger.info(f"[PROCESSOR_ITEMS] Found {len(items)} order items for order {order.id}")

            for it in items:
                logger.info(f"[PROCESSOR_ITEM] Processing item {it.id}, type={it.item_type}")

                if it.item_type == ItemType.daily_pass:
                    logger.info(f"[PROCESSOR_DAILYPASS] Calling _process_daily_pass_activation_async for item {it.id}")
                    dp_details = await _process_daily_pass_activation_async(
                        db=db,
                        order_item=it,
                        customer_id=order.customer_id,
                        payment_id=request.razorpay_payment_id,
                    )
                    dp_ok = True
                    logger.info(f"[PROCESSOR_DAILYPASS_DONE] Created daily pass: {dp_details}")

                    daily_pass_id = dp_details.get("daily_pass_id")
                    if daily_pass_id:
                        day_records = (
                            await db.execute(
                                select(DailyPassDay).where(DailyPassDay.pass_id == daily_pass_id)
                            )
                        ).scalars().all()

                        gym_id = int(it.gym_id)
                        client_id = int(order.customer_id)

                        # Get pricing info from OrderItem metadata
                        item_metadata = it.item_metadata or {}
                        daily_pass_pricing = item_metadata.get("daily_pass_pricing", {})
                        pricing_breakdown = item_metadata.get("pricing_breakdown", {})
                        reward_details = item_metadata.get("reward_details", {})

                        # Calculate per-day amounts
                        per_day_with_markup_minor = daily_pass_pricing.get("per_day_minor", it.unit_price_minor)
                        per_day_base_minor = daily_pass_pricing.get("actual_price_minor", per_day_with_markup_minor)

                        # Client paid total (after discount and rewards) - in minor units
                        subtotal_minor = pricing_breakdown.get("subtotal_minor", per_day_with_markup_minor * it.qty)
                        reward_amount_minor = reward_details.get("reward_amount_minor", 0)
                        client_paid_total_minor = subtotal_minor - reward_amount_minor

                        # Calculate per-day amounts in rupees
                        num_days = len(day_records) if day_records else 1
                        client_paid_per_day_minor = client_paid_total_minor / num_days if num_days > 0 else 0
                        client_paid_per_day = int(round(client_paid_per_day_minor / 100))  # Convert to rupees

                        # Gym owner gets base price per day (WITHOUT markup) - convert to rupees
                        gym_owner_per_day = int(round(per_day_base_minor / 100))

                        for day in day_records:
                            fittbot_payment = FittbotPayment(
                                source_type="daily_pass",
                                source_id=str(daily_pass_id),
                                entitlement_id=str(day.id),  # DailyPassDay.id used during scan
                                gym_id=gym_id,
                                client_id=client_id,
                                amount_gross=client_paid_per_day,  # What client paid per day (WITH 30% markup, after discounts/rewards)
                                amount_net=gym_owner_per_day,      # What gym owner gets per day (WITHOUT markup)
                                currency="INR",
                                gateway="razorpay",
                                gateway_payment_id=request.razorpay_payment_id,
                                status="paid",
                                paid_at=datetime.now(IST),
                            )
                            db.add(fittbot_payment)

                        logger.info(
                            "FITTBOT_PAYMENTS_DAILYPASS_CREATED",
                            extra={
                                "daily_pass_id": daily_pass_id,
                                "days_count": len(day_records),
                                "client_paid_per_day": client_paid_per_day,
                                "gym_owner_per_day": gym_owner_per_day,
                                "per_day_with_markup": per_day_with_markup_minor / 100,
                                "per_day_base": per_day_base_minor / 100,
                            }
                        )

                elif it.item_type == ItemType.app_subscription:
                    logger.info(f"[PROCESSOR_SUBSCRIPTION] Calling _process_local_subscription_activation_async for item {it.id}")
                    sub_details = await _process_local_subscription_activation_async(
                        db=db,
                        order_item=it,
                        customer_id=order.customer_id,
                        payment_id=request.razorpay_payment_id,
                    )
                    sub_ok = True
                    logger.info(f"[PROCESSOR_SUBSCRIPTION_DONE] Created subscription: {sub_details}")

            # Add reward program entries (best-effort)
            try:
                reward_client_id = int(order.customer_id) if order.customer_id is not None else None
                if reward_client_id is not None:
                    if dp_ok:
                        days_count = dp_details.get("days_total", 1) if dp_details else 1
                        reward_ok, entries_added, reward_msg = await add_dailypass_entry(
                            db,
                            client_id=reward_client_id,
                            source_id=request.razorpay_payment_id,
                            days_count=days_count,
                        )
                        pel.side_effect_success(command_id=request.razorpay_order_id,
                                                side_effect="reward_dailypass",
                                                client_id=str(reward_client_id))
                        logger.info(
                            "DAILYPASS_REWARD_ENTRY",
                            extra={
                                "client_id": reward_client_id,
                                "success": reward_ok,
                                "entries_added": entries_added,
                                "reward_msg": reward_msg,
                                "order_id": order.id,
                            },
                        )
                    if sub_ok:
                        reward_ok, entries_added, reward_msg = await add_subscription_entry(
                            db,
                            client_id=reward_client_id,
                            source_id=request.razorpay_payment_id,
                        )
                        pel.side_effect_success(command_id=request.razorpay_order_id,
                                                side_effect="reward_subscription",
                                                client_id=str(reward_client_id))
                        logger.info(
                            "SUBSCRIPTION_REWARD_ENTRY",
                            extra={
                                "client_id": reward_client_id,
                                "success": reward_ok,
                                "entries_added": entries_added,
                                "reward_msg": reward_msg,
                                "order_id": order.id,
                            },
                        )
                else:
                    pel.side_effect_skipped(command_id=request.razorpay_order_id,
                                            side_effect="reward", reason="missing_client_id")
                    logger.info(
                        "REWARD_ENTRY_SKIPPED",
                        extra={"reason": "missing_client_id", "order_id": order.id},
                    )
            except Exception as reward_exc:
                pel.side_effect_failed(command_id=request.razorpay_order_id,
                                       side_effect="reward", error_detail=str(reward_exc),
                                       client_id=str(order.customer_id) if order else None)
                logger.warning(
                    "REWARD_ENTRY_FAILED",
                    extra={
                        "client_id": order.customer_id if order else None,
                        "order_id": order.id if order else None,
                        "error": repr(reward_exc),
                    },
                )

            logger.info(f"[PROCESSOR_COMMITTING] Committing transaction for order {order.id}")
            try:
                await db.commit()
            except IntegrityError as e:
                # Race condition: another request already created the records
                if "Duplicate entry" in str(e) or "uq_pass_day_unique_date" in str(e):
                    await db.rollback()
                    logger.info(f"[PROCESSOR_DUPLICATE_DETECTED] Duplicate entry detected for order {order.id}, returning already_processed")
                    return UnifiedVerificationResponse(
                        success=True,
                        payment_captured=True,
                        order_id=order.id,
                        payment_id=request.razorpay_payment_id,
                        daily_pass_activated=True,
                        daily_pass_details=dp_details,
                        subscription_activated=sub_ok,
                        subscription_details=sub_details,
                        total_amount=order.gross_amount_minor,
                        currency="INR",
                        message="Payment already processed",
                    ).dict()
                await db.rollback()
                raise

            logger.info(f"[PROCESSOR_COMMIT_SUCCESS] Transaction committed successfully")
            await self._invalidate_home_booking_cache(gym_ids)

            # Queue owner notification (fire-and-forget, never blocks payment flow)
            try:
                if dp_ok and dp_details:
                    # Get starting date from dp_details
                    starting_date = None
                    if dp_details.get("start_date"):
                        starting_date = date.fromisoformat(dp_details["start_date"])
                    for it in items:
                        if it.item_type == ItemType.daily_pass:
                            queue_dailypass_notification(
                                gym_id=int(it.gym_id) if it.gym_id else 0,
                                client_id=int(order.customer_id),
                                amount=order.gross_amount_minor / 100,
                                days_count=dp_details.get("days_total", 1),
                                starting_date=starting_date,
                            )
                            break  # Only send one notification per order
            except Exception as e:
                logger.warning(f"[DAILYPASS_NOTIFICATION_ERROR] Failed to queue owner notification: {e}")

            response = UnifiedVerificationResponse(
                success=True,
                payment_captured=True,
                order_id=order.id,
                payment_id=request.razorpay_payment_id,
                daily_pass_activated=dp_ok,
                daily_pass_details=dp_details,
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
        delay = max(0.2, self.config.verify_db_poll_base_delay_ms / 1000)
        max_delay = max(delay, self.config.verify_db_poll_max_delay_ms / 1000)
        deadline = time.monotonic() + max(1, self.config.verify_db_poll_total_timeout_seconds)
        attempts = max(1, self.config.verify_db_poll_attempts)

        for attempt in range(1, attempts + 1):
            marker = await self._capture_marker_snapshot(payment_id)
            if marker:
                logger.info(
                    "[PROCESSOR_VERIFY_CAPTURE_CACHE_HIT]",
                    extra={
                        "payment_id": _mask_sensitive(payment_id),
                        "attempt": attempt,
                    },
                )
                return marker
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 1.5)
        return None

    async def process_upgrade_checkout(self, command_id: str, store) -> None:
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, flow="upgrade")
        logger.info(f"[UPGRADE_CHECKOUT_START] Processing command {command_id}")
        try:
            await store.mark_processing(command_id)
            record = await store.get(command_id)
            if not record:
                raise ValueError("Command not found")

            SessionLocal = create_celery_async_sessionmaker()
            async with SessionLocal() as db:
                result = await self._upgrade_checkout_async(db, record.payload)
            pel.checkout_completed(command_id=command_id, flow="upgrade",
                                   duration_ms=int((time.perf_counter() - _start) * 1000))
            await store.mark_completed(command_id, result)
        except Exception as e:
            pel.checkout_failed(command_id=command_id, flow="upgrade",
                                error_code=type(e).__name__, error_detail=str(e),
                                duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception(f"[UPGRADE_CHECKOUT_ERROR] Command {command_id} failed")
            await store.mark_failed(command_id, str(e))
            raise

    async def process_upgrade_verify(self, command_id: str, store) -> None:
        """Process dailypass upgrade verification command."""
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id, flow="upgrade")
        logger.info(f"[UPGRADE_VERIFY_START] Processing command {command_id}")
        try:
            await store.mark_processing(command_id)
            record = await store.get(command_id)
            if not record:
                raise ValueError("Command not found")

            SessionLocal = create_celery_async_sessionmaker()
            async with SessionLocal() as db:
                result = await self._upgrade_verify_async(db, record.payload)
            pel.verify_completed(command_id=command_id, flow="upgrade", verify_path="upgrade",
                                 duration_ms=int((time.perf_counter() - _start) * 1000))
            await store.mark_completed(command_id, result)
        except Exception as e:
            pel.verify_failed(command_id=command_id, flow="upgrade",
                              error_code=type(e).__name__, error_detail=str(e),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception(f"[UPGRADE_VERIFY_ERROR] Command {command_id} failed")
            await store.mark_failed(command_id, str(e))
            raise

    async def process_edit_topup_checkout(self, command_id: str, store) -> None:
        _start = time.perf_counter()
        pel.checkout_started(command_id=command_id, flow="edit_topup")
        logger.info(f"[EDIT_TOPUP_CHECKOUT_START] Processing command {command_id}")
        try:
            await store.mark_processing(command_id)
            record = await store.get(command_id)
            if not record:
                raise ValueError("Command not found")
            SessionLocal = create_celery_async_sessionmaker()
            async with SessionLocal() as db:
                result = await self._edit_topup_checkout_async(db, record.payload)
            pel.checkout_completed(command_id=command_id, flow="edit_topup",
                                   duration_ms=int((time.perf_counter() - _start) * 1000))
            await store.mark_completed(command_id, result)
        except Exception as e:
            pel.checkout_failed(command_id=command_id, flow="edit_topup",
                                error_code=type(e).__name__, error_detail=str(e),
                                duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception(f"[EDIT_TOPUP_CHECKOUT_ERROR] Command {command_id} failed")
            await store.mark_failed(command_id, str(e))
            raise

    async def process_edit_topup_verify(self, command_id: str, store) -> None:
        _start = time.perf_counter()
        pel.verify_started(command_id=command_id, flow="edit_topup")
        logger.info(f"[EDIT_TOPUP_VERIFY_START] Processing command {command_id}")
        try:
            await store.mark_processing(command_id)
            record = await store.get(command_id)
            if not record:
                raise ValueError("Command not found")
            SessionLocal = create_celery_async_sessionmaker()
            async with SessionLocal() as db:
                result = await self._edit_topup_verify_async(db, record.payload)
            pel.verify_completed(command_id=command_id, flow="edit_topup", verify_path="edit_topup",
                                 duration_ms=int((time.perf_counter() - _start) * 1000))
            await store.mark_completed(command_id, result)
        except Exception as e:
            pel.verify_failed(command_id=command_id, flow="edit_topup",
                              error_code=type(e).__name__, error_detail=str(e),
                              duration_ms=int((time.perf_counter() - _start) * 1000))
            logger.exception(f"[EDIT_TOPUP_VERIFY_ERROR] Command {command_id} failed")
            await store.mark_failed(command_id, str(e))
            raise

    async def _upgrade_checkout_async(self, db: AsyncSession, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Async upgrade checkout logic - validates pass and creates Razorpay order."""
        from datetime import date as date_type

        pass_id = payload_dict.get("pass_id")
        new_gym_id = int(payload_dict.get("new_gym_id"))
        client_id = str(payload_dict.get("client_id"))
        remaining_days_count = int(payload_dict.get("remaining_days_count"))
        delta_minor = float(payload_dict.get("delta_minor")) * 100  # Convert to paisa

        settings = get_payment_settings()

        # Validate pass - async query
        p = (
            await db.execute(
                select(DailyPass).where(DailyPass.id == pass_id)
            )
        ).scalars().first()
        if not p or p.status != "active":
            raise HTTPException(status_code=404, detail="Pass not found or inactive")

        # Check if upgrade already used - async query for audit
        existing_upgrade = (
            await db.execute(
                select(DailyPassAudit).where(
                    DailyPassAudit.pass_id == pass_id,
                    DailyPassAudit.action == "upgrade"
                )
            )
        ).scalars().first()
        if existing_upgrade:
            raise HTTPException(status_code=409, detail="Upgrade already used for this pass")

        old_gym_id = int(p.gym_id)
        if new_gym_id == old_gym_id:
            raise HTTPException(status_code=409, detail="New gym must be different from current gym")

        # Get remaining days - async query
        today = date_type.today()
        remaining_days_query = (
            await db.execute(
                select(DailyPassDay)
                .where(
                    DailyPassDay.pass_id == pass_id,
                    DailyPassDay.scheduled_date > today,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .order_by(DailyPassDay.scheduled_date.asc())
            )
        ).scalars().all()
        if not remaining_days_query:
            raise HTTPException(status_code=409, detail="No remaining days to upgrade")

        # Create order with metadata (so verify can read new_gym_id without calling Razorpay)
        order_metadata = {
            "flow": "dailypass_upgrade",
            "pass_id": pass_id,
            "old_gym_id": old_gym_id,
            "new_gym_id": new_gym_id,
            "remaining_days": remaining_days_count,
        }
        order = Order(
            id=_new_id("ord_"),
            customer_id=client_id,
            provider="razorpay_pg",
            currency="INR",
            gross_amount_minor=int(delta_minor),
            status=StatusOrder.pending,
            order_metadata=order_metadata,
        )
        db.add(order)
        await db.flush()

        # Create Razorpay order - async client to avoid blocking
        rzp_order = await rzp_create_order(
            amount_minor=int(delta_minor),
            currency="INR",
            receipt=order.id,
            notes=order_metadata,
        )
        order.provider_order_id = rzp_order["id"]
        db.add(order)
        await db.commit()

        upgrade_start_date = remaining_days_query[0].scheduled_date.isoformat()
        upgrade_end_date = remaining_days_query[-1].scheduled_date.isoformat()

        logger.info(f"[UPGRADE_CHECKOUT] Created order {order.id} for pass {pass_id}")

        return {
            "success": True,
            "orderId": order.id,
            "razorpayOrderId": rzp_order["id"],
            "razorpayKeyId": settings.razorpay_key_id,
            "amount": int(delta_minor),
            "currency": "INR",
            "description": f"Daily pass upgrade from Gym {old_gym_id} to Gym {new_gym_id}",
            "pass_id": pass_id,
            "old_gym_id": old_gym_id,
            "new_gym_id": new_gym_id,
            "remaining_days": remaining_days_count,
            "upgrade_date_range": {
                "from": upgrade_start_date,
                "to": upgrade_end_date
            }
        }

    async def _upgrade_verify_async(self, db: AsyncSession, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Async upgrade verification logic - verifies payment and applies upgrade."""
        from datetime import date as date_type

        razorpay_order_id = payload_dict.get("razorpay_order_id")
        razorpay_payment_id = payload_dict.get("razorpay_payment_id")
        pass_id = payload_dict.get("pass_id")

        settings = get_payment_settings()

        # Find order - async query
        order = (
            await db.execute(
                select(Order).where(Order.provider_order_id == razorpay_order_id)
            )
        ).scalars().first()
        if not order:
            raise HTTPException(status_code=404, detail="Upgrade order not found")

        # Verify payment - run Razorpay API call in thread
        payment_data = await rzp_get_payment(razorpay_payment_id)
        if payment_data.get("status") != "captured":
            raise HTTPException(status_code=400, detail=f"Payment not captured (status={payment_data.get('status')})")

        paid_amount = int(payment_data.get("amount", 0))
        if paid_amount != order.gross_amount_minor:
            raise HTTPException(status_code=409, detail="Payment amount mismatch")

        # Idempotency check - async query
        existing_payment = (
            await db.execute(
                select(Payment).where(
                    Payment.provider_payment_id == razorpay_payment_id,
                    Payment.status == "captured"
                )
            )
        ).scalars().first()

        # Create payment record only if it doesn't exist
        if not existing_payment:
            pay = Payment(
                id=_new_id("pay_"),
                order_id=order.id,
                customer_id=order.customer_id,
                provider="razorpay_pg",
                provider_payment_id=razorpay_payment_id,
                amount_minor=paid_amount,
                currency=payment_data.get("currency", "INR"),
                status="captured",
                captured_at=datetime.now(UTC),
                payment_metadata={"method": payment_data.get("method"), "source": "dailypass_upgrade"},
            )
            db.add(pay)
            order.status = StatusOrder.paid
            db.add(order)
            await db.commit()
            logger.info(f"[UPGRADE_VERIFY] Created payment record for {razorpay_payment_id}")
        else:
            logger.info(f"[UPGRADE_VERIFY] Payment already exists for {razorpay_payment_id} (webhook arrived first)")

        # Apply upgrade (ALWAYS check audit, even if payment existed from webhook)
        p = (
            await db.execute(
                select(DailyPass).where(DailyPass.id == pass_id)
            )
        ).scalars().first()
        if not p or p.status != "active":
            raise HTTPException(status_code=404, detail="Pass not found / inactive")

        # Check if upgrade already applied - async query for audit
        existing_upgrade = (
            await db.execute(
                select(DailyPassAudit).where(
                    DailyPassAudit.pass_id == pass_id,
                    DailyPassAudit.action == "upgrade"
                )
            )
        ).scalars().first()
        if existing_upgrade:
            logger.info(f"[UPGRADE_VERIFY] Upgrade already applied for pass {pass_id}")
            return {
                "success": True,
                "payment_captured": True,
                "order_id": order.id,
                "payment_id": razorpay_payment_id,
                "message": "Upgrade already applied",
                "pass_id": pass_id,
                "gym_id": int(p.gym_id),
            }

        # Get new_gym_id from order metadata (stored during checkout)
        new_gym_id = None
        try:
            # First try to get from local order metadata (no API call needed)
            if order.order_metadata and 'new_gym_id' in order.order_metadata:
                new_gym_id = order.order_metadata.get('new_gym_id')

            # Fallback: try Razorpay order notes if not in local metadata
            if not new_gym_id and order.provider_order_id:
                rzp_order = await rzp_get_order(order.provider_order_id)
                if 'notes' in rzp_order:
                    new_gym_id = rzp_order['notes'].get('new_gym_id')

            # Last fallback: try payment details
            if not new_gym_id:
                payment_full = await rzp_get_payment(razorpay_payment_id)
                if 'notes' in payment_full:
                    new_gym_id = payment_full['notes'].get('new_gym_id')

            if new_gym_id:
                new_gym_id = int(new_gym_id)
            else:
                raise HTTPException(status_code=500, detail="Could not determine new gym ID from order notes")

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Could not determine new_gym_id: {e}")
            raise HTTPException(status_code=500, detail=f"Could not determine new gym ID for upgrade: {str(e)}")

        old_gym_id = int(p.gym_id)
        old_order_id = p.id  # Original pass ID for linking

        # Get days for upgrade - separate today and future days
        today = date_type.today()
        tomorrow = today + timedelta(days=1)

        # Get today's day (if exists) - stays with OLD gym
        todays_day_result = (
            await db.execute(
                select(DailyPassDay)
                .where(
                    DailyPassDay.pass_id == pass_id,
                    DailyPassDay.scheduled_date == today,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
            )
        )
        todays_day = todays_day_result.scalars().first()

        # Get future days (tomorrow onwards) - goes to NEW gym
        future_days_result = (
            await db.execute(
                select(DailyPassDay)
                .where(
                    DailyPassDay.pass_id == pass_id,
                    DailyPassDay.scheduled_date >= tomorrow,
                    DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                )
                .order_by(DailyPassDay.scheduled_date.asc())
            )
        )
        future_days = future_days_result.scalars().all()

        if not future_days:
            raise HTTPException(status_code=409, detail="No future days to upgrade")

        # Create upgraded passes - TWO if today exists, ONE if not
        today_pass_id = None
        future_pass_id = None

        # If today's day exists, create pass for today with OLD gym
        if todays_day:
            today_pass_id = _new_id("dps")
            today_pass = DailyPass(
                id=today_pass_id,
                user_id=p.user_id if hasattr(p, 'user_id') else None,
                client_id=p.client_id,
                gym_id=str(old_gym_id),  # OLD gym for today
                order_id=old_order_id,  # Links to original pass
                payment_id=razorpay_payment_id,
                days_total=1,
                days_used=0,
                valid_from=today,
                valid_until=today,
                amount_paid=0,  # No extra payment for today
                selected_time=p.selected_time,
                status="upgraded",
                policy=p.policy if hasattr(p, 'policy') else None,
                partial_schedule=p.partial_schedule if hasattr(p, 'partial_schedule') else None,
            )
            db.add(today_pass)
            await db.flush()

            # Update today's day record to point to today_pass
            todays_day.pass_id = today_pass_id
            todays_day.gym_id = str(old_gym_id)  # Keep old gym
            db.add(todays_day)

        # Create pass for future days with NEW gym
        future_pass_id = _new_id("dps")
        future_pass = DailyPass(
            id=future_pass_id,
            user_id=p.user_id if hasattr(p, 'user_id') else None,
            client_id=p.client_id,
            gym_id=str(new_gym_id),  # NEW gym for future
            order_id=old_order_id,  # Links to original pass
            payment_id=razorpay_payment_id,
            days_total=len(future_days),
            days_used=0,
            valid_from=future_days[0].scheduled_date,
            valid_until=future_days[-1].scheduled_date,
            amount_paid=paid_amount,
            selected_time=p.selected_time,
            status="upgraded",
            policy=p.policy if hasattr(p, 'policy') else None,
            partial_schedule=p.partial_schedule if hasattr(p, 'partial_schedule') else None,
        )
        db.add(future_pass)
        await db.flush()

        # Update future day records to point to future_pass
        for day in future_days:
            day.pass_id = future_pass_id
            day.gym_id = str(new_gym_id)
            day.client_id = p.client_id
            db.add(day)

        # Add audit record to OLD pass (this prevents re-upgrade)
        audit_dict = {
            "pass_id": pass_id,  # Audit on OLD pass
            "action": "upgrade",
            "details": f"Upgraded from gym {old_gym_id} to gym {new_gym_id}, today_pass={today_pass_id}, future_pass={future_pass_id}",
        }

        # Add optional fields if they exist in the model
        if hasattr(DailyPassAudit, 'actor'):
            audit_dict['actor'] = "user"
        if hasattr(DailyPassAudit, 'before'):
            audit_dict['before'] = {"gym_id": old_gym_id, "status": p.status}
        if hasattr(DailyPassAudit, 'after'):
            audit_dict['after'] = {
                "gym_id": new_gym_id,
                "status": "upgraded",
                "today_pass_id": today_pass_id,
                "future_pass_id": future_pass_id
            }
        if hasattr(DailyPassAudit, 'client_id'):
            audit_dict['client_id'] = p.client_id
        if hasattr(DailyPassAudit, 'changed_by'):
            audit_dict['changed_by'] = p.client_id

        audit = DailyPassAudit(**audit_dict)
        db.add(audit)

        # OLD pass remains "active" - filtered out in /all by order_id check
        # The audit record prevents re-upgrade via existing_upgrade check

        await db.commit()
        await self._invalidate_home_booking_cache({old_gym_id, new_gym_id})

        logger.info(f"[UPGRADE_VERIFY] Upgraded pass {pass_id} from gym {old_gym_id} to {new_gym_id}, today_pass={today_pass_id}, future_pass={future_pass_id}")

        return {
            "success": True,
            "payment_captured": True,
            "order_id": order.id,
            "payment_id": razorpay_payment_id,
            "message": "Upgrade completed successfully",
            "daily_pass_details": {
                "today_pass_id": today_pass_id,
                "today_gym_id": old_gym_id if today_pass_id else None,
                "future_pass_id": future_pass_id,
                "future_gym_id": new_gym_id,
                "old_pass_id": pass_id,
                "future_days_count": len(future_days),
                "today_valid": today_pass_id is not None,
                "valid_from": future_days[0].scheduled_date.isoformat(),
                "valid_until": future_days[-1].scheduled_date.isoformat(),
                "status": "upgraded",
                "scheduled_dates": [day.scheduled_date.isoformat() for day in future_days],
                "upgrade_info": {
                    "original_gym_id": old_gym_id,
                    "original_pass_id": pass_id,
                    "upgrade_date": tomorrow.isoformat(),
                }
            },
            "old_pass_id": pass_id,
            "gym_id": new_gym_id,
            "old_gym_id": old_gym_id,
        }

    async def fulfill_upgrade_from_webhook(
        self, razorpay_order_id: str, payment_id: str, payment_data: Dict[str, Any]
    ) -> None:
        """Called by WebhookProcessor on payment.captured for dailypass_upgrade orders.

        Mirrors _upgrade_verify_async business logic: creates Payment, marks order paid,
        splits pass into today (old gym) + future (new gym), updates DailyPassDay records,
        and adds audit record. Fully idempotent.
        """
        from datetime import date as date_type

        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            # Find order
            order = (
                await db.execute(
                    select(Order).where(Order.provider_order_id == razorpay_order_id)
                )
            ).scalars().first()
            if not order:
                logger.warning(
                    "UPGRADE_WEBHOOK_FULFILL_ORDER_NOT_FOUND",
                    extra={"razorpay_order_id": razorpay_order_id, "payment_id": _mask_sensitive(payment_id)},
                )
                return

            pass_id = (order.order_metadata or {}).get("pass_id")
            new_gym_id = (order.order_metadata or {}).get("new_gym_id")
            if not pass_id or not new_gym_id:
                logger.warning(
                    "UPGRADE_WEBHOOK_FULFILL_MISSING_METADATA",
                    extra={"order_id": order.id, "pass_id": pass_id, "new_gym_id": new_gym_id},
                )
                return
            new_gym_id = int(new_gym_id)

            # Idempotency: check if upgrade already applied via audit
            existing_upgrade = (
                await db.execute(
                    select(DailyPassAudit).where(
                        DailyPassAudit.pass_id == pass_id,
                        DailyPassAudit.action == "upgrade"
                    )
                )
            ).scalars().first()
            if existing_upgrade:
                logger.info(
                    "UPGRADE_WEBHOOK_FULFILL_ALREADY_DONE",
                    extra={"order_id": order.id, "pass_id": pass_id},
                )
                return

            paid_amount = int(payment_data.get("amount") or order.gross_amount_minor)

            try:
                # Create Payment record if not exists
                existing_payment = (
                    await db.execute(
                        select(Payment).where(
                            Payment.provider_payment_id == payment_id,
                            Payment.status == "captured"
                        )
                    )
                ).scalars().first()

                if not existing_payment:
                    pay = Payment(
                        id=_new_id("pay_"),
                        order_id=order.id,
                        customer_id=order.customer_id,
                        provider="razorpay_pg",
                        provider_payment_id=payment_id,
                        amount_minor=paid_amount,
                        currency=payment_data.get("currency", "INR"),
                        status="captured",
                        captured_at=datetime.now(IST),
                        payment_metadata={
                            "method": payment_data.get("method"),
                            "source": "webhook_fulfillment_upgrade",
                            "razorpay_order_id": razorpay_order_id,
                        },
                    )
                    db.add(pay)
                    order.status = StatusOrder.paid
                    db.add(order)

                # Load pass
                p = (
                    await db.execute(
                        select(DailyPass).where(DailyPass.id == pass_id)
                    )
                ).scalars().first()
                if not p or p.status != "active":
                    logger.warning(
                        "UPGRADE_WEBHOOK_FULFILL_PASS_NOT_FOUND",
                        extra={"pass_id": pass_id, "order_id": order.id},
                    )
                    # Still commit the payment record
                    await db.commit()
                    return

                old_gym_id = int(p.gym_id)
                old_order_id = p.id
                today = date_type.today()
                tomorrow = today + timedelta(days=1)

                # Get today's day (stays with OLD gym)
                todays_day = (
                    await db.execute(
                        select(DailyPassDay).where(
                            DailyPassDay.pass_id == pass_id,
                            DailyPassDay.scheduled_date == today,
                            DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                        )
                    )
                ).scalars().first()

                # Get future days (go to NEW gym)
                future_days = (
                    await db.execute(
                        select(DailyPassDay).where(
                            DailyPassDay.pass_id == pass_id,
                            DailyPassDay.scheduled_date >= tomorrow,
                            DailyPassDay.status.in_(["scheduled", "available", "rescheduled"]),
                        ).order_by(DailyPassDay.scheduled_date.asc())
                    )
                ).scalars().all()

                if not future_days:
                    logger.warning(
                        "UPGRADE_WEBHOOK_FULFILL_NO_FUTURE_DAYS",
                        extra={"pass_id": pass_id, "order_id": order.id},
                    )
                    await db.commit()
                    return

                today_pass_id = None
                future_pass_id = None

                # If today's day exists, create pass for today with OLD gym
                if todays_day:
                    today_pass_id = _new_id("dps")
                    today_pass = DailyPass(
                        id=today_pass_id,
                        user_id=p.user_id if hasattr(p, 'user_id') else None,
                        client_id=p.client_id,
                        gym_id=str(old_gym_id),
                        order_id=old_order_id,
                        payment_id=payment_id,
                        days_total=1,
                        days_used=0,
                        valid_from=today,
                        valid_until=today,
                        amount_paid=0,
                        selected_time=p.selected_time,
                        status="upgraded",
                        policy=p.policy if hasattr(p, 'policy') else None,
                        partial_schedule=p.partial_schedule if hasattr(p, 'partial_schedule') else None,
                    )
                    db.add(today_pass)
                    await db.flush()
                    todays_day.pass_id = today_pass_id
                    todays_day.gym_id = str(old_gym_id)
                    db.add(todays_day)

                # Create pass for future days with NEW gym
                future_pass_id = _new_id("dps")
                future_pass = DailyPass(
                    id=future_pass_id,
                    user_id=p.user_id if hasattr(p, 'user_id') else None,
                    client_id=p.client_id,
                    gym_id=str(new_gym_id),
                    order_id=old_order_id,
                    payment_id=payment_id,
                    days_total=len(future_days),
                    days_used=0,
                    valid_from=future_days[0].scheduled_date,
                    valid_until=future_days[-1].scheduled_date,
                    amount_paid=paid_amount,
                    selected_time=p.selected_time,
                    status="upgraded",
                    policy=p.policy if hasattr(p, 'policy') else None,
                    partial_schedule=p.partial_schedule if hasattr(p, 'partial_schedule') else None,
                )
                db.add(future_pass)
                await db.flush()

                for day in future_days:
                    day.pass_id = future_pass_id
                    day.gym_id = str(new_gym_id)
                    day.client_id = p.client_id
                    db.add(day)

                # Add audit record (prevents re-upgrade)
                audit_dict = {
                    "pass_id": pass_id,
                    "action": "upgrade",
                    "details": f"Upgraded from gym {old_gym_id} to gym {new_gym_id}, today_pass={today_pass_id}, future_pass={future_pass_id} (webhook)",
                }
                if hasattr(DailyPassAudit, 'actor'):
                    audit_dict['actor'] = "webhook"
                if hasattr(DailyPassAudit, 'before'):
                    audit_dict['before'] = {"gym_id": old_gym_id, "status": p.status}
                if hasattr(DailyPassAudit, 'after'):
                    audit_dict['after'] = {
                        "gym_id": new_gym_id,
                        "status": "upgraded",
                        "today_pass_id": today_pass_id,
                        "future_pass_id": future_pass_id,
                    }
                if hasattr(DailyPassAudit, 'client_id'):
                    audit_dict['client_id'] = p.client_id
                if hasattr(DailyPassAudit, 'changed_by'):
                    audit_dict['changed_by'] = p.client_id

                audit = DailyPassAudit(**audit_dict)
                db.add(audit)

                await db.commit()
                await self._invalidate_home_booking_cache({old_gym_id, new_gym_id})

                logger.info(
                    "UPGRADE_WEBHOOK_FULFILL_SUCCESS",
                    extra={
                        "order_id": order.id,
                        "pass_id": pass_id,
                        "old_gym_id": old_gym_id,
                        "new_gym_id": new_gym_id,
                        "today_pass_id": today_pass_id,
                        "future_pass_id": future_pass_id,
                        "future_days_count": len(future_days),
                    },
                )

            except IntegrityError as e:
                await db.rollback()
                if "Duplicate entry" in str(e):
                    logger.info(
                        "UPGRADE_WEBHOOK_FULFILL_DUPLICATE",
                        extra={"order_id": order.id, "pass_id": pass_id},
                    )
                    return
                raise

    async def fulfill_edit_topup_from_webhook(
        self, razorpay_order_id: str, payment_id: str, payment_data: Dict[str, Any]
    ) -> None:
        """Called by WebhookProcessor on payment.captured for dailypass_edit_topup orders.

        Mirrors _edit_topup_verify_async: creates Payment record and marks order paid.
        No DailyPass/day changes (those happen in separate /edit endpoint).
        Fully idempotent.
        """
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            # Find order
            order = (
                await db.execute(
                    select(Order).where(Order.provider_order_id == razorpay_order_id)
                )
            ).scalars().first()
            if not order:
                logger.warning(
                    "EDIT_TOPUP_WEBHOOK_FULFILL_ORDER_NOT_FOUND",
                    extra={"razorpay_order_id": razorpay_order_id, "payment_id": _mask_sensitive(payment_id)},
                )
                return

            # Idempotency: check if payment already exists
            existing_payment = (
                await db.execute(
                    select(Payment).where(
                        Payment.provider_payment_id == payment_id,
                        Payment.status == "captured"
                    )
                )
            ).scalars().first()
            if existing_payment:
                logger.info(
                    "EDIT_TOPUP_WEBHOOK_FULFILL_ALREADY_DONE",
                    extra={"order_id": order.id, "payment_id": _mask_sensitive(payment_id)},
                )
                return

            paid_amount = int(payment_data.get("amount") or order.gross_amount_minor)

            try:
                pay = Payment(
                    id=_new_id("pay_"),
                    order_id=order.id,
                    customer_id=order.customer_id,
                    provider="razorpay_pg",
                    provider_payment_id=payment_id,
                    amount_minor=paid_amount,
                    currency=payment_data.get("currency", "INR"),
                    status="captured",
                    captured_at=datetime.now(IST),
                    payment_metadata={
                        "method": payment_data.get("method"),
                        "source": "webhook_fulfillment_edit_topup",
                        "razorpay_order_id": razorpay_order_id,
                    },
                )
                db.add(pay)
                order.status = StatusOrder.paid
                db.add(order)
                await db.commit()

                logger.info(
                    "EDIT_TOPUP_WEBHOOK_FULFILL_SUCCESS",
                    extra={
                        "order_id": order.id,
                        "payment_id": _mask_sensitive(payment_id),
                        "amount": paid_amount,
                    },
                )

            except IntegrityError as e:
                await db.rollback()
                if "Duplicate entry" in str(e):
                    logger.info(
                        "EDIT_TOPUP_WEBHOOK_FULFILL_DUPLICATE",
                        extra={"order_id": order.id},
                    )
                    return
                raise

    async def _edit_topup_checkout_async(self, db: AsyncSession, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Create Razorpay order for edit top-up delta."""
        pass_id = payload_dict.get("pass_id")
        client_id = payload_dict.get("client_id")
        new_start_date = payload_dict.get("new_start_date")
        delta_minor = int(float(payload_dict.get("delta_minor", 0)) * 100) if isinstance(payload_dict.get("delta_minor"), float) else int(payload_dict.get("delta_minor", 0))
        settings = get_payment_settings()

        p = (
            await db.execute(
                select(DailyPass).where(DailyPass.id == pass_id)
            )
        ).scalars().first()
        if not p or p.status != "active":
            raise HTTPException(status_code=404, detail="Pass not found or inactive")
        if delta_minor <= 0:
            raise HTTPException(status_code=409, detail="No top-up required")

        order_metadata = {
            "flow": "dailypass_edit_topup",
            "pass_id": pass_id,
            "new_start_date": new_start_date,
            "gym_id": int(p.gym_id),
        }
        order = Order(
            id=_new_id("ord_"),
            customer_id=str(client_id or p.client_id),
            provider="razorpay_pg",
            currency="INR",
            gross_amount_minor=delta_minor,
            status=StatusOrder.pending,
            order_metadata=order_metadata,
        )
        db.add(order)
        await db.flush()

        rzp_order = await rzp_create_order(
            amount_minor=delta_minor,
            currency="INR",
            receipt=order.id,
            notes=order_metadata,
        )
        order.provider_order_id = rzp_order["id"]
        db.add(order)
        await db.commit()

        return {
            "success": True,
            "orderId": order.id,
            "razorpayOrderId": rzp_order["id"],
            "razorpayKeyId": settings.razorpay_key_id,
            "amount": delta_minor,
            "currency": "INR",
            "message": "Top-up order created",
            "pass_id": pass_id,
        }

    async def _edit_topup_verify_async(self, db: AsyncSession, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Verify edit top-up payment; no ledger changes here (finalized in /edit)."""
        razorpay_order_id = payload_dict.get("razorpay_order_id")
        razorpay_payment_id = payload_dict.get("razorpay_payment_id")
        pass_id = payload_dict.get("pass_id")
        settings = get_payment_settings()

        order = (
            await db.execute(
                select(Order).where(Order.provider_order_id == razorpay_order_id)
            )
        ).scalars().first()
        if not order:
            raise HTTPException(status_code=404, detail="Top-up order not found")

        payment_data = await rzp_get_payment(razorpay_payment_id)
        if payment_data.get("status") != "captured":
            raise HTTPException(status_code=400, detail=f"Payment not captured (status={payment_data.get('status')})")
        paid_amount = int(payment_data.get("amount", 0))
        if paid_amount != order.gross_amount_minor:
            raise HTTPException(status_code=409, detail="Payment amount mismatch")

        existing_payment = (
            await db.execute(
                select(Payment).where(
                    Payment.provider_payment_id == razorpay_payment_id,
                    Payment.status == "captured"
                )
            )
        ).scalars().first()

        if not existing_payment:
            pay = Payment(
                id=_new_id("pay_"),
                order_id=order.id,
                customer_id=order.customer_id,
                provider="razorpay_pg",
                provider_payment_id=razorpay_payment_id,
                amount_minor=paid_amount,
                currency=payment_data.get("currency", "INR"),
                status="captured",
                captured_at=datetime.now(UTC),
                payment_metadata={"method": payment_data.get("method"), "source": "dailypass_edit_topup"},
            )
            db.add(pay)
            order.status = StatusOrder.paid
            db.add(order)
            await db.commit()
        else:
            logger.info(f"[EDIT_TOPUP_VERIFY] Payment already exists for {razorpay_payment_id}")

        return {
            "success": True,
            "payment_captured": True,
            "order_id": order.id,
            "payment_id": razorpay_payment_id,
            "message": "Top-up payment verified. Call /get_dailypass/edit with topup_payment_id to apply changes.",
            "pass_id": pass_id,
        }
