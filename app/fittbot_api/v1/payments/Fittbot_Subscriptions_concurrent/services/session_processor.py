import asyncio
import hashlib
import hmac
import json
import logging
import time as time_module
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, time
from typing import Any, Dict, Optional, List, Set

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from redis import Redis

from app.fittbot_api.v1.payments.routes import gym_membership as gm_routes
from app.fittbot_api.v1.payments.razorpay_async_gateway import (
    create_order as rzp_create_order,
    get_payment as rzp_get_payment,
)
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_api.v1.payments.models.enums import StatusOrder, StatusPayment, ItemType
from app.fittbot_api.v1.payments.models.payments import Payment
from app.models.fittbot_payments_models import Payment as FittbotPayment
from app.models.fittbot_models import (
    SessionSetting,
    SessionSchedule,
    ReferralFittbotCash,
    SessionPurchase,
    SessionBookingDay,
    SessionBookingAudit,
    ClassSession,
    NewOffer,
    Gym,
)
from app.models.async_database import create_celery_async_sessionmaker
from app.fittbot_api.v1.client.client_api.reward_program.reward_service import add_session_entry
from app.tasks.notification_tasks import queue_session_notification

from ...config.database import PaymentDatabase
from ...config.settings import get_payment_settings
from ..config import HighConcurrencyConfig
from ..schemas import SessionCheckoutRequest, SessionVerifyRequest, SessionType
from app.config.pricing import get_markup_multiplier
from .payment_event_logger import PaymentEventLogger

logger = logging.getLogger("payments.sessions.v2.processor")
pel = PaymentEventLogger("razorpay", "session")

# Capacity limits by session type
DEFAULT_CAPACITY_PERSONAL_TRAINER = 5
DEFAULT_CAPACITY_OTHER = 25
PERSONAL_TRAINER_SESSION_ID = 2

# Redis lock settings
IDEMPOTENCY_LOCK_TTL = 30  # seconds
CHECKOUT_LOCK_TTL = 60  # seconds

_verify_checkout_sig = gm_routes._verify_checkout_sig
_mask = gm_routes._mask
_new_id = gm_routes._new_id


def _parse_date(val: str) -> date:
    try:
        return datetime.fromisoformat(val).date()
    except Exception:
        return datetime.strptime(val, "%Y-%m-%d").date()


def _parse_time(val) -> Optional[time]:
    """Parse time from various formats (string like '05:00 PM' or time object)."""
    if val is None:
        return None
    if isinstance(val, time):
        return val
    if isinstance(val, str):
        val = val.strip()
        # Try common formats
        for fmt in ["%I:%M %p", "%H:%M:%S", "%H:%M", "%I:%M%p"]:
            try:
                return datetime.strptime(val, fmt).time()
            except ValueError:
                continue
    return None


async def _check_session_offer_eligibility(db: AsyncSession, client_id: int, gym_id: int) -> bool:
    """
    Check if user is eligible for ₹99 session offer at a specific gym.

    Returns True if ALL conditions are met:
    1. User has < 3 total session bookings (any price, from PAID purchases only)
    2. Gym has opted into the offer (NewOffer.session = True)
    3. Gym has < 50 unique users who booked at ₹99
    4. Gym has sessions available (SessionSetting.is_enabled = True)
    5. User hasn't already booked ₹99 session at this gym
    """
    try:
        # 1. Check user eligibility: User must have < 3 session booking days total (any price, PAID only)
        # Match gym_studios.py lines 156-167
        user_session_count_stmt = (
            select(func.count())
            .select_from(SessionBookingDay)
            .join(SessionPurchase, SessionPurchase.id == SessionBookingDay.purchase_id)
            .where(
                SessionBookingDay.client_id == client_id,
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.status == "paid",  # Only count paid purchases
            )
        )
        user_session_result = await db.execute(user_session_count_stmt)
        user_session_count = user_session_result.scalar() or 0

        if user_session_count >= 3:
            logger.info(
                "SESSION_OFFER_INELIGIBLE_USER_LIMIT",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "user_booking_count": user_session_count,
                    "reason": "User has >= 3 session bookings"
                }
            )
            return False

        # 2. Check gym offer flags: Gym must have opted into the offer
        offer_stmt = select(NewOffer).where(NewOffer.gym_id == gym_id)
        offer_result = await db.execute(offer_stmt)
        offer_entry = offer_result.scalars().first()

        if not offer_entry or not offer_entry.session:
            logger.info(
                "SESSION_OFFER_INELIGIBLE_GYM_NOT_OPTED_IN",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "reason": "Gym has not opted into the session offer"
                }
            )
            return False

        # 3. Check gym cap: Gym must have < 50 unique users who booked at ₹99
        # Match gym_studios.py lines 212-234 (using distinct clients subquery)
        distinct_clients_subquery = (
            select(SessionPurchase.gym_id, SessionPurchase.client_id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .distinct()
        ).subquery()

        gym_promo_count_stmt = (
            select(func.count(distinct_clients_subquery.c.client_id))
        )
        gym_promo_result = await db.execute(gym_promo_count_stmt)
        gym_promo_count = gym_promo_result.scalar() or 0

        if gym_promo_count >= 50:
            logger.info(
                "SESSION_OFFER_INELIGIBLE_GYM_CAP_REACHED",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "gym_promo_user_count": gym_promo_count,
                    "reason": "Gym has >= 50 users who used the ₹99 offer"
                }
            )
            return False

        # 4. Check gym has sessions available (SessionSetting.is_enabled = True)
        # Match gym_studios.py line 1119 (session_settings check)
        session_settings_stmt = (
            select(SessionSetting)
            .where(
                SessionSetting.gym_id == gym_id,
                SessionSetting.is_enabled.is_(True),
                SessionSetting.final_price.isnot(None)
            )
            .limit(1)
        )
        session_settings_result = await db.execute(session_settings_stmt)
        has_sessions = session_settings_result.scalars().first() is not None

        if not has_sessions:
            logger.info(
                "SESSION_OFFER_INELIGIBLE_NO_SESSIONS",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "reason": "Gym has no enabled sessions"
                }
            )
            return False

        # 5. Check if user already booked ₹99 at this gym
        # Match gym_studios.py lines 271-285
        user_gym_promo_stmt = (
            select(SessionPurchase.id)
            .select_from(SessionPurchase)
            .join(SessionBookingDay, SessionBookingDay.purchase_id == SessionPurchase.id)
            .where(
                SessionPurchase.client_id == client_id,
                SessionPurchase.gym_id == gym_id,
                SessionPurchase.status == "paid",
                SessionBookingDay.status.in_(["booked", "attended", "no_show"]),
                SessionPurchase.price_per_session == 99,
            )
            .limit(1)
        )
        user_gym_promo_result = await db.execute(user_gym_promo_stmt)
        user_already_used_promo = user_gym_promo_result.scalars().first() is not None

        if user_already_used_promo:
            logger.info(
                "SESSION_OFFER_INELIGIBLE_ALREADY_USED",
                extra={
                    "client_id": client_id,
                    "gym_id": gym_id,
                    "reason": "User already used ₹99 offer at this gym"
                }
            )
            return False

        # All conditions met - user is eligible!
        logger.info(
            "SESSION_OFFER_ELIGIBLE",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "user_booking_count": user_session_count,
                "gym_promo_user_count": gym_promo_count,
                "slots_remaining": 50 - gym_promo_count
            }
        )
        return True

    except Exception as e:
        logger.error(
            "SESSION_OFFER_CHECK_ERROR",
            extra={
                "client_id": client_id,
                "gym_id": gym_id,
                "error": repr(e)
            }
        )
        # On error, default to not eligible (safer)
        return False


class DistributedLock:
    """Redis-based distributed lock to prevent race conditions."""

    def __init__(self, redis: Redis, key: str, ttl_seconds: int = 30):
        self.redis = redis
        self.key = f"lock:{key}"
        self.ttl = ttl_seconds
        self.token = uuid.uuid4().hex
        self._acquired = False

    def acquire(self) -> bool:
        """Attempt to acquire lock. Returns True if successful."""
        if self.redis is None:
            return True  # No Redis = no lock (graceful degradation)
        result = self.redis.set(self.key, self.token, nx=True, ex=self.ttl)
        self._acquired = bool(result)
        return self._acquired

    def release(self) -> bool:
        """Release lock only if we own it (prevents releasing another process's lock)."""
        if self.redis is None or not self._acquired:
            return True
        # Lua script for atomic check-and-delete
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            result = self.redis.eval(lua_script, 1, self.key, self.token)
            return bool(result)
        except Exception:
            return False

    def __enter__(self):
        if not self.acquire():
            raise HTTPException(
                status_code=409,
                detail="Another request is processing. Please retry."
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


@asynccontextmanager
async def distributed_lock(redis: Optional[Redis], key: str, ttl: int = 30):
    """Async context manager for distributed locking."""
    lock = DistributedLock(redis, key, ttl) if redis else None
    if lock and not lock.acquire():
        raise HTTPException(
            status_code=409,
            detail="Another request is being processed. Please retry in a moment."
        )
    try:
        yield lock
    finally:
        if lock:
            lock.release()


class SessionProcessor:
    """Runs session booking checkout + verification in Celery workers."""

    def __init__(
        self,
        config: HighConcurrencyConfig,
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
                logger.warning("SESSION_HOME_BOOKING_CACHE_DELETE_FAILED", extra={"gym_id": gid, "key": key})

    async def process_checkout(self, command_id: str, store) -> None:
        record = await store.mark_processing(command_id)
        payload = SessionCheckoutRequest(**record.payload)
        _start = time_module.perf_counter()
        pel.checkout_started(command_id=command_id, client_id=str(payload.client_id),
                             gym_id=payload.gym_id, session_type=payload.session_type)
        try:
            result = await self._execute_checkout(payload)
        except Exception as exc:  # pragma: no cover
            pel.checkout_failed(command_id=command_id, client_id=str(payload.client_id),
                                error_code=type(exc).__name__, error_detail=str(exc),
                                duration_ms=int((time_module.perf_counter() - _start) * 1000))
            logger.exception("Session checkout failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        pel.checkout_completed(command_id=command_id, client_id=str(payload.client_id),
                               duration_ms=int((time_module.perf_counter() - _start) * 1000),
                               gym_id=payload.gym_id, session_type=payload.session_type)
        pel.order_created(command_id=command_id, client_id=str(payload.client_id),
                          gym_id=payload.gym_id)
        await store.mark_completed(command_id, result)

    async def process_verify(self, command_id: str, store) -> None:
        record = await store.mark_processing(command_id)
        payload = SessionVerifyRequest(**record.payload)
        _start = time_module.perf_counter()
        pel.verify_started(command_id=command_id,
                           razorpay_payment_id=payload.razorpay_payment_id,
                           razorpay_order_id=payload.razorpay_order_id)
        try:
            capture_marker = await self._capture_marker_snapshot(payload.razorpay_payment_id)
            if capture_marker:
                logger.info(
                    "SESSION_VERIFY_CAPTURE_CACHE_HIT",
                    extra={
                        "payment_id": _mask(payload.razorpay_payment_id),
                        "order_id": capture_marker.get("order_id"),
                    },
                )
            result = await self._execute_verify(payload, capture_marker)
        except Exception as exc:  # pragma: no cover
            pel.verify_failed(command_id=command_id,
                              error_code=type(exc).__name__, error_detail=str(exc),
                              duration_ms=int((time_module.perf_counter() - _start) * 1000))
            logger.exception("Session verify failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        _dur = int((time_module.perf_counter() - _start) * 1000)
        if result.get("success"):
            pel.verify_completed(command_id=command_id, verify_path="session",
                                 duration_ms=_dur)
            pel.payment_captured(command_id=command_id,
                                 razorpay_payment_id=payload.razorpay_payment_id)
        else:
            pel.verify_failed(command_id=command_id,
                              error_code="verify_unsuccessful", duration_ms=_dur)
        await store.mark_completed(command_id, result)

    async def process_webhook(self, command_id: str, store) -> None:
        """Process Razorpay webhook for session payments."""
        record = await store.mark_processing(command_id)
        payload = record.payload
        try:
            result = await self._execute_webhook(payload)
        except Exception as exc:
            logger.exception("Session webhook failed: %s", exc)
            await store.mark_failed(command_id, str(exc))
            return
        await store.mark_completed(command_id, result)

    async def _execute_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Razorpay webhook payload for session payments."""
        raw_body = payload.get("raw_body", "")
        signature = payload.get("signature", "")
        settings = get_payment_settings()

        # Verify webhook signature
        expected_sig = hmac.new(
            settings.razorpay_webhook_secret.encode("utf-8"),
            raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, signature):
            logger.error("SESSION_WEBHOOK_INVALID_SIGNATURE")
            raise HTTPException(403, "Invalid webhook signature")

        # Parse webhook body
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            raise HTTPException(400, "Invalid webhook payload")

        event = body.get("event")
        payment_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
        order_id = payment_entity.get("order_id")
        payment_id = payment_entity.get("id")

        logger.info(
            "SESSION_WEBHOOK_RECEIVED",
            extra={
                "event": event,
                "order_id": order_id,
                "payment_id": payment_id,
            }
        )

        # Record capture marker for faster verify (before processing)
        if event == "payment.captured" and payment_id and order_id:
            await self._record_capture_marker(body, payment_id, order_id)

        # Handle payment.captured event
        if event == "payment.captured" and order_id:
            SessionLocal = create_celery_async_sessionmaker()
            async with SessionLocal() as db:
                purchase = (
                    await db.execute(
                        select(SessionPurchase)
                        .where(SessionPurchase.razorpay_order_id == order_id)
                        .with_for_update()
                    )
                ).scalars().first()

                if purchase and purchase.status != "paid":
                    purchase.status = "paid"
                    purchase.updated_at = datetime.now()

                    # Deduct reward if applicable
                    if purchase.reward_applied and purchase.reward_amount:
                        # Cast reward_amount to int to ensure exact deduction (no floating point)
                        reward_to_deduct = int(round(purchase.reward_amount))

                        cash_row = (
                            await db.execute(
                                select(ReferralFittbotCash)
                                .where(ReferralFittbotCash.client_id == purchase.client_id)
                                .with_for_update()
                            )
                        ).scalars().first()

                        if cash_row:
                            old_balance = int(cash_row.fittbot_cash) if cash_row.fittbot_cash else 0
                            new_cash = max(old_balance - reward_to_deduct, 0)
                            cash_row.fittbot_cash = new_cash
                            cash_row.updated_at = datetime.now()
                            logger.info(
                                "WEBHOOK_REWARD_DEDUCTED",
                                extra={
                                    "client_id": purchase.client_id,
                                    "old_balance": old_balance,
                                    "deducted": reward_to_deduct,
                                    "new_balance": new_cash,
                                    "order_id": order_id,
                                }
                            )

                    # Add audit entry
                    db.add(
                        SessionBookingAudit(
                            purchase_id=purchase.id,
                            event="webhook_payment_captured",
                            actor_role="system",
                            actor_id=None,
                            notes={"payment_id": payment_id, "webhook_event": event},
                        )
                    )
                    await db.commit()

                    logger.info(
                        "SESSION_WEBHOOK_PAYMENT_CAPTURED",
                        extra={
                            "order_id": order_id,
                            "payment_id": payment_id,
                            "client_id": purchase.client_id,
                        }
                    )

        return {"event": event, "order_id": order_id, "processed": True}

    async def _execute_checkout(self, payload: SessionCheckoutRequest) -> Dict[str, Any]:
        validation = await self._validate_and_price(payload)
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as session:  
            return await self._checkout_async(session, payload, validation)

    async def _execute_verify(self, payload: SessionVerifyRequest, capture_marker: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as session:
            return await self._verify_async(session, payload, capture_marker)

    async def _checkout_async(self, db: AsyncSession, payload: SessionCheckoutRequest, validation: Dict[str, Any]) -> Dict[str, Any]:
        notes = validation["notes"]
        payable_rupees = validation["payable_rupees"]
        reward_amount = validation["reward_amount"]
        total_rupees = validation["total_rupees"]
        scheduled_sessions = validation["scheduled_sessions"]

        # ═══════════════════════════════════════════════════════════════════
        # FIX #3: IDEMPOTENCY RACE - Use Redis distributed lock
        # Before: check-then-insert was NOT atomic (race window)
        # After: Lock on idempotency_key prevents concurrent duplicate creation
        # ═══════════════════════════════════════════════════════════════════
        lock_key = f"checkout:{payload.gym_id}:{payload.client_id}:{payload.session_id}:{payload.idempotency_key or uuid.uuid4().hex}"
        async with distributed_lock(self.redis, lock_key, CHECKOUT_LOCK_TTL):
            # Idempotency check (now safe under lock)
            if payload.idempotency_key:
                existing = (
                    await db.execute(
                        select(SessionPurchase).where(
                            SessionPurchase.gym_id == payload.gym_id,
                            SessionPurchase.client_id == payload.client_id,
                            SessionPurchase.session_id == payload.session_id,
                            SessionPurchase.trainer_id == payload.trainer_id,
                            SessionPurchase.idempotency_key == payload.idempotency_key,
                        )
                    )
                ).scalars().first()
                if existing:
                    idem_settings = get_payment_settings()
                    return {
                        "success": True,
                        "razorpayOrderId": existing.razorpay_order_id,
                        "razorpayKeyId": idem_settings.razorpay_key_id,
                        "orderId": existing.razorpay_order_id,
                        "amount": int(existing.payable_rupees * 100),
                        "amountMinor": int(existing.payable_rupees * 100),
                        "currency": "INR",
                        "rewardAmount": existing.reward_amount,
                        "totalRupees": existing.total_rupees,
                        "payableRupees": existing.payable_rupees,
                        "metadata": notes,
                        "idempotent": True,
                    }

            # ═══════════════════════════════════════════════════════════════════
            # FIX #1 & #5: OVERBOOKING + CAPACITY ENFORCEMENT
            # Before: No capacity check - could overbook slots
            # After: Check capacity with row lock BEFORE inserting bookings
            # ═══════════════════════════════════════════════════════════════════
            is_personal_trainer = payload.session_id == PERSONAL_TRAINER_SESSION_ID
            max_capacity = DEFAULT_CAPACITY_PERSONAL_TRAINER if is_personal_trainer else DEFAULT_CAPACITY_OTHER

            # Get custom capacity from settings (if configured)
            setting_row = (
                await db.execute(
                    select(SessionSetting).where(
                        SessionSetting.gym_id == payload.gym_id,
                        SessionSetting.session_id == payload.session_id,
                        SessionSetting.trainer_id == payload.trainer_id,
                    )
                )
            ).scalars().first()
            if setting_row and setting_row.capacity:
                max_capacity = setting_row.capacity

            # Check capacity for each scheduled session WITH ROW LOCK
            for entry in scheduled_sessions:
                dt = _parse_date(entry["date"])
                schedule_id = entry.get("schedule_id")

                # COUNT with FOR UPDATE locks the rows being counted
                # This prevents concurrent transactions from reading stale counts
                count_stmt = (
                    select(func.count(SessionBookingDay.id))
                    .where(
                        SessionBookingDay.schedule_id == schedule_id,
                        SessionBookingDay.booking_date == dt,
                        SessionBookingDay.status.in_(["booked", "attended"]),
                    )
                    .with_for_update()  # Row-level lock
                )
                current_count = (await db.execute(count_stmt)).scalar() or 0

                if current_count >= max_capacity:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Slot for {dt.isoformat()} is fully booked ({current_count}/{max_capacity})"
                    )

                logger.info(
                    "CAPACITY_CHECK_PASSED",
                    extra={
                        "schedule_id": schedule_id,
                        "date": dt.isoformat(),
                        "current": current_count,
                        "max": max_capacity,
                        "client_id": payload.client_id,
                    }
                )

            amount_minor = round(payable_rupees * 100)

            # Fetch session name from ClassSession using session_id
            class_session = (
                await db.execute(
                    select(ClassSession).where(ClassSession.id == payload.session_id)
                )
            ).scalars().first()
            session_name = class_session.internal if class_session and class_session.internal else (
                class_session.name if class_session else "session"
            )

            # Add session name to notes (order_metadata)
            notes["name"] = session_name

            # Generate receipt and get settings for Razorpay
            receipt = f"sess_{payload.client_id}_{int(datetime.now().timestamp())}"
            settings = get_payment_settings()

            pel.provider_call_started(command_id=receipt, provider_endpoint="create_order")
            _prov_start = time_module.perf_counter()
            try:
                order = await rzp_create_order(
                    amount_minor=amount_minor,
                    currency="INR",
                    receipt=receipt,
                    notes=notes,
                )
                pel.provider_call_completed(command_id=receipt, provider_endpoint="create_order",
                                            duration_ms=int((time_module.perf_counter() - _prov_start) * 1000))
            except Exception as prov_exc:
                pel.provider_call_failed(command_id=receipt, provider_endpoint="create_order",
                                         error_code=type(prov_exc).__name__,
                                         duration_ms=int((time_module.perf_counter() - _prov_start) * 1000))
                raise
            order_id = order.get("id")

            # Persist Order row for traceability
            order_row = Order(
                id=_new_id("ord_"),
                customer_id=str(payload.client_id),
                provider="razorpay_pg",
                provider_order_id=order_id,
                currency="INR",
                gross_amount_minor=amount_minor,
                status=StatusOrder.pending,
                order_metadata=notes,
            )
            db.add(order_row)
            await db.flush()

            # Create OrderItem for the session booking
            session_item = OrderItem(
                id=_new_id("itm_"),
                order_id=order_row.id,
                item_type=session_name,
                gym_id=str(payload.gym_id),
                trainer_id=str(payload.trainer_id) if payload.trainer_id else None,
                unit_price_minor=amount_minor,
                qty=payload.sessions_count,
                item_metadata={
                    "session_id": payload.session_id,
                    "name": session_name,
                    "trainer_id": payload.trainer_id,
                    "scheduled_sessions": scheduled_sessions,
                    "reward_applied": payload.reward,
                    "reward_amount": reward_amount,
                    "base_price_per_session": validation["base_price_per_session"],  # Gym owner's base price per session
                },
            )
            db.add(session_item)

            # Persist SessionPurchase + booking day rows
            purchase = SessionPurchase(
                razorpay_order_id=order_id,
                payment_order_pk=None,
                gym_id=payload.gym_id,
                client_id=payload.client_id,
                session_id=payload.session_id,
                trainer_id=payload.trainer_id,
                sessions_count=payload.sessions_count,
                scheduled_sessions=scheduled_sessions,
                reward_applied=payload.reward,
                reward_amount=reward_amount,
                total_rupees=total_rupees,
                payable_rupees=payable_rupees,
                price_per_session=validation["base_price_per_session"],  # Store original per-session price (99 for promo)
                idempotency_key=payload.idempotency_key,
                status="pending",
            )
            db.add(purchase)
            await db.flush()

            # NOTE: SessionBookingDay records are NOW created during verify (after payment)
            # This prevents counting unpaid/pending bookings towards offer eligibility

            # Audit - checkout initiated (booking days will be created after payment)
            db.add(
                SessionBookingAudit(
                    purchase_id=purchase.id,
                    event="checkout_initiated",
                    actor_role="client",
                    actor_id=payload.client_id,
                    notes={"scheduled_sessions": scheduled_sessions},
                )
            )

            await db.commit()

            return {
                "success": True,
                "razorpayOrderId": order_id,
                "razorpayKeyId": settings.razorpay_key_id,
                "orderId": order_row.id,
                "amount": amount_minor,
                "amountMinor": amount_minor,
                "currency": "INR",
                "rewardAmount": reward_amount,
                "totalRupees": total_rupees,
                "payableRupees": payable_rupees,
                "metadata": notes,
            }

    async def _verify_async(self, db: AsyncSession, payload: SessionVerifyRequest, capture_marker: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        settings = get_payment_settings()

        # Verify Razorpay signature (same as gym_membership)
        if not _verify_checkout_sig(
            settings.razorpay_key_secret,
            payload.razorpay_order_id,
            payload.razorpay_payment_id,
            payload.razorpay_signature,
        ):
            pel.verify_signature_invalid(command_id=payload.razorpay_order_id,
                                          razorpay_payment_id=payload.razorpay_payment_id)
            logger.error(f"[SESSION_VERIFY_ERROR] Invalid signature for order {payload.razorpay_order_id}")
            raise HTTPException(403, "Invalid signature")

        # Use capture marker if available (from webhook cache), else call Razorpay API
        if capture_marker:
            payment_data = capture_marker.copy()
            payment_data.setdefault("amount", capture_marker.get("amount"))
            payment_data.setdefault("currency", capture_marker.get("currency"))
            payment_data.setdefault("method", capture_marker.get("method"))
            payment_data.setdefault("order_id", capture_marker.get("order_id"))
            payment_data.setdefault("status", "captured")
        else:
            capture_marker = await self._await_capture_marker(payload.razorpay_payment_id)
            if capture_marker:
                payment_data = capture_marker.copy()
                payment_data.setdefault("amount", capture_marker.get("amount"))
                payment_data.setdefault("currency", capture_marker.get("currency"))
                payment_data.setdefault("method", capture_marker.get("method"))
                payment_data.setdefault("order_id", capture_marker.get("order_id"))
                payment_data.setdefault("status", "captured")
            else:
                logger.info(
                    "[SESSION_VERIFY_PROVIDER_FALLBACK]",
                    extra={
                        "payment_id": _mask(payload.razorpay_payment_id),
                        "order_id": payload.razorpay_order_id,
                        "redis_prefix": self.config.redis_prefix,
                    },
                )
                pel.provider_call_started(command_id=payload.razorpay_order_id,
                                           provider_endpoint="get_payment")
                _prov_start = time_module.perf_counter()
                try:
                    payment_data = await rzp_get_payment(payload.razorpay_payment_id)
                    pel.provider_call_completed(command_id=payload.razorpay_order_id,
                                                provider_endpoint="get_payment",
                                                duration_ms=int((time_module.perf_counter() - _prov_start) * 1000))
                except Exception as prov_exc:
                    pel.provider_call_failed(command_id=payload.razorpay_order_id,
                                             provider_endpoint="get_payment",
                                             error_code=type(prov_exc).__name__,
                                             duration_ms=int((time_module.perf_counter() - _prov_start) * 1000))
                    raise

        # MUST check payment captured status (same as dailypass/gym_membership)
        if payment_data.get("status") != "captured":
            logger.error(f"[SESSION_VERIFY_ERROR] Payment not captured: {payment_data.get('status')}")
            raise HTTPException(400, f"Payment not captured (status={payment_data.get('status')})")

        total_amount = payment_data.get("amount") if payment_data else None
        currency = payment_data.get("currency") if payment_data else "INR"
        gym_ids: Set[int] = set()

        # Check for existing payment (idempotency - same as dailypass/gym_membership)
        existing_payment = (
            await db.execute(
                select(Payment)
                .where(
                    Payment.provider_payment_id == payload.razorpay_payment_id,
                    Payment.status == StatusPayment.captured,
                )
            )
        ).scalars().first()

        if existing_payment:
            logger.info(f"[SESSION_EXISTING_PAYMENT] Payment {payload.razorpay_payment_id} already exists")
            # Check if purchase was already marked paid
            purchase_check = (
                await db.execute(
                    select(SessionPurchase)
                    .where(SessionPurchase.razorpay_order_id == payload.razorpay_order_id)
                    .with_for_update()
                )
            ).scalars().first()

            if purchase_check and purchase_check.gym_id:
                try:
                    gym_ids.add(int(purchase_check.gym_id))
                except Exception:
                    logger.warning("SESSION_GYM_ID_PARSE_FAILED", extra={"gym_id": purchase_check.gym_id, "purchase_id": purchase_check.id})

            if purchase_check and purchase_check.status == "paid":
                await self._invalidate_home_booking_cache(gym_ids)
                return {
                    "success": True,
                    "payment_captured": True,
                    "order_id": payload.razorpay_order_id,
                    "payment_id": payload.razorpay_payment_id,
                    "total_amount": total_amount,
                    "currency": "INR",
                    "message": "Payment already processed",
                }

            # Payment exists but purchase not marked paid - fix it now (same as dailypass/gym_membership)
            if purchase_check and purchase_check.status != "paid":
                logger.warning(f"[SESSION_MISSING_STATUS] Payment exists but purchase not paid! Fixing for order {payload.razorpay_order_id}")

                # Deduct reward if applicable
                if purchase_check.reward_applied and purchase_check.reward_amount:
                    # Cast reward_amount to int to ensure exact deduction (no floating point)
                    reward_to_deduct = int(round(purchase_check.reward_amount))

                    cash_row = (
                        await db.execute(
                            select(ReferralFittbotCash)
                            .where(ReferralFittbotCash.client_id == purchase_check.client_id)
                            .with_for_update()
                        )
                    ).scalars().first()

                    if cash_row:
                        old_balance = int(cash_row.fittbot_cash) if cash_row.fittbot_cash else 0
                        new_cash = max(old_balance - reward_to_deduct, 0)
                        cash_row.fittbot_cash = new_cash
                        cash_row.updated_at = datetime.now()
                        logger.info(f"[SESSION_REWARD_DEDUCTED_RECOVERY] old={old_balance}, deducted={reward_to_deduct}, new={new_cash} for client {purchase_check.client_id}")

                purchase_check.status = "paid"
                purchase_check.updated_at = datetime.now()

                # Create booking days if they don't exist (recovery scenario)
                existing_booking_days = (
                    await db.execute(
                        select(SessionBookingDay)
                        .where(SessionBookingDay.purchase_id == purchase_check.id)
                    )
                ).scalars().all()

                if not existing_booking_days:
                    booking_rows = []
                    scheduled_sessions = purchase_check.scheduled_sessions or []
                    for entry in scheduled_sessions:
                        dt = _parse_date(entry["date"])
                        start_time_val = _parse_time(entry.get("start_time"))
                        end_time_val = _parse_time(entry.get("end_time"))
                        schedule_id = entry.get("schedule_id")
                        booking_rows.append(
                            SessionBookingDay(
                                purchase_id=purchase_check.id,
                                schedule_id=schedule_id,
                                gym_id=purchase_check.gym_id,
                                client_id=purchase_check.client_id,
                                session_id=purchase_check.session_id,
                                trainer_id=purchase_check.trainer_id,
                                booking_date=dt,
                                start_time=start_time_val,
                                end_time=end_time_val,
                                status="booked",
                                checkin_token=uuid.uuid4().hex,
                            )
                        )
                    db.add_all(booking_rows)
                    logger.info(f"[SESSION_RECOVERY_BOOKING_DAYS] Created {len(booking_rows)} booking days for purchase {purchase_check.id}")

                db.add(
                    SessionBookingAudit(
                        purchase_id=purchase_check.id,
                        event="payment_captured_recovery",
                        actor_role="system",
                        actor_id=None,
                        notes={"payment_id": payload.razorpay_payment_id, "recovery": True},
                    )
                )
                await db.commit()
                logger.info(f"[SESSION_RECOVERY_COMPLETE] Fixed purchase status for order {payload.razorpay_order_id}")
                await self._invalidate_home_booking_cache(gym_ids)

                return {
                    "success": True,
                    "payment_captured": True,
                    "order_id": payload.razorpay_order_id,
                    "payment_id": payload.razorpay_payment_id,
                    "total_amount": total_amount,
                    "currency": "INR",
                    "message": "Payment recovered and processed",
                }

        # ═══════════════════════════════════════════════════════════════════
        # FIX #4: PAYMENT VERIFY RACE - Use SELECT FOR UPDATE
        # Before: Status check-then-update was NOT atomic (race window)
        # After: Row lock prevents concurrent verify from double-processing
        # ═══════════════════════════════════════════════════════════════════
        purchase_row = (
            await db.execute(
                select(SessionPurchase)
                .where(SessionPurchase.razorpay_order_id == payload.razorpay_order_id)
                .with_for_update()  # ROW LOCK - blocks other transactions
            )
        ).scalars().first()
        if purchase_row and purchase_row.gym_id:
            try:
                gym_ids.add(int(purchase_row.gym_id))
            except Exception:
                logger.warning("SESSION_GYM_ID_PARSE_FAILED", extra={"gym_id": purchase_row.gym_id, "purchase_id": purchase_row.id})

        # Fetch Order row by provider_order_id (Razorpay order ID)
        order_row = (
            await db.execute(
                select(Order)
                .where(Order.provider_order_id == payload.razorpay_order_id)
                .with_for_update()
            )
        ).scalars().first()

        # Fetch session name from ClassSession for payment metadata
        session_name = "session"
        if purchase_row and purchase_row.session_id:
            class_session = (
                await db.execute(
                    select(ClassSession).where(ClassSession.id == purchase_row.session_id)
                )
            ).scalars().first()
            session_name = class_session.internal if class_session and class_session.internal else (
                class_session.name if class_session else "session"
            )

        # Create payment row with correct fields (matching gym_membership pattern)
        # Use order_row.customer_id like dailypass/gym_membership does
        pay_row = Payment(
            id=_new_id("pay_"),
            order_id=order_row.id if order_row else None,
            customer_id=order_row.customer_id if order_row else str(purchase_row.client_id) if purchase_row else None,
            provider="razorpay_pg",
            provider_payment_id=payload.razorpay_payment_id,
            amount_minor=total_amount,
            currency=currency,
            status=StatusPayment.captured,
            captured_at=datetime.now(),
            payment_metadata={
                "method": payment_data.get("method"),
                "source": "session_verify",
                "razorpay_order_id": payload.razorpay_order_id,
                "name": session_name,
                "session_id": purchase_row.session_id if purchase_row else None,
            },
        )
        db.add(pay_row)

        # Update Order status to paid (matching gym_membership pattern)
        if order_row:
            order_row.status = StatusOrder.paid
            db.add(order_row)

        await db.flush()

        if purchase_row:
            # Check status UNDER LOCK - prevents double-processing
            if purchase_row.status == "paid":
                # Already processed - idempotent success
                logger.info(
                    "VERIFY_IDEMPOTENT_SKIP",
                    extra={
                        "order_id": payload.razorpay_order_id,
                        "payment_id": payload.razorpay_payment_id,
                        "client_id": purchase_row.client_id,
                    }
                )
                return {
                    "success": True,
                    "payment_captured": True,
                    "order_id": payload.razorpay_order_id,
                    "payment_id": payload.razorpay_payment_id,
                    "total_amount": total_amount,
                    "currency": "INR",
                    "message": "Payment already processed",
                }

            # Mark as paid ATOMICALLY (still under row lock)
            purchase_row.status = "paid"
            purchase_row.updated_at = datetime.now()
            await db.flush()  # Keep transaction open for reward deduction

        if purchase_row:
            # ═══════════════════════════════════════════════════════════════════
            # CREATE SessionBookingDay records NOW (after payment confirmed)
            # Previously created during checkout - caused unpaid bookings to be counted
            # ═══════════════════════════════════════════════════════════════════
            # Check if booking days already exist (backward compatibility for old flow)
            existing_booking_days = (
                await db.execute(
                    select(SessionBookingDay)
                    .where(SessionBookingDay.purchase_id == purchase_row.id)
                )
            ).scalars().all()

            if not existing_booking_days:
                # Create booking days from scheduled_sessions stored in purchase
                booking_rows = []
                scheduled_sessions = purchase_row.scheduled_sessions or []
                for entry in scheduled_sessions:
                    dt = _parse_date(entry["date"])
                    start_time_val = _parse_time(entry.get("start_time"))
                    end_time_val = _parse_time(entry.get("end_time"))
                    schedule_id = entry.get("schedule_id")
                    booking_rows.append(
                        SessionBookingDay(
                            purchase_id=purchase_row.id,
                            schedule_id=schedule_id,
                            gym_id=purchase_row.gym_id,
                            client_id=purchase_row.client_id,
                            session_id=purchase_row.session_id,
                            trainer_id=purchase_row.trainer_id,
                            booking_date=dt,
                            start_time=start_time_val,
                            end_time=end_time_val,
                            status="booked",
                            checkin_token=uuid.uuid4().hex,
                        )
                    )
                db.add_all(booking_rows)
                await db.flush()
                logger.info(f"[SESSION_BOOKING_DAYS_CREATED] Created {len(booking_rows)} booking days for purchase {purchase_row.id}")

            # Audit entry
            db.add(
                SessionBookingAudit(
                    purchase_id=purchase_row.id,
                    event="payment_captured",
                    actor_role="system",
                    actor_id=None,
                    notes={"payment_id": payload.razorpay_payment_id},
                )
            )

            # ═══════════════════════════════════════════════════════════════════
            # Create fittbot_payments.Payment for each booking day
            # - entitlement_id = checkin_token (used later during scan to create Payout)
            # - source_type = ClassSession.internal (dynamic, not hardcoded)
            # ═══════════════════════════════════════════════════════════════════
            booking_days = (
                await db.execute(
                    select(SessionBookingDay)
                    .where(SessionBookingDay.purchase_id == purchase_row.id)
                )
            ).scalars().all()

            # Get base price per session from OrderItem metadata
            order_items = await db.execute(
                select(OrderItem).where(OrderItem.order_id == order_row.id)
            )
            base_price_per_session = 0
            for order_item in order_items.scalars().all():
                if order_item.item_metadata and order_item.item_metadata.get("session_id") == purchase_row.session_id:
                    base_price_per_session = int(order_item.item_metadata.get("base_price_per_session", 0))
                    break

            # Update price_per_session on purchase if not set (backward compatibility for old records)
            if base_price_per_session > 0 and not purchase_row.price_per_session:
                purchase_row.price_per_session = base_price_per_session
                logger.info(
                    "PRICE_PER_SESSION_SET_ON_VERIFY",
                    extra={
                        "purchase_id": purchase_row.id,
                        "price_per_session": base_price_per_session,
                    }
                )

            # Calculate per-session amounts
            client_paid_total = purchase_row.payable_rupees or purchase_row.total_rupees
            client_paid_per_session = client_paid_total / purchase_row.sessions_count if purchase_row.sessions_count else 0
            client_paid_per_session_rounded = int(round(client_paid_per_session))

            # Gym owner gets base price (without 30% markup)
            gym_owner_per_session = base_price_per_session if base_price_per_session > 0 else int(round(client_paid_per_session_rounded / get_markup_multiplier()))

            for booking_day in booking_days:
                fittbot_payment = FittbotPayment(
                    source_type=session_name,  # ClassSession.internal (e.g., "yoga", "zumba", etc.)
                    source_id=str(purchase_row.session_id),
                    booking_day_id=booking_day.id,
                    purchase_id=purchase_row.id,
                    entitlement_id=booking_day.checkin_token,  # Used during scan to find this Payment
                    gym_id=purchase_row.gym_id,
                    client_id=purchase_row.client_id,
                    session_id=purchase_row.session_id,
                    amount_gross=client_paid_per_session_rounded,  # What client paid per session (WITH 30% markup)
                    amount_net=gym_owner_per_session,              # What gym owner gets per session (WITHOUT markup)
                    currency="INR",
                    gateway="razorpay",
                    gateway_payment_id=payload.razorpay_payment_id,
                    status="paid",
                    paid_at=datetime.now(),
                )
                db.add(fittbot_payment)

            logger.info(
                "FITTBOT_PAYMENTS_CREATED",
                extra={
                    "purchase_id": purchase_row.id,
                    "booking_days_count": len(booking_days),
                    "source_type": session_name,
                    "per_session_amount": client_paid_per_session_rounded,
                    "gym_owner_per_session": gym_owner_per_session,
                }
            )

            try:
                if purchase_row.client_id is not None:
                    sessions_count = purchase_row.sessions_count or 1
                    reward_ok, entries_added, reward_msg = await add_session_entry(
                        db,
                        client_id=int(purchase_row.client_id),
                        source_id=payload.razorpay_payment_id,
                        sessions_count=sessions_count,
                    )
                    pel.side_effect_success(command_id=payload.razorpay_order_id,
                                            side_effect="reward_session",
                                            client_id=str(purchase_row.client_id))
                    logger.info(
                        "SESSION_REWARD_ENTRY",
                        extra={
                            "client_id": purchase_row.client_id,
                            "success": reward_ok,
                            "entries_added": entries_added,
                            "reward_msg": reward_msg,
                            "purchase_id": purchase_row.id,
                        },
                    )
                else:
                    pel.side_effect_skipped(command_id=payload.razorpay_order_id,
                                            side_effect="reward_session", reason="missing_client_id")
                    logger.info(
                        "SESSION_REWARD_ENTRY_SKIPPED",
                        extra={"reason": "missing_client_id", "purchase_id": purchase_row.id},
                    )
            except Exception as reward_exc:
                pel.side_effect_failed(command_id=payload.razorpay_order_id,
                                       side_effect="reward_session", error_detail=str(reward_exc),
                                       client_id=str(purchase_row.client_id) if purchase_row else None)
                logger.warning(
                    "SESSION_REWARD_ENTRY_FAILED",
                    extra={
                        "client_id": purchase_row.client_id if purchase_row else None,
                        "purchase_id": purchase_row.id if purchase_row else None,
                        "error": repr(reward_exc),
                    },
                )

            if purchase_row.reward_applied and purchase_row.reward_amount:
 
                reward_to_deduct = int(round(purchase_row.reward_amount))
                cash_row = (
                    await db.execute(
                        select(ReferralFittbotCash)
                        .where(ReferralFittbotCash.client_id == purchase_row.client_id)
                        .with_for_update() 
                    )
                ).scalars().first()

                if cash_row:
                    old_balance = int(cash_row.fittbot_cash) if cash_row.fittbot_cash else 0
                    new_cash = max(old_balance - reward_to_deduct, 0)
                    cash_row.fittbot_cash = new_cash
                    cash_row.updated_at = datetime.now()

                    logger.info(
                        "REWARD_DEDUCTED_ATOMIC",
                        extra={
                            "client_id": purchase_row.client_id,
                            "old_balance": old_balance,
                            "deducted": reward_to_deduct,
                            "new_balance": new_cash,
                            "order_id": payload.razorpay_order_id,
                        }
                    )

                    db.add(
                        SessionBookingAudit(
                            purchase_id=purchase_row.id,
                            event="reward_deducted",
                            actor_role="system",
                            notes={
                                "reward_amount": reward_to_deduct,
                                "old_balance": old_balance,
                                "new_balance": new_cash,
                            },
                        )
                    )

            try:
                await db.commit()
            except IntegrityError as e:
                # Race condition: another request already created the records
                if "Duplicate entry" in str(e) or "checkin_token" in str(e) or "uq_session_purchase" in str(e):
                    await db.rollback()
                    logger.info(f"[SESSION_DUPLICATE_DETECTED] Duplicate entry detected for order {payload.razorpay_order_id}, returning already_processed")
                    return {
                        "success": True,
                        "payment_captured": True,
                        "order_id": payload.razorpay_order_id,
                        "payment_id": payload.razorpay_payment_id,
                        "total_amount": total_amount,
                        "currency": "INR",
                        "message": "Payment already processed",
                    }
                await db.rollback()
                raise

            await self._invalidate_home_booking_cache(gym_ids)

            # Queue owner notification (fire-and-forget, never blocks payment flow)
            try:
                if purchase_row:
                    # Get starting date from scheduled_sessions
                    starting_date = None
                    scheduled = purchase_row.scheduled_sessions or []
                    if scheduled and scheduled[0].get("date"):
                        starting_date = _parse_date(scheduled[0]["date"])
                    queue_session_notification(
                        gym_id=int(purchase_row.gym_id) if purchase_row.gym_id else 0,
                        client_id=int(purchase_row.client_id) if purchase_row.client_id else 0,
                        amount=purchase_row.payable_rupees or purchase_row.total_rupees or 0,
                        session_name=session_name,
                        sessions_count=purchase_row.sessions_count or 1,
                        starting_date=starting_date,
                    )
            except Exception as e:
                logger.warning(f"[SESSION_NOTIFICATION_ERROR] Failed to queue owner notification: {e}")

        self._process_business_operations_stub(payload, purchase_row)

        return {
            "success": True,
            "payment_captured": True,
            "order_id": payload.razorpay_order_id,
            "payment_id": payload.razorpay_payment_id,
            "total_amount": total_amount,
            "currency": "INR",
            "message": "Payment verified and session booking confirmed",
        }

    def _process_business_operations_stub(self, payload: SessionVerifyRequest, purchase_row) -> None:

        logger.info(
            "SESSION_BUSINESS_STUB",
            extra={
                "gym_id": purchase_row.gym_id if purchase_row else payload.gym_id,
                "client_id": purchase_row.client_id if purchase_row else payload.client_id,
                "session_id": purchase_row.session_id if purchase_row else payload.session_id,
                "trainer_id": purchase_row.trainer_id if purchase_row else payload.trainer_id,
                "order_id": payload.razorpay_order_id,
            },
        )

    async def fulfill_from_webhook(
        self, razorpay_order_id: str, payment_id: str, payment_data: Dict[str, Any]
    ) -> None:
        """Called by WebhookProcessor on payment.captured for session orders.

        Performs the same fulfillment as _verify_async but without needing
        client-provided signature (webhook already verified by Razorpay).
        Creates Payment, SessionBookingDay, FittbotPayment records, deducts reward,
        adds reward entries, queues notification. Fully idempotent.
        """
        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as db:
            # Find SessionPurchase by razorpay_order_id
            purchase_row = (
                await db.execute(
                    select(SessionPurchase)
                    .where(SessionPurchase.razorpay_order_id == razorpay_order_id)
                    .with_for_update()
                )
            ).scalars().first()
            if not purchase_row:
                logger.warning(
                    "SESSION_WEBHOOK_FULFILL_PURCHASE_NOT_FOUND",
                    extra={"razorpay_order_id": razorpay_order_id, "payment_id": _mask(payment_id)},
                )
                return

            # Idempotency: already paid + booking days exist
            if purchase_row.status == "paid":
                existing_booking_days = (
                    await db.execute(
                        select(SessionBookingDay)
                        .where(SessionBookingDay.purchase_id == purchase_row.id)
                    )
                ).scalars().all()
                if existing_booking_days:
                    logger.info(
                        "SESSION_WEBHOOK_FULFILL_ALREADY_DONE",
                        extra={"purchase_id": purchase_row.id, "razorpay_order_id": razorpay_order_id},
                    )
                    return

            # Find Order row
            order_row = (
                await db.execute(
                    select(Order)
                    .where(Order.provider_order_id == razorpay_order_id)
                    .with_for_update()
                )
            ).scalars().first()

            gym_ids: Set[int] = set()
            if purchase_row.gym_id:
                try:
                    gym_ids.add(int(purchase_row.gym_id))
                except Exception:
                    pass

            paid_amount = int(payment_data.get("amount") or (order_row.gross_amount_minor if order_row else 0))

            try:
                # Check if Payment already exists
                existing_payment = (
                    await db.execute(
                        select(Payment).where(
                            Payment.provider_payment_id == payment_id,
                            Payment.status == StatusPayment.captured,
                        )
                    )
                ).scalars().first()

                # Fetch session name
                session_name = "session"
                if purchase_row.session_id:
                    class_session = (
                        await db.execute(
                            select(ClassSession).where(ClassSession.id == purchase_row.session_id)
                        )
                    ).scalars().first()
                    session_name = class_session.internal if class_session and class_session.internal else (
                        class_session.name if class_session else "session"
                    )

                # Create Payment record if not exists
                if not existing_payment:
                    pay_row = Payment(
                        id=_new_id("pay_"),
                        order_id=order_row.id if order_row else None,
                        customer_id=order_row.customer_id if order_row else str(purchase_row.client_id),
                        provider="razorpay_pg",
                        provider_payment_id=payment_id,
                        amount_minor=paid_amount,
                        currency=payment_data.get("currency", "INR"),
                        status=StatusPayment.captured,
                        captured_at=datetime.now(),
                        payment_metadata={
                            "method": payment_data.get("method"),
                            "source": "webhook_fulfillment_session",
                            "razorpay_order_id": razorpay_order_id,
                            "name": session_name,
                            "session_id": purchase_row.session_id,
                        },
                    )
                    db.add(pay_row)

                # Update Order status
                if order_row and order_row.status != StatusOrder.paid:
                    order_row.status = StatusOrder.paid
                    db.add(order_row)

                # Mark purchase as paid
                if purchase_row.status != "paid":
                    purchase_row.status = "paid"
                    purchase_row.updated_at = datetime.now()

                await db.flush()

                # Create SessionBookingDay records (if not already created)
                existing_booking_days = (
                    await db.execute(
                        select(SessionBookingDay)
                        .where(SessionBookingDay.purchase_id == purchase_row.id)
                    )
                ).scalars().all()

                if not existing_booking_days:
                    booking_rows = []
                    scheduled_sessions = purchase_row.scheduled_sessions or []
                    for entry in scheduled_sessions:
                        dt = _parse_date(entry["date"])
                        start_time_val = _parse_time(entry.get("start_time"))
                        end_time_val = _parse_time(entry.get("end_time"))
                        schedule_id = entry.get("schedule_id")
                        booking_rows.append(
                            SessionBookingDay(
                                purchase_id=purchase_row.id,
                                schedule_id=schedule_id,
                                gym_id=purchase_row.gym_id,
                                client_id=purchase_row.client_id,
                                session_id=purchase_row.session_id,
                                trainer_id=purchase_row.trainer_id,
                                booking_date=dt,
                                start_time=start_time_val,
                                end_time=end_time_val,
                                status="booked",
                                checkin_token=uuid.uuid4().hex,
                            )
                        )
                    db.add_all(booking_rows)
                    await db.flush()
                    logger.info(
                        "SESSION_WEBHOOK_BOOKING_DAYS_CREATED",
                        extra={"purchase_id": purchase_row.id, "count": len(booking_rows)},
                    )

                # Create FittbotPayment for each booking day
                booking_days = (
                    await db.execute(
                        select(SessionBookingDay)
                        .where(SessionBookingDay.purchase_id == purchase_row.id)
                    )
                ).scalars().all()

                # Get base price per session from OrderItem metadata
                base_price_per_session = 0
                if order_row:
                    order_items = await db.execute(
                        select(OrderItem).where(OrderItem.order_id == order_row.id)
                    )
                    for order_item in order_items.scalars().all():
                        if order_item.item_metadata and order_item.item_metadata.get("session_id") == purchase_row.session_id:
                            base_price_per_session = int(order_item.item_metadata.get("base_price_per_session", 0))
                            break

                # Update price_per_session on purchase if not set
                if base_price_per_session > 0 and not purchase_row.price_per_session:
                    purchase_row.price_per_session = base_price_per_session

                # Calculate per-session amounts
                client_paid_total = purchase_row.payable_rupees or purchase_row.total_rupees
                client_paid_per_session = client_paid_total / purchase_row.sessions_count if purchase_row.sessions_count else 0
                client_paid_per_session_rounded = int(round(client_paid_per_session))
                gym_owner_per_session = base_price_per_session if base_price_per_session > 0 else int(round(client_paid_per_session_rounded / get_markup_multiplier()))

                for booking_day in booking_days:
                    # Check if FittbotPayment already exists for this booking day
                    existing_fp = (
                        await db.execute(
                            select(FittbotPayment).where(
                                FittbotPayment.entitlement_id == booking_day.checkin_token
                            )
                        )
                    ).scalars().first()
                    if existing_fp:
                        continue
                    fittbot_payment = FittbotPayment(
                        source_type=session_name,
                        source_id=str(purchase_row.session_id),
                        booking_day_id=booking_day.id,
                        purchase_id=purchase_row.id,
                        entitlement_id=booking_day.checkin_token,
                        gym_id=purchase_row.gym_id,
                        client_id=purchase_row.client_id,
                        session_id=purchase_row.session_id,
                        amount_gross=client_paid_per_session_rounded,
                        amount_net=gym_owner_per_session,
                        currency="INR",
                        gateway="razorpay",
                        gateway_payment_id=payment_id,
                        status="paid",
                        paid_at=datetime.now(),
                    )
                    db.add(fittbot_payment)

                # Audit entry
                db.add(
                    SessionBookingAudit(
                        purchase_id=purchase_row.id,
                        event="webhook_fulfillment",
                        actor_role="system",
                        actor_id=None,
                        notes={"payment_id": payment_id, "source": "webhook"},
                    )
                )

                # Reward program entries (best-effort)
                try:
                    if purchase_row.client_id is not None:
                        sessions_count = purchase_row.sessions_count or 1
                        reward_ok, entries_added, reward_msg = await add_session_entry(
                            db,
                            client_id=int(purchase_row.client_id),
                            source_id=payment_id,
                            sessions_count=sessions_count,
                        )
                        logger.info(
                            "SESSION_WEBHOOK_REWARD_ENTRY",
                            extra={
                                "client_id": purchase_row.client_id,
                                "success": reward_ok,
                                "entries_added": entries_added,
                                "purchase_id": purchase_row.id,
                            },
                        )
                except Exception as reward_exc:
                    logger.warning(
                        "SESSION_WEBHOOK_REWARD_ENTRY_FAILED",
                        extra={
                            "client_id": purchase_row.client_id,
                            "purchase_id": purchase_row.id,
                            "error": repr(reward_exc),
                        },
                    )

                # Deduct reward if applicable
                if purchase_row.reward_applied and purchase_row.reward_amount:
                    reward_to_deduct = int(round(purchase_row.reward_amount))
                    cash_row = (
                        await db.execute(
                            select(ReferralFittbotCash)
                            .where(ReferralFittbotCash.client_id == purchase_row.client_id)
                            .with_for_update()
                        )
                    ).scalars().first()

                    if cash_row:
                        old_balance = int(cash_row.fittbot_cash) if cash_row.fittbot_cash else 0
                        new_cash = max(old_balance - reward_to_deduct, 0)
                        cash_row.fittbot_cash = new_cash
                        cash_row.updated_at = datetime.now()
                        logger.info(
                            "SESSION_WEBHOOK_REWARD_DEDUCTED",
                            extra={
                                "client_id": purchase_row.client_id,
                                "old_balance": old_balance,
                                "deducted": reward_to_deduct,
                                "new_balance": new_cash,
                            },
                        )
                        db.add(
                            SessionBookingAudit(
                                purchase_id=purchase_row.id,
                                event="webhook_reward_deducted",
                                actor_role="system",
                                notes={
                                    "reward_amount": reward_to_deduct,
                                    "old_balance": old_balance,
                                    "new_balance": new_cash,
                                },
                            )
                        )

                await db.commit()
                await self._invalidate_home_booking_cache(gym_ids)

                # Queue owner notification (fire-and-forget)
                try:
                    starting_date = None
                    scheduled = purchase_row.scheduled_sessions or []
                    if scheduled and scheduled[0].get("date"):
                        starting_date = _parse_date(scheduled[0]["date"])
                    queue_session_notification(
                        gym_id=int(purchase_row.gym_id) if purchase_row.gym_id else 0,
                        client_id=int(purchase_row.client_id) if purchase_row.client_id else 0,
                        amount=purchase_row.payable_rupees or purchase_row.total_rupees or 0,
                        session_name=session_name,
                        sessions_count=purchase_row.sessions_count or 1,
                        starting_date=starting_date,
                    )
                except Exception as e:
                    logger.warning(f"[SESSION_WEBHOOK_NOTIFICATION_ERROR] {e}")

                logger.info(
                    "SESSION_WEBHOOK_FULFILL_SUCCESS",
                    extra={
                        "purchase_id": purchase_row.id,
                        "razorpay_order_id": razorpay_order_id,
                        "payment_id": _mask(payment_id),
                        "booking_days_count": len(booking_days),
                        "session_name": session_name,
                    },
                )

            except IntegrityError as e:
                await db.rollback()
                if "Duplicate entry" in str(e) or "checkin_token" in str(e) or "uq_session_purchase" in str(e):
                    logger.info(
                        "SESSION_WEBHOOK_FULFILL_DUPLICATE",
                        extra={"purchase_id": purchase_row.id, "razorpay_order_id": razorpay_order_id},
                    )
                    return
                raise

    async def _validate_and_price(self, payload: SessionCheckoutRequest) -> Dict[str, Any]:
        if payload.sessions_count <= 0:
            raise HTTPException(400, "sessions_count must be > 0")

        if not payload.scheduled_dates or len(payload.scheduled_dates) != payload.sessions_count:
            raise HTTPException(400, f"scheduled_dates count ({len(payload.scheduled_dates) if payload.scheduled_dates else 0}) does not match sessions_count ({payload.sessions_count})")

        SessionLocal = create_celery_async_sessionmaker()
        async with SessionLocal() as session:
            setting = await session.execute(
                select(SessionSetting).where(
                    SessionSetting.gym_id == payload.gym_id,
                    SessionSetting.session_id == payload.session_id,
                    SessionSetting.trainer_id == payload.trainer_id,
                    SessionSetting.is_enabled.is_(True),
                    SessionSetting.final_price.isnot(None),
                )
            )
            setting_row = setting.scalars().first()
            if not setting_row or not setting_row.final_price:
                raise HTTPException(404, "Session not available or not priced")

            scheduled_sessions = []

            if payload.session_type == SessionType.custom:
                # Custom: Use custom_slot dict to build scheduled_sessions
                if not payload.custom_slot:
                    raise HTTPException(400, "custom_slot is required when session_type is 'custom'")

                for date_str in payload.scheduled_dates:
                    if date_str not in payload.custom_slot:
                        raise HTTPException(400, f"Missing custom_slot for date {date_str}")

                    slots = payload.custom_slot[date_str]
                    if not slots or len(slots) == 0:
                        raise HTTPException(400, f"Empty slot for date {date_str}")

                    # Take the first slot for this date
                    slot = slots[0]
                    scheduled_sessions.append({
                        "date": date_str,
                        "start_time": slot.get("start_time"),
                        "schedule_id": slot.get("schedule_id"),
                        "end_time": None,
                    })

                logger.info(
                    "SESSION_TYPE_CUSTOM",
                    extra={
                        "client_id": payload.client_id,
                        "dates_count": len(scheduled_sessions),
                        "session_type": "custom",
                    }
                )

            else:  # same_time (default)
                # Same time: All dates use default_slot, need to find schedule_id from DB
                if not payload.default_slot:
                    raise HTTPException(400, "default_slot is required when session_type is 'same_time'")

                # Get all active schedules for this session to match the default_slot time
                schedules_result = await session.execute(
                    select(SessionSchedule).where(
                        SessionSchedule.gym_id == payload.gym_id,
                        SessionSchedule.session_id == payload.session_id,
                        SessionSchedule.trainer_id == payload.trainer_id,
                        SessionSchedule.is_active.is_(True),
                    )
                )
                all_schedules = schedules_result.scalars().all()

                # Build a lookup for schedule_id by matching start_time
                # Parse default_slot time (e.g., "02:00 PM")
                default_slot_normalized = payload.default_slot.strip().upper()

                for date_str in payload.scheduled_dates:
                    dt = _parse_date(date_str)
                    weekday = dt.weekday()

                    # Find matching schedule for this date's weekday and time
                    matching_schedule_id = None
                    for sch in all_schedules:
                        # Check if schedule applies to this weekday
                        if sch.weekday is not None and sch.weekday != weekday:
                            continue

                        # Check date bounds
                        if sch.start_date and dt < sch.start_date:
                            continue
                        if sch.end_date and dt > sch.end_date:
                            continue

                        # Match the start_time with default_slot
                        if sch.start_time:
                            # Format schedule start_time to match default_slot format
                            sch_time_str = sch.start_time.strftime("%I:%M %p").upper().lstrip("0")
                            if sch_time_str == default_slot_normalized or sch.start_time.strftime("%I:%M %p").upper() == default_slot_normalized:
                                matching_schedule_id = sch.id
                                break

                    if not matching_schedule_id:
                        raise HTTPException(
                            409,
                            f"No matching schedule found for date {date_str} with slot {payload.default_slot}"
                        )

                    scheduled_sessions.append({
                        "date": date_str,
                        "start_time": payload.default_slot,
                        "schedule_id": matching_schedule_id,
                        "end_time": None,
                    })

                logger.info(
                    "SESSION_TYPE_SAME_TIME",
                    extra={
                        "client_id": payload.client_id,
                        "dates_count": len(scheduled_sessions),
                        "default_slot": payload.default_slot,
                        "session_type": "same_time",
                    }
                )

            # Validate all dates are allowed
            allowed_dates = await self._collect_allowed_dates_async(session, payload)
            for entry in scheduled_sessions:
                dt = _parse_date(entry["date"])
                if dt not in allowed_dates:
                    raise HTTPException(409, f"Date {dt} not allowed for this session")

            # Calculate offer eligibility based on client_id and gym_id (don't trust client input)
            is_offer_eligible = False
            try:
                client_id_int = int(payload.client_id)
                is_offer_eligible = await _check_session_offer_eligibility(session, client_id_int, payload.gym_id)
                logger.info(
                    "SESSION_CHECKOUT_ELIGIBILITY_CHECK",
                    extra={
                        "client_id": client_id_int,
                        "gym_id": payload.gym_id,
                        "session_id": payload.session_id,
                        "is_offer_eligible": is_offer_eligible,
                    }
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "SESSION_CHECKOUT_INVALID_CLIENT_ID",
                    extra={
                        "client_id": payload.client_id,
                        "gym_id": payload.gym_id,
                        "error": repr(e),
                    }
                )

            # Calculate pricing
            base_price = setting_row.base_price or 0
            discount_percent = setting_row.discount_percent or 0
            final_price = setting_row.final_price

            # If client is eligible for offer, force ₹99 pricing (no markup)
            # This takes precedence over gym's configured pricing
            if is_offer_eligible:
                final_price = 99
                final_price_with_markup = 99
                logger.info(
                    "SESSION_OFFER_APPLIED",
                    extra={
                        "client_id": payload.client_id,
                        "gym_id": payload.gym_id,
                        "session_id": payload.session_id,
                        "offer_price": 99,
                        "original_price": setting_row.final_price,
                    }
                )
            # Skip 30% markup if final_price is exactly 99 rupees (gym-configured offer)
            elif final_price == 99:
                final_price_with_markup = 99
            else:
                # Apply 30% markup to final price for client payment
                final_price_with_markup = round(final_price * get_markup_multiplier())

            # Calculate total
            total_rupees = final_price_with_markup * payload.sessions_count

            reward_amount = 0

            # ═══════════════════════════════════════════════════════════════════
            # FIX #2 (Part 2): REWARD CALCULATION RACE
            # Before: Two concurrent checkouts could read same balance
            # After: SELECT FOR UPDATE ensures accurate balance at calculation time
            # Note: Actual deduction happens in _verify_async with another lock
            # Formula: Same as dailypass - 10% of total, no cap, min with available balance
            # ═══════════════════════════════════════════════════════════════════
            if payload.reward:
                # Use integer math: 10% in paise = total_rupees * 10
                ten_percent_minor = total_rupees * 10

                # Use SELECT FOR UPDATE to get accurate balance
                cash_row = (
                    await session.execute(
                        select(ReferralFittbotCash)
                        .where(ReferralFittbotCash.client_id == payload.client_id)
                        .with_for_update()  # Lock to get accurate balance
                    )
                ).scalars().first()

                available_fittbot_cash_rupees = cash_row.fittbot_cash if cash_row else 0
                available_fittbot_cash_minor = int(available_fittbot_cash_rupees * 100)

                # Same logic as dailypass - min of 10% and available balance
                reward_amount_minor = min(ten_percent_minor, available_fittbot_cash_minor)

                # Round to nearest rupee to avoid decimal in final payment
                reward_amount = int(round(reward_amount_minor / 100))

                logger.info(
                    "REWARD_CALCULATED_WITH_LOCK",
                    extra={
                        "client_id": payload.client_id,
                        "available_cash_rupees": available_fittbot_cash_rupees,
                        "ten_percent_minor": ten_percent_minor,
                        "calculated_reward_rupees": reward_amount,
                        "total_rupees": total_rupees,
                    }
                )

            payable_rupees = max(total_rupees - reward_amount, 0)
            notes = {
                "type": "session_booking",
                "gym_id": payload.gym_id,
                "client_id": payload.client_id,
                "session_id": payload.session_id,
                "trainer_id": payload.trainer_id,
                "sessions_count": payload.sessions_count,
                "session_type": payload.session_type.value,
                "scheduled_sessions": json.dumps(scheduled_sessions),
                "scheduled_dates": json.dumps(payload.scheduled_dates),
                "default_slot": payload.default_slot,
                "reward": payload.reward,
                "reward_amount": reward_amount,
            }

            data={
                "total_rupees": total_rupees,
                "reward_amount": reward_amount,
                "payable_rupees": payable_rupees,
                "notes": notes,
                "scheduled_sessions": scheduled_sessions,
            }

            return {
                "total_rupees": total_rupees,
                "reward_amount": reward_amount,
                "payable_rupees": payable_rupees,
                "notes": notes,
                "scheduled_sessions": scheduled_sessions,
                "base_price_per_session": final_price,  # Gym owner's base price (without 30% markup)
            }

    async def _collect_allowed_dates_async(self, session: AsyncSession, payload: SessionCheckoutRequest) -> set:
        today = date.today()
        end_range = today + timedelta(days=30)
        rows = (
            await session.execute(
                select(SessionSchedule).where(
                    SessionSchedule.gym_id == payload.gym_id,
                    SessionSchedule.session_id == payload.session_id,
                    SessionSchedule.trainer_id == payload.trainer_id,
                    SessionSchedule.is_active.is_(True),
                )
            )
        ).scalars().all()
        allowed = set()
        for sch in rows:
            start_bound = sch.start_date or today
            end_bound = sch.end_date or end_range
            start = max(start_bound, today)
            end = min(end_bound, end_range)
            if end < start:
                continue
            d = start
            while d <= end:
                if sch.weekday is None or d.weekday() == sch.weekday:
                    allowed.add(d)
                d = d + timedelta(days=1)
        return allowed

    async def _capture_marker_snapshot(self, payment_id: str) -> Optional[Dict[str, Any]]:
        """Read capture marker from Redis cache (set by webhook)."""
        if not self.redis or not payment_id:
            return None
        # Webhook processor stores markers under the shared redis_prefix
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
        deadline = time_module.monotonic() + max(1, self.config.verify_db_poll_total_timeout_seconds)
        attempts = max(1, self.config.verify_db_poll_attempts)

        for attempt in range(1, attempts + 1):
            marker = await self._capture_marker_snapshot(payment_id)
            if marker:
                logger.info(
                    "[SESSION_VERIFY_CAPTURE_CACHE_HIT]",
                    extra={
                        "payment_id": _mask(payment_id),
                        "attempt": attempt,
                        "redis_prefix": self.config.redis_prefix,
                    },
                )
                return marker
            if time_module.monotonic() >= deadline:
                break
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 1.5)
        return None

    async def _record_capture_marker(self, body: Dict[str, Any], payment_id: str, order_id: str) -> None:
        """Store capture marker in Redis for faster verify."""
        if not self.redis:
            return
        pay_entity = body.get("payload", {}).get("payment", {}).get("entity", {})
        marker = {
            "amount": pay_entity.get("amount"),
            "currency": pay_entity.get("currency"),
            "method": pay_entity.get("method"),
            "order_id": order_id,
            "captured_at": pay_entity.get("created_at") or int(time_module.time()),
        }
        key = f"{self.config.sessions_redis_prefix}:capture:{payment_id}"
        try:
            await asyncio.to_thread(
                self.redis.set,
                key,
                json.dumps(marker),
                ex=self.config.verify_capture_cache_ttl_seconds,
            )
            logger.info(
                "SESSION_CAPTURE_MARKER_SET",
                extra={
                    "payment_id": _mask(payment_id),
                    "order_id": order_id,
                    "ttl_seconds": self.config.verify_capture_cache_ttl_seconds,
                },
            )
        except Exception:
            logger.warning("SESSION_CAPTURE_MARKER_SET_FAILED", exc_info=True)
