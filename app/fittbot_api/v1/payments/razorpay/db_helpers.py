from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..models.orders import Order, OrderItem
from ..models.payments import Payment
from ..models.subscriptions import Subscription


PROVIDER = "razorpay_pg"


def new_id(prefix: str) -> str:
    import random, string

    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    rnd = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}_{ts}_{rnd}"


def create_pending_order(
    db: Session, *, user_id: str, amount_minor: int, sub_id: str, sku: str, title: str
) -> Order:
    order = Order(
        id=new_id("order"),
        customer_id=user_id,
        currency="INR",
        provider=PROVIDER,
        provider_order_id=sub_id,  # store subscription_id
        gross_amount_minor=amount_minor,
        status="pending",
    )
    db.add(order)
    db.flush()
    item = OrderItem(
        id=new_id("item"),
        order_id=order.id,
        item_type="app_subscription",
        sku=sku,
        title=title,
        unit_price_minor=amount_minor,
        qty=1,
        item_metadata={},
    )
    db.add(item)
    db.flush()
    return order


def create_or_update_subscription_pending(
    db: Session, *, user_id: str, plan_sku: str, provider_subscription_id: str
) -> Subscription:
    like_val = f"%provider_subscription_id:{provider_subscription_id}%"
    sub = (
        db.query(Subscription)
        .filter(Subscription.customer_id == user_id)
        .filter(
            or_(
                Subscription.cancel_reason.like(like_val),
                Subscription.latest_txn_id == provider_subscription_id,
            )
        )
        .order_by(Subscription.created_at.desc())
        .first()
    )

    if sub:
        if sub.status not in ("active", "renewed"):
            sub.status = "pending"
        if not sub.cancel_reason or "provider_subscription_id:" not in (sub.cancel_reason or ""):
            sub.cancel_reason = f"provider_subscription_id:{provider_subscription_id}"
        db.add(sub)
        db.flush()
        return sub

    sub = Subscription(
        id=new_id("sub"),
        customer_id=user_id,
        provider=PROVIDER,
        product_id=plan_sku,
        status="pending",
        auto_renew=True,
        cancel_reason=f"provider_subscription_id:{provider_subscription_id}",
    )
    db.add(sub)
    db.flush()
    return sub


def get_subscription_by_provider_id(db: Session, provider_subscription_id: str) -> Optional[Subscription]:
    like_val = f"%provider_subscription_id:{provider_subscription_id}%"
    return (
        db.query(Subscription)
        .filter(Subscription.cancel_reason.like(like_val))
        .order_by(Subscription.created_at.desc())
        .first()
    )


def mark_order_paid(db: Session, order: Order) -> None:
    order.status = "paid"
    db.add(order)


def create_payment(
    db: Session,
    *,
    order: Order,
    user_id: str,
    amount_minor: int,
    currency: str,
    provider_payment_id: str,
    meta: Dict[str, Any],
    status: str = "captured",
) -> None:
    # Idempotency guard: avoid duplicate payments for the same provider payment id
    existing = (
        db.query(Payment)
        .filter(
            Payment.provider == PROVIDER,
            Payment.provider_payment_id == provider_payment_id,
        )
        .first()
    )
    if existing:
        return

    now = datetime.now(timezone.utc)
    pay = Payment(
        id=new_id("pay"),
        order_id=order.id,
        customer_id=user_id,
        amount_minor=amount_minor,
        currency=currency,
        provider=PROVIDER,
        provider_payment_id=provider_payment_id,
        status=status,
        captured_at=now if status == "captured" else None,
        failed_at=now if status == "failed" else None,
        payment_metadata=meta,
    )
    db.add(pay)


def find_order_by_sub_id(db: Session, sub_id: str) -> Optional[Order]:
    return (
        db.query(Order)
        .filter(Order.provider == PROVIDER, Order.provider_order_id == sub_id)
        .order_by(Order.created_at.desc())
        .first()
    )


def cycle_window_from_sub_entity(ent: Dict[str, Any]) -> Tuple[datetime, Optional[datetime]]:
    def _unix_to_dt(ts: Optional[int]) -> Optional[datetime]:
        if ts is None:
            return None
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)

    start_ts = ent.get("current_start") or ent.get("start_at")
    end_ts = ent.get("current_end") or ent.get("end_at") or ent.get("charge_at")
    start = _unix_to_dt(start_ts) or datetime.now(timezone.utc)
    end = _unix_to_dt(end_ts) if end_ts else None
    return start, end
