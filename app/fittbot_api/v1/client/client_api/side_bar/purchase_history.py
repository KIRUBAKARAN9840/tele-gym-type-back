# app/fittbot_api/v1/client/client_api/side_bar/purchase_history.py

from typing import Dict, Any
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.catalog import CatalogProduct
from app.fittbot_api.v1.payments.models.entitlements import Entitlement
from app.fittbot_api.v1.payments.models.orders import OrderItem
from app.fittbot_api.v1.payments.models.payments import Payment
from app.models.dailypass_models import DailyPass
from app.models.fittbot_models import Gym, FittbotGymMembership, SessionPurchase, ClassSession, GymPlans
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/purchase_history", tags=["Purchase History"])


@router.get("/get_subscription")
async def get_subscription_history(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        # Fetch all subscriptions for the client using async
        subscriptions_stmt = (
            select(Subscription)
            .where(Subscription.customer_id == str(client_id))
            .order_by(Subscription.created_at.desc())
        )
        subscriptions_result = await db.execute(subscriptions_stmt)
        subscriptions = subscriptions_result.scalars().all()

        if not subscriptions:
            return {
                "status": 200,
                "message": "No subscription history found for this client",
                "data": []
            }

        # Batch load catalog products
        product_skus = list({s.product_id for s in subscriptions if s.product_id})
        catalog_map = {}
        if product_skus:
            catalog_stmt = select(CatalogProduct).where(CatalogProduct.sku.in_(product_skus))
            catalog_result = await db.execute(catalog_stmt)
            catalog_map = {cp.sku: cp for cp in catalog_result.scalars().all()}

        # Batch load payments by latest_txn_id to get actual paid amounts
        txn_ids = list({s.latest_txn_id for s in subscriptions if s.latest_txn_id})
        payment_map = {}
        if txn_ids:
            payment_stmt = select(Payment).where(Payment.provider_payment_id.in_(txn_ids))
            payment_result = await db.execute(payment_stmt)
            payment_map = {p.provider_payment_id: p for p in payment_result.scalars().all()}

        history_data = []

        for idx, subscription in enumerate(subscriptions, start=1):
            try:
                if subscription.provider not in ["razorpay_pg", "google_play"]:
                    continue

                catalog_product = catalog_map.get(subscription.product_id)

                # Get actual paid amount from Payment table, fallback to catalog price
                actual_amount = 0
                if subscription.latest_txn_id and subscription.latest_txn_id in payment_map:
                    payment = payment_map[subscription.latest_txn_id]
                    actual_amount = payment.amount_minor / 100 if payment.amount_minor else 0
                elif catalog_product:
                    actual_amount = (getattr(catalog_product, "base_amount_minor", 0) / 100)

                subscription_data = {
                    "id": idx,
                    "plan_name": catalog_product.title if catalog_product else "Unknown Plan",
                    "date": subscription.active_from.date().isoformat() if subscription.active_from else None,
                    "amount": actual_amount,
                    "status": subscription.status
                }

                history_data.append(subscription_data)

            except Exception as item_error:
                print(f"Error processing subscription (index={idx}, sku={subscription.product_id}): {item_error}")
                continue

        return {
            "status": 200,
            "message": "Purchase history retrieved successfully",
            "data": history_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve purchase history",
            error_code="PURCHASE_HISTORY_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


@router.get("/get_daily_pass")
async def get_daily_pass_history(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        # Fetch all daily passes for the client using async
        daily_passes_stmt = (
            select(DailyPass)
            .where(DailyPass.client_id == str(client_id))
            .order_by(DailyPass.created_at.desc())
        )
        daily_passes_result = await db.execute(daily_passes_stmt)
        daily_passes = daily_passes_result.scalars().all()

        if not daily_passes:
            return {
                "status": 200,
                "message": "No daily pass history found for this client",
                "data": []
            }

        # Batch load gyms using async
        gym_ids = list({dp.gym_id for dp in daily_passes if dp.gym_id})
        gym_map = {}
        if gym_ids:
            gym_stmt = select(Gym).where(Gym.gym_id.in_(gym_ids))
            gym_result = await db.execute(gym_stmt)
            gym_map = {str(g.gym_id): g for g in gym_result.scalars().all()}

        def _format_gym_address(gym):
            if not gym:
                return ""
            address_parts = []
            if gym.street:
                address_parts.append(gym.street)
            if gym.area:
                address_parts.append(gym.area)
            if gym.city:
                address_parts.append(gym.city)
            if gym.state:
                address_parts.append(gym.state)
            return ", ".join(address_parts)

        history_data = []

        for idx, daily_pass in enumerate(daily_passes, start=1):
            try:
                gym = gym_map.get(str(daily_pass.gym_id))

                # amount_paid is already the actual paid amount in paisa
                daily_pass_data = {
                    "id": idx,
                    "name": gym.name if gym else "Unknown Gym",
                    "address": _format_gym_address(gym),
                    "date": daily_pass.created_at.date().isoformat() if daily_pass.created_at else None,
                    "no_of_days": daily_pass.days_total,
                    "status": daily_pass.status,
                    "amount": (daily_pass.amount_paid * 0.01) if daily_pass.amount_paid else 0
                }

                history_data.append(daily_pass_data)

            except Exception as item_error:
                print(f"Error processing daily pass {daily_pass.id}: {item_error}")
                continue

        return {
            "status": 200,
            "message": "Daily pass history retrieved successfully",
            "data": history_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve daily pass history",
            error_code="DAILY_PASS_HISTORY_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


@router.get("/get_membership")
async def get_membership_history(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        # Fetch all gym memberships for the client using async
        memberships_stmt = (
            select(FittbotGymMembership)
            .where(FittbotGymMembership.client_id == str(client_id))
            .order_by(FittbotGymMembership.purchased_at.desc())
        )
        memberships_result = await db.execute(memberships_stmt)
        gym_memberships = memberships_result.scalars().all()

        if not gym_memberships:
            return {
                "status": 200,
                "message": "No membership history found for this client",
                "data": []
            }

        # Batch load gyms using async
        gym_ids = list({m.gym_id for m in gym_memberships if m.gym_id})
        gym_map = {}
        if gym_ids:
            gym_stmt = select(Gym).where(Gym.gym_id.in_(gym_ids))
            gym_result = await db.execute(gym_stmt)
            gym_map = {str(g.gym_id): g for g in gym_result.scalars().all()}

        # Batch load gym plans to get duration
        plan_ids = list({m.plan_id for m in gym_memberships if m.plan_id})
        plan_map = {}
        if plan_ids:
            plan_stmt = select(GymPlans).where(GymPlans.id.in_(plan_ids))
            plan_result = await db.execute(plan_stmt)
            plan_map = {p.id: p for p in plan_result.scalars().all()}

        # Batch load payments for memberships that have entitlement_id
        entitlement_ids = list({m.entitlement_id for m in gym_memberships if m.entitlement_id})
        payment_map = {}
        if entitlement_ids:
            # Get entitlements to find order_item_ids
            entitlement_stmt = select(Entitlement).where(Entitlement.id.in_(entitlement_ids))
            entitlement_result = await db.execute(entitlement_stmt)
            entitlements = {e.id: e for e in entitlement_result.scalars().all()}

            # Get order_item_ids from entitlements
            order_item_ids = list({e.order_item_id for e in entitlements.values() if e.order_item_id})
            if order_item_ids:
                # Get order items to find order_ids
                order_item_stmt = select(OrderItem).where(OrderItem.id.in_(order_item_ids))
                order_item_result = await db.execute(order_item_stmt)
                order_items = {oi.id: oi for oi in order_item_result.scalars().all()}

                # Get order_ids
                order_ids = list({oi.order_id for oi in order_items.values() if oi.order_id})
                if order_ids:
                    # Get payments by order_id
                    payment_stmt = select(Payment).where(
                        Payment.order_id.in_(order_ids),
                        Payment.status == "captured"
                    )
                    payment_result = await db.execute(payment_stmt)
                    payments_by_order = {p.order_id: p for p in payment_result.scalars().all()}

                    # Build entitlement_id -> payment map
                    for ent_id, ent in entitlements.items():
                        if ent.order_item_id and ent.order_item_id in order_items:
                            order_item = order_items[ent.order_item_id]
                            if order_item.order_id in payments_by_order:
                                payment_map[ent_id] = payments_by_order[order_item.order_id]

        def _format_gym_address(gym):
            if not gym:
                return ""
            address_parts = []
            if gym.street:
                address_parts.append(gym.street)
            if gym.area:
                address_parts.append(gym.area)
            if gym.city:
                address_parts.append(gym.city)
            if gym.state:
                address_parts.append(gym.state)
            return ", ".join(address_parts)

        history_data = []

        for idx, membership in enumerate(gym_memberships, start=1):
            try:
                gym = gym_map.get(str(membership.gym_id))

                membership_type = membership.type
                if membership.type == "gym_membership":
                    membership_type = "Gym Membership"
                elif membership.type == "personal_training":
                    membership_type = "Personal Training"

                # Get duration from gym plan
                months = 0
                if membership.plan_id and membership.plan_id in plan_map:
                    months = plan_map[membership.plan_id].duration or 0

                # Get actual paid amount from Payment table, fallback to membership.amount
                actual_amount = membership.amount or 0
                if membership.entitlement_id and membership.entitlement_id in payment_map:
                    payment = payment_map[membership.entitlement_id]
                    actual_amount = payment.amount_minor / 100 if payment.amount_minor else actual_amount

                membership_data = {
                    "id": idx,
                    "name": gym.name if gym else "Unknown Gym",
                    "address": _format_gym_address(gym),
                    "type": membership_type,
                    "months": months,
                    "amount": actual_amount,
                    "date": membership.purchased_at.date().isoformat() if membership.purchased_at else None,
                    "status": membership.status
                }

                history_data.append(membership_data)

            except Exception as item_error:
                print(f"Error processing gym membership {membership.id}: {item_error}")
                continue

        return {
            "status": 200,
            "message": "Membership history retrieved successfully",
            "data": history_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve membership history",
            error_code="MEMBERSHIP_HISTORY_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


@router.get("/get_session")
async def get_session_history(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> Dict[str, Any]:

    try:
        # Fetch all session purchases for the client
        purchases_stmt = (
            select(SessionPurchase)
            .where(SessionPurchase.client_id == client_id, SessionPurchase.status == "paid")
            .order_by(SessionPurchase.created_at.desc())
        )
        purchases_result = await db.execute(purchases_stmt)
        purchases = purchases_result.scalars().all()

        if not purchases:
            return {
                "status": 200,
                "message": "No session history found for this client",
                "data": []
            }

        # Get unique session_ids to fetch session names
        session_ids = list({p.session_id for p in purchases if p.session_id})
        sessions_map = {}
        if session_ids:
            session_stmt = select(ClassSession).where(ClassSession.id.in_(session_ids))
            session_result = await db.execute(session_stmt)
            sessions_map = {s.id: s for s in session_result.scalars().all()}

        # Get unique gym_ids to fetch gym names
        gym_ids = list({p.gym_id for p in purchases if p.gym_id})
        gyms_map = {}
        if gym_ids:
            gym_stmt = select(Gym).where(Gym.gym_id.in_(gym_ids))
            gym_result = await db.execute(gym_stmt)
            gyms_map = {g.gym_id: g for g in gym_result.scalars().all()}

        history_data = []

        for idx, purchase in enumerate(purchases, start=1):
            try:
                # Get session name
                session_meta = sessions_map.get(purchase.session_id)
                session_name = session_meta.internal if session_meta and session_meta.internal else (
                    session_meta.name if session_meta else "Session"
                )

                # Get gym name
                gym = gyms_map.get(purchase.gym_id)
                gym_name = gym.name if gym else "Unknown Gym"

                # Parse scheduled_sessions to get date range
                scheduled = purchase.scheduled_sessions or []
                dates = []
                for entry in scheduled:
                    if isinstance(entry, dict) and "date" in entry:
                        dates.append(entry["date"])
                    elif isinstance(entry, str):
                        dates.append(entry)

                dates.sort()
                from_date = dates[0] if dates else None
                to_date = dates[-1] if dates else None

                purchase_data = {
                    "id": idx,
                    "session_name": session_name,
                    "session_id": purchase.session_id,
                    "gym_id": purchase.gym_id,
                    "gym_name": gym_name,
                    "trainer_id": purchase.trainer_id,
                    "sessions_count": purchase.sessions_count,
                    "from_date": from_date,
                    "to_date": to_date,
                    "amount": purchase.payable_rupees,
                    "total_amount": purchase.total_rupees,
                    "reward_applied": purchase.reward_applied,
                    "reward_amount": purchase.reward_amount,
                    "status": purchase.status,
                    "purchased_at": purchase.created_at.isoformat() if purchase.created_at else None,
                }

                history_data.append(purchase_data)

            except Exception as item_error:
                print(f"Error processing session purchase {purchase.id}: {item_error}")
                continue

        return {
            "status": 200,
            "message": "Session history retrieved successfully",
            "data": history_data
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve session history",
            error_code="SESSION_HISTORY_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


