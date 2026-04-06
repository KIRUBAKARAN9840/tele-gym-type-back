"""
Centralized Revenue Service

This module contains ALL revenue calculation logic for the platform.
All other APIs (Financials, Tax & Compliance, MRR, Cash Flow, Dashboard)
should use this service to get revenue data.

REVENUE SOURCES:
1. Daily Pass - One-time product, counted on purchase date
2. Sessions - One-time product, counted on booking/purchase date (stored in rupees)
3. Fymble Subscription - Recurring, can be amortized for MRR
4. Gym Membership - Recurring, can be amortized for MRR

IMPORTANT:
- Sessions revenue is stored in RUPEES (SessionPurchase.payable_rupees)
- All other revenues are stored in PAISA (minor units)
- This service returns all values in PAISA for consistency
- Divide by 100 only at display time

FUTURE CHANGES:
- All revenue logic changes should be made ONLY in this file
- No duplicate revenue logic anywhere else
"""

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, datetime, timedelta
from sqlalchemy import func, and_, select, or_, distinct
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from pydantic import BaseModel
import calendar

from app.models.async_database import get_async_db
from app.models.dailypass_models import get_dailypass_session, DailyPass
from app.models.fittbot_models import (
    SessionBookingDay, SessionBooking, SessionPurchase,
    GymPlans, FittbotGymMembership
)
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_api.v1.payments.models.entitlements import Entitlement


# ============================================================================
# PYDANTIC MODELS FOR INPUT/OUTPUT
# ============================================================================

class RevenueFilters(BaseModel):
    """Filters for revenue queries"""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    exclude_gym_id_one: bool = True  # Default: exclude gym_id = 1
    specific_gym_id: Optional[int] = None  # Filter for specific gym


class RevenueBreakdown(BaseModel):
    """Revenue breakdown by source (all values in PAISA)"""
    total_revenue: int
    daily_pass: int
    sessions: int
    fittbot_subscription: int
    gym_membership: int
    ai_credits: int


class AmortizedRevenueBreakdown(BaseModel):
    """Revenue breakdown with amortization for MRR (all values in PAISA)"""
    total_revenue: float
    daily_pass: int
    sessions: int
    fittbot_subscription: float  # Can be fractional due to amortization
    gym_membership: float  # Can be fractional due to amortization
    ai_credits: float  # Can be fractional due to amortization


class DailyRevenuePoint(BaseModel):
    """Single day's revenue"""
    date: str
    revenue: float  # In rupees


class GymRevenuePoint(BaseModel):
    """Single gym's revenue"""
    gym_id: int
    gym_name: str
    revenue: float  # In rupees


class DetailedRevenueBreakdown(BaseModel):
    """
    Complete revenue breakdown with daily and gym-wise analytics.
    Used by /portal/admin/revenue page.
    """
    total_revenue: float  # In rupees
    source_split: Dict[str, int]  # Each source in PAISA
    source_split_rupees: Dict[str, float]  # Each source in rupees
    daily_revenue: List[DailyRevenuePoint]  # Daily revenue over time
    gym_breakdown: List[GymRevenuePoint]  # Gym-wise revenue


# ============================================================================
# SKU TO DURATION MAPPING (FOR FITTBOT SUBSCRIPTIONS)
# ============================================================================

PRODUCT_PLAN_MAPPING = {
    # Monthly subscriptions
    'FYMBLE_MONTHLY': 1,
    'APP_SUB_MONTHLY': 1,
    'FYMBLE_SUB_MONTHLY': 1,
    'MONTHLY_PREMIUM': 1,

    # Quarterly subscriptions (3 months)
    'FYMBLE_QUARTERLY': 3,
    'APP_SUB_QUARTERLY': 3,
    'FYMBLE_SUB_QUARTERLY': 3,
    'QUARTERLY_PREMIUM': 3,

    # Yearly subscriptions (12 months)
    'FYMBLE_YEARLY': 12,
    'APP_SUB_YEARLY': 12,
    'FYMBLE_SUB_YEARLY': 12,
    'YEARLY_PREMIUM': 12,
    'ANNUAL_PREMIUM': 12,
}


# ============================================================================
# CORE REVENUE QUERIES
# ============================================================================

async def get_daily_pass_revenue(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> int:
    """
    Get Daily Pass revenue for a date range.

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1
        specific_gym_id: Filter for specific gym (overrides exclude_gym_id_one)

    Returns:
        Revenue in PAISA
    """
    try:
        dailypass_session = get_dailypass_session()

        # Build conditions (for sync session)
        conditions = [
            func.date(DailyPass.created_at) >= start_date,
            func.date(DailyPass.created_at) <= end_date
        ]

        # Gym filtering
        if specific_gym_id is not None:
            conditions.append(DailyPass.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            conditions.append(DailyPass.gym_id != "1")

        # Execute query using sync session
        stmt = (
            select(func.coalesce(func.sum(DailyPass.amount_paid), 0))
            .where(and_(*conditions))
        )
        result = dailypass_session.execute(stmt)
        revenue = result.scalar() or 0

        dailypass_session.close()
        return int(revenue)

    except Exception as e:
        return 0


async def get_sessions_revenue(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> int:
    """
    Get Sessions revenue for a date range.

    IMPORTANT: SessionPurchase.payable_rupees is stored in RUPEES.
    This function converts to PAISA for consistency.

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1
        specific_gym_id: Filter for specific gym (overrides exclude_gym_id_one)

    Returns:
        Revenue in PAISA
    """
    try:
        # Build conditions
        conditions = [
            SessionPurchase.status == "paid"
        ]

        # Gym filtering - SessionPurchase.gym_id is INTEGER
        if specific_gym_id is not None:
            conditions.append(SessionPurchase.gym_id == specific_gym_id)
        elif exclude_gym_id_one:
            conditions.append(SessionPurchase.gym_id != 1)

        # Date filtering: only use created_at (purchase date)
        # Revenue is attributed to when the session was purchased, not when it was last updated
        conditions.append(func.date(SessionPurchase.created_at) >= start_date)
        conditions.append(func.date(SessionPurchase.created_at) <= end_date)

        # Execute query (returns RUPEES)
        stmt = (
            select(func.coalesce(func.sum(SessionPurchase.payable_rupees), 0))
            .where(and_(*conditions))
        )

        result = await db.execute(stmt)
        revenue_rupees = result.scalar() or 0

        # Convert RUPEES to PAISA for consistency
        return int(revenue_rupees * 100) if revenue_rupees else 0

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


async def get_fittbot_subscription_revenue(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> int:
    """
    Get Nutritionist Plan (Fymble Subscription) revenue for a date range.

    NEW LOGIC:
    - Query payments.payments table
    - Filter where payment_metadata -> 'flow' contains 'nutrition_purchase_googleplay'
    - Filter where status = 'captured'
    - Sum amount_minor values

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1 (NOT APPLIED for nutritionist plans)
        specific_gym_id: Filter for specific gym (NOT APPLIED for nutritionist plans)

    Returns:
        Revenue in PAISA (amount_minor is already in minor units)
    """
    try:
        import logging
        logger = logging.getLogger("revenue_service")

        # For JSON querying in SQLAlchemy with PostgreSQL
        from sqlalchemy import cast, String
        import json

        # Build conditions
        conditions = [
            Payment.status == "captured",
        ]

        # Date filter on captured_at
        conditions.append(func.date(Payment.captured_at) >= start_date)
        conditions.append(func.date(Payment.captured_at) <= end_date)

        # Fetch all captured payments in date range
        fetch_stmt = (
            select(Payment)
            .where(Payment.status == "captured")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )

        fetch_result = await db.execute(fetch_stmt)
        payments = fetch_result.scalars().all()

        logger.info(f"[NUTRITIONIST_PLAN] Total captured payments in range: {len(payments)}")

        # Filter by payment_metadata['flow'] containing 'nutrition_purchase_googleplay'
        nutritionist_revenue = 0
        matched_count = 0
        sample_metadata = None

        for payment in payments:
            if payment.payment_metadata and isinstance(payment.payment_metadata, dict):
                # Check flow at different nesting levels
                flow = None

                # Level 1: Direct flow key
                if "flow" in payment.payment_metadata:
                    flow = payment.payment_metadata.get("flow")

                # Level 2: flow inside order_info
                elif "order_info" in payment.payment_metadata:
                    order_info = payment.payment_metadata.get("order_info")
                    if isinstance(order_info, dict) and "flow" in order_info:
                        flow = order_info.get("flow")

                # Check if flow contains nutrition_purchase_googleplay
                if flow and "nutrition_purchase_googleplay" in str(flow):
                    nutritionist_revenue += payment.amount_minor or 0
                    matched_count += 1
                    if matched_count <= 3:  # Store first 3 samples
                        if sample_metadata is None:
                            sample_metadata = []
                        sample_metadata.append({
                            "payment_id": payment.id,
                            "amount_minor": payment.amount_minor,
                            "flow": flow,
                            "metadata": payment.payment_metadata
                        })

        logger.info(f"[NUTRITIONIST_PLAN] Matched payments: {matched_count}, Revenue: {nutritionist_revenue}")
        if sample_metadata:
            logger.info(f"[NUTRITIONIST_PLAN] Sample metadata: {sample_metadata}")

        return int(nutritionist_revenue)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


async def get_gym_membership_revenue(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> int:
    """
    Get Gym Membership revenue for a date range.

    Filters by metadata conditions:
    - audit.source = "dailypass_checkout_api"
    - order_info.flow = "unified_gym_membership_with_sub"
    - order_info.flow = "unified_gym_membership_with_free_fittbot"

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1
        specific_gym_id: Filter for specific gym (overrides exclude_gym_id_one)

    Returns:
        Revenue in PAISA
    """
    try:
        # SQL-level metadata filter using JSON functions — avoids loading all payments into Python memory
        meta_cond = or_(
            func.json_unquote(func.json_extract(Order.order_metadata, "$.audit.source")) == "dailypass_checkout_api",
            func.json_unquote(func.json_extract(Order.order_metadata, "$.order_info.flow")) == "unified_gym_membership_with_sub",
            func.json_unquote(func.json_extract(Order.order_metadata, "$.order_info.flow")) == "unified_gym_membership_with_free_fittbot"
        )

        # EXISTS subquery: ensure order has at least one valid order_item with an eligible gym_id
        oi_conditions = [
            OrderItem.order_id == Order.id,
            OrderItem.gym_id.isnot(None),
            OrderItem.gym_id != ""
        ]
        if specific_gym_id is not None:
            oi_conditions.append(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            oi_conditions.append(OrderItem.gym_id != "1")

        gym_exists = select(1).select_from(OrderItem).where(and_(*oi_conditions)).exists()

        stmt = (
            select(func.coalesce(func.sum(Order.gross_amount_minor), 0))
            .select_from(Payment)
            .join(Order, Order.id == Payment.order_id)
            .where(
                Payment.status == "captured",
                Order.status == "paid",
                func.date(Payment.captured_at) >= start_date,
                func.date(Payment.captured_at) <= end_date,
                meta_cond,
                gym_exists
            )
        )

        result = await db.execute(stmt)
        revenue = result.scalar() or 0
        return int(revenue)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


# ============================================================================
# HIGH-LEVEL REVENUE FUNCTIONS
# ============================================================================

async def get_revenue_breakdown(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> RevenueBreakdown:
    """
    Get complete revenue breakdown for a date range.

    This is the MAIN function that should be used by all APIs.

    NOTE: Queries run sequentially (not concurrently) because SQLAlchemy's
    async session doesn't support concurrent operations on the same session.

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1
        specific_gym_id: Filter for specific gym

    Returns:
        RevenueBreakdown with all values in PAISA
    """
    # Run queries sequentially (SQLAlchemy async session doesn't support concurrent operations)
    daily_pass = await get_daily_pass_revenue(db, start_date, end_date, exclude_gym_id_one, specific_gym_id)
    sessions = await get_sessions_revenue(db, start_date, end_date, exclude_gym_id_one, specific_gym_id)
    fittbot_subscription = await get_fittbot_subscription_revenue(db, start_date, end_date, exclude_gym_id_one, specific_gym_id)
    gym_membership = await get_gym_membership_revenue(db, start_date, end_date, exclude_gym_id_one, specific_gym_id)
    ai_credits = await get_ai_credits_revenue(db, start_date, end_date, exclude_gym_id_one, specific_gym_id)

    total_revenue = daily_pass + sessions + fittbot_subscription + gym_membership + ai_credits

    return RevenueBreakdown(
        total_revenue=total_revenue,
        daily_pass=daily_pass,
        sessions=sessions,
        fittbot_subscription=fittbot_subscription,
        gym_membership=gym_membership,
        ai_credits=ai_credits
    )


async def get_ai_credits_revenue(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    exclude_gym_id_one: bool = True,
    specific_gym_id: Optional[int] = None
) -> int:
    """
    Get AI Credits revenue for a date range.

    NEW LOGIC:
    - Query payments.payments table
    - Filter where payment_metadata -> 'flow' contains 'food_scanner_credits'
    - Filter where status = 'captured'
    - Filter where captured_at is within the date range
    - Sum amount_minor values

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1 (NOT APPLIED for AI credits)
        specific_gym_id: Filter for specific gym (NOT APPLIED for AI credits)

    Returns:
        Revenue in PAISA
    """
    total_revenue = 0

    try:
        import logging
        logger = logging.getLogger("revenue_service")

        # Fetch all captured payments in the date range
        fetch_stmt = (
            select(Payment)
            .where(Payment.status == "captured")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )

        fetch_result = await db.execute(fetch_stmt)
        payments = fetch_result.scalars().all()

        logger.info(f"[AI_CREDITS] Total captured payments: {len(payments)}")

        matched_count = 0
        for payment in payments:
            if payment.payment_metadata and isinstance(payment.payment_metadata, dict):
                # Check flow at different nesting levels
                flow = None

                # Level 1: Direct flow key
                if "flow" in payment.payment_metadata:
                    flow = payment.payment_metadata.get("flow")

                # Level 2: flow inside order_info
                elif "order_info" in payment.payment_metadata:
                    order_info = payment.payment_metadata.get("order_info")
                    if isinstance(order_info, dict) and "flow" in order_info:
                        flow = order_info.get("flow")

                # Check if flow contains food_scanner_credits
                if flow and "food_scanner_credits" in str(flow):
                    total_revenue += payment.amount_minor or 0
                    matched_count += 1

        logger.info(f"[AI_CREDITS] Matched: {matched_count}, Revenue: {total_revenue}")

    except Exception as e:
        import traceback
        traceback.print_exc()

    return total_revenue


# ============================================================================
# MRR-SPECIFIC FUNCTIONS (WITH AMORTIZATION)
# ============================================================================

async def get_amortized_fittbot_subscription_revenue(
    db: AsyncSession,
    target_month_start: date,
    target_month_end: date,
    exclude_gym_id_one: bool = True
) -> float:
    """
    Get Nutritionist Plan (Fymble Subscription) revenue for MRR.

    NEW LOGIC:
    - Query payments.payments table
    - Filter where payment_metadata -> 'flow' contains 'nutrition_purchase_googleplay'
    - Filter where status = 'captured'
    - Filter where captured_at is within the target month
    - Sum amount_minor values

    Note: Nutritionist plans from Google Play are one-time purchases,
    so no amortization is needed. We count them when captured in the target month.

    Args:
        db: AsyncSession
        target_month_start: First day of target month
        target_month_end: Last day of target month
        exclude_gym_id_one: Whether to exclude gym_id = 1 (NOT APPLIED for nutritionist plans)

    Returns:
        Revenue in PAISA
    """
    total_revenue = 0.0

    try:
        import logging
        logger = logging.getLogger("revenue_service")

        # Fetch all captured payments in the target month
        fetch_stmt = (
            select(Payment)
            .where(Payment.status == "captured")
            .where(func.date(Payment.captured_at) >= target_month_start)
            .where(func.date(Payment.captured_at) <= target_month_end)
        )

        fetch_result = await db.execute(fetch_stmt)
        payments = fetch_result.scalars().all()

        logger.info(f"[NUTRITIONIST_PLAN_MRR] Total payments: {len(payments)}")

        matched_count = 0
        for payment in payments:
            if payment.payment_metadata and isinstance(payment.payment_metadata, dict):
                # Check flow at different nesting levels
                flow = None

                # Level 1: Direct flow key
                if "flow" in payment.payment_metadata:
                    flow = payment.payment_metadata.get("flow")

                # Level 2: flow inside order_info
                elif "order_info" in payment.payment_metadata:
                    order_info = payment.payment_metadata.get("order_info")
                    if isinstance(order_info, dict) and "flow" in order_info:
                        flow = order_info.get("flow")

                # Check if flow contains nutrition_purchase_googleplay
                if flow and "nutrition_purchase_googleplay" in str(flow):
                    total_revenue += payment.amount_minor or 0
                    matched_count += 1

        logger.info(f"[NUTRITIONIST_PLAN_MRR] Matched: {matched_count}, Revenue: {total_revenue}")

    except Exception as e:
        import traceback
        traceback.print_exc()

    return total_revenue


async def get_amortized_gym_membership_revenue(
    db: AsyncSession,
    target_month_start: date,
    target_month_end: date,
    exclude_gym_id_one: bool = True
) -> float:
    """
    Get amortized Gym Membership revenue for MRR.

    Includes ALL memberships active during the target month,
    with revenue distributed monthly based on plan duration.

    Args:
        db: AsyncSession
        target_month_start: First day of target month
        target_month_end: Last day of target month
        exclude_gym_id_one: Whether to exclude gym_id = 1

    Returns:
        Amortized revenue in PAISA (can be fractional)
    """
    total_revenue = 0.0

    try:
        # Fetch payments and orders with metadata conditions
        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(Payment.status == "captured")
            .where(Order.status == "paid")
        )

        payment_result = await db.execute(payment_stmt)
        all_payments = payment_result.all()

        if not all_payments:
            return 0.0

        # Collect order IDs
        order_ids = [row.Order.id for row in all_payments]

        # Fetch order items
        order_items_conditions = [
            OrderItem.order_id.in_(order_ids),
            OrderItem.gym_id.isnot(None)
        ]

        if exclude_gym_id_one:
            order_items_conditions.append(OrderItem.gym_id != "1")

        order_items_stmt = (
            select(OrderItem)
            .where(and_(*order_items_conditions))
        )
        order_items_result = await db.execute(order_items_stmt)
        order_items = order_items_result.scalars().all()

        # Mappings
        order_gym_mapping = {}
        order_item_mapping = {}
        for item in order_items:
            if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                order_gym_mapping[item.order_id] = int(item.gym_id)
                order_item_mapping[item.order_id] = item

        # Fetch entitlements
        order_item_ids = [item.id for item in order_items]
        entitlement_mapping = {}
        if order_item_ids:
            entitlements_stmt = (
                select(Entitlement)
                .where(Entitlement.order_item_id.in_(order_item_ids))
            )
            entitlements_result = await db.execute(entitlements_stmt)
            entitlements = entitlements_result.scalars().all()
            for ent in entitlements:
                entitlement_mapping[ent.order_item_id] = ent

        # Fetch FittbotGymMembership records
        entitlement_ids = [ent.id for ent in entitlement_mapping.values()]
        membership_mapping = {}
        if entitlement_ids:
            memberships_stmt = (
                select(FittbotGymMembership)
                .where(FittbotGymMembership.entitlement_id.in_(entitlement_ids))
            )
            memberships_result = await db.execute(memberships_stmt)
            memberships = memberships_result.scalars().all()
            for memb in memberships:
                membership_mapping[memb.entitlement_id] = memb

        # Fetch GymPlans for durations
        plan_ids = list({m.plan_id for m in membership_mapping.values() if m.plan_id})
        plan_duration_mapping = {}
        if plan_ids:
            plans_stmt = (
                select(GymPlans)
                .where(GymPlans.id.in_(plan_ids))
            )
            plans_result = await db.execute(plans_stmt)
            plans = plans_result.scalars().all()
            for plan in plans:
                plan_duration_mapping[plan.id] = plan.duration or 1

        # Process each payment
        for row in all_payments:
            payment = row.Payment
            order = row.Order

            payment_date = payment.captured_at.date() if payment.captured_at else None
            if not payment_date:
                continue

            # Check metadata conditions
            if not order.order_metadata or not isinstance(order.order_metadata, dict):
                continue

            metadata = order.order_metadata

            condition1 = False
            if metadata.get("audit") and isinstance(metadata.get("audit"), dict):
                if metadata["audit"].get("source") == "dailypass_checkout_api":
                    condition1 = True

            condition2 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_sub":
                    condition2 = True

            condition3 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_free_fittbot":
                    condition3 = True

            if not (condition1 or condition2 or condition3):
                continue

            if order.id not in order_gym_mapping:
                continue

            # Get duration
            duration_months = 1
            if order.id in order_item_mapping:
                order_item = order_item_mapping[order.id]
                if order_item.id in entitlement_mapping:
                    entitlement = entitlement_mapping[order_item.id]
                    if entitlement.id in membership_mapping:
                        membership = membership_mapping[entitlement.id]
                        if membership.plan_id and membership.plan_id in plan_duration_mapping:
                            duration_months = plan_duration_mapping[membership.plan_id] or 1

            # Calculate validity
            validity_end_date = (
                date(payment_date.year, payment_date.month, 1) +
                timedelta(days=32 * duration_months)
            )
            validity_end_date = date(validity_end_date.year, validity_end_date.month, 1) - timedelta(days=1)

            # Check if target month overlaps with validity period
            if validity_end_date >= target_month_start and payment_date <= target_month_end:
                amount = order.gross_amount_minor or 0
                monthly_amount = amount / duration_months
                total_revenue += monthly_amount

    except Exception as e:
        import traceback
        traceback.print_exc()

    return total_revenue


async def get_mrr_revenue_breakdown(
    db: AsyncSession,
    target_month_start: date,
    target_month_end: date,
    exclude_gym_id_one: bool = True
) -> AmortizedRevenueBreakdown:
    """
    Get MRR revenue breakdown with amortization for recurring products.

    - Daily Pass: Full amount for passes purchased in target month
    - Sessions: Full amount for sessions booked in target month
    - Fymble Subscription: Monthly amortized amount for ALL active subscriptions
    - Gym Membership: Monthly amortized amount for ALL active memberships

    NOTE: Queries run sequentially (not concurrently) because SQLAlchemy's
    async session doesn't support concurrent operations on the same session.

    Args:
        db: AsyncSession
        target_month_start: First day of target month
        target_month_end: Last day of target month
        exclude_gym_id_one: Whether to exclude gym_id = 1

    Returns:
        AmortizedRevenueBreakdown with values in PAISA
    """
    # Run queries sequentially (SQLAlchemy async session doesn't support concurrent operations)
    daily_pass = await get_daily_pass_revenue(db, target_month_start, target_month_end, exclude_gym_id_one)
    sessions = await get_sessions_revenue(db, target_month_start, target_month_end, exclude_gym_id_one)
    fittbot_subscription = await get_amortized_fittbot_subscription_revenue(db, target_month_start, target_month_end, exclude_gym_id_one)
    gym_membership = await get_amortized_gym_membership_revenue(db, target_month_start, target_month_end, exclude_gym_id_one)
    ai_credits = await get_ai_credits_revenue(db, target_month_start, target_month_end, exclude_gym_id_one)

    total_revenue = float(daily_pass) + float(sessions) + fittbot_subscription + gym_membership + float(ai_credits)

    return AmortizedRevenueBreakdown(
        total_revenue=total_revenue,
        daily_pass=daily_pass,
        sessions=sessions,
        fittbot_subscription=fittbot_subscription,
        gym_membership=gym_membership,
        ai_credits=float(ai_credits)
    )


# ============================================================================
# DETAILED REVENUE FUNCTIONS (with daily & gym breakdowns)
# ============================================================================

async def get_detailed_revenue_with_breakdowns(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    source: Optional[str] = None,
    specific_gym_id: Optional[int] = None,
    exclude_gym_id_one: bool = True
) -> DetailedRevenueBreakdown:
    """
    Get complete revenue breakdown with daily and gym-wise analytics.
    Used by /portal/admin/revenue page.

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        source: Filter by specific source (daily_pass, sessions, fittbot_subscription, gym_membership, ai_credits)
        specific_gym_id: Filter for specific gym
        exclude_gym_id_one: Whether to exclude gym_id = 1

    Returns:
        DetailedRevenueBreakdown with total revenue, source splits, daily revenue, and gym breakdown
    """
    from app.models.fittbot_models import Gym

    # Track revenue by date and gym
    daily_revenue = {}  # date -> amount (in PAISA)
    gym_revenue = {}   # gym_id -> amount (in PAISA)

    # Initialize source revenue
    source_revenue_paisa = {
        "daily_pass": 0,
        "sessions": 0,
        "fittbot_subscription": 0,
        "gym_membership": 0,
        "ai_credits": 0
    }

    # Determine which sources to query
    query_sources = []
    if not source or source == "daily_pass":
        query_sources.append("daily_pass")
    if not source or source == "sessions":
        query_sources.append("sessions")
    if (not source or source == "fittbot_subscription") and not specific_gym_id:
        query_sources.append("fittbot_subscription")
    if not source or source == "gym_membership":
        query_sources.append("gym_membership")
    if (not source or source == "ai_credits") and not specific_gym_id:
        query_sources.append("ai_credits")

    # Query each source and collect daily/gym breakdowns
    for query_source in query_sources:
        if query_source == "daily_pass":
            await _get_daily_pass_detailed(
                db, start_date, end_date, specific_gym_id, exclude_gym_id_one,
                daily_revenue, gym_revenue, source_revenue_paisa
            )
        elif query_source == "sessions":
            await _get_sessions_detailed(
                db, start_date, end_date, specific_gym_id, exclude_gym_id_one,
                daily_revenue, gym_revenue, source_revenue_paisa
            )
        elif query_source == "fittbot_subscription":
            await _get_fittbot_subscription_detailed(
                db, start_date, end_date,
                daily_revenue, source_revenue_paisa
            )
        elif query_source == "gym_membership":
            await _get_gym_membership_detailed(
                db, start_date, end_date, specific_gym_id, exclude_gym_id_one,
                daily_revenue, gym_revenue, source_revenue_paisa
            )
        elif query_source == "ai_credits":
            await _get_ai_credits_detailed(
                db, start_date, end_date,
                daily_revenue, source_revenue_paisa
            )

    # Calculate total revenue (in PAISA)
    total_revenue_paisa = sum(source_revenue_paisa.values())

    # Convert daily_revenue to sorted array
    revenue_over_time = [
        DailyRevenuePoint(date=date, revenue=amount / 100)
        for date, amount in sorted(daily_revenue.items())
    ]

    # Get gym names for gym-wise breakdown
    gym_names = {}
    if gym_revenue:
        gym_ids = list(gym_revenue.keys())
        gym_stmt = select(Gym.gym_id, Gym.name).where(Gym.gym_id.in_(gym_ids))
        gym_result = await db.execute(gym_stmt)
        for gym_id_val, gym_name in gym_result.all():
            gym_names[gym_id_val] = gym_name

    # Convert gym_revenue to array
    gym_breakdown = [
        GymRevenuePoint(
            gym_id=gym_id,
            gym_name=gym_names.get(gym_id, f"Gym {gym_id}"),
            revenue=amount / 100
        )
        for gym_id, amount in sorted(gym_revenue.items(), key=lambda x: x[1], reverse=True)
    ]

    # Convert source revenue to rupees for display
    source_split_rupees = {
        "daily_pass": source_revenue_paisa["daily_pass"] / 100,
        "sessions": source_revenue_paisa["sessions"] / 100,  # Convert from paisa to rupees
        "fittbot_subscription": source_revenue_paisa["fittbot_subscription"] / 100,
        "gym_membership": source_revenue_paisa["gym_membership"] / 100,
        "ai_credits": source_revenue_paisa["ai_credits"] / 100
    }

    return DetailedRevenueBreakdown(
        total_revenue=total_revenue_paisa / 100,
        source_split=source_revenue_paisa,
        source_split_rupees=source_split_rupees,
        daily_revenue=revenue_over_time,
        gym_breakdown=gym_breakdown
    )


async def _get_daily_pass_detailed(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    specific_gym_id: Optional[int],
    exclude_gym_id_one: bool,
    daily_revenue: dict,
    gym_revenue: dict,
    source_revenue: dict
):
    """Get Daily Pass revenue with daily and gym breakdowns."""
    try:
        dailypass_session = get_dailypass_session()

        conditions = [
            func.date(DailyPass.created_at) >= start_date,
            func.date(DailyPass.created_at) <= end_date
        ]

        if specific_gym_id is not None:
            conditions.append(DailyPass.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            conditions.append(DailyPass.gym_id != "1")

        stmt = (
            select(
                DailyPass.amount_paid,
                DailyPass.created_at,
                DailyPass.gym_id
            )
            .where(and_(*conditions))
        )

        result = dailypass_session.execute(stmt)
        daily_passes = result.all()

        for dp in daily_passes:
            amount = dp.amount_paid or 0
            source_revenue["daily_pass"] += amount

            # Track daily revenue
            date_key = dp.created_at.date().isoformat() if dp.created_at else None
            if date_key:
                if date_key not in daily_revenue:
                    daily_revenue[date_key] = 0
                daily_revenue[date_key] += amount

            # Track gym-wise revenue
            if dp.gym_id:
                try:
                    gym_key = int(dp.gym_id)
                    if gym_key not in gym_revenue:
                        gym_revenue[gym_key] = 0
                    gym_revenue[gym_key] += amount
                except (ValueError, TypeError):
                    pass

        dailypass_session.close()

    except Exception as e:
        pass


async def _get_sessions_detailed(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    specific_gym_id: Optional[int],
    exclude_gym_id_one: bool,
    daily_revenue: dict,
    gym_revenue: dict,
    source_revenue: dict
):
    """Get Sessions revenue with daily and gym breakdowns."""
    try:
        conditions = [SessionPurchase.status == "paid"]

        if specific_gym_id is not None:
            conditions.append(SessionPurchase.gym_id == specific_gym_id)
        elif exclude_gym_id_one:
            conditions.append(SessionPurchase.gym_id != 1)

        # Date filtering: only use created_at (purchase date)
        conditions.append(func.date(SessionPurchase.created_at) >= start_date)
        conditions.append(func.date(SessionPurchase.created_at) <= end_date)

        stmt = (
            select(
                SessionPurchase.payable_rupees,
                SessionPurchase.created_at,
                SessionPurchase.gym_id
            )
            .where(and_(*conditions))
        )

        result = await db.execute(stmt)
        sessions = result.all()

        for purchase in sessions:
            amount_rupees = purchase.payable_rupees or 0
            amount_paisa = amount_rupees * 100

            source_revenue["sessions"] += amount_paisa

            # Track daily revenue
            date_key = purchase.created_at.date().isoformat() if purchase.created_at else None
            if date_key:
                if date_key not in daily_revenue:
                    daily_revenue[date_key] = 0
                daily_revenue[date_key] += amount_paisa

            # Track gym-wise revenue
            if purchase.gym_id:
                if purchase.gym_id not in gym_revenue:
                    gym_revenue[purchase.gym_id] = 0
                gym_revenue[purchase.gym_id] += amount_paisa

    except Exception as e:
        pass


async def _get_fittbot_subscription_detailed(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    daily_revenue: dict,
    source_revenue: dict
):
    """
    Get Nutritionist Plan (Fymble Subscription) revenue with daily breakdown.

    NEW LOGIC:
    - Query payments.payments table
    - Filter where payment_metadata -> 'flow' contains 'nutrition_purchase_googleplay'
    - Filter where status = 'captured'
    - Sum amount_minor values
    """
    try:
        import logging
        logger = logging.getLogger("revenue_service")

        # Fetch all captured payments in date range
        fetch_stmt = (
            select(Payment)
            .where(Payment.status == "captured")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )

        fetch_result = await db.execute(fetch_stmt)
        payments = fetch_result.scalars().all()

        logger.info(f"[NUTRITIONIST_PLAN_DETAILED] Total payments: {len(payments)}")

        matched_count = 0
        sample_count = 0
        for payment in payments:
            if payment.payment_metadata and isinstance(payment.payment_metadata, dict):
                # Log first 5 payment metadata samples to debug
                if sample_count < 5:
                    logger.info(f"[NUTRITIONIST_PLAN_DETAILED] Sample {sample_count + 1} metadata: {payment.payment_metadata}")
                    sample_count += 1

                # Check flow at different nesting levels
                flow = None

                # Level 1: Direct flow key
                if "flow" in payment.payment_metadata:
                    flow = payment.payment_metadata.get("flow")

                # Level 2: flow inside order_info
                elif "order_info" in payment.payment_metadata:
                    order_info = payment.payment_metadata.get("order_info")
                    if isinstance(order_info, dict) and "flow" in order_info:
                        flow = order_info.get("flow")

                # Check if flow contains nutrition_purchase_googleplay
                if flow and "nutrition_purchase_googleplay" in str(flow):
                    amount = payment.amount_minor or 0
                    source_revenue["fittbot_subscription"] += amount
                    matched_count += 1

                    # Track daily revenue
                    date_key = payment.captured_at.date().isoformat() if payment.captured_at else None
                    if date_key:
                        if date_key not in daily_revenue:
                            daily_revenue[date_key] = 0
                        daily_revenue[date_key] += amount

        logger.info(f"[NUTRITIONIST_PLAN_DETAILED] Matched: {matched_count}, Revenue: {source_revenue['fittbot_subscription']}")

    except Exception as e:
        import traceback
        traceback.print_exc()


async def _get_gym_membership_detailed(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    specific_gym_id: Optional[int],
    exclude_gym_id_one: bool,
    daily_revenue: dict,
    gym_revenue: dict,
    source_revenue: dict
):
    """Get Gym Membership revenue with daily and gym breakdowns."""
    try:
        # SQL-level metadata filter — avoids loading all payments into Python memory
        meta_cond = or_(
            func.json_unquote(func.json_extract(Order.order_metadata, "$.audit.source")) == "dailypass_checkout_api",
            func.json_unquote(func.json_extract(Order.order_metadata, "$.order_info.flow")) == "unified_gym_membership_with_sub",
            func.json_unquote(func.json_extract(Order.order_metadata, "$.order_info.flow")) == "unified_gym_membership_with_free_fittbot"
        )

        # OrderItem join conditions for gym_id validation
        oi_conditions = [
            OrderItem.order_id == Order.id,
            OrderItem.gym_id.isnot(None),
            OrderItem.gym_id != ""
        ]
        if specific_gym_id is not None:
            oi_conditions.append(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            oi_conditions.append(OrderItem.gym_id != "1")

        # Fetch only SQL-filtered rows — include order_id for deduplication, gym_id for breakdown
        stmt = (
            select(
                Payment.captured_at,
                Order.id.label("order_id"),
                Order.gross_amount_minor,
                OrderItem.gym_id
            )
            .select_from(Payment)
            .join(Order, Order.id == Payment.order_id)
            .join(OrderItem, and_(*oi_conditions))
            .where(
                Payment.status == "captured",
                Order.status == "paid",
                func.date(Payment.captured_at) >= start_date,
                func.date(Payment.captured_at) <= end_date,
                meta_cond
            )
        )

        result = await db.execute(stmt)
        rows = result.all()

        # Deduplicate by order_id to avoid double-counting orders with multiple order_items
        seen_order_ids = set()
        for row in rows:
            if row.order_id in seen_order_ids:
                continue
            seen_order_ids.add(row.order_id)

            amount = row.gross_amount_minor or 0
            source_revenue["gym_membership"] += amount

            # Track daily revenue
            date_key = row.captured_at.date().isoformat() if row.captured_at else None
            if date_key:
                if date_key not in daily_revenue:
                    daily_revenue[date_key] = 0
                daily_revenue[date_key] += amount

            # Track gym-wise revenue
            if row.gym_id:
                try:
                    gym_key = int(row.gym_id)
                    if gym_key not in gym_revenue:
                        gym_revenue[gym_key] = 0
                    gym_revenue[gym_key] += amount
                except (ValueError, TypeError):
                    pass

    except Exception as e:
        import traceback
        traceback.print_exc()


async def _get_ai_credits_detailed(
    db: AsyncSession,
    start_date: date,
    end_date: date,
    daily_revenue: dict,
    source_revenue: dict
):
    """
    Get AI Credits revenue with daily breakdown.

    NEW LOGIC:
    - Query payments.payments table
    - Filter where payment_metadata -> 'flow' contains 'food_scanner_credits'
    - Filter where status = 'captured'
    - Sum amount_minor values
    """
    try:
        import logging
        logger = logging.getLogger("revenue_service")

        # Fetch all captured payments in date range
        fetch_stmt = (
            select(Payment)
            .where(Payment.status == "captured")
            .where(func.date(Payment.captured_at) >= start_date)
            .where(func.date(Payment.captured_at) <= end_date)
        )

        fetch_result = await db.execute(fetch_stmt)
        payments = fetch_result.scalars().all()

        matched_count = 0
        for payment in payments:
            if payment.payment_metadata and isinstance(payment.payment_metadata, dict):
                # Check flow at different nesting levels
                flow = None

                # Level 1: Direct flow key
                if "flow" in payment.payment_metadata:
                    flow = payment.payment_metadata.get("flow")

                # Level 2: flow inside order_info
                elif "order_info" in payment.payment_metadata:
                    order_info = payment.payment_metadata.get("order_info")
                    if isinstance(order_info, dict) and "flow" in order_info:
                        flow = order_info.get("flow")

                # Check if flow contains food_scanner_credits
                if flow and "food_scanner_credits" in str(flow):
                    amount = payment.amount_minor or 0
                    source_revenue["ai_credits"] += amount
                    matched_count += 1

                    # Track daily revenue
                    date_key = payment.captured_at.date().isoformat() if payment.captured_at else None
                    if date_key:
                        if date_key not in daily_revenue:
                            daily_revenue[date_key] = 0
                        daily_revenue[date_key] += amount

        logger.info(f"[AI_CREDITS_DETAILED] Matched: {matched_count}, Revenue: {source_revenue['ai_credits']}")

    except Exception as e:
        import traceback
        traceback.print_exc()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_month_date_range(year: int, month: int) -> Tuple[date, date]:
    """Get start and end date for a given month/year."""
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)
    return start_date, end_date


def paise_to_rupees(paise: int) -> float:
    """Convert paise to rupees."""
    return round(paise / 100, 2)


def paise_to_rupees_float(paise: float) -> float:
    """Convert paise (can be fractional) to rupees."""
    return round(paise / 100, 2)


def calculate_nutritionist_plan_net_revenue(revenue_in_paise: int) -> dict:
    """
    Calculate Net Revenue and GST for Nutritionist Plan (Fittbot Subscription).

    Nutritionist Plan revenue is inclusive of GST, sold through Google Play:
    Step 1: Reverse GST calculation = Revenue / 1.18
    Step 2: Deduct 15% Google commission from taxable value
    Final Net = (Revenue / 1.18) - (Revenue × 0.15)

    Args:
        revenue_in_paise: Revenue amount in PAISA (minor units)

    Returns:
        Dictionary with revenue, gst, google_commission, and net_revenue in PAISA (int)
    """
    from decimal import Decimal

    GST_RATE = Decimal("0.18")  # 18% GST
    GOOGLE_COMMISSION_RATE = Decimal("0.15")  # 15% Google commission

    # Convert to Decimal for precise calculation
    revenue = Decimal(str(revenue_in_paise))

    # Step 1: Reverse GST calculation (amount is inclusive of GST)
    taxable_value = revenue / Decimal("1.18")
    gst = revenue - taxable_value

    # Step 2: Google commission (15% of total revenue)
    google_commission = revenue * GOOGLE_COMMISSION_RATE

    # Step 3: Net revenue after GST and Google commission
    net_revenue = taxable_value - google_commission

    return {
        "revenue": int(revenue),
        "gst": int(gst),
        "google_commission": int(google_commission),
        "net_revenue": int(max(0, net_revenue))
    }


def calculate_ai_credits_net_revenue(revenue_in_paise: int) -> dict:
    """
    Calculate Net Revenue and GST for AI Credits.

    AI Credits revenue is inclusive of GST, sold through Google Play:
    Step 1: Reverse GST calculation = Revenue / 1.18
    Step 2: Deduct 15% Google commission from taxable value
    Final Net = (Revenue / 1.18) - (Revenue × 0.15)

    Args:
        revenue_in_paise: Revenue amount in PAISA (minor units)

    Returns:
        Dictionary with revenue, gst, google_commission, and net_revenue in PAISA (int)
    """
    from decimal import Decimal

    GST_RATE = Decimal("0.18")  # 18% GST
    GOOGLE_COMMISSION_RATE = Decimal("0.15")  # 15% Google commission

    # Convert to Decimal for precise calculation
    revenue = Decimal(str(revenue_in_paise))

    # Step 1: Reverse GST calculation (amount is inclusive of GST)
    taxable_value = revenue / Decimal("1.18")
    gst = revenue - taxable_value

    # Step 2: Google commission (15% of total revenue)
    google_commission = revenue * GOOGLE_COMMISSION_RATE

    # Step 3: Net revenue after GST and Google commission
    net_revenue = taxable_value - google_commission

    return {
        "revenue": int(revenue),
        "gst": int(gst),
        "google_commission": int(google_commission),
        "net_revenue": int(max(0, net_revenue))
    }

