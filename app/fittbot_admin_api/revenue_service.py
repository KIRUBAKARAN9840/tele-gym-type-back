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


class AmortizedRevenueBreakdown(BaseModel):
    """Revenue breakdown with amortization for MRR (all values in PAISA)"""
    total_revenue: float
    daily_pass: int
    sessions: int
    fittbot_subscription: float  # Can be fractional due to amortization
    gym_membership: float  # Can be fractional due to amortization


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

        # Date filtering: created OR updated within range
        created_in_range = and_(
            func.date(SessionPurchase.created_at) >= start_date,
            func.date(SessionPurchase.created_at) <= end_date
        )
        updated_in_range = and_(
            func.date(SessionPurchase.updated_at) >= start_date,
            func.date(SessionPurchase.updated_at) <= end_date
        )

        conditions.append(or_(created_in_range, updated_in_range))

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
    Get Fymble Subscription revenue for a date range.

    Uses two methods (EXACTLY matching original admindashboard.py logic):
    1. Orders with provider_order_id like 'sub_%' -> Payment.amount_minor (with captured_at date filter)
    2. Payments with provider = 'google_play' -> Payment.amount_minor (with captured_at date filter)

    Args:
        db: AsyncSession
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        exclude_gym_id_one: Whether to exclude gym_id = 1
        specific_gym_id: Filter for specific gym (overrides exclude_gym_id_one)

    Returns:
        Revenue in PAISA
    """
    total_revenue = 0

    # METHOD 1: Orders table -> Payments table (provider_order_id like 'sub_%')
    try:
        order_stmt = (
            select(Order.id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
        )

        # Gym filtering
        if specific_gym_id is not None:
            order_stmt = order_stmt.where(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            order_stmt = order_stmt.where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))

        order_result = await db.execute(order_stmt)
        orders = order_result.all()

        if orders:
            order_ids = [order.id for order in orders]

            # Query payments table using the order IDs WITH date filter
            payment_from_order_stmt = (
                select(func.coalesce(func.sum(Payment.amount_minor), 0))
                .where(Payment.order_id.in_(order_ids))
                .where(func.date(Payment.captured_at) >= start_date)
                .where(func.date(Payment.captured_at) <= end_date)
            )

            payment_from_order_result = await db.execute(payment_from_order_stmt)
            total_revenue += payment_from_order_result.scalar() or 0

    except Exception as e:
        pass

    # METHOD 2: Direct query on payments table (provider = 'google_play')
    # Note: This catches Google Play payments that may not have 'sub_%' in provider_order_id
    try:
        conditions = [
            Payment.provider == "google_play",
            Payment.status == "captured",
            Order.status == "paid",
            func.date(Payment.captured_at) >= start_date,
            func.date(Payment.captured_at) <= end_date
        ]

        # Gym filtering
        if specific_gym_id is not None:
            conditions.append(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            conditions.append(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))

        stmt_2 = (
            select(func.coalesce(func.sum(Payment.amount_minor), 0))
            .join(Order, Order.id == Payment.order_id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(and_(*conditions))
        )

        result_2 = await db.execute(stmt_2)
        total_revenue += result_2.scalar() or 0

    except Exception as e:
        pass

    return int(total_revenue)


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
        # print(f"[REVENUE_SERVICE] Gym Membership query: {start_date} to {end_date}, exclude_gym_1={exclude_gym_id_one}")

        # Fetch payments and orders
        conditions = [
            Payment.status == "captured",
            Order.status == "paid",
            func.date(Payment.captured_at) >= start_date,
            func.date(Payment.captured_at) <= end_date
        ]

        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(and_(*conditions))
        )
        payment_result = await db.execute(payment_stmt)
        payments = payment_result.all()

        # print(f"[REVENUE_SERVICE] Gym Membership - Found {len(payments)} payments")

        if not payments:
            return 0

        # Collect order IDs
        order_ids = [row.Order.id for row in payments]

        # Fetch order items to get gym_ids
        order_items_conditions = [
            OrderItem.order_id.in_(order_ids),
            OrderItem.gym_id.isnot(None)
        ]

        if specific_gym_id is not None:
            order_items_conditions.append(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            order_items_conditions.append(OrderItem.gym_id != "1")

        order_items_stmt = (
            select(OrderItem)
            .where(and_(*order_items_conditions))
        )
        order_items_result = await db.execute(order_items_stmt)
        order_items = order_items_result.scalars().all()

        # print(f"[REVENUE_SERVICE] Gym Membership - Found {len(order_items)} order items after gym filter")

        # Create mapping: order_id -> gym_id
        order_gym_mapping = {}
        for item in order_items:
            if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                order_gym_mapping[item.order_id] = int(item.gym_id)

        # Filter by metadata conditions and sum revenue
        total_revenue = 0
        matching_orders = 0
        for row in payments:
            order = row.Order

            # Check order_metadata
            if not order.order_metadata or not isinstance(order.order_metadata, dict):
                continue

            metadata = order.order_metadata

            # Condition 1: audit.source = "dailypass_checkout_api"
            condition1 = False
            if metadata.get("audit") and isinstance(metadata.get("audit"), dict):
                if metadata["audit"].get("source") == "dailypass_checkout_api":
                    condition1 = True

            # Condition 2: order_info.flow = "unified_gym_membership_with_sub"
            condition2 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_sub":
                    condition2 = True

            # Condition 3: order_info.flow = "unified_gym_membership_with_free_fittbot"
            condition3 = False
            if metadata.get("order_info") and isinstance(metadata.get("order_info"), dict):
                if metadata["order_info"].get("flow") == "unified_gym_membership_with_free_fittbot":
                    condition3 = True

            # Only include if any condition matches AND order has valid gym_id
            if not (condition1 or condition2 or condition3):
                continue

            if order.id not in order_gym_mapping:
                continue

            matching_orders += 1
            total_revenue += order.gross_amount_minor or 0

        # print(f"[REVENUE_SERVICE] Gym Membership - Found {matching_orders} matching orders, total revenue: {total_revenue}")

        return int(total_revenue)

    except Exception as e:
        # print(f"[REVENUE_SERVICE] Error fetching Gym Membership: {e}")
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

    total_revenue = daily_pass + sessions + fittbot_subscription + gym_membership

    return RevenueBreakdown(
        total_revenue=total_revenue,
        daily_pass=daily_pass,
        sessions=sessions,
        fittbot_subscription=fittbot_subscription,
        gym_membership=gym_membership
    )


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
    Get amortized Fymble Subscription revenue for MRR.

    Includes ALL subscriptions active during the target month,
    with revenue distributed monthly based on subscription duration.

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
        # Build conditions
        conditions = [
            Order.status == "paid",
            or_(
                Payment.provider == "google_play",
                Order.provider_order_id.like("sub_%"),
                OrderItem.item_type == "app_subscription",
                OrderItem.item_type == "fittbot_subscription"
            )
        ]

        if exclude_gym_id_one:
            conditions.append(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))

        # Fetch subscriptions (using Payment.amount_minor to match original logic)
        stmt = (
            select(
                Payment.amount_minor,
                OrderItem.sku,
                Payment.captured_at,
                Order.created_at
            )
            .select_from(Order)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .join(Payment, Payment.order_id == Order.id)
            .where(and_(*conditions))
        )

        result = await db.execute(stmt)
        subscriptions = result.all()

        for sub in subscriptions:
            amount = sub.amount_minor or 0  # Use Payment.amount_minor, not Order.gross_amount_minor
            sku = sub.sku
            payment_date = sub.captured_at or sub.created_at

            if not payment_date:
                continue

            payment_date_only = payment_date.date() if isinstance(payment_date, datetime) else payment_date
            duration_months = PRODUCT_PLAN_MAPPING.get(sku, 1)

            # Calculate validity end date
            validity_end_date = (
                date(payment_date_only.year, payment_date_only.month, 1) +
                timedelta(days=32 * duration_months)
            )
            validity_end_date = date(validity_end_date.year, validity_end_date.month, 1) - timedelta(days=1)

            # Check if target month overlaps with validity period
            if validity_end_date >= target_month_start and payment_date_only <= target_month_end:
                mrr_contribution = amount / duration_months
                total_revenue += mrr_contribution

    except Exception as e:
        pass

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

    total_revenue = float(daily_pass) + float(sessions) + fittbot_subscription + gym_membership

    return AmortizedRevenueBreakdown(
        total_revenue=total_revenue,
        daily_pass=daily_pass,
        sessions=sessions,
        fittbot_subscription=fittbot_subscription,
        gym_membership=gym_membership
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
        source: Filter by specific source (daily_pass, sessions, fittbot_subscription, gym_membership)
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
        "gym_membership": 0
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
        "gym_membership": source_revenue_paisa["gym_membership"] / 100
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

        created_in_range = and_(
            func.date(SessionPurchase.created_at) >= start_date,
            func.date(SessionPurchase.created_at) <= end_date
        )
        updated_in_range = and_(
            func.date(SessionPurchase.updated_at) >= start_date,
            func.date(SessionPurchase.updated_at) <= end_date
        )
        conditions.append(or_(created_in_range, updated_in_range))

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
    """Get Fymble Subscription revenue with daily breakdown using Payment.amount_minor."""
    # METHOD 1: Orders -> Payments (provider_order_id like 'sub_%')
    # Two-step approach to avoid duplicate rows from Order-Payment join
    try:
        # Step 1: Get Order IDs (no Payment join yet)
        order_stmt = (
            select(Order.id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(Order.provider_order_id.like("sub_%"))
            .where(Order.status == "paid")
            .where(or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None)))
        )

        order_result = await db.execute(order_stmt)
        orders = order_result.all()

        if orders:
            order_ids = [order.id for order in orders]

            # Step 2: Get Payments for those orders with date filter
            payment_stmt = (
                select(Payment.amount_minor, Payment.captured_at)
                .where(Payment.order_id.in_(order_ids))
                .where(func.date(Payment.captured_at) >= start_date)
                .where(func.date(Payment.captured_at) <= end_date)
            )

            payment_result = await db.execute(payment_stmt)
            for row in payment_result.all():
                amount = row.amount_minor or 0
                source_revenue["fittbot_subscription"] += amount

                # Track daily revenue
                date_key = row.captured_at.date().isoformat() if row.captured_at else None
                if date_key:
                    if date_key not in daily_revenue:
                        daily_revenue[date_key] = 0
                    daily_revenue[date_key] += amount

    except Exception as e:
        pass

    # METHOD 2: Direct query on payments (provider = 'google_play')
    try:
        conditions = [
            Payment.provider == "google_play",
            Payment.status == "captured",
            Order.status == "paid",
            func.date(Payment.captured_at) >= start_date,
            func.date(Payment.captured_at) <= end_date,
            or_(OrderItem.gym_id != "1", OrderItem.gym_id.is_(None))
        ]

        stmt = (
            select(
                Payment.amount_minor,
                Payment.captured_at
            )
            .join(Order, Order.id == Payment.order_id)
            .join(OrderItem, OrderItem.order_id == Order.id)
            .where(and_(*conditions))
        )

        result = await db.execute(stmt)
        for row in result.all():
            amount = row.amount_minor or 0
            source_revenue["fittbot_subscription"] += amount

            # Track daily revenue
            date_key = row.captured_at.date().isoformat() if row.captured_at else None
            if date_key:
                if date_key not in daily_revenue:
                    daily_revenue[date_key] = 0
                daily_revenue[date_key] += amount

    except Exception as e:
        pass


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
        conditions = [
            Payment.status == "captured",
            Order.status == "paid",
            func.date(Payment.captured_at) >= start_date,
            func.date(Payment.captured_at) <= end_date
        ]

        payment_stmt = (
            select(Payment, Order)
            .join(Order, Order.id == Payment.order_id)
            .where(and_(*conditions))
        )

        payment_result = await db.execute(payment_stmt)
        payments = payment_result.all()

        if not payments:
            return

        # Collect order IDs
        order_ids = [row.Order.id for row in payments]

        # Fetch order items to get gym_ids
        order_items_conditions = [
            OrderItem.order_id.in_(order_ids),
            OrderItem.gym_id.isnot(None)
        ]

        if specific_gym_id is not None:
            order_items_conditions.append(OrderItem.gym_id == str(specific_gym_id))
        elif exclude_gym_id_one:
            order_items_conditions.append(OrderItem.gym_id != "1")

        order_items_stmt = (
            select(OrderItem)
            .where(and_(*order_items_conditions))
        )
        order_items_result = await db.execute(order_items_stmt)
        order_items = order_items_result.scalars().all()

        # Create mapping: order_id -> gym_id
        order_gym_mapping = {}
        for item in order_items:
            if item.gym_id and item.gym_id.strip() and item.gym_id.isdigit():
                order_gym_mapping[item.order_id] = int(item.gym_id)

        # Process payments and track revenue
        for row in payments:
            order = row.Order

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

            amount = order.gross_amount_minor or 0
            source_revenue["gym_membership"] += amount

            # Track daily revenue
            date_key = row.Payment.captured_at.date().isoformat() if row.Payment.captured_at else None
            if date_key:
                if date_key not in daily_revenue:
                    daily_revenue[date_key] = 0
                daily_revenue[date_key] += amount

            # Track gym-wise revenue
            gym_key = order_gym_mapping.get(order.id)
            if gym_key:
                if gym_key not in gym_revenue:
                    gym_revenue[gym_key] = 0
                gym_revenue[gym_key] += amount

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
