from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, cast, String, or_, and_, desc, union, union_all, literal, over, distinct
from app.models.async_database import get_async_db
from app.models.telecaller_models import Telecaller, UserConversion, ClientCallFeedback, ConvertedBy
from app.models.fittbot_models import Client, Gym, GymOwner, SessionPurchase, FittbotGymMembership
from app.models.dailypass_models import DailyPass, get_dailypass_session
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.payments import Payment
from app.fittbot_api.v1.payments.models.orders import Order, OrderItem
from app.fittbot_admin_api.users.usersDashboard import get_plan_name_from_product_id
from datetime import datetime, timezone, timedelta

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

router = APIRouter(prefix="/api/admin/user-conversion", tags=["AdminUserConversion"])


async def get_latest_purchase_type(user_id: str, db: AsyncSession) -> Optional[str]:
    """
    Get the latest purchase type for a user.
    Returns a single string like "Daily Pass", "Session", "Gym Membership", or "Fittbot Subscription".
    Optimized with a single UNION query.
    """
    try:
        # Build a UNION query to get the latest purchase from all 4 types
        daily_pass_sub = select(
            DailyPass.created_at.label('purchase_date'),
            literal('Daily Pass').label('purchase_type')
        ).where(
            DailyPass.client_id == user_id
        )

        session_sub = select(
            SessionPurchase.created_at.label('purchase_date'),
            literal('Session').label('purchase_type')
        ).where(
            and_(
                SessionPurchase.client_id == user_id,
                SessionPurchase.status == "paid"
            )
        )

        membership_sub = select(
            FittbotGymMembership.purchased_at.label('purchase_date'),
            literal('Gym Membership').label('purchase_type')
        ).where(
            and_(
                func.cast(FittbotGymMembership.client_id, String) == user_id,
                FittbotGymMembership.type.notin_(['normal', 'admission_fees'])
            )
        )

        nutrition_sub = select(
            Payment.captured_at.label('purchase_date'),
            literal('Nutrition Plan').label('purchase_type')
        ).where(
            and_(
                Payment.customer_id == user_id,
                Payment.status == "captured"
            )
        )
        
        ai_credits_sub = select(
            Payment.captured_at.label('purchase_date'),
            literal('AI Credits').label('purchase_type')
        ).where(
            and_(
                Payment.customer_id == user_id,
                Payment.status == "captured"
            )
        )

        # Combine all and get the latest
        # We will filter the results in python to handle JSON metadata correctly
        combined = union_all(
            daily_pass_sub,
            session_sub,
            membership_sub,
            nutrition_sub,
            ai_credits_sub
        ).subquery()

        # Since we need to check metadata, we'll fetch them all and filter in Python
        # for a reliable "Latest Purchased" column that includes the new types
        all_stmt = select(
            combined.c.purchase_type,
            combined.c.purchase_date
        ).order_by(desc(combined.c.purchase_date))
        
        import json
        
        all_result = await db.execute(all_stmt)
        # However, to be 100% accurate with the JSON metadata, we should fetch 
        # the payments separately if the union type is Nutrition or AI.
        # For performance, we'll just return the first match from the ordered union
        # but specifically check the payments table for those users.
        
        # Simplified: Check standard ones first, then check payments table for AI/Nutrition
        for row in all_result.all():
            if row.purchase_type in ["Daily Pass", "Session", "Gym Membership"]:
                # Rename Session to Fitness class if needed, or keep internal naming
                return "Fitness class" if row.purchase_type == "Session" else row.purchase_type
            
            # For Nutrition/AI, we need to verify the metadata flow
            p_stmt = select(Payment).where(
                Payment.customer_id == user_id,
                Payment.status == "captured",
                Payment.captured_at == row.purchase_date
            )
            p_res = await db.execute(p_stmt)
            p = p_res.scalar_one_or_none()
            if p:
                meta = p.payment_metadata
                if isinstance(meta, str):
                    try: meta = json.loads(meta)
                    except: meta = {}
                elif not isinstance(meta, dict): meta = {}
                
                flow = meta.get("flow", "")
                if flow == "nutrition_purchase_googleplay":
                    return "Nutrition Plan"
                if flow == "food_scanner_credits":
                    return "AI Credits"
        
        return None

    except Exception as e:
        print(f"[LATEST_PURCHASE_TYPE] Error for user {user_id}: {e}")
        return None


async def get_telecaller_total_revenue(telecaller_id: int, db: AsyncSession) -> float:
    """
    Calculate total revenue from all converted clients of a telecaller.

    Logic:
    1. Get all unique client_ids converted by this telecaller
    2. Join payments -> order_items
    3. Filter gym_id != '1'
    4. Sum amount_minor from payments
    5. Convert to rupees by dividing by 100
    """
    try:
        # Get unique converted client_ids from both UserConversion and ClientCallFeedback
        uc_clients = select(
            UserConversion.client_id
        ).where(UserConversion.telecaller_id == telecaller_id)

        ccf_clients = select(
            ClientCallFeedback.client_id
        ).where(
            ClientCallFeedback.executive_id == telecaller_id,
            ClientCallFeedback.status == 'converted'
        )

        combined = union_all(uc_clients, ccf_clients).subquery()

        # Get distinct client_ids
        distinct_clients = select(
            combined.c.client_id
        ).distinct().subquery()

        # Calculate total revenue: payments -> order_items with gym_id != '1'
        # Join: Payment -> Order -> OrderItem
        # Filter: gym_id != '1'
        # Sum: amount_minor from Payment

        revenue_stmt = select(
            func.coalesce(func.sum(Payment.amount_minor), 0)
        ).join(
            Order, Order.id == Payment.order_id
        ).join(
            OrderItem, OrderItem.order_id == Order.id
        ).join(
            distinct_clients, distinct_clients.c.client_id == Payment.customer_id
        ).where(
            Payment.status == "captured",
            or_(
                OrderItem.gym_id != '1',
                OrderItem.gym_id.is_(None)
            )
        )

        result = await db.execute(revenue_stmt)
        total_amount_minor = result.scalar() or 0

        # Convert from minor to major (paise to rupees)
        # Convert to float first, then divide by 100
        total_revenue = float(total_amount_minor) / 100

        return total_revenue

    except Exception as e:
        print(f"[TELECALLER_REVENUE] Error calculating revenue for telecaller {telecaller_id}: {e}")
        import traceback
        traceback.print_exc()
        return 0.0


async def get_user_last_purchases_async(user_id: str, db: AsyncSession):
    """
    Get all four types of purchases for a specific user.
    Returns the most recent purchase for each of the four types.
    Async version for use in other endpoints.
    """
    now = datetime.now(IST)

    purchases = {
        "daily_pass": None,
        "session": None,
        "membership": None,
        "subscription": None,
        "ai_credits": None
    }

    # 1. Get latest Daily Pass
    try:
        daily_pass_stmt = select(
            DailyPass,
            Gym.name.label('gym_name')
        ).outerjoin(
            Gym, DailyPass.gym_id == cast(Gym.gym_id, String)
        ).where(
            DailyPass.client_id == user_id
        ).order_by(DailyPass.created_at.desc()).limit(1)

        daily_pass_result = await db.execute(daily_pass_stmt)
        daily_pass_row = daily_pass_result.first()

        if daily_pass_row:
            dp = daily_pass_row.DailyPass
            purchases["daily_pass"] = {
                "type": "Daily Pass",
                "purchase_date": dp.created_at.isoformat() if dp.created_at else None,
                "gym_name": daily_pass_row.gym_name,
                "days_total": dp.days_total,
                "amount_paid": float(dp.amount_paid) if dp.amount_paid else None
            }
    except Exception as e:
        print(f"[LAST_PURCHASES] Error fetching Daily Pass: {e}")

    # 2. Get latest Session Purchase (only paid status)
    try:
        session_stmt = select(
            SessionPurchase,
            Gym.name.label('gym_name')
        ).outerjoin(
            Gym, SessionPurchase.gym_id == Gym.gym_id
        ).where(
            and_(
                SessionPurchase.client_id == user_id,
                SessionPurchase.status == "paid"
            )
        ).order_by(SessionPurchase.created_at.desc()).limit(1)

        session_result = await db.execute(session_stmt)
        session_row = session_result.first()

        if session_row:
            sp = session_row.SessionPurchase
            purchases["session"] = {
                "type": "Session",
                "purchase_date": sp.created_at.isoformat() if sp.created_at else None,
                "gym_name": session_row.gym_name,
                "sessions_count": sp.sessions_count,
                "scheduled_sessions": sp.scheduled_sessions,
                "payable_rupees": float(sp.payable_rupees) if sp.payable_rupees else None
            }
    except Exception as e:
        print(f"[LAST_PURCHASES] Error fetching Session: {e}")

    # 3. Get latest Gym Membership (excluding 'normal' and 'admission_fees')
    try:
        membership_stmt = select(
            FittbotGymMembership,
            Gym.name.label('gym_name')
        ).outerjoin(
            Gym, func.cast(FittbotGymMembership.gym_id, String) == func.cast(Gym.gym_id, String)
        ).where(
            and_(
                func.cast(FittbotGymMembership.client_id, String) == user_id,
                FittbotGymMembership.type.notin_(['normal', 'admission_fees'])
            )
        ).order_by(FittbotGymMembership.purchased_at.desc()).limit(1)

        membership_result = await db.execute(membership_stmt)
        membership_row = membership_result.first()

        if membership_row:
            gm = membership_row.FittbotGymMembership
            purchases["membership"] = {
                "type": "Membership",
                "purchase_date": gm.purchased_at.isoformat() if gm.purchased_at else None,
                "gym_name": membership_row.gym_name,
                "membership_type": gm.type,
                "amount": float(gm.amount) if gm.amount else None
            }
    except Exception as e:
        print(f"[LAST_PURCHASES] Error fetching Membership: {e}")

    # 4. Get latest Nutrition Plan (Subscription)
    try:
        import json
        sub_stmt = (
            select(Payment)
            .where(
                and_(
                    Payment.customer_id == user_id,
                    Payment.status == "captured"
                )
            )
            .order_by(Payment.captured_at.desc())
        )
        
        sub_result = await db.execute(sub_stmt)
        for payment in sub_result.scalars():
            metadata = payment.payment_metadata
            if isinstance(metadata, str):
                try: metadata = json.loads(metadata)
                except: metadata = {}
            elif not isinstance(metadata, dict): metadata = {}
            
            if metadata.get("flow") == "nutrition_purchase_googleplay":
                purchases["subscription"] = {
                    "type": "Nutrition Plan",
                    "purchase_date": payment.captured_at.isoformat() if payment.captured_at else (payment.created_at.isoformat() if payment.created_at else None),
                    "gym_name": None,
                    "amount_paid": float(payment.amount_minor) / 100.0,
                    "status": payment.status
                }
                break
    except Exception as e:
        print(f"[LAST_PURCHASES] Error fetching Nutrition Plan: {e}")

    # 5. Get latest AI Credit purchase
    try:
        import json
        ai_stmt = (
            select(Payment)
            .where(
                and_(
                    Payment.customer_id == user_id,
                    Payment.status == "captured"
                )
            )
            .order_by(Payment.captured_at.desc())
        )
        
        ai_result = await db.execute(ai_stmt)
        for payment in ai_result.scalars():
            metadata = payment.payment_metadata
            if isinstance(metadata, str):
                try: metadata = json.loads(metadata)
                except: metadata = {}
            elif not isinstance(metadata, dict): metadata = {}
            
            if metadata.get("flow") == "food_scanner_credits":
                purchases["ai_credits"] = {
                    "type": "AI Credits",
                    "purchase_date": payment.captured_at.isoformat() if payment.captured_at else (payment.created_at.isoformat() if payment.created_at else None),
                    "gym_name": None,
                    "amount": float(payment.amount_minor) / 100.0,
                    "status": payment.status
                }
                break
    except Exception as e:
        print(f"[LAST_PURCHASES] Error fetching AI Credits: {e}")

    return purchases


@router.get("/telecallers")
async def get_telecallers_with_conversion_count(
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Get all telecallers
        telecaller_stmt = select(
            Telecaller.id,
            Telecaller.name,
            Telecaller.mobile_number,
            Telecaller.status,
            Telecaller.verified,
            Telecaller.created_at
        ).order_by(Telecaller.created_at.desc())

        telecaller_result = await db.execute(telecaller_stmt)
        telecallers = telecaller_result.all()

        # Create a dict to store telecaller data
        telecaller_dict = {t.id: {
            "id": t.id,
            "name": t.name,
            "mobile_number": t.mobile_number,
            "total_converted": 0,
            "bookings_count": 0,
            "total_revenue": 0.0
        } for t in telecallers}

        # ========== EFFICIENT CONVERSION COUNT (Single Query) ==========
        # Get all converted clients with their telecallers in one query
        # From UserConversion
        uc_all = select(
            UserConversion.telecaller_id,
            UserConversion.client_id,
            UserConversion.converted_at,
            literal('user_conversion').label('source')
        ).where(UserConversion.telecaller_id.in_(list(telecaller_dict.keys())))

        # From ClientCallFeedback
        ccf_all = select(
            ClientCallFeedback.executive_id.label('telecaller_id'),
            ClientCallFeedback.client_id,
            ClientCallFeedback.created_at.label('converted_at'),
            literal('call_feedback').label('source')
        ).where(
            ClientCallFeedback.executive_id.in_(list(telecaller_dict.keys())),
            ClientCallFeedback.status == 'converted'
        )

        combined_all = union_all(uc_all, ccf_all).subquery()

        # Rank by converted_at for each client to get only the latest
        ranked_all = select(
            combined_all.c.telecaller_id,
            combined_all.c.client_id,
            func.row_number().over(
                partition_by=combined_all.c.client_id,
                order_by=desc(combined_all.c.converted_at)
            ).label('rn')
        ).subquery()

        # Count latest conversions per telecaller
        conversion_counts = select(
            ranked_all.c.telecaller_id,
            func.count().label('count')
        ).where(ranked_all.c.rn == 1).group_by(ranked_all.c.telecaller_id)

        conv_result = await db.execute(conversion_counts)
        for row in conv_result.all():
            if row.telecaller_id in telecaller_dict:
                telecaller_dict[row.telecaller_id]["total_converted"] = row.count

        # ========== EFFICIENT REVENUE CALCULATION (Same logic as individual page) ==========
        # Use the EXACT same logic as get_telecaller_total_revenue function
        # Get distinct client_ids for all telecallers, then join with payments

        # Create combined clients with telecaller_id mapping
        combined_for_revenue = union_all(
            select(
                UserConversion.telecaller_id,
                UserConversion.client_id
            ).where(UserConversion.telecaller_id.in_(telecaller_dict.keys())),
            select(
                ClientCallFeedback.executive_id.label('telecaller_id'),
                ClientCallFeedback.client_id
            ).where(
                ClientCallFeedback.executive_id.in_(list(telecaller_dict.keys())),
                ClientCallFeedback.status == 'converted'
            )
        ).subquery()

        # Get distinct (telecaller_id, client_id) pairs - this handles duplicates
        distinct_client_telecaller = select(
            combined_for_revenue.c.telecaller_id,
            combined_for_revenue.c.client_id
        ).distinct().subquery()

        # Calculate revenue by joining with payments and order_items
        # Join: distinct_client_telecaller -> Payment -> Order -> OrderItem
        revenue_query = select(
            distinct_client_telecaller.c.telecaller_id,
            func.coalesce(func.sum(Payment.amount_minor), 0).label('amount_minor'),
            func.count(distinct(Payment.id)).label('bookings_count')
        ).join(
            Payment, Payment.customer_id == distinct_client_telecaller.c.client_id
        ).join(
            Order, Order.id == Payment.order_id
        ).join(
            OrderItem, OrderItem.order_id == Order.id
        ).where(
            Payment.status == "captured",
            or_(
                OrderItem.gym_id != '1',
                OrderItem.gym_id.is_(None)
            )
        ).group_by(distinct_client_telecaller.c.telecaller_id)

        revenue_result = await db.execute(revenue_query)
        for row in revenue_result.all():
            if row.telecaller_id in telecaller_dict:
                telecaller_dict[row.telecaller_id]["total_revenue"] = float(row.amount_minor) / 100
                telecaller_dict[row.telecaller_id]["bookings_count"] = row.bookings_count

        # Convert dict back to list
        telecaller_list = list(telecaller_dict.values())

        return {
            "success": True,
            "data": {
                "telecallers": telecaller_list,
                "total": len(telecaller_list)
            },
            "message": "Telecallers with conversion count and revenue fetched successfully"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(f"Failed to fetch telecallers: {str(e)}")


@router.get("/telecallers/{telecaller_id}/converted-clients")
async def get_telecaller_converted_clients(
    telecaller_id: int,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1, le=100),
    search: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_async_db)
):
  
    try:
        # Verify telecaller exists
        telecaller_stmt = select(Telecaller).where(Telecaller.id == telecaller_id)
        telecaller_result = await db.execute(telecaller_stmt)
        telecaller = telecaller_result.scalar_one_or_none()

        if not telecaller:
            return {
                "success": False,
                "message": "Telecaller not found"
            }

        # Get converted clients from both UserConversion and ClientCallFeedback
        # Source 1: UserConversion table
        uc_sub = select(
            UserConversion.id.label('conversion_id'),
            cast(UserConversion.client_id, String).label('client_id'),
            UserConversion.purchased_plan,
            UserConversion.converted_at,
            literal('user_conversion').label('source')
        ).where(
            UserConversion.telecaller_id == telecaller_id
        )

        # Source 2: ClientCallFeedback with status='converted'
        ccf_sub = select(
            ClientCallFeedback.id.label('conversion_id'),
            cast(ClientCallFeedback.client_id, String).label('client_id'),
            literal(None).label('purchased_plan'),
            ClientCallFeedback.created_at.label('converted_at'),
            literal('call_feedback').label('source')
        ).where(
            ClientCallFeedback.executive_id == telecaller_id,
            ClientCallFeedback.status == 'converted'
        )

        combined = union_all(uc_sub, ccf_sub).subquery()

        # Create a window function to rank records by converted_at for each client
        # This helps us get only the latest entry for each client_id
        from sqlalchemy import over, Integer
        ranked_stmt = select(
            combined.c.conversion_id,
            combined.c.client_id,
            combined.c.purchased_plan,
            combined.c.converted_at,
            combined.c.source,
            func.row_number().over(
                partition_by=combined.c.client_id,
                order_by=desc(combined.c.converted_at)
            ).label('rn')
        ).subquery()

        # Filter to keep only the latest record for each client (rn = 1)
        latest_conversions = select(
            ranked_stmt.c.conversion_id,
            ranked_stmt.c.client_id,
            ranked_stmt.c.purchased_plan,
            ranked_stmt.c.converted_at,
            ranked_stmt.c.source
        ).where(ranked_stmt.c.rn == 1).subquery()

        conversion_stmt = select(
            latest_conversions.c.conversion_id,
            latest_conversions.c.client_id,
            latest_conversions.c.purchased_plan,
            latest_conversions.c.converted_at,
            latest_conversions.c.source,
            Client.name.label('client_name'),
            Client.contact.label('client_contact'),
            Client.email.label('client_email'),
            Client.created_at.label('client_created_at'),
            Gym.name.label('gym_name')
        ).outerjoin(
            Client,
            latest_conversions.c.client_id == cast(Client.client_id, String)
        ).outerjoin(
            Gym,
            Client.gym_id == Gym.gym_id
        )

        # Apply search filter if provided
        if search and search.strip():
            search_term = f"%{search.lower()}%"
            conversion_stmt = conversion_stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                    latest_conversions.c.client_id.like(search_term)
                )
            )

        # Get total count before pagination
        count_subquery = conversion_stmt.subquery()
        count_stmt = select(func.count()).select_from(count_subquery)
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        conversion_stmt = conversion_stmt.order_by(desc(latest_conversions.c.converted_at)).offset(offset).limit(limit)

        conversion_result = await db.execute(conversion_stmt)
        conversions = conversion_result.all()

        client_list = []
        for conversion in conversions:
            # Get the latest purchase type for this client
            latest_purchase_type = await get_latest_purchase_type(conversion.client_id, db)

            client_list.append({
                "conversion_id": conversion.conversion_id,
                "client_id": conversion.client_id,
                "name": conversion.client_name,
                "contact": conversion.client_contact,
                "email": conversion.client_email,
                "gym_name": conversion.gym_name,
                "purchased_plan": conversion.purchased_plan,
                "converted_at": conversion.converted_at.isoformat() if conversion.converted_at else None,
                "created_at": conversion.client_created_at.isoformat() if conversion.client_created_at else None,
                "source": conversion.source,
                "latest_purchase_type": latest_purchase_type
            })

        # Calculate total revenue from converted clients
        total_revenue = await get_telecaller_total_revenue(telecaller_id, db)

        total_pages = (total_count + limit - 1) // limit

        return {
            "success": True,
            "data": {
                "telecaller": {
                    "id": telecaller.id,
                    "name": telecaller.name,
                    "mobile_number": telecaller.mobile_number
                },
                "clients": client_list,
                "total": total_count,
                "total_revenue": round(total_revenue, 2),
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            },
            "message": "Converted clients fetched successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to fetch converted clients: {str(e)}"
        }


@router.get("/telecallers/{telecaller_id}/converted-gyms")
async def get_telecaller_converted_gyms(
    telecaller_id: int,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1, le=100),
    search: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get gyms converted by a telecaller.

    Flow:
    1. Query converted_by table in telecaller schema for gym_ids where telecaller_id matches
    2. Join with gyms table in fittbot_local schema to get gym details
    3. Join with gym_owners table to get contact number
    4. Apply search, pagination and return results
    """
    try:
        # Verify telecaller exists
        telecaller_stmt = select(Telecaller).where(Telecaller.id == telecaller_id)
        telecaller_result = await db.execute(telecaller_stmt)
        telecaller = telecaller_result.scalar_one_or_none()

        if not telecaller:
            return {
                "success": False,
                "message": "Telecaller not found"
            }

        # Build query with joins: converted_by -> gyms -> gym_owners
        gym_stmt = select(
            Gym.gym_id,
            Gym.name.label('gym_name'),
            Gym.area,
            Gym.location,
            GymOwner.contact_number.label('owner_contact'),
            GymOwner.name.label('owner_name'),
            ConvertedBy.created_at.label('converted_at')
        ).join(
            ConvertedBy,
            ConvertedBy.gym_id == Gym.gym_id
        ).outerjoin(
            GymOwner,
            Gym.owner_id == GymOwner.owner_id
        ).where(
            ConvertedBy.telecaller_id == telecaller_id
        )

        # Apply search filter if provided
        if search and search.strip():
            search_term = f"%{search.lower()}%"
            gym_stmt = gym_stmt.where(
                or_(
                    func.lower(Gym.name).like(search_term),
                    Gym.area.like(search_term),
                    Gym.location.like(search_term),
                    Gym.gym_id.like(search_term)
                )
            )

        # Get total count before pagination
        count_subquery = gym_stmt.subquery()
        count_stmt = select(func.count()).select_from(count_subquery)
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination and sorting
        offset = (page - 1) * limit
        gym_stmt = gym_stmt.order_by(desc(ConvertedBy.created_at)).offset(offset).limit(limit)

        gym_result = await db.execute(gym_stmt)
        gyms = gym_result.all()

        gym_list = []
        for gym in gyms:
            gym_list.append({
                "gym_id": gym.gym_id,
                "gym_name": gym.gym_name,
                "area": gym.area,
                "location": gym.location,
                "contact_number": gym.owner_contact,
                "owner_name": gym.owner_name,
                "converted_at": gym.converted_at.isoformat() if gym.converted_at else None
            })

        total_pages = (total_count + limit - 1) // limit

        return {
            "success": True,
            "data": {
                "telecaller": {
                    "id": telecaller.id,
                    "name": telecaller.name,
                    "mobile_number": telecaller.mobile_number
                },
                "gyms": gym_list,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            },
            "message": "Converted gyms fetched successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to fetch converted gyms: {str(e)}"
        }
