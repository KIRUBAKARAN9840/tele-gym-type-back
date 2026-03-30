from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc, func, or_, exists, cast, String, and_, Date
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from datetime import datetime, date

from app.models.async_database import get_async_db
from app.models.client_activity_models import ClientActivitySummary, ClientActivityEvent
from app.models.fittbot_models import Client, Gym, SessionPurchase, FittbotGymMembership, SessionBookingDay
from app.models.dailypass_models import DailyPass, DailyPassDay
from app.models.telecaller_models import ClientCallFeedback, Telecaller

router = APIRouter(prefix="/client-tracking", tags=["Client Tracking"])



@router.get("/clients-summary")
async def get_clients_summary(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by client name or phone"),
    call_status: Optional[str] = Query(None, description="Filter by latest call status: interested, not_interested, callback, no_answer, converted, follow_up, checkout, purchased"),
    last_called_by: Optional[int] = Query(None, description="Filter by executive id (Last Called By)"),
    last_activity_date: Optional[str] = Query(None, description="Filter by last activity date (format: YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_async_db),
):
    try:

        offset = (page - 1) * limit

        # Parse last_activity_date if provided
        parsed_activity_date = None
        if last_activity_date:
            try:
                parsed_activity_date = datetime.strptime(last_activity_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid date format for last_activity_date. Expected YYYY-MM-DD, got: {last_activity_date}"
                )

        # Build feedback subquery when call_status OR last_called_by filter is used
        is_checkout = call_status == "checkout"
        is_purchased = call_status == "purchased"
        latest_fb_detail = None
        if (call_status or last_called_by) and not is_checkout and not is_purchased:
            latest_fb_id_subq = (
                select(
                    ClientCallFeedback.client_id,
                    func.max(ClientCallFeedback.id).label("max_id"),
                )
                .group_by(ClientCallFeedback.client_id)
                .subquery()
            )
            latest_fb_detail_query = (
                select(
                    ClientCallFeedback.client_id,
                    ClientCallFeedback.status.label("call_status"),
                    ClientCallFeedback.executive_id,
                    Telecaller.name.label("executive_name"),
                )
                .join(latest_fb_id_subq, ClientCallFeedback.id == latest_fb_id_subq.c.max_id)
                .join(Telecaller, Telecaller.id == ClientCallFeedback.executive_id)
            )

            # Apply executive filter if last_called_by is specified
            if last_called_by:
                latest_fb_detail_query = latest_fb_detail_query.where(ClientCallFeedback.executive_id == last_called_by)

            latest_fb_detail = latest_fb_detail_query.subquery()

        # --- Count query ---
        count_query = (
            select(func.count(func.distinct(ClientActivitySummary.client_id)))
            .join(Client, Client.client_id == ClientActivitySummary.client_id)
        )

        if is_checkout:
            # Checkout filter: clients with checkout_attempts > 0
            checkout_subq = (
                select(ClientActivitySummary.client_id)
                .group_by(ClientActivitySummary.client_id)
                .having(func.sum(ClientActivitySummary.checkout_attempts) > 0)
                .subquery()
            )
            count_query = count_query.join(
                checkout_subq,
                checkout_subq.c.client_id == ClientActivitySummary.client_id,
            )
        elif is_purchased:
            # Purchased filter: clients with at least one purchase in any of 3 tables
            dp_exists = exists().where(DailyPass.client_id == cast(ClientActivitySummary.client_id, String))
            sp_exists = exists().where((SessionPurchase.client_id == ClientActivitySummary.client_id) & (SessionPurchase.status == "paid"))
            gm_exists = exists().where(
                (FittbotGymMembership.client_id == cast(ClientActivitySummary.client_id, String))
                & (FittbotGymMembership.type.in_(["gym_membership", "personal_training"]))
            )
            count_query = count_query.where(or_(dp_exists, sp_exists, gm_exists))
        elif call_status:
            count_query = count_query.join(
                latest_fb_detail,
                latest_fb_detail.c.client_id == ClientActivitySummary.client_id,
            ).where(latest_fb_detail.c.call_status == call_status)
            # Also filter by last_called_by if specified
            if last_called_by:
                count_query = count_query.where(latest_fb_detail.c.executive_id == last_called_by)
        elif last_called_by:
            # Apply last_called_by filter to count query
            count_query = count_query.join(
                latest_fb_detail,
                latest_fb_detail.c.client_id == ClientActivitySummary.client_id,
            )

        if search:
            search_term = f"%{search.lower()}%"
            count_query = count_query.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                )
            )

        # Apply last_activity_date filter to count query
        if parsed_activity_date:
            activity_date_subq = (
                select(ClientActivitySummary.client_id)
                .group_by(ClientActivitySummary.client_id)
                .having(cast(func.max(ClientActivitySummary.last_viewed_at), Date) == parsed_activity_date)
                .subquery()
            )
            count_query = count_query.join(
                activity_date_subq,
                activity_date_subq.c.client_id == ClientActivitySummary.client_id,
            )

        total_count = await db.scalar(count_query) or 0

        if total_count == 0:
            return {
                "status": 200,
                "message": "No clients found",
                "data": [],
                "pagination": {
                    "total": 0,
                    "limit": limit,
                    "page": page,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                },
            }

        # --- Main query ---
        if is_purchased:
            # Purchased filter: same exists logic
            dp_exists = exists().where(DailyPass.client_id == cast(ClientActivitySummary.client_id, String))
            sp_exists = exists().where((SessionPurchase.client_id == ClientActivitySummary.client_id) & (SessionPurchase.status == "paid"))
            gm_exists = exists().where(
                (FittbotGymMembership.client_id == cast(ClientActivitySummary.client_id, String))
                & (FittbotGymMembership.type.in_(["gym_membership", "personal_training"]))
            )

            main_query = (
                select(
                    ClientActivitySummary.client_id,
                    Client.name.label("client_name"),
                    Client.profile.label("dp"),
                    Client.contact.label("phone"),
                    func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                    func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                )
                .join(Client, Client.client_id == ClientActivitySummary.client_id)
                .where(or_(dp_exists, sp_exists, gm_exists))
            )

            group_by_cols = [
                ClientActivitySummary.client_id,
                Client.name, Client.profile, Client.contact,
            ]
        elif is_checkout:
            # Checkout filter: clients with checkout_attempts > 0
            main_query = (
                select(
                    ClientActivitySummary.client_id,
                    Client.name.label("client_name"),
                    Client.profile.label("dp"),
                    Client.contact.label("phone"),
                    func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                    func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                )
                .join(Client, Client.client_id == ClientActivitySummary.client_id)
                .join(
                    checkout_subq,
                    checkout_subq.c.client_id == ClientActivitySummary.client_id,
                )
            )

            group_by_cols = [
                ClientActivitySummary.client_id,
                Client.name, Client.profile, Client.contact,
            ]
        elif call_status:
            # With status filter: join feedback, select call_status + executive_name
            main_query = (
                select(
                    ClientActivitySummary.client_id,
                    Client.name.label("client_name"),
                    Client.profile.label("dp"),
                    Client.contact.label("phone"),
                    func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                    func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                    latest_fb_detail.c.call_status,
                    latest_fb_detail.c.executive_name,
                )
                .join(Client, Client.client_id == ClientActivitySummary.client_id)
                .join(
                    latest_fb_detail,
                    latest_fb_detail.c.client_id == ClientActivitySummary.client_id,
                )
                .where(latest_fb_detail.c.call_status == call_status)
            )

            group_by_cols = [
                ClientActivitySummary.client_id,
                Client.name, Client.profile, Client.contact,
                latest_fb_detail.c.call_status,
                latest_fb_detail.c.executive_name,
            ]
        elif last_called_by:
            # Apply last_called_by filter to main query
            main_query = (
                select(
                    ClientActivitySummary.client_id,
                    Client.name.label("client_name"),
                    Client.profile.label("dp"),
                    Client.contact.label("phone"),
                    func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                    func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                    latest_fb_detail.c.call_status,
                    latest_fb_detail.c.executive_name,
                )
                .join(Client, Client.client_id == ClientActivitySummary.client_id)
                .join(
                    latest_fb_detail,
                    latest_fb_detail.c.client_id == ClientActivitySummary.client_id,
                )
            )

            group_by_cols = [
                ClientActivitySummary.client_id,
                Client.name, Client.profile, Client.contact,
                latest_fb_detail.c.call_status,
                latest_fb_detail.c.executive_name,
            ]
        else:
            # Without status filter: simple query, no feedback join
            if last_called_by and latest_fb_detail:
                # Include executive information when last_called_by filter is applied
                main_query = (
                    select(
                        ClientActivitySummary.client_id,
                        Client.name.label("client_name"),
                        Client.profile.label("dp"),
                        Client.contact.label("phone"),
                        func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                        func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                        latest_fb_detail.c.executive_name,
                    )
                    .join(Client, Client.client_id == ClientActivitySummary.client_id)
                    .join(
                        latest_fb_detail,
                        latest_fb_detail.c.client_id == ClientActivitySummary.client_id,
                    )
                )

                group_by_cols = [
                    ClientActivitySummary.client_id,
                    Client.name, Client.profile, Client.contact,
                    latest_fb_detail.c.executive_name,
                ]
            else:
                main_query = (
                    select(
                        ClientActivitySummary.client_id,
                        Client.name.label("client_name"),
                        Client.profile.label("dp"),
                        Client.contact.label("phone"),
                        func.count(func.distinct(ClientActivitySummary.gym_id)).label("total_gyms_viewed"),
                        func.max(ClientActivitySummary.last_viewed_at).label("last_viewed_at"),
                    )
                    .join(Client, Client.client_id == ClientActivitySummary.client_id)
                )

                group_by_cols = [
                    ClientActivitySummary.client_id,
                    Client.name, Client.profile, Client.contact,
                ]

        if search:
            search_term = f"%{search.lower()}%"
            main_query = main_query.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                )
            )

        # Apply last_activity_date filter using HAVING clause
        query_to_execute = main_query.group_by(*group_by_cols)
        if parsed_activity_date:
            query_to_execute = query_to_execute.having(cast(func.max(ClientActivitySummary.last_viewed_at), Date) == parsed_activity_date)

        result = await db.execute(
            query_to_execute
            .order_by(desc("last_viewed_at"))
            .offset(offset)
            .limit(limit)
        )
        rows = result.all()

        if not rows:
            return {
                "status": 200,
                "message": "No clients found",
                "data": [],
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "page": page,
                    "totalPages": (total_count + limit - 1) // limit,
                    "hasNext": False,
                    "hasPrev": page > 1,
                },
            }

        # batch-fetch interested_products for these clients
        client_ids = [row.client_id for row in rows]

        products_result = await db.execute(
            select(
                ClientActivitySummary.client_id,
                ClientActivitySummary.interested_products,
            ).where(ClientActivitySummary.client_id.in_(client_ids))
        )
        products_rows = products_result.all()

        # merge unique products per client across all gym rows
        client_products_map: Dict[int, set] = {}
        for prod_row in products_rows:
            cid = prod_row.client_id
            products = prod_row.interested_products
            if products:
                if cid not in client_products_map:
                    client_products_map[cid] = set()
                if isinstance(products, list):
                    client_products_map[cid].update(products)
                elif isinstance(products, str):
                    client_products_map[cid].add(products)

        products_final: Dict[int, List[str]] = {
            cid: sorted(prods) for cid, prods in client_products_map.items()
        }

        # batch-fetch last_called_by when NO call_status filter or checkout (not in rows)
        last_called_map = {}
        if not call_status or is_checkout or is_purchased:
            latest_call_subq = (
                select(
                    ClientCallFeedback.client_id,
                    func.max(ClientCallFeedback.id).label("max_id"),
                )
                .where(ClientCallFeedback.client_id.in_(client_ids))
                .group_by(ClientCallFeedback.client_id)
                .subquery()
            )
            last_called_result = await db.execute(
                select(
                    ClientCallFeedback.client_id,
                    Telecaller.name.label("executive_name"),
                    ClientCallFeedback.status.label("call_status"),
                )
                .join(latest_call_subq, ClientCallFeedback.id == latest_call_subq.c.max_id)
                .join(Telecaller, Telecaller.id == ClientCallFeedback.executive_id)
            )
            last_called_map = {
                r.client_id: {"executive_name": r.executive_name, "call_status": r.call_status}
                for r in last_called_result.all()
            }

        # batch-fetch purchase counts from 3 tables
        str_client_ids = [str(cid) for cid in client_ids]

        dp_count_result = await db.execute(
            select(
                DailyPass.client_id,
                func.count(DailyPass.id).label("cnt"),
            )
            .where(DailyPass.client_id.in_(str_client_ids))
            .group_by(DailyPass.client_id)
        )
        dp_count_map = {int(r.client_id): r.cnt for r in dp_count_result.all()}

        sp_count_result = await db.execute(
            select(
                SessionPurchase.client_id,
                func.count(SessionPurchase.id).label("cnt"),
            )
            .where(SessionPurchase.client_id.in_(client_ids), SessionPurchase.status == "paid")
            .group_by(SessionPurchase.client_id)
        )
        sp_count_map = {r.client_id: r.cnt for r in sp_count_result.all()}

        gm_count_result = await db.execute(
            select(
                FittbotGymMembership.client_id,
                func.count(FittbotGymMembership.id).label("cnt"),
            )
            .where(
                FittbotGymMembership.client_id.in_(str_client_ids),
                FittbotGymMembership.type.in_(["gym_membership", "personal_training"]),
            )
            .group_by(FittbotGymMembership.client_id)
        )
        gm_count_map = {int(r.client_id): r.cnt for r in gm_count_result.all()}

        # batch-fetch last purchased date from each table
        dp_date_result = await db.execute(
            select(
                DailyPass.client_id,
                func.max(DailyPass.created_at).label("last_date"),
            )
            .where(DailyPass.client_id.in_(str_client_ids))
            .group_by(DailyPass.client_id)
        )
        dp_date_map = {int(r.client_id): r.last_date for r in dp_date_result.all()}

        sp_date_result = await db.execute(
            select(
                SessionPurchase.client_id,
                func.max(SessionPurchase.created_at).label("last_date"),
            )
            .where(SessionPurchase.client_id.in_(client_ids), SessionPurchase.status == "paid")
            .group_by(SessionPurchase.client_id)
        )
        sp_date_map = {r.client_id: r.last_date for r in sp_date_result.all()}

        gm_date_result = await db.execute(
            select(
                FittbotGymMembership.client_id,
                func.max(FittbotGymMembership.purchased_at).label("last_date"),
            )
            .where(
                FittbotGymMembership.client_id.in_(str_client_ids),
                FittbotGymMembership.type.in_(["gym_membership", "personal_training"]),
            )
            .group_by(FittbotGymMembership.client_id)
        )
        gm_date_map = {int(r.client_id): r.last_date for r in gm_date_result.all()}

        # build response
        clients_data = []
        for row in rows:
            purchase_count = (
                dp_count_map.get(row.client_id, 0)
                + sp_count_map.get(row.client_id, 0)
                + gm_count_map.get(row.client_id, 0)
            )

            if call_status and not is_checkout and not is_purchased:
                # call_status + executive_name come from the joined row
                row_call_status = row.call_status
                row_executive_name = row.executive_name
            else:
                # get from batch-fetched map
                fb_info = last_called_map.get(row.client_id, {})
                row_call_status = fb_info.get("call_status")
                row_executive_name = fb_info.get("executive_name")

            # latest purchase date across all 3 tables
            dates = [d for d in (
                dp_date_map.get(row.client_id),
                sp_date_map.get(row.client_id),
                gm_date_map.get(row.client_id),
            ) if d is not None]
            last_purchased_date = max(dates) if dates else None

            clients_data.append({
                "client_id": row.client_id,
                "client_name": row.client_name,
                "dp": row.dp,
                "phone": row.phone,
                "total_gyms_viewed": row.total_gyms_viewed,
                "last_viewed_at": row.last_viewed_at.isoformat() if row.last_viewed_at else None,
                "interested_products": products_final.get(row.client_id, []),
                "call_status": row_call_status,
                "executive_name": row_executive_name,
                "purchases": purchase_count,
                "last_purchased_date": last_purchased_date.isoformat() if last_purchased_date else None,
            })

        total_pages = (total_count + limit - 1) // limit

        return {
            "status": 200,
            "message": f"Successfully retrieved {len(clients_data)} clients",
            "data": clients_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "page": page,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch clients summary: {str(e)}",
        )

@router.get("/client-detail/{client_id}")
async def get_client_detail(
    client_id: int,
    page: int = Query(1, ge=1, description="Page number for events"),
    limit: int = Query(10, ge=1, le=100, description="Events per page"),
    db: AsyncSession = Depends(get_async_db),
):
    try:
        offset = (page - 1) * limit

        # client info
        client_result = await db.execute(
            select(Client).where(Client.client_id == client_id)
        )
        client = client_result.scalars().first()

        if not client:
            raise HTTPException(status_code=404, detail="Client not found")

        # summary rows for this client (one per gym)
        summary_result = await db.execute(
            select(
                ClientActivitySummary.gym_id,
                Gym.name.label("gym_name"),
                Gym.area.label("gym_area"),
                ClientActivitySummary.total_views,
                ClientActivitySummary.lead_status,
                ClientActivitySummary.lead_score,
                ClientActivitySummary.interested_products,
                ClientActivitySummary.last_viewed_at,
                ClientActivitySummary.checkout_attempts,
                ClientActivitySummary.purchases,
            )
            .outerjoin(Gym, Gym.gym_id == ClientActivitySummary.gym_id)
            .where(ClientActivitySummary.client_id == client_id)
            .order_by(desc(ClientActivitySummary.last_viewed_at))
        )
        summary_rows = summary_result.all()

        gym_summaries = []
        for s in summary_rows:
            gym_summaries.append({
                "gym_id": s.gym_id,
                "gym_name": s.gym_name,
                "gym_area": s.gym_area,
                "total_views": s.total_views,
                "interested_products": s.interested_products or [],
                "last_viewed_at": s.last_viewed_at.isoformat() if s.last_viewed_at else None,
                "checkout_attempts": s.checkout_attempts,
                "purchases": s.purchases,
            })

        # event history count
        total_events = await db.scalar(
            select(func.count(ClientActivityEvent.id))
            .where(ClientActivityEvent.client_id == client_id)
        ) or 0

        events_result = await db.execute(
            select(
                ClientActivityEvent.id,
                ClientActivityEvent.event_type,
                ClientActivityEvent.gym_id,
                Gym.name.label("gym_name"),
                ClientActivityEvent.product_type,
                ClientActivityEvent.product_details,
                ClientActivityEvent.source,
                ClientActivityEvent.created_at,
            )
            .outerjoin(Gym, Gym.gym_id == ClientActivityEvent.gym_id)
            .where(ClientActivityEvent.client_id == client_id)
            .order_by(desc(ClientActivityEvent.created_at))
            .offset(offset)
            .limit(limit)
        )
        event_rows = events_result.all()

        events = []
        for ev in event_rows:
            events.append({
                "id": ev.id,
                "event_type": ev.event_type,
                "gym_id": ev.gym_id,
                "gym_name": ev.gym_name,
                "product_type": ev.product_type,
                "product_details": ev.product_details,
                "source": ev.source,
                "created_at": ev.created_at.isoformat() if ev.created_at else None,
            })

        total_pages = (total_events + limit - 1) // limit if total_events > 0 else 0

        # ── recent purchases (dailypass + session + membership) ──
        str_client_id = str(client_id)
        all_purchases = []

        # daily passes - get day status from daily_pass_days table with aggregated info
        from sqlalchemy import case, literal_column, over

        # Query to get attendance stats for each pass
        day_stats_subq = select(
            DailyPassDay.pass_id,
            func.count().label('total_days'),
            func.sum(case(
                (DailyPassDay.status == 'attended', 1),
                else_=0
            )).label('attended_days'),
            func.sum(case(
                (DailyPassDay.status == 'missed', 1),
                else_=0
            )).label('missed_days'),
            func.sum(case(
                (DailyPassDay.status == 'scheduled', 1),
                else_=0
            )).label('scheduled_days')
        ).group_by(DailyPassDay.pass_id).subquery()

        # Get the most recent past day's status
        latest_past_day_subq = select(
            DailyPassDay.pass_id,
            DailyPassDay.status.label('latest_past_status'),
            func.row_number().over(
                partition_by=DailyPassDay.pass_id,
                order_by=desc(DailyPassDay.scheduled_date)
            ).label('rn')
        ).where(
            # Only get past days (today or before)
            DailyPassDay.scheduled_date <= func.current_date()
        ).subquery()

        dp_result = await db.execute(
            select(
                DailyPass,
                day_stats_subq.c.total_days,
                day_stats_subq.c.attended_days,
                day_stats_subq.c.missed_days,
                day_stats_subq.c.scheduled_days,
                latest_past_day_subq.c.latest_past_status
            )
            .join(day_stats_subq, DailyPass.id == day_stats_subq.c.pass_id)
            .outerjoin(
                latest_past_day_subq,
                and_(
                    DailyPass.id == latest_past_day_subq.c.pass_id,
                    latest_past_day_subq.c.rn == 1
                )
            )
            .where(DailyPass.client_id == str_client_id)
            .order_by(desc(DailyPass.created_at))
            .limit(5)
        )
        daily_passes = dp_result.all()

        if daily_passes:
            dp_gym_ids = list({dp.DailyPass.gym_id for dp in daily_passes if dp.DailyPass.gym_id})
            dp_gym_map = {}
            if dp_gym_ids:
                dp_gym_result = await db.execute(
                    select(Gym).where(Gym.gym_id.in_(dp_gym_ids))
                )
                dp_gym_map = {str(g.gym_id): g.name for g in dp_gym_result.scalars().all()}

            # Get all pass IDs from the result
            pass_ids = [dp.DailyPass.id for dp in daily_passes]

            # Fetch all scheduled days for these passes
            all_days_result = await db.execute(
                select(DailyPassDay)
                .where(DailyPassDay.pass_id.in_(pass_ids))
                .order_by(DailyPassDay.scheduled_date)
            )
            all_days = all_days_result.scalars().all()

            # Group days by pass_id
            pass_days_map = {}
            for day in all_days:
                if day.pass_id not in pass_days_map:
                    pass_days_map[day.pass_id] = []
                pass_days_map[day.pass_id].append({
                    "scheduled_date": day.scheduled_date.isoformat() if day.scheduled_date else None,
                    "status": day.status
                })

            for dp in daily_passes:
                total_days = dp.total_days or 0
                attended = dp.attended_days or 0
                missed = dp.missed_days or 0
                scheduled = dp.scheduled_days or 0

                # Calculate attendance percentage
                attendance_percent = round((attended / total_days * 100)) if total_days > 0 else 0

                # Determine display status
                # If all days are completed (no scheduled days remaining)
                if scheduled == 0:
                    if attended == total_days:
                        status_display = f"Completed ({attended}/{total_days} attended)"
                    elif missed > 0:
                        status_display = f"Partially attended ({attended}/{total_days})"
                    else:
                        status_display = f"{missed}/{total_days} missed"
                else:
                    # Some days still remaining
                    status_display = f"In progress ({attended}/{total_days} attended)"

                all_purchases.append({
                    "type": "dailypass",
                    "gym_name": dp_gym_map.get(str(dp.DailyPass.gym_id), "Unknown Gym"),
                    "amount": (dp.DailyPass.amount_paid * 0.01) if dp.DailyPass.amount_paid else 0,
                    "status": status_display,
                    "days": dp.DailyPass.days_total,
                    "date": dp.DailyPass.created_at.isoformat() if dp.DailyPass.created_at else None,
                    "scheduled_dates": pass_days_map.get(dp.DailyPass.id, [])
                })

        # session purchases
        sp_result = await db.execute(
            select(SessionPurchase)
            .where(SessionPurchase.client_id == client_id, SessionPurchase.status == "paid")
            .order_by(desc(SessionPurchase.created_at))
            .limit(5)
        )
        session_purchases = sp_result.scalars().all()

        if session_purchases:
            sp_gym_ids = list({sp.gym_id for sp in session_purchases if sp.gym_id})
            sp_gym_map = {}
            if sp_gym_ids:
                sp_gym_result = await db.execute(
                    select(Gym).where(Gym.gym_id.in_(sp_gym_ids))
                )
                sp_gym_map = {g.gym_id: g.name for g in sp_gym_result.scalars().all()}

            # Extract schedule_ids from scheduled_sessions JSON and group by schedule_id
            schedule_ids = []
            purchase_schedule_map = {}  # Maps schedule_id to purchase info

            for sp in session_purchases:
                if sp.scheduled_sessions:
                    import json
                    try:
                        scheduled = json.loads(sp.scheduled_sessions) if isinstance(sp.scheduled_sessions, str) else sp.scheduled_sessions
                        if isinstance(scheduled, list) and len(scheduled) > 0:
                            schedule_id = scheduled[0].get('schedule_id')
                            if schedule_id:
                                schedule_ids.append(schedule_id)
                                purchase_schedule_map[schedule_id] = sp
                    except:
                        pass

            # Fetch all booking days for these schedule_ids
            schedule_booking_map = {}

            if schedule_ids:
                booking_days_result = await db.execute(
                    select(SessionBookingDay)
                    .where(SessionBookingDay.schedule_id.in_(schedule_ids))
                    .order_by(SessionBookingDay.booking_date)
                )
                all_booking_days = booking_days_result.scalars().all()

                for booking in all_booking_days:
                    sid = booking.schedule_id
                    if sid not in schedule_booking_map:
                        schedule_booking_map[sid] = []

                    schedule_booking_map[sid].append({
                        "booking_date": booking.booking_date.isoformat() if booking.booking_date else None,
                        "status": booking.status,
                        "start_time": booking.start_time.isoformat() if booking.start_time else None,
                        "end_time": booking.end_time.isoformat() if booking.end_time else None
                    })

            # Create one card per schedule_id
            for schedule_id, sp in purchase_schedule_map.items():
                all_purchases.append({
                    "type": "session",
                    "gym_name": sp_gym_map.get(sp.gym_id, "Unknown Gym"),
                    "amount": sp.payable_rupees,
                    "status": sp.status,
                    "sessions_count": sp.sessions_count,
                    "date": sp.created_at.isoformat() if sp.created_at else None,
                    "scheduled_dates": schedule_booking_map.get(schedule_id, [])
                })

        # gym memberships
        gm_result = await db.execute(
            select(FittbotGymMembership)
            .where(FittbotGymMembership.client_id == str_client_id,FittbotGymMembership.type=="gym_membership")
            .order_by(desc(FittbotGymMembership.purchased_at))
            .limit(5)
        )
        gym_memberships = gm_result.scalars().all()

        if gym_memberships:
            gm_gym_ids = list({m.gym_id for m in gym_memberships if m.gym_id})
            gm_gym_map = {}
            if gm_gym_ids:
                gm_gym_result = await db.execute(
                    select(Gym).where(Gym.gym_id.in_(gm_gym_ids))
                )
                gm_gym_map = {str(g.gym_id): g.name for g in gm_gym_result.scalars().all()}

            for m in gym_memberships:
                all_purchases.append({
                    "type": "membership",
                    "gym_name": gm_gym_map.get(str(m.gym_id), "Unknown Gym"),
                    "amount": m.amount or 0,
                    "status": m.status,
                    "expires_at": m.expires_at.isoformat() if m.expires_at else None,
                    "date": m.purchased_at.isoformat() if m.purchased_at else None,
                })

        # sort all by date descending, take latest 10
        all_purchases.sort(key=lambda x: x.get("date") or "", reverse=True)
        recent_purchases = all_purchases[:10]

        return {
            "status": 200,
            "message": "Successfully retrieved client detail",
            "data": {
                "client_id": client.client_id,
                "client_name": client.name,
                "dp": client.profile,
                "phone": client.contact,
                "gym_summaries": gym_summaries,
                "recent_purchases": recent_purchases,
                "events": events,
            },
            "pagination": {
                "total": total_events,
                "limit": limit,
                "page": page,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch client detail: {str(e)}",
        )


# ──────────────────────────────────────────────────────────────────────
# API 3 – Insert Call Feedback
# ──────────────────────────────────────────────────────────────────────


class CallFeedbackRequest(BaseModel):
    client_id: int
    executive_id: int
    feedback: str
    status: str


@router.post("/call-feedback")
async def create_call_feedback(
    body: CallFeedbackRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        #print(f"Creating call feedback for client_id={body.client_id}, executive_id={body.executive_id}")

        new_feedback = ClientCallFeedback(
            client_id=body.client_id,
            executive_id=body.executive_id,
            feedback=body.feedback,
            status=body.status,
            created_at=datetime.now()
        )
        db.add(new_feedback)
        await db.commit()
        await db.refresh(new_feedback)

        # get executive name
        exec_result = await db.execute(
            select(Telecaller.name).where(Telecaller.id == new_feedback.executive_id)
        )
        executive_name = exec_result.scalar_one_or_none()

        return {
            "status": 200,
            "message": "Call feedback added successfully",
            "data": {
                "id": new_feedback.id,
                "client_id": new_feedback.client_id,
                "executive_id": new_feedback.executive_id,
                "executive_name": executive_name,
                "feedback": new_feedback.feedback,
                "status": new_feedback.status,
                "created_at": new_feedback.created_at.isoformat() if new_feedback.created_at else None,
            },
        }

    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add call feedback: {str(e)}",
        )


# ──────────────────────────────────────────────────────────────────────
# API 4 – Get All Call Feedback for a Client (latest first)
# ──────────────────────────────────────────────────────────────────────

@router.get("/call-feedback/{client_id}")
async def get_call_feedback(
    client_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_async_db),
):
    try:
        #print(f"Fetching call feedback for client_id={client_id}, page={page}, limit={limit}")
        offset = (page - 1) * limit

        total_count = await db.scalar(
            select(func.count(ClientCallFeedback.id))
            .where(ClientCallFeedback.client_id == client_id)
        ) or 0

        if total_count == 0:
            return {
                "status": 200,
                "message": "No call feedback found",
                "data": [],
                "pagination": {
                    "total": 0,
                    "limit": limit,
                    "page": page,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False,
                },
            }

        result = await db.execute(
            select(
                ClientCallFeedback.id,
                ClientCallFeedback.client_id,
                ClientCallFeedback.executive_id,
                Telecaller.name.label("executive_name"),
                ClientCallFeedback.feedback,
                ClientCallFeedback.status,
                ClientCallFeedback.created_at,
            )
            .outerjoin(Telecaller, Telecaller.id == ClientCallFeedback.executive_id)
            .where(ClientCallFeedback.client_id == client_id)
            .order_by(desc(ClientCallFeedback.created_at))
            .offset(offset)
            .limit(limit)
        )
        rows = result.all()

        feedback_data = []
        for row in rows:
            feedback_data.append({
                "id": row.id,
                "client_id": row.client_id,
                "executive_id": row.executive_id,
                "executive_name": row.executive_name,
                "feedback": row.feedback,
                "status": row.status,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            })

        total_pages = (total_count + limit - 1) // limit

        return {
            "status": 200,
            "message": f"Successfully retrieved {len(feedback_data)} call feedbacks",
            "data": feedback_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "page": page,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch call feedback: {str(e)}",
        )


