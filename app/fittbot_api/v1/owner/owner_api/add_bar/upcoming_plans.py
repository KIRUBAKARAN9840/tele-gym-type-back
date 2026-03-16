
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List, Dict, Any
from app.models.async_database import get_async_db
from app.models.fittbot_models import FittbotGymMembership, GymPlans, Client

router = APIRouter(prefix="/pnb", tags=["Upcoming Plans"])


@router.get("/gym/upcoming_plans")
async def get_upcoming_plans(
    gym_id: int = Query(...),
    db: AsyncSession = Depends(get_async_db)
):

    try:

        memberships_result = await db.execute(
            select(FittbotGymMembership)
            .where(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.type=="gym_membership",
                FittbotGymMembership.status == "upcoming"
            )
            .order_by(FittbotGymMembership.purchased_at.desc())
        )
        memberships = memberships_result.scalars().all()

        if not memberships:
            return {
                "data": [],
                "total_bookings": 0,
                "status": 200,
                "message": "No upcoming plans found"
            }

        # Collect unique plan_ids and client_ids
        plan_ids = list(set(m.plan_id for m in memberships if m.plan_id))
        client_ids = list(set(int(m.client_id) for m in memberships if m.client_id))

        # Fetch all plans in one query
        plans_dict = {}
        if plan_ids:
            plans_result = await db.execute(
                select(GymPlans).where(GymPlans.id.in_(plan_ids))
            )
            plans = plans_result.scalars().all()
            plans_dict = {p.id: p for p in plans}

        # Fetch all clients in one query
        clients_dict = {}
        if client_ids:
            clients_result = await db.execute(
                select(Client).where(Client.client_id.in_(client_ids))
            )
            clients = clients_result.scalars().all()
            clients_dict = {c.client_id: c for c in clients}

        # Build response
        upcoming_data = []
        for idx, membership in enumerate(memberships, start=1):
            plan = plans_dict.get(membership.plan_id) if membership.plan_id else None
            client = clients_dict.get(int(membership.client_id)) if membership.client_id else None

            plan_details = None
            if plan:
                plan_details = {
                    "id": plan.id,
                    "duration": plan.duration,
                    "personal_training": plan.personal_training,
                    "plan_for": plan.plan_for if plan.plan_for else "individual",
                    "booking_date": membership.purchased_at.isoformat() if membership.purchased_at else None
                }

            client_details = None
            if client:
                client_details = {
                    "client_id": client.client_id,
                    "name": client.name,
                    "contact": client.contact,
                    "dp": client.profile
                }

            upcoming_data.append({
                "id": idx,
                "plan": plan_details,
                "client": client_details
            })

        return {
            "data": upcoming_data,
            "total_bookings": len(upcoming_data),
            "status": 200,
            "message": "Upcoming plans listed successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
