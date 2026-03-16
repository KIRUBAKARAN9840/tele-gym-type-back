# app/routers/owner_newbies.py

from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Client, GymBatches, GymPlans, FittbotGymMembership
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/owner", tags=["Gymowner"])


@router.get("/newbies_v1")
async def get_monthly_newbies(
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        # basic validation
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        today = date.today()
        first_of_month = today.replace(day=1)

        # count for the month
        total = (
            db.query(Client)
            .filter(
                Client.gym_id == gym_id,
                Client.status == "active",
                Client.joined_date >= first_of_month,
            )
            .count()
        )

        if total == 0:
            return {"status": 200, "data": {"total_entrants": 0, "clients": []}}

        # fetch details (outer joins so clients without batch/plan still included)
        rows = (
            db.query(
                Client.name,
                Client.contact.label("phone"),
                Client.batch_id,
                GymBatches.batch_name,
                Client.training_id,
                GymPlans.plans.label("training_name"),
                Client.joined_date,
            )
            .outerjoin(GymBatches, GymBatches.batch_id == Client.batch_id)
            .outerjoin(GymPlans, GymPlans.id == Client.training_id)
            .filter(
                Client.gym_id == gym_id,
                Client.status == "active",
                Client.joined_date >= first_of_month,
            )
            .order_by(desc(Client.joined_date))
            .all()
        )

        clients = [
            {
                "name": r.name,
                "phone": r.phone,
                "batch_id": r.batch_id,
                "batch_name": r.batch_name,
                "training_id": r.training_id,
                "training_name": r.training_name,
                "joined_date": (
                    r.joined_date.isoformat()
                    if hasattr(r.joined_date, "isoformat")
                    else str(r.joined_date)
                ),
            }
            for r in rows
        ]

        payload = {"total_entrants": total, "clients": clients}

        return {"status": 200, "data": payload}

    except FittbotHTTPException:
        # pass through structured errors
        raise
    except Exception as e:
        # wrap any unexpected error
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch monthly new clients",
            error_code="NEWBIES_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


@router.get("/newbies")
async def get_monthly_newbies_from_membership(
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        # basic validation
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        today = date.today()
        first_of_month = today.replace(day=1)

        # count for the month from FittbotGymMembership
        total = (
            db.query(FittbotGymMembership)
            .filter(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.joined_at >= first_of_month,
            )
            .count()
        )

        if total == 0:
            return {"status": 200, "data": {"total_entrants": 0, "clients": []}}

        # fetch details using client_id from FittbotGymMembership
        rows = (
            db.query(
                Client.name,
                Client.contact.label("phone"),
                Client.batch_id,
                GymBatches.batch_name,
                Client.training_id,
                GymPlans.plans.label("training_name"),
                FittbotGymMembership.joined_at.label("joined_date"),
                Client.goals,
            )
            .join(Client, Client.client_id == FittbotGymMembership.client_id)
            .outerjoin(GymBatches, GymBatches.batch_id == Client.batch_id)
            .outerjoin(GymPlans, GymPlans.id == Client.training_id)
            .filter(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.joined_at >= first_of_month,
            )
            .order_by(desc(FittbotGymMembership.joined_at))
            .all()
        )

        clients = [
            {
                "name": r.name,
                "phone": r.phone,
                "batch_id": r.batch_id,
                "batch_name": r.batch_name,
                "training_id": r.training_id,
                "training_name": r.training_name,
                "joined_date": (
                    r.joined_date.isoformat()
                    if hasattr(r.joined_date, "isoformat")
                    else str(r.joined_date)
                ),
                "goal": r.goals,
            }
            for r in rows
        ]

        payload = {"total_entrants": total, "clients": clients}

        return {"status": 200, "data": payload}

    except FittbotHTTPException:
        # pass through structured errors
        raise
    except Exception as e:
        # wrap any unexpected error
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch monthly new clients from membership",
            error_code="NEWBIES_MEMBERSHIP_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )
