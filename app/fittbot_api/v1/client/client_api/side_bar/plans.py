# app/routers/fittbot_plans_router.py

from typing import Dict, Any, List
from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ClientFittbotAccess, FittbotPlans
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/fittbot_plans", tags=["Plans"])


@router.get("/get_plans")
async def get_client_plan(
    request: Request,
    client_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Return the active plan for a given client_id.
    """
    try:
        # 1) fetch the active access record
        access = (
            db.query(ClientFittbotAccess)
            .filter(
                ClientFittbotAccess.client_id == client_id,
                ClientFittbotAccess.access_status == "active",
            )
            .first()
        )
        if not access:
            raise FittbotHTTPException(
                status_code=404,
                detail="No active plan found for this client",
                error_code="PLAN_ACTIVE_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        # 2) parse plan ID
        try:
            plan_id = int(access.plan)
        except Exception as conv_err:
            raise FittbotHTTPException(
                status_code=500,
                detail="Invalid plan ID stored for this client",
                error_code="PLAN_ID_INVALID",
                log_data={"client_id": client_id, "plan_raw": getattr(access, "plan", None), "exc": repr(conv_err)},
            )

        # 3) fetch plan details
        plan = db.query(FittbotPlans).filter(FittbotPlans.id == plan_id).first()
        if not plan:
            raise FittbotHTTPException(
                status_code=404,
                detail="Plan not found",
                error_code="PLAN_NOT_FOUND",
                log_data={"client_id": client_id, "plan_id": plan_id},
            )

        # 4) build payload
        data = {
            "plan_name": plan.plan_name
        }

        plans: List[FittbotPlans] = db.query(FittbotPlans).all()
        all_data = [
            {
                "plan_name": p.plan_name,
                "duration": p.duration,
                "image_url": p.image_url,
                "package_identifier": p.package_identifier,
            }
            for p in plans
        ]

        # Keep original response shape (note: "currrent_data" spelling preserved)
        return {"status": 200, "currrent_data": data, "all_data": all_data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve plan information",
            error_code="PLANS_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
