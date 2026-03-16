from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models.database import get_db
from app.models.fittbot_models import GymJoinRequest
from app.utils.logging_utils import FittbotHTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/view_join_requests", tags=["Join Requests"])


@router.get("/get")
async def view_join_requests(gym_id: int, db: Session = Depends(get_db)):
    try:
        # Get all pending join requests for this gym, ordered by latest first
        pending_requests = (
            db.query(GymJoinRequest)
            .filter(
                GymJoinRequest.gym_id == gym_id,
                GymJoinRequest.status == "pending"
            )
            .order_by(desc(GymJoinRequest.created_at))
            .all()
        )

        requests_list = []
        for request in pending_requests:
            requests_list.append({
                "id": request.id,
                "client_id": request.client_id,
                "name": request.name,
                "mobile_number": request.mobile_number,
                "alternate_mobile_number": request.alternate_mobile_number,
                "dp": request.dp,
                "status": request.status,
                "created_at": request.created_at.isoformat() if request.created_at else None,
            })

        return {
            "status": 200,
            "message": "Join requests retrieved successfully",
            "count": len(requests_list),
            "data": requests_list,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="VIEW_JOIN_REQUESTS_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )

class EditRequest(BaseModel):
    id:int


@router.put("/reject")
async def reject_join_request(request:EditRequest, db: Session = Depends(get_db)):
    try:
        # Find the join request by id
        id=request.id
        join_request = db.query(GymJoinRequest).filter(GymJoinRequest.id == id).first()

        if not join_request:
            raise FittbotHTTPException(
                status_code=404,
                detail="Join request not found",
                error_code="JOIN_REQUEST_NOT_FOUND",
                log_data={"id": id},
            )

        # Update status to rejected
        join_request.status = "rejected"
        db.commit()

        return {
            "status": 200,
            "message": "Join request rejected successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="REJECT_JOIN_REQUEST_ERROR",
            log_data={"id": id, "error": str(e)},
        )
