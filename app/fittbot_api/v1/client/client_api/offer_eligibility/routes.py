
from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from typing import Literal, Optional

from app.models.async_database import get_async_db
from app.utils.logging_utils import FittbotHTTPException
from .eligibility_service import check_offer_eligibility_detailed

router = APIRouter(prefix="/offer_eligibility", tags=["Offer Eligibility"])


class OfferEligibilityRequest(BaseModel):
    """Request model for checking offer eligibility"""
    gym_id: int = Field(..., description="Gym ID to check eligibility at")
    mode: Literal["dailypass", "session"] = Field(..., description="Type of offer: 'dailypass' for ₹49 or 'session' for ₹99")


class OfferEligibilityResponse(BaseModel):
    """Response model for offer eligibility check"""
    status: int = 200
    is_eligible: bool = Field(..., description="True if BOTH user AND gym are eligible for the offer")
    client_eligible: bool = Field(..., description="True if user meets eligibility criteria (< 3 bookings)")
    gym_id: int
    client_id: int
    mode: str
    message: str
    available_count: Optional[int] = Field(None, description="Remaining offer bookings available (out of 3)")


@router.post("/check", response_model=OfferEligibilityResponse)
async def check_eligibility(
    request: Request,
    payload: OfferEligibilityRequest,
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Get client_id from request state (set by authentication middleware)
        client_id = getattr(request.state, 'user', None)

        if not client_id:
            raise FittbotHTTPException(
                status_code=401,
                detail="Unauthorized - User not authenticated",
                error_code="UNAUTHORIZED"
            )

        try:
            client_id_int = int(client_id)
        except (ValueError, TypeError):
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid client ID",
                error_code="INVALID_CLIENT_ID"
            )

        # Check eligibility using the detailed service
        eligibility_result = await check_offer_eligibility_detailed(
            db=db,
            client_id=client_id_int,
            gym_id=payload.gym_id,
            mode=payload.mode
        )

        is_eligible = eligibility_result["is_eligible"]
        client_eligible = eligibility_result["client_eligible"]
        gym_eligible = eligibility_result["gym_eligible"]
        available_count = eligibility_result.get("available_count")

        # Generate appropriate message
        if is_eligible:
            if payload.mode == "dailypass":
                message = "User is eligible for the ₹49 daily pass offer at this gym"
            else:
                message = "User is eligible for the ₹99 session offer at this gym"

        elif client_eligible and not gym_eligible:

            if payload.mode == "dailypass":
                message = "You're eligible for ₹49 daily pass but this gym hasn't opted into the offer"
            else:
                message = "You're eligible for ₹99 sessions but this gym hasn't opted into the offer"
        
        else:
            if payload.mode == "dailypass":
                message = "User is not eligible for the ₹49 daily pass offer"
            else:
                message = "User is not eligible for the ₹99 session offer"

        # Track product view (non-blocking)
        from app.services.activity_tracker import track_event
        event_map = {"dailypass": "dailypass_viewed", "session": "session_viewed"}
        event_type = event_map.get(payload.mode)
        if event_type:
            await track_event(
                client_id_int, event_type,
                gym_id=payload.gym_id,
                source="offer_eligibility",
            )

        return OfferEligibilityResponse(
            status=200,
            is_eligible=is_eligible,
            client_eligible=client_eligible,
            client_id=client_id_int,
            gym_id=payload.gym_id,
            mode=payload.mode,
            message=message,
            available_count=available_count,
        )

    except FittbotHTTPException:
        raise

    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to check offer eligibility",
            error_code="OFFER_ELIGIBILITY_CHECK_ERROR",
            log_data={"exc": repr(e), "gym_id": payload.gym_id, "mode": payload.mode}
        )
