from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from starlette.responses import JSONResponse

from app.models.async_database import get_async_db
from app.models.fittbot_models import GymOwner, Gym, GymBatches, LiveCount
from app.utils.logging_utils import FittbotHTTPException, auth_logger
from app.utils.security import create_access_token, create_refresh_token
from app.config.settings import settings

DEFAULT_SERVICES = ["General Fitness", "Weight Training", "Cardio", "Personal Training", "Group Classes"]

router = APIRouter(prefix="/owner/new_registration", tags=["Owner Registration"])


# ==================== Request Models ====================

class GymAddress(BaseModel):
    doorNo: Optional[str] = None
    building: Optional[str] = None
    street: Optional[str] = None
    area: Optional[str] = None
    city: str
    state: str
    pincode: str


class GymData(BaseModel):
    name: str
    location: str
    contactNumber: str
    address: GymAddress
    fitness_type: List[str] = None 


class OwnerRegistrationRequest(BaseModel):
    mobile: str
    name: str
    gyms: List[GymData]
    device: Optional[str] = None  # "mobile" or "web"


# ==================== Registration Endpoint ====================

@router.post("/register")
async def register_owner(
    request: OwnerRegistrationRequest,
    db: AsyncSession = Depends(get_async_db),
):
    try:
        if not request.gyms or len(request.gyms) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one gym is required for registration",
            )


        stmt = select(GymOwner).where(GymOwner.contact_number == request.mobile)
        result = await db.execute(stmt)
        owner = result.scalars().first()

        if not owner:
            raise HTTPException(
                status_code=404,
                detail="Owner not found",
            )

        if not owner.incomplete:
            raise HTTPException(
                status_code=400,
                detail="Owner is already registered.",
            )


        owner.name = request.name
        owner.verification = '{"mobile": true, "email": false}'
        owner.incomplete = False
        owner.updated_at = datetime.now()

        gym_ids = []

        for gym_data in request.gyms:
            new_gym = Gym(
                owner_id=owner.owner_id,
                name=gym_data.name,
                location=gym_data.location,
                contact_number=gym_data.contactNumber,
                door_no=gym_data.address.doorNo,
                building=gym_data.address.building,
                street=gym_data.address.street,
                area=gym_data.address.area,
                city=gym_data.address.city,
                state=gym_data.address.state,
                pincode=gym_data.address.pincode,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                logo="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png",
                cover_pic="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/studios.png",
                fittbot_verified=False,
                referal_id="",
                services=DEFAULT_SERVICES,
                fitness_type=gym_data.fitness_type if gym_data.fitness_type else ["gym"]
            )
            db.add(new_gym)
            await db.flush()

            gym_ids.append(new_gym.gym_id)

            default_batches = [
                GymBatches(gym_id=new_gym.gym_id, batch_name="Early Morning", timing="4:00 am - 7:00 am", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Morning", timing="7:00 am - 10:00 am", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Forenoon", timing="10:00 am - 1:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Afternoon", timing="1:00 pm - 4:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Evening", timing="4:00 pm - 7:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Night", timing="7:00 pm - 10:00 pm", description=""),
            ]
            db.add_all(default_batches)

            live_count = LiveCount(gym_id=new_gym.gym_id, count=0)
            db.add(live_count)

    
        access_token = create_access_token({"sub": str(owner.owner_id), "role": "owner"})
        refresh_token = create_refresh_token({"sub": str(owner.owner_id)})
        owner.refresh_token = refresh_token

        await db.commit()

    
        gym_data_resp = {}
        if len(request.gyms) == 1:
            gym_data_resp = {
                "gym_id": gym_ids[0],
                "name": request.gyms[0].name,
                "owner_id": owner.owner_id,
                "logo":"https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png"
            }
        else:
            gym_data_resp = [
                {"gym_id": gym_ids[i], "name": request.gyms[i].name, "location": request.gyms[i].location, "owner_id": owner.owner_id, "logo":"https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png"}
                for i in range(len(request.gyms))
            ]

        response_data = {
            "status": 200,
            "message": "Registration successful",
            "data": {
                "owner_id": owner.owner_id,
                "name": owner.name,
                "gym_ids": gym_ids,
                "gyms": gym_data_resp,
                
            },
        }


        is_mobile = request.device and request.device.lower() == "mobile"

        if is_mobile:
            response_data["data"]["access_token"] = access_token
            response_data["data"]["refresh_token"] = refresh_token
            return response_data


        response = JSONResponse(content=response_data)

        response.set_cookie(
            key="access_token",
            value=access_token,
            max_age=3600,
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            max_age=604800,
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        return response

    except HTTPException:
        await db.rollback()
        raise
    except Exception as exc:
        await db.rollback()
        auth_logger.error(f"Error during owner registration: {str(exc)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred during registration: {str(exc)}",
            error_code="OWNER_REGISTRATION_ERROR",
        )
