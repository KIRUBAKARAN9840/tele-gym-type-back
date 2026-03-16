from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import random

DEFAULT_SERVICES = ["General Fitness", "Weight Training", "Cardio", "Personal Training", "Group Classes"]

from app.models.async_database import get_async_db
from app.models.fittbot_models import GymOwner, Gym, GymBatches, LiveCount
from app.utils.security import get_password_hash, create_access_token, create_refresh_token
from app.utils.logging_utils import auth_logger, FittbotHTTPException
from app.utils.redis_config import get_redis
from app.fittbot_api.v1.auth.auth import is_mobile_request
from app.utils.otp import generate_otp, async_send_verification_sms
from app.config.settings import settings

router = APIRouter(prefix="/owner_registration", tags=["New Registration"])




class CheckRegisteredResponse(BaseModel):
    is_registered: bool
    message: str


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


class RegistrationRequest(BaseModel):
    name: str
    mobile: str
    password: str
    confirmPassword: str
    gyms: List[GymData]


class RegistrationData(BaseModel):
    owner_id: Optional[int] = None
    gym_ids: Optional[List[int]] = None


class RegistrationResponse(BaseModel):
    status: int
    message: str
    data: Optional[RegistrationData] = None


class VerifyOtpRequest(BaseModel):
    mobile: str
    otp: str


class ResendOtpRequest(BaseModel):
    mobile: str


# Models for Add New Gym endpoint
class AddGymAddress(BaseModel):
    doorNo: Optional[str] = None
    building: Optional[str] = None
    street: Optional[str] = None
    area: Optional[str] = None
    city: str
    state: str
    pincode: str


class AddGymData(BaseModel):
    name: str
    contact_number: str
    location: str
    address: AddGymAddress
    fitness_type: List[str] 


class AddNewGymRequest(BaseModel):
    owner_id: int
    gym: AddGymData


class AddNewGymResponse(BaseModel):
    status: int
    message: str
    data: Optional[dict] = None


@router.post("/register", response_model=RegistrationResponse)
async def register_owner(
    request: RegistrationRequest,
    db: AsyncSession = Depends(get_async_db),
    redis=Depends(get_redis)
):
    try:

        if request.password != request.confirmPassword:
            raise HTTPException(
                status_code=400,
                detail="Passwords do not match"
            )

        stmt = select(GymOwner).where(GymOwner.contact_number == request.mobile)
        result = await db.execute(stmt)
        existing_owner = result.scalar_one_or_none()

        if existing_owner:
            raise HTTPException(
                status_code=400,
                detail="User is already registered. Please login."
            )

        if not request.gyms or len(request.gyms) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one gym is required for registration"
            )

        hashed_password = get_password_hash(request.password)

        new_owner = GymOwner(
            name=request.name,
            email="",
            contact_number=request.mobile,
            password=hashed_password,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            verification='{"mobile": false, "email": false}'
        )
        db.add(new_owner)
        await db.flush()

        gym_ids = []

        for gym_data in request.gyms:
            # Create gym with new address fields
            new_gym = Gym(
                owner_id=new_owner.owner_id,
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
                logo='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png',
                cover_pic='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/studios.png',
                fittbot_verified=False,
                referal_id="",
                services=DEFAULT_SERVICES
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

        await db.commit()

        otp = generate_otp()
        await redis.set(f"otp:{request.mobile}", otp, ex=300)
        if await async_send_verification_sms(request.mobile, otp):
            auth_logger.info(f"OTP sent successfully to {request.mobile}")
        else:
            auth_logger.warning(f"Failed to send OTP to {request.mobile}")

        return RegistrationResponse(
            status=200,
            message="Registration successful. OTP sent for verification.",
            data=RegistrationData(
                owner_id=new_owner.owner_id,
                gym_ids=gym_ids
            )
        )

    except HTTPException:
        await db.rollback()
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error during registration: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred during registration: {str(e)}",
            error_code="REGISTRATION_ERROR"
        )



@router.post("/add_new_gym", response_model=AddNewGymResponse)
async def add_new_gym(
    request: AddNewGymRequest,
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Verify owner exists
        stmt = select(GymOwner).where(GymOwner.owner_id == request.owner_id)
        result = await db.execute(stmt)
        owner = result.scalar_one_or_none()

        if not owner:
            raise HTTPException(
                status_code=404,
                detail="Owner not found"
            )

        # Create the new gym
        new_gym = Gym(
            owner_id=request.owner_id,
            name=request.gym.name,
            location=request.gym.location,
            contact_number=request.gym.contact_number,
            door_no=request.gym.address.doorNo,
            building=request.gym.address.building,
            street=request.gym.address.street,
            area=request.gym.address.area,
            city=request.gym.address.city,
            state=request.gym.address.state,
            pincode=request.gym.address.pincode,
            fitness_type=request.gym.fitness_type if request.gym.fitness_type else ["gym"],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            logo='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png',
            cover_pic='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/studios.png',
            fittbot_verified=False,
            referal_id="",
            services=DEFAULT_SERVICES
        )
        
        db.add(new_gym)
        await db.flush()

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

        await db.commit()

        # Fetch all gyms for this owner to return in response
        stmt = select(Gym).where(Gym.owner_id == request.owner_id)
        result = await db.execute(stmt)
        all_gyms = result.scalars().all()

        gyms_list = [
            {
                "gym_id": gym.gym_id,
                "owner_id": gym.owner_id,
                "name": gym.name,
                "location": gym.location,
                "contact_number": gym.contact_number,
                "city": gym.city,
                "state": gym.state,
                "logo": gym.logo
            }
            for gym in all_gyms
        ]

        return AddNewGymResponse(
            status=200,
            message="Gym added successfully",
            data={
                "gym_id": new_gym.gym_id,
                "gyms": gyms_list
            }
        )

    except HTTPException:
        await db.rollback()
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error adding new gym: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while adding the gym: {str(e)}",
            error_code="ADD_GYM_ERROR"
        )


@router.get("/get_owner_gyms")
async def get_owner_gyms(
    owner_id: int,
    db: AsyncSession = Depends(get_async_db)
):

    try:
  
        stmt = select(GymOwner).where(GymOwner.owner_id == owner_id)
        result = await db.execute(stmt)
        owner = result.scalar_one_or_none()

        if not owner:
            raise HTTPException(
                status_code=404,
                detail="Owner not found"
            )

        stmt = select(Gym).where(Gym.owner_id == owner_id)
        result = await db.execute(stmt)
        all_gyms = result.scalars().all()

        gyms_list = [
            {
                "gym_id": gym.gym_id,
                "owner_id": gym.owner_id,
                "name": gym.name,
                "location": gym.location,
                "contact_number": gym.contact_number,
                "city": gym.city,
                "state": gym.state,
                "logo": gym.logo
            }
            for gym in all_gyms
        ]

        return {
            "status": 200,
            "message": "Gyms fetched successfully",
            "data": {
                "gyms": gyms_list,
                "owner_name": owner.name
            }
        }

    except HTTPException:
        raise

    except Exception as e:
        auth_logger.error(f"Error fetching owner gyms: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while fetching gyms: {str(e)}",
            error_code="GET_GYMS_ERROR"
        )


