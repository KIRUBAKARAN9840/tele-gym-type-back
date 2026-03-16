from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from redis.asyncio import Redis
from starlette.responses import JSONResponse

from app.models.async_database import get_async_db
from app.models.fittbot_models import GymOwner, Gym, Trainer, TrainerProfile
from app.utils.logging_utils import FittbotHTTPException
from app.utils.otp import generate_otp, async_send_verification_sms
from app.utils.redis_config import get_redis
from app.utils.security import create_access_token, create_refresh_token
from app.config.settings import settings

router = APIRouter(prefix="/owner/new_registration", tags=["Owner Login"])


# ==================== Request Models ====================

class LoginRequest(BaseModel):
    mobile_number: str
    role: str  # "owner" or "trainer"


class OTPVerificationRequest(BaseModel):
    data: str
    otp: str
    role: str  # "owner" or "trainer"
    device: Optional[str] = None  # "mobile" or "web"


# ==================== Login V1 Endpoint ====================

@router.post("/login")
async def login_v1(
    request: LoginRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        mobile_number = request.mobile_number
        role = request.role

        if role == "owner":
            stmt = select(GymOwner).where(GymOwner.contact_number == mobile_number)
            result = await db.execute(stmt)
            owner = result.scalars().first()

            # If owner does not exist, auto-register with minimal info
            if not owner:
                owner = GymOwner(
                    name="",
                    contact_number=mobile_number,
                    password="",
                    verification='{"mobile": false, "email": false}',
                    incomplete=True,
                )
                db.add(owner)
                await db.commit()
                await db.refresh(owner)

        elif role == "trainer":
            stmt = select(Trainer).where(Trainer.contact == mobile_number)
            result = await db.execute(stmt)
            trainer = result.scalars().first()

            if not trainer:
                raise FittbotHTTPException(
                    status_code=401,
                    detail="Mobile number not registered.",
                    error_code="TRAINER_NOT_FOUND",
                )

        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid role. Must be 'owner' or 'trainer'.",
                error_code="INVALID_ROLE",
            )

        # Generate OTP - use fixed OTP for test numbers
        if mobile_number == "8667458723" or mobile_number == "9486987082":
            mobile_otp = "123456"
        else:
            mobile_otp = generate_otp()

        # Store OTP in Redis with 5 minute expiry
        await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)

        # Send OTP via SMS
        if not await async_send_verification_sms(mobile_number, mobile_otp):
            raise FittbotHTTPException(
                status_code=500,
                detail="Failed to send OTP",
                error_code="SMS_SEND_FAILED",
            )

        return {
            "status": 200,
            "message": "OTP sent successfully",
        }

    except FittbotHTTPException:
        raise
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(exc)}",
            error_code="LOGIN_V1_ERROR",
        )


# ==================== OTP Verification V1 Endpoint ====================

@router.post("/otp_verification")
async def otp_verification_v1(
    request: OTPVerificationRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        mobile_number = request.data
        otp = request.otp
        role = request.role

        stored_otp = await redis.get(f"otp:{mobile_number}")
        if not stored_otp or stored_otp != str(otp):
            raise FittbotHTTPException(
                status_code=400,
                detail="Incorrect OTP entered",
                error_code="INVALID_OTP",
            )

        await redis.delete(f"otp:{mobile_number}")

        if role == "owner":
            return await _verify_owner(request, mobile_number, db)
        elif role == "trainer":
            return await _verify_trainer(request, mobile_number, db)
        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid role. Must be 'owner' or 'trainer'.",
                error_code="INVALID_ROLE",
            )

    except FittbotHTTPException:
        raise
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(exc)}",
            error_code="OTP_VERIFICATION_V1_ERROR",
        )


# ==================== Owner Verification ====================

async def _verify_owner(request: OTPVerificationRequest, mobile_number: str, db: AsyncSession):
    stmt = select(GymOwner).where(GymOwner.contact_number == mobile_number)
    result = await db.execute(stmt)
    owner = result.scalars().first()

    if not owner:
        raise FittbotHTTPException(
            status_code=404,
            detail="Owner not found",
            error_code="OWNER_NOT_FOUND",
        )

    # If registration is incomplete, return early with flag
    if owner.incomplete:
        return {
            "status": 200,
            "message": "OTP verified successfully",
            "incomplete_registration": True,
            "data": {
                "owner_id": owner.owner_id,
            },
        }

    # Registration is complete - full flow
    access_token = create_access_token({"sub": str(owner.owner_id), "role": "owner"})
    refresh_token = create_refresh_token({"sub": str(owner.owner_id)})

    owner.refresh_token = refresh_token
    await db.commit()

    # Fetch associated gyms
    gym_stmt = select(Gym).where(Gym.owner_id == owner.owner_id)
    gym_result = await db.execute(gym_stmt)
    gyms = gym_result.scalars().all()

    # Build gym_data
    gym_data = {}
    if len(gyms) == 1:
        gym_data = {
            "gym_id": gyms[0].gym_id,
            "name": gyms[0].name,
            "logo": gyms[0].logo,
            "owner_id": gyms[0].owner_id,
        }
    elif len(gyms) > 1:
        gym_data = [
            {
                "gym_id": gym.gym_id,
                "name": gym.name,
                "location": gym.location,
                "logo": gym.logo,
                "owner_id": gym.owner_id,
            }
            for gym in gyms
        ]

    response_data = {
        "status": 200,
        "message": "OTP verified successfully",
        "incomplete_registration": False,
        "data": {
            "owner_id": owner.owner_id,
            "name": owner.name,
            "gyms": gym_data,
        },
    }

    return await _deliver_tokens(request, response_data, access_token, refresh_token)


# ==================== Trainer Verification ====================

async def _verify_trainer(request: OTPVerificationRequest, mobile_number: str, db: AsyncSession):
    stmt = select(Trainer).where(Trainer.contact == mobile_number)
    result = await db.execute(stmt)
    trainer = result.scalars().first()

    if not trainer:
        raise FittbotHTTPException(
            status_code=404,
            detail="Trainer not found",
            error_code="TRAINER_NOT_FOUND",
        )

    access_token = create_access_token({"sub": str(trainer.trainer_id), "role": "trainer"})
    refresh_token = create_refresh_token({"sub": str(trainer.trainer_id)})

    trainer.refresh_token = refresh_token
    await db.commit()

    # Fetch gyms via TrainerProfile join
    gym_stmt = (
        select(Gym)
        .join(TrainerProfile, Gym.gym_id == TrainerProfile.gym_id)
        .where(TrainerProfile.trainer_id == trainer.trainer_id)
    )
    gym_result = await db.execute(gym_stmt)
    gyms = gym_result.scalars().all()

    if not gyms:
        raise FittbotHTTPException(
            status_code=400,
            detail="No gyms associated with this trainer",
            error_code="NO_GYMS_FOUND",
        )

    # Build gym_data
    gym_data = {}
    if len(gyms) == 1:
        gym_data = {
            "gym_id": gyms[0].gym_id,
            "name": gyms[0].name,
            "logo": gyms[0].logo,
            "owner_id": gyms[0].owner_id,
        }
    else:
        gym_data = [
            {
                "gym_id": gym.gym_id,
                "name": gym.name,
                "location": gym.location,
                "logo": gym.logo,
                "owner_id": gym.owner_id,
            }
            for gym in gyms
        ]

    response_data = {
        "status": 200,
        "message": "OTP verified successfully",
        "data": {
            "trainer_id": trainer.trainer_id,
            "name": trainer.full_name,
            "gyms": gym_data,
        },
    }

    return await _deliver_tokens(request, response_data, access_token, refresh_token)


# ==================== Token Delivery Helper ====================

async def _deliver_tokens(request: OTPVerificationRequest, response_data: dict, access_token: str, refresh_token: str):
    """Return tokens in JSON body for mobile, or set HTTP-only cookies for web."""
    is_mobile = request.device and request.device.lower() == "mobile"

    if is_mobile:
        response_data["data"]["access_token"] = access_token
        response_data["data"]["refresh_token"] = refresh_token
        return response_data

    # For web, set HTTP-only cookies
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
