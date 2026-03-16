from datetime import datetime, date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from redis.asyncio import Redis

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, ClientTarget, ReferralCode, Gym, ClientFittbotAccess
from app.utils.logging_utils import FittbotHTTPException, auth_logger
from app.utils.otp import generate_otp, async_send_verification_sms
from app.utils.redis_config import get_redis
from app.utils.referral_code_generator import generate_referral_code_random
from app.utils.security import create_access_token, create_refresh_token

router = APIRouter(prefix="/client/new_registration", tags=["Client Login"])


# ==================== Request Models ====================

class ClientLoginRequest(BaseModel):
    mobile_number: str


class ClientLoginResponse(BaseModel):
    status: int
    message: str
    data: dict = None


# ==================== Login Endpoint ====================

@router.post("/login", response_model=ClientLoginResponse)
async def client_login(
    request: ClientLoginRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
) -> ClientLoginResponse:

    try:
        mobile_number = request.mobile_number

        stmt = select(Client).where(Client.contact == mobile_number)
        result = await db.execute(stmt)
        client = result.scalars().first()

        if not client:
            raise FittbotHTTPException(
                status_code=401,
                detail="Mobile number not registered.",
                error_code="CLIENT_NOT_FOUND",
            )

        # Generate OTP - use fixed OTP for test number
        if mobile_number == "8667458723" or mobile_number=="9486987082":
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

        return ClientLoginResponse(
            status=200,
            message="OTP sent successfully",
            data={
                "contact": client.contact,
                "full_name": client.name,
            }
        )

    except FittbotHTTPException:
        raise
    except HTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(exc)}",
            error_code="LOGIN_ERROR",
        )


# ==================== Login V1 Endpoint ====================

@router.post("/login_v1", response_model=ClientLoginResponse)
async def client_login_v1(
    request: ClientLoginRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
) -> ClientLoginResponse:

    try:
        mobile_number = request.mobile_number
        is_new_user = False

        stmt = select(Client).where(Client.contact == mobile_number)
        result = await db.execute(stmt)
        client = result.scalars().first()

        # If client does not exist, auto-register with minimal info
        if not client:
            is_new_user = True

            client = Client(
                name="",
                contact=mobile_number,
                gender="",
                email="",
                password="",
                verification='{"mobile": true, "password": false}',
                profile="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png",
                access=False,
                incomplete=True,
                modal_shown=True,
            )
            db.add(client)
            await db.commit()
            await db.refresh(client)


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

        return ClientLoginResponse(
            status=200,
            message="OTP sent successfully"
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
            error_code="LOGIN_V1_ERROR",
        )


# ==================== OTP Verification V1 Endpoint ====================

class OTPVerificationRequest(BaseModel):
    data: str
    otp: str


@router.post("/otp_verification_v1")
async def otp_verification_v1(
    request: OTPVerificationRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        mobile_number = request.data
        otp = request.otp

        stored_otp = await redis.get(f"otp:{mobile_number}")
        if not stored_otp or stored_otp != str(otp):
            raise FittbotHTTPException(
                status_code=400,
                detail="Incorrect OTP entered",
                error_code="INVALID_OTP",
            )

        await redis.delete(f"otp:{mobile_number}")

        stmt = select(Client).where(Client.contact == mobile_number)
        result = await db.execute(stmt)
        client = result.scalars().first()

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
            )

        # Generate tokens


        # If registration is incomplete, return early with flag
        if client.incomplete:
            return {
                "status": 200,
                "message": "OTP verified successfully",
                "incomplete_registration": True
                }
            

        access_token = create_access_token({"sub": str(client.client_id), "role": "client"})
        refresh_token = create_refresh_token({"sub": str(client.client_id)})

        client.refresh_token = refresh_token
        await db.commit()
        # Registration is complete - return full response like auth.py
        gym = None
        if client.gym_id:
            stmt = select(Gym).where(Gym.gym_id == client.gym_id)
            result = await db.execute(stmt)
            gym = result.scalars().first()


        return {
            "status": 200,
            "message": "OTP verified successfully",
            "incomplete_registration": False,
            "data": {
                "gym_id": client.gym_id if client.gym_id is not None else None,
                "client_id": client.client_id,
                "gym_name": gym.name if gym else "",
                "gender": client.gender,
                "gym_logo": gym.logo if gym else "",
                "name": client.name if client.name else "",
                "mobile": client.contact if client.contact else "",
                "profile": client.profile if client.profile else "",
                "weight": client.weight if client.weight else 0,
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
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
            error_code="OTP_VERIFICATION_V1_ERROR",
        )
