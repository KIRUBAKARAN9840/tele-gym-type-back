from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.async_database import get_async_db
from app.models.telecaller_models import Telecaller, Manager
from app.services.otp_auth_service import otp_auth_service
from app.utils.security import SECRET_KEY, ALGORITHM
from jose import jwt
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional
from app.config.settings import settings

from app.utils.security import (
    verify_password, create_access_token, create_refresh_token,
    SECRET_KEY, ALGORITHM, get_password_hash
)

router = APIRouter(prefix="/telecaller")

class SendOTPRequest(BaseModel):
    mobile_number: str

class VerifyOTPRequest(BaseModel):
    mobile_number: str
    otp: str
    device_type: Optional[str] = "web"  # web or mobile

@router.post("/send-otp")
async def telecaller_send_otp(
    data: SendOTPRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Send OTP to telecaller mobile number - async database operations"""
    try:
        # Check if telecaller exists and is active using async query
        stmt = select(Telecaller).where(
            Telecaller.mobile_number == data.mobile_number,
            Telecaller.verified == True,
            Telecaller.status == "active"
        )
        result = await db.execute(stmt)
        telecaller = result.scalar_one_or_none()

        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Mobile number not registered or inactive"
            )

        # Check if telecaller's manager is active using async query
        stmt_manager = select(Manager).where(
            Manager.id == telecaller.manager_id,
            Manager.status == "active"
        )
        result_manager = await db.execute(stmt_manager)
        manager = result_manager.scalar_one_or_none()

        if not manager:
            raise HTTPException(
                status_code=404,
                detail="Manager account inactive"
            )

        # Check account lock status
        if telecaller.locked_until and telecaller.locked_until > datetime.utcnow():
            raise HTTPException(
                status_code=423,
                detail="Account temporarily locked. Please try again later."
            )

        # Send OTP using the service
        result = await otp_auth_service.send_otp(data.mobile_number, "telecaller")

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send OTP: {str(e)}"
        )

@router.post("/verify-otp")
async def telecaller_verify_otp(
    data: VerifyOTPRequest,
    response: Response,
    db: AsyncSession = Depends(get_async_db)
):
    """Verify OTP and complete login - async database operations"""
    telecaller = None
    try:
        # Get telecaller using async query
        stmt = select(Telecaller).where(
            Telecaller.mobile_number == data.mobile_number
        )
        result = await db.execute(stmt)
        telecaller = result.scalar_one_or_none()

        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Telecaller not found"
            )

        if not telecaller.verified:
            raise HTTPException(
                status_code=401,
                detail="Account not verified"
            )

        if telecaller.status != "active":
            raise HTTPException(
                status_code=401,
                detail="Account is inactive"
            )

        # Check if telecaller's manager is active using async query
        stmt_manager = select(Manager).where(
            Manager.id == telecaller.manager_id,
            Manager.status == "active"
        )
        result_manager = await db.execute(stmt_manager)
        manager = result_manager.scalar_one_or_none()

        if not manager:
            raise HTTPException(
                status_code=401,
                detail="Manager account inactive"
            )

        # Check account lock status
        if telecaller.locked_until and telecaller.locked_until > datetime.utcnow():
            raise HTTPException(
                status_code=423,
                detail="Account temporarily locked. Please try again later."
            )

        # Verify OTP and get tokens
        auth_result = await otp_auth_service.verify_otp(
            data.mobile_number,
            data.otp,
            data.device_type
        )

        # Update telecaller session info using async operations
        telecaller.otp_session_token = auth_result.get("session_token")
        telecaller.otp_session_expires_at = datetime.utcnow() + timedelta(hours=1)
        telecaller.last_login_at = datetime.utcnow()
        telecaller.login_attempts = 0
        telecaller.locked_until = None
        await db.commit()
        await db.refresh(telecaller)

        # Update user data in auth_result with actual telecaller information
        auth_result["user"]["id"] = telecaller.id
        auth_result["user"]["name"] = telecaller.name

        access_token = create_access_token({
            "sub": telecaller.mobile_number,  # subject is mobile number
            "mobile_number": telecaller.mobile_number,
            "role": "telecaller",
            "type": "telecaller",  # type should be "telecaller" for both roles
            "id": telecaller.id,  # Add id field
            "manager_id": telecaller.manager_id
        })
        refresh_token = create_refresh_token({
            "sub": str(telecaller.id),
            "type": "refresh" # Refresh token expiry
        })

        # Create JWT tokens for the response
        # token_payload = {
        #     "sub": telecaller.mobile_number,  # subject is mobile number
        #     "mobile_number": telecaller.mobile_number,
        #     "role": "telecaller",
        #     "type": "telecaller",
        #     "id": telecaller.id,  # Add id field
        #     "manager_id": telecaller.manager_id,
        #     "exp": datetime.utcnow() + timedelta(minutes=15)  # Access token expiry
        # }

        # access_token = jwt.encode(token_payload, SECRET_KEY, algorithm=ALGORITHM)

        # refresh_payload = {
        #     "sub": str(telecaller.id),
        #     "type": "refresh",
        #     "exp": datetime.utcnow() + timedelta(days=7)  # Refresh token expiry
        # }
        # refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)

        # Store refresh token in database for token refresh flow
        telecaller.refresh_token = refresh_token
        await db.commit()
        await db.refresh(telecaller)

        response.set_cookie(
                        key="access_token",
                        value=access_token,
                        max_age=3600,  # 1 hour
                        httponly=True,
                        secure=settings.cookie_secure,
                        domain=settings.cookie_domain_value,
                        samesite=settings.cookie_samesite_value,
                    )
        response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            max_age=604800,  # 7 days
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        result = {
            "status": auth_result["status"],
            "user": auth_result["user"],
            "redirect_to": f"/portal/manager/dashboard"
        }
    
        return result

    except HTTPException:
        # Handle failed OTP attempt
        if telecaller:
            telecaller.login_attempts = (telecaller.login_attempts or 0) + 1

            # Lock account after 5 failed attempts
            if telecaller.login_attempts >= 5:
                telecaller.locked_until = datetime.utcnow() + timedelta(minutes=30)

            await db.commit()
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Authentication failed: {str(e)}"
        )


# TODO: Fix dependency injection for logout and session-status endpoints
# These endpoints require proper auth middleware to inject current_user

# @router.post("/logout")
# async def telecaller_logout(
#     response: Response,
#     db: AsyncSession = Depends(get_async_db),
#     current_telecaller: Optional[Telecaller] = None  # This will be injected by auth dependency
# ):
#     """Logout telecaller and clear session"""
#     try:
#         # Clear cookies
#         response.delete_cookie(key="access_token")
#         response.delete_cookie(key="refresh_token")
#
#         # Clear session from database if telecaller is provided
#         if current_telecaller:
#             current_telecaller.otp_session_token = None
#             current_telecaller.otp_session_expires_at = None
#             await db.commit()
#
#         return {"status": "success", "message": "Logged out successfully"}
#
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Logout failed: {str(e)}"
#         )

# @router.get("/session-status")
# async def telecaller_session_status(
#     current_telecaller: Optional[Telecaller] = None  # This will be injected by auth dependency
# ):
#     """Check current session status"""
#     if current_telecaller:
#         return {
#             "status": "active",
#             "user": {
#                 "id": current_telecaller.id,
#                 "name": current_telecaller.name,
#                 "mobile_number": current_telecaller.mobile_number,
#                 "role": "telecaller",
#                 "manager_id": current_telecaller.manager_id,
#                 "last_login": current_telecaller.last_login_at
#             }
#         }
#     else:
#         return {"status": "inactive"}
