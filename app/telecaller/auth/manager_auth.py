from fastapi import APIRouter, Depends, HTTPException, status, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.async_database import get_async_db
from app.models.telecaller_models import Manager
from app.services.otp_auth_service import otp_auth_service
from app.utils.security import SECRET_KEY, ALGORITHM
from jose import jwt
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional, Union
from app.config.settings import settings
from app.utils.security import (
    verify_password, create_access_token, create_refresh_token,
    SECRET_KEY, ALGORITHM, get_password_hash
)

router = APIRouter(prefix="/manager")

class SendOTPRequest(BaseModel):
    mobile_number: str

class VerifyOTPRequest(BaseModel):
    mobile_number: str
    otp: str
    device_type: Optional[str] = "web"  # web or mobile

@router.post("/send-otp")
async def manager_send_otp(
    data: SendOTPRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Send OTP to manager mobile number - async database operations"""
    try:
        # Check if manager exists and is active using async query
        stmt = select(Manager).where(
            Manager.mobile_number == data.mobile_number,
            Manager.verified == True,
            Manager.status == "active"
        )
        result = await db.execute(stmt)
        manager = result.scalar_one_or_none()

        if not manager:
            raise HTTPException(
                status_code=404,
                detail="Mobile number not registered or inactive"
            )

        # Check account lock status
        if manager.locked_until and manager.locked_until > datetime.utcnow():
            raise HTTPException(
                status_code=423,
                detail="Account temporarily locked. Please try again later."
            )

        # Send OTP using the service
        result = await otp_auth_service.send_otp(data.mobile_number, "manager")

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send OTP: {str(e)}"
        )

@router.post("/verify-otp")
async def manager_verify_otp(
    data: VerifyOTPRequest,
    response: Response,
    db: AsyncSession = Depends(get_async_db)
):
    """Verify OTP and complete login - async database operations"""
    manager = None
    try:
        # Get manager using async query
        stmt = select(Manager).where(
            Manager.mobile_number == data.mobile_number
        )
        result = await db.execute(stmt)
        manager = result.scalar_one_or_none()

        if not manager:
            raise HTTPException(
                status_code=404,
                detail="Manager not found"
            )

        if not manager.verified:
            raise HTTPException(
                status_code=401,
                detail="Account not verified"
            )

        if manager.status != "active":
            raise HTTPException(
                status_code=401,
                detail="Account is inactive"
            )

        # Check account lock status
        if manager.locked_until and manager.locked_until > datetime.utcnow():
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

        # Update manager session info using async operations
        manager.otp_session_token = auth_result.get("session_token")
        manager.otp_session_expires_at = datetime.utcnow() + timedelta(hours=1)
        manager.last_login_at = datetime.utcnow()
        manager.login_attempts = 0
        manager.locked_until = None

        # Async commit and refresh
        await db.commit()
        await db.refresh(manager)

        # Update user data in auth_result with actual manager information
        auth_result["user"]["id"] = manager.id
        auth_result["user"]["name"] = manager.name
        auth_result["user"]["is_super_admin"] = manager.is_super_admin if hasattr(manager, 'is_super_admin') else 0

    


        access_token = create_access_token({
            "sub": manager.mobile_number,  # subject is mobile number
            "mobile_number": manager.mobile_number,
            "role": "manager",
            "type": "telecaller",  # type should be "telecaller" for both roles
            "id": manager.id  # Add id field
        })
        refresh_token = create_refresh_token({
            "sub": str(manager.id),
            "type": "refresh" # Refresh token expiry
        })


      
        # Create JWT tokens for the response
        # token_payload = {
        #     "sub": manager.mobile_number,  # subject is mobile number
        #     "mobile_number": manager.mobile_number,
        #     "role": "manager",
        #     "type": "telecaller",  # type should be "telecaller" for both roles
        #     "id": manager.id,  # Add id field
        #     "exp": datetime.utcnow() + timedelta(minutes=15)  # Access token expiry
        # }

        # access_token = jwt.encode(token_payload, SECRET_KEY, algorithm=ALGORITHM)

        # refresh_payload = {
        #     "sub": str(manager.id),
        #     "type": "refresh",
        #     "exp": datetime.utcnow() + timedelta(days=7)  # Refresh token expiry
        # }
        # refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)

        # Store refresh token in database for token refresh flow
        manager.refresh_token = refresh_token
        await db.commit()
        await db.refresh(manager)

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
        if manager:
            manager.login_attempts = (manager.login_attempts or 0) + 1

            # Lock account after 5 failed attempts
            if manager.login_attempts >= 5:
                manager.locked_until = datetime.utcnow() + timedelta(minutes=30)

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
# async def manager_logout(
#     response: Response,
#     db: AsyncSession = Depends(get_async_db),
#     current_manager: Optional[Manager] = None  # This will be injected by auth dependency
# ):
#     """Logout manager and clear session"""
#     try:
#         # Clear cookies
#         response.delete_cookie(key="access_token")
#         response.delete_cookie(key="refresh_token")
#
#         # Clear session from database if manager is provided
#         if current_manager:
#             current_manager.otp_session_token = None
#             current_manager.otp_session_expires_at = None
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
# async def manager_session_status(
#     current_manager: Optional[Manager] = None  # This will be injected by auth dependency
# ):
#     """Check current session status"""
#     if current_manager:
#         return {
#             "status": "active",
#             "user": {
#                 "id": current_manager.id,
#                 "name": current_manager.name,
#                 "mobile_number": current_manager.mobile_number,
#                 "role": "manager",
#                 "last_login": current_manager.last_login_at
#             }
#         }
#     else:
#         return {"status": "inactive"}
