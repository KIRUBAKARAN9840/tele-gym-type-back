import redis
import json
import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session
from jose import jwt
from app.models.telecaller_models import Manager, Telecaller
from app.utils.security import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS
from app.utils.redis_config import get_redis
from app.utils.otp import generate_otp
from app.services.sms_service import SMSService
from app.config.settings import settings
import logging

logger = logging.getLogger(__name__)

class OTPAuthService:
    # Configuration
    OTP_EXPIRY_MINUTES = 5
    MAX_LOGIN_ATTEMPTS = 5
    LOCKOUT_DURATION_MINUTES = 30
    RATE_LIMIT_SECONDS = int(os.getenv("RATE_LIMIT_SECONDS", "60"))  # Configurable rate limit

    # Environment settings
    SKIP_SMS_IN_DEV = os.getenv("SKIP_SMS_IN_DEV", "false").lower() == "true"
    DEV_TEST_OTP = os.getenv("DEV_TEST_OTP", None)  # Optional override for dev

    def __init__(self):
        # Note: Redis client will be initialized as needed
        self.sms_service = SMSService()

    async def send_otp(self, mobile_number: str, user_type: str = None) -> Dict[str, Any]:
        """Send OTP for login"""
        try:
            # Get Redis client
            redis_client = await get_redis()

            # Rate limiting check
            rate_limit_key = f"otp_rate_limit:{mobile_number}"


            # Generate OTP
            if self.SKIP_SMS_IN_DEV and self.DEV_TEST_OTP:
                otp = self.DEV_TEST_OTP
                print(f"\n🔓 DEVELOPMENT OTP: {otp} for mobile {mobile_number} 🔓\n")
                #logger.info(f"Using dev test OTP: {otp}")
                sms_sent = False
            else:
                otp = generate_otp()
                print(f"\n🔓 GENERATED OTP: {otp} for mobile {mobile_number} 🔓\n")
                #logger.info(f"Generated OTP for {mobile_number}: {otp}")

                # Send SMS
                sms_sent = await self.sms_service.send_otp_sms(mobile_number, otp)

                if not sms_sent:
                    logger.error(f"Failed to send SMS to {mobile_number}")
                    # Continue anyway - OTP is stored but user won't receive SMS
                    # In production, you might want to raise an exception here
                    # raise HTTPException(
                    #     status_code=500,
                    #     detail="Failed to send OTP via SMS. Please try again."
                    # )

            # Store OTP in Redis with expiry
            otp_key = f"telecaller:otp:{mobile_number}"
            await redis_client.setex(
                otp_key,
                self.OTP_EXPIRY_MINUTES * 60,
                json.dumps({
                    "otp": otp,
                    "mobile_number": mobile_number,
                    "created_at": datetime.utcnow().isoformat(),
                    "user_type": user_type,
                    "sms_sent": sms_sent
                })
            )

            # Set rate limit
            await redis_client.setex(rate_limit_key, self.RATE_LIMIT_SECONDS, "1")

            return {
                "status": "success",
                "message": "OTP sent successfully",
                "mobile_masked": self._mask_mobile_number(mobile_number),
                "delivery_method": "sms" if sms_sent else ("dev_test" if self.SKIP_SMS_IN_DEV and self.DEV_TEST_OTP else "failed")
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error sending OTP: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to send OTP"
            )

    async def verify_otp(
        self,
        mobile_number: str,
        otp: str,
        device_type: str = "web"
    ) -> Dict[str, Any]:
        """Verify OTP and create session"""
        try:
            # Get Redis client
            redis_client = await get_redis()

            # Get OTP from Redis
            otp_key = f"telecaller:otp:{mobile_number}"
            otp_data = await redis_client.get(otp_key)

            if not otp_data:
                raise HTTPException(
                    status_code=400,
                    detail="OTP expired or not found"
                )

            otp_info = json.loads(otp_data)

            # Verify OTP
            if otp_info["otp"] != otp:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid OTP"
                )

            # Delete OTP from Redis
            await redis_client.delete(otp_key)

            # Generate session token
            session_token = self._generate_session_token()

            response = {
                "status": "success",
                "session_token": session_token,
                "user": {
                    "id": None,  # Will be set by auth endpoint
                    "name": None,  # Will be set by auth endpoint
                    "mobile_number": mobile_number,
                    "role": otp_info["user_type"]
                }
            }

            if device_type == "mobile":
                tokens = await self.create_session_tokens_by_type(mobile_number, otp_info["user_type"], device_type)
                response.update(tokens)
            else:
                response["redirect_to"] = f"/portal/{otp_info['user_type']}/dashboard"

            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error verifying OTP: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to verify OTP"
            )

    async def create_session_tokens(
        self,
        user,
        user_type: str,
        device_type: str = "web"
    ) -> Dict[str, str]:
        """Generate JWT and set cookies if needed"""
        try:
            # Token payload
            payload = {
                "sub": str(user.id),
                "mobile_number": user.mobile_number,
                "role": user_type,
                "type": user_type,
                "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            }

            if user_type == "telecaller":
                payload["manager_id"] = user.manager_id

            # Generate access token
            access_token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

            # Generate refresh token
            refresh_payload = {
                "sub": str(user.id),
                "type": "refresh",
                "exp": datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
            }
            refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)

            tokens = {
                "access_token": access_token,
                "refresh_token": refresh_token
            }

            return tokens

        except Exception as e:
            logger.error(f"Error creating session tokens: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create session"
            )

    async def create_session_tokens_by_type(
        self,
        mobile_number: str,
        user_type: str,
        device_type: str = "web"
    ) -> Dict[str, str]:
        """Generate JWT tokens without user object (for mobile)"""
        try:
            # Token payload without user ID for now
            payload = {
                "mobile_number": mobile_number,
                "role": user_type,
                "type": user_type,
                "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            }

            # Generate access token
            access_token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

            # Generate refresh token
            refresh_payload = {
                "mobile_number": mobile_number,
                "type": "refresh",
                "exp": datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
            }
            refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)

            tokens = {
                "access_token": access_token,
                "refresh_token": refresh_token
            }

            return tokens

        except Exception as e:
            logger.error(f"Error creating session tokens: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create session"
            )

    async def logout(self, user_id: int, user_type: str) -> Dict[str, str]:
        """Logout and clear session"""
        try:
            # Get user and clear session
            if user_type == "manager":
                user = self._get_manager_by_id(user_id)
            else:
                user = self._get_telecaller_by_id(user_id)

            if user:
                user.otp_session_token = None
                user.otp_session_expires_at = None

            return {"status": "success", "message": "Logged out successfully"}

        except Exception as e:
            logger.error(f"Error during logout: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to logout"
            )

    def _get_manager_by_mobile(self, mobile_number: str) -> Optional[Manager]:
        """Get manager by mobile number"""
        # This would use database session in real implementation
        # For now, returning None to be implemented in auth endpoints
        return None

    def _get_telecaller_by_mobile(self, mobile_number: str) -> Optional[Telecaller]:
        """Get telecaller by mobile number"""
        # This would use database session in real implementation
        # For now, returning None to be implemented in auth endpoints
        return None

    def _get_manager_by_id(self, user_id: int) -> Optional[Manager]:
        """Get manager by ID"""
        # This would use database session in real implementation
        return None

    def _get_telecaller_by_id(self, user_id: int) -> Optional[Telecaller]:
        """Get telecaller by ID"""
        # This would use database session in real implementation
        return None

    def _generate_session_token(self) -> str:
        """Generate a unique session token"""
        import secrets
        return secrets.token_urlsafe(32)

    def _mask_mobile_number(self, mobile_number: str) -> str:
        """Mask mobile number for security"""
        if len(mobile_number) <= 4:
            return "*" * len(mobile_number)
        return mobile_number[:2] + "*" * (len(mobile_number) - 4) + mobile_number[-2:]

    async def _handle_failed_attempt(self, mobile_number: str):
        """Handle failed login attempt"""
        # This would update the user's login attempts in database
        # Implementation to be added in auth endpoints
        pass

    async def _send_sms(self, mobile_number: str, message: str):
        """Send SMS via SMS gateway"""
        # Implementation for actual SMS sending
        logger.info(f"SMS to {mobile_number}: {message}")
        pass


# Singleton instance
otp_auth_service = OTPAuthService()