import os
import logging
from typing import Optional
from datetime import datetime
from app.utils.otp import async_send_verification_sms
from app.config.settings import settings
from app.utils.redis_config import get_redis

logger = logging.getLogger(__name__)

class SMSService:
    """Enhanced SMS service with monitoring and error handling"""

    def __init__(self):
        self.max_retries = int(os.getenv("SMS_MAX_RETRIES", "3"))
        self.timeout_seconds = int(os.getenv("SMS_TIMEOUT_SECONDS", "10"))

    async def send_otp_sms(self, mobile_number: str, otp: str) -> bool:
        """Send OTP SMS with enhanced error handling"""
        try:
            # Log SMS attempt
            await self._log_sms_attempt(mobile_number, "attempt")

            # Use existing SMS function
            success = await async_send_verification_sms(mobile_number, otp)

            if success:
                await self._log_sms_attempt(mobile_number, "success")
                logger.info(f"SMS sent successfully to {self._mask_mobile(mobile_number)}")
                return True
            else:
                await self._log_sms_attempt(mobile_number, "failed")
                logger.error(f"SMS failed to send to {self._mask_mobile(mobile_number)}")
                return False

        except Exception as e:
            await self._log_sms_attempt(mobile_number, "error")
            logger.error(f"SMS error for {self._mask_mobile(mobile_number)}: {str(e)}")
            return False

    async def _log_sms_attempt(self, mobile_number: str, status: str):
        """Log SMS attempts for monitoring"""
        try:
            redis_client = await get_redis()
            key = f"sms_log:{datetime.utcnow().strftime('%Y%m%d')}:{status}"
            await redis_client.incr(key)
            await redis_client.expire(key, 86400)  # Expire after 24 hours
        except Exception as e:
            logger.error(f"Failed to log SMS attempt: {str(e)}")

    def _mask_mobile(self, mobile_number: str) -> str:
        """Mask mobile number for logs"""
        if len(mobile_number) <= 4:
            return "*" * len(mobile_number)
        return mobile_number[:2] + "*" * (len(mobile_number) - 4) + mobile_number[-2:]