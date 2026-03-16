"""Payment system configuration settings"""

import os
from datetime import timedelta, timezone
from typing import Optional
from app.config.settings import settings


class PaymentSettings:
    """Payment system configuration using main app settings"""
    
    def __init__(self):
        """Initialize payment settings from main app settings"""
        self._settings = settings
    
    # Database Configuration
    @property
    def database_url(self) -> str:
        """Get database URL from main settings"""
        return self._settings.database_url
    
    # Timezone Configuration
    @property
    def ist_offset_hours(self) -> int:
        return 5
    
    @property
    def ist_offset_minutes(self) -> int:
        return 30
    
    # Provider Webhook Secrets
    @property
    def razorpay_webhook_secret(self) -> str:
        return self._settings.razorpay_webhook_secret
    
    @property
    def razorpayx_webhook_secret(self) -> str:
        return self._settings.razorpay_webhook_secret
    
    @property
    def revenuecat_webhook_secret(self) -> str:
        return "martinrajunaveenfromtamilnadu"

    @property
    def revenuecat_api_key(self) -> str:
        """RevenueCat REST API Key (Secret API Key from RevenueCat dashboard)"""
        api_key = self._settings.revenuecat_api_key
        if not api_key:
            # Log warning if API key is not set
            import logging
            logger = logging.getLogger("payments.settings")
            logger.warning("⚠️ REVENUECAT_API_KEY not set! Verify endpoint will fail. Add it to .env file.")
            return ""
        return api_key

    # Razorpay API Configuration
    @property
    def razorpay_key_id(self) -> str:
        return self._settings.razorpay_key_id
    
    @property
    def razorpay_key_secret(self) -> str:
        return self._settings.razorpay_key_secret
    
    # RazorpayX Configuration (use same as Razorpay for now)
    @property
    def razorpayx_key_id(self) -> str:
        return self._settings.razorpay_key_id
    
    @property
    def razorpayx_key_secret(self) -> str:
        return self._settings.razorpay_key_secret
    
    # Commission Configuration
    @property
    def default_commission_pct(self) -> float:
        return 5.0
    
    @property
    def default_commission_fixed_minor(self) -> int:
        return 0
    
    # Payout Configuration
    @property
    def payout_batch_threshold_minor(self) -> int:
        return 10000  # Rs. 100

    @property
    def payout_processing_hour(self) -> int:
        return 18  # 6 PM IST

    @property
    def razorpay_payout_account_number(self) -> str:
        """Razorpay payout account number for gym settlements"""
        return getattr(self._settings, 'razorpay_payout_account_number', "2323230084229691")  # Default or from settings

    @property
    def razorpay_payouts_webhook_secret(self) -> str:
        """Webhook secret for payout events"""
        return getattr(self._settings, 'razorpay_payouts_webhook_secret', self.razorpay_webhook_secret)
    
    # Idempotency Configuration
    @property
    def idempotency_ttl_hours(self) -> int:
        return 24
    
    # Webhook Configuration
    @property
    def webhook_timeout_seconds(self) -> int:
        return 30
    
    @property
    def webhook_retry_attempts(self) -> int:
        return 3
    
    # Environment
    @property
    def environment(self) -> str:
        return self._settings.environment
    
    @property
    def ist_timezone(self) -> timezone:
        """Get IST timezone object"""
        return timezone(timedelta(
            hours=self.ist_offset_hours,
            minutes=self.ist_offset_minutes
        ))
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.environment.lower() == "development"
    
    @property
    def idempotency_ttl_delta(self) -> timedelta:
        """Get idempotency TTL as timedelta"""
        return timedelta(hours=self.idempotency_ttl_hours)


# Global settings instance
_payment_settings: Optional[PaymentSettings] = None


def get_payment_settings() -> PaymentSettings:
    """Get payment settings singleton"""
    global _payment_settings
    if _payment_settings is None:
        _payment_settings = PaymentSettings()
    return _payment_settings
