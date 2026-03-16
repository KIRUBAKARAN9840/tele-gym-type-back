"""Webhook signature verification utilities"""

import hmac
import hashlib
from typing import Optional


def verify_webhook_signature(
    payload: str, 
    signature: str, 
    secret: str,
    algorithm: str = "sha256"
) -> bool:
    """Verify webhook signature"""
    if not secret or secret == "replace_me":
        # In development, skip verification if secret not configured
        return True
    
    try:
        # Generate expected signature
        if algorithm == "sha256":
            expected_signature = hmac.new(
                secret.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
        else:
            return False
        
        # Compare signatures securely
        return hmac.compare_digest(signature, expected_signature)
    
    except Exception:
        return False


def verify_razorpay_signature(
    payload: str,
    signature: str,
    secret: str
) -> bool:
    """Verify Razorpay webhook signature"""
    return verify_webhook_signature(payload, signature, secret)


def verify_revenuecat_signature(
    payload: str,
    signature: str,
    secret: str
) -> bool:
    """Verify RevenueCat webhook signature"""
    return verify_webhook_signature(payload, signature, secret)