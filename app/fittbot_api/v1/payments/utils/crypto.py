"""Cryptographic utilities for webhook verification"""

import hmac
import hashlib


def verify_webhook_sig(secret: str, payload: bytes, signature: str) -> bool:
    """
    Verify webhook signature for Razorpay
    Used by gym settlement system
    """
    if not secret or secret == "replace_me":
        # Development mode - skip verification
        return True

    try:
        # Create expected signature
        expected = hmac.new(
            secret.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()

        # Compare with provided signature
        return hmac.compare_digest(signature, expected)
    except Exception:
        return False