"""
Razorpay Subscriptions (end-to-end) + Premium Status Gate

This package provides a clean, modular implementation of Razorpay
subscription flows, webhook handling, and a premium gating dependency.

Routers exposed:
 - POST /payments/razorpay/subscriptions/create
 - POST /payments/razorpay/subscriptions/verify
 - POST /payments/webhooks/razorpay
 - GET  /payments/user/{client_id}/premium-status

Also exports:
 - premium_required: FastAPI dependency to protect premium-only routes
"""

from .routes import router, premium_required

__all__ = ["router", "premium_required"]

