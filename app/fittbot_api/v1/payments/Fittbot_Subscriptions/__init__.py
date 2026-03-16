"""
Fittbot Subscriptions Module
Handles all subscription-related functionality including RevenueCat and Razorpay integrations
"""

from .revenue_cat import router as revenue_cat_router
from .razorpay import router as razorpay_router

__all__ = ["revenue_cat_router", "razorpay_router"]
