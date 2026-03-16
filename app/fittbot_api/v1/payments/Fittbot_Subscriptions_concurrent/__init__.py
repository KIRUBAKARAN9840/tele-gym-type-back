"""
High-concurrency payment surfaces (Razorpay + RevenueCat).

Legacy routers remain untouched; this package exposes queue-backed variants that
decouple HTTP latency from provider/API bottlenecks with Redis + Celery.
"""

# Import Celery tasks for side effects (registration) when package is loaded
from . import tasks  # noqa: F401
from .router import router as razorpay_router
from .revenuecat_router import router as revenuecat_router
from .dailypass_router import router as dailypass_router
from .gym_membership_router import router as gym_membership_router
from .sessions_router import router as sessions_router

# Maintain original import contract
router = razorpay_router

__all__ = [
    "router",
    "razorpay_router",
    "revenuecat_router",
    "dailypass_router",
    "gym_membership_router",
    "sessions_router",
]
