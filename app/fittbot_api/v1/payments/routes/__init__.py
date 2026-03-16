"""Payment routes package"""

from .payments import router as payments_router
from .checkins import router as checkins_router
from .payouts import router as payouts_router
from .settlements import router as settlements_router
from .refunds import router as refunds_router
from .admin import router as admin_router
from .status_routes import router as status_router
from .recovery_routes import router as recovery_router
from .user_premium import router as user_premium_router

# Import Razorpay routes
from ..razorpay.routes import subscription_routes_router as razorpay_subscription_router

# Import Gym Settlement routes
from ..services.gym_membership_settlements import router as gym_settlements_router

__all__ = [
    "payments_router",
    "checkins_router",
    "payouts_router",
    "settlements_router",
    "refunds_router",
    "admin_router",
    "status_router",
    "recovery_router",
    "user_premium_router",
    "razorpay_subscription_router",
    "gym_settlements_router"
]