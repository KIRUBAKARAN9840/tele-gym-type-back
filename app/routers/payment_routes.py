"""Payment routes: webhooks, subscriptions, daily pass, gym membership."""

from fastapi import APIRouter

# ── Webhooks ────────────────────────────────────────────────────────
from app.fittbot_api.v1.payments.webhooks.razorpay_handler import router as rp_webhook_router
from app.fittbot_api.v1.webhooks.leegality_handler import router as leegality_webhook_router

# ── Subscriptions ───────────────────────────────────────────────────
from app.fittbot_api.v1.payments.Fittbot_Subscriptions.razorpay import router as razorpay_subscription_new_router
from app.fittbot_api.v1.payments.Fittbot_Subscriptions.revenue_cat import router as revenue_cat_router

# ── Queue-backed (v2) ──────────────────────────────────────────────
from app.fittbot_api.v1.payments.Fittbot_Subscriptions_concurrent import (
    razorpay_router as razorpay_payments_v2_router,
    revenuecat_router as revenuecat_payments_v2_router,
    dailypass_router as dailypass_payments_v2_router,
    gym_membership_router as gym_membership_payments_v2_router,
    sessions_router as sessions_payments_v2_router,
)

# ── Core Payment Routes ─────────────────────────────────────────────
from app.fittbot_api.v1.payments.routes.user_premium import router as user_premium_router
from app.fittbot_api.v1.payments.routes.gym_membership import router as gym_membership_rp_router

# ── Daily Pass System ───────────────────────────────────────────────
from app.fittbot_api.v1.payments.dailypass import (
    daily_pass_router,
    daily_pass_checkin_router,
    daily_pass_settlement_router,
    daily_pass_recon_router,
)

# ── Auto Settlements & Payouts (new) ────────────────────────────────
from app.fittbot_api.v1.payments.auto_settlements.routes import router as auto_settlements_router
from app.fittbot_api.v1.payments.auto_settlements.webhook_handler import router as auto_settlements_webhook_router

# ── Collector ───────────────────────────────────────────────────────
router = APIRouter()

# Registration order preserved from original main.py
router.include_router(rp_webhook_router)
router.include_router(leegality_webhook_router)
router.include_router(razorpay_subscription_new_router)
router.include_router(razorpay_payments_v2_router)
router.include_router(revenue_cat_router)
router.include_router(revenuecat_payments_v2_router)
router.include_router(dailypass_payments_v2_router)
router.include_router(gym_membership_payments_v2_router)
router.include_router(sessions_payments_v2_router)
router.include_router(user_premium_router)
router.include_router(gym_membership_rp_router)
router.include_router(daily_pass_router)
router.include_router(daily_pass_checkin_router)
router.include_router(daily_pass_settlement_router)
router.include_router(daily_pass_recon_router)
router.include_router(auto_settlements_router)
router.include_router(auto_settlements_webhook_router)
