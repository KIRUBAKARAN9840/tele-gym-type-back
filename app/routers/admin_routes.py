"""Admin dashboard and management routes."""

from fastapi import APIRouter

# ── Admin API (new-style routers) ───────────────────────────────────
from app.fittbot_admin_api.marketing.marketing import router as admin_marketing_router
from app.fittbot_admin_api.gymowners import router as admin_gymowners_router
from app.fittbot_admin_api.fittbot_subscriptions.fittbot_subscriptions import router as admin_fittbot_subscriptions_router
from app.fittbot_admin_api.telecaller_managers.telecaller_managers import router as admin_telecaller_managers_router
from app.fittbot_admin_api.user_conversion import router as admin_user_conversion_router
from app.fittbot_admin_api.purchases import router as admin_purchases_router
from app.fittbot_admin_api.telecaller_activity import router as admin_telecaller_activity_router

# ── Admin API (module-level .router access) ─────────────────────────
from app.fittbot_admin_api.auth import authentication
from app.fittbot_admin_api.dashboard import admindashboard
from app.fittbot_admin_api.users import usersDashboard
from app.fittbot_admin_api.gymstats import gymstats
from app.fittbot_admin_api.supportticket import supporttickets
from app.fittbot_admin_api.allgyms import allgyms
from app.fittbot_admin_api.gymdetailsrequests import gymdetailsrequests
from app.fittbot_admin_api.employees import employees
from app.fittbot_admin_api.telecalling_assignments import telecalling_assignments
from app.fittbot_admin_api.reward_participants import reward_participants

from app.fittbot_admin_api.expenses import router as admin_expenses_router
from app.fittbot_admin_api.mrr import router as admin_mrr_router
from app.fittbot_admin_api.gyms.gyms import router as admin_gyms_router
from app.fittbot_admin_api.users_stats import router as admin_users_stats_router
from app.fittbot_admin_api.unit_economics.uniteconomics import router as admin_unit_economics_router
from app.fittbot_admin_api.tax_compliance.tax_compliance import router as admin_tax_compliance_router
from app.fittbot_admin_api.cash_flow.cash_flow import router as admin_cash_flow_router
from app.fittbot_admin_api.financials.financials import router as financials_router
from app.fittbot_admin_api.unverified_gyms import unverified_gyms

# ── Collector ───────────────────────────────────────────────────────
router = APIRouter()

# New-style admin routers (registered first in original)
router.include_router(admin_marketing_router)
router.include_router(admin_gymowners_router)
router.include_router(admin_fittbot_subscriptions_router)
router.include_router(admin_telecaller_managers_router)
router.include_router(admin_user_conversion_router)
router.include_router(admin_telecaller_activity_router)

# Legacy admin routers
router.include_router(authentication.router)
router.include_router(admindashboard.router)
router.include_router(usersDashboard.router)
router.include_router(gymstats.router)
router.include_router(supporttickets.router)
router.include_router(allgyms.router)
router.include_router(gymdetailsrequests.router)
router.include_router(employees.router)
router.include_router(telecalling_assignments.router)
router.include_router(admin_purchases_router)
router.include_router(reward_participants.router)


router.include_router(financials_router)
router.include_router(admin_expenses_router)
router.include_router(admin_mrr_router)
router.include_router(admin_gyms_router)
router.include_router(admin_users_stats_router)
router.include_router(admin_unit_economics_router)
router.include_router(admin_tax_compliance_router)
router.include_router(admin_cash_flow_router)

router.include_router(unverified_gyms.router)
