"""Marketing team routes: leads, planner, profile, attendance, gym database."""

from fastapi import APIRouter

from app.marketing_api import (
    authSession,
    leads,
    planner,
    profile,
    home,
    managers,
    marketfeedback,
    gymDatabase,
    leaderboard,
    stats,
    gymDetailsRequest,
    gym_documents,
    local_documents,
    attendance,
    leave,
)

router = APIRouter()

router.include_router(authSession.router)
router.include_router(leads.router)
router.include_router(planner.router)
router.include_router(profile.router)
router.include_router(home.router)
router.include_router(managers.router)
router.include_router(marketfeedback.router)
router.include_router(gymDatabase.router)
router.include_router(leaderboard.router)
router.include_router(stats.router)
router.include_router(gymDetailsRequest.router)
router.include_router(gym_documents.router)
router.include_router(local_documents.router)
router.include_router(attendance.router)
router.include_router(leave.router)
