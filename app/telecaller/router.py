from fastapi import APIRouter
from app.telecaller.auth import manager_auth, telecaller_auth
from app.telecaller.managers import assignments, all_assignments, async_assignments, manager_tracker, report
from app.telecaller.telecallers import assignments as telecaller_assignments, call_logs, quick_call_log, gyms, test_auth
from app.telecaller.dashboard import manager_dashboard, telecaller_dashboard
from app.telecaller.status import gym_registration_status
from app.telecaller.client_tracking import client_tracking
from app.telecaller import leave
from app.telecaller import users
from app.telecaller import notifications

router = APIRouter(prefix="/telecaller", tags=["telecaller"])

# Authentication routes - no additional prefix needed since auth files already have role prefixes
router.include_router(
    manager_auth.router,
    tags=["manager-auth"]
)

router.include_router(
    telecaller_auth.router,
    tags=["telecaller-auth"]
)

# Manager routes
router.include_router(
    assignments.router,
    prefix="/manager",
    tags=["manager-assignments"]
)

router.include_router(
    all_assignments.router,
    prefix="/manager",
    tags=["manager-all-assignments"]
)

# Optimized async routes
router.include_router(
    async_assignments.router,
    prefix="/manager",
    tags=["manager-async-assignments"]
)

# Manager tracker routes (optimized)
router.include_router(
    manager_tracker.router,
    prefix="/manager",
    tags=["manager-tracker"]
)

# Manager report routes (optimized)
router.include_router(
    report.router,
    prefix="/manager",
    tags=["manager-report"]
)

# Add dashboard routes for manager
router.include_router(
    manager_dashboard.router,
    prefix="/manager/dashboard",
    tags=["manager-dashboard"]
)

# Telecaller routes
router.include_router(
    telecaller_assignments.router,
    prefix="/telecaller",
    tags=["telecaller-assignments"]
)

router.include_router(
    call_logs.router,
    prefix="/telecaller",
    tags=["telecaller-call-logs"]
)

router.include_router(
    quick_call_log.router,
    prefix="/telecaller",
    tags=["telecaller-quick-call-log"]
)

router.include_router(
    gyms.router,
    prefix="/telecaller",
    tags=["telecaller-gyms"]
)

router.include_router(
    test_auth.router,
    prefix="/telecaller",
    tags=["telecaller-test-auth"]
)

# Add dashboard routes for telecaller
router.include_router(
    telecaller_dashboard.router,
    prefix="/telecaller/dashboard",
    tags=["telecaller-dashboard"]
)

# Add status routes for gym registration
router.include_router(
    gym_registration_status.router,
    tags=["telecaller-status"]
)

# Add client tracking routes
router.include_router(
    client_tracking.router,
    tags=["telecaller-client-tracking"]
)

# Add leave routes for both manager and telecaller
router.include_router(
    leave.router,
    tags=["telecaller-leave"]
)

# Add users routes for both manager and telecaller
router.include_router(
    users.router,
    tags=["telecaller-users"]
)

# Add notification routes for telecaller new user alerts
router.include_router(
    notifications.router,
    tags=["telecaller-notifications"]
)



