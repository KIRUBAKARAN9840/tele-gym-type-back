"""Admin dashboard and analytics routes"""

from typing import Optional
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.admin_service import AdminService

router = APIRouter(prefix="/admin", tags=["Admin"])


@router.get("/dashboard/summary")
async def dashboard_summary(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    db: Session = Depends(get_db_session)
):
    """Get payment system dashboard summary"""
    with AdminService(db) as admin_service:
        return admin_service.get_dashboard_summary(from_date, to_date)


@router.get("/analytics/gmv")
async def gmv_analytics(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    db: Session = Depends(get_db_session)
):
    """Get GMV analytics"""
    with AdminService(db) as admin_service:
        return admin_service.get_gmv_analytics(from_date, to_date)


@router.get("/analytics/commissions")
async def commission_analytics(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    gym_id: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """Get commission analytics"""
    with AdminService(db) as admin_service:
        return admin_service.get_commission_analytics(from_date, to_date, gym_id)


@router.get("/analytics/payouts")
async def payout_analytics(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    gym_id: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """Get payout analytics"""
    with AdminService(db) as admin_service:
        return admin_service.get_payout_analytics(from_date, to_date, gym_id)