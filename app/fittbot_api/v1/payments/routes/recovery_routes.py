"""
Recovery API Routes - For webhook recovery and system maintenance
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from ..config.database import get_db_session
from ..services.webhook_recovery_service_fixed import WebhookRecoveryService

logger = logging.getLogger("payments.recovery_routes")
router = APIRouter(prefix="/payments/admin", tags=["Recovery"])


@router.post("/recover-stuck-webhooks")
async def recover_stuck_webhooks(
    max_age_minutes: int = Query(10, description="Max age in minutes for stuck webhooks"),
    db: Session = Depends(get_db_session)
):
    """
    Recover webhooks that have been processing for too long
    """
    try:
        recovery_service = WebhookRecoveryService(db)
        result = recovery_service.recover_stuck_webhooks(max_age_minutes)
        
        logger.info(f"Webhook recovery completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error during webhook recovery: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error during webhook recovery: {str(e)}"
        )


@router.post("/recover-missing-entitlements")
async def recover_missing_entitlements(db: Session = Depends(get_db_session)):
    """
    Recover missing entitlements for customers with active subscriptions
    """
    try:
        recovery_service = WebhookRecoveryService(db)
        result = recovery_service.recover_missing_entitlements()
        
        logger.info(f"Entitlement recovery completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error during entitlement recovery: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error during entitlement recovery: {str(e)}"
        )


@router.post("/sync-subscription-entitlements")
async def sync_subscription_entitlements(db: Session = Depends(get_db_session)):
    """
    Sync subscription status with entitlement status
    """
    try:
        recovery_service = WebhookRecoveryService(db)
        result = recovery_service.sync_subscription_entitlements()
        
        logger.info(f"Subscription-Entitlement sync completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error during sync: {str(e)}"
        )


@router.delete("/cleanup-old-webhook-logs")
async def cleanup_old_webhook_logs(
    days_old: int = Query(30, description="Delete logs older than this many days"),
    db: Session = Depends(get_db_session)
):
    """
    Cleanup old webhook logs to prevent database bloat
    """
    try:
        recovery_service = WebhookRecoveryService(db)
        result = recovery_service.cleanup_old_webhook_logs(days_old)
        
        logger.info(f"Webhook log cleanup completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error during cleanup: {str(e)}"
        )


@router.get("/webhook-stats")
async def get_webhook_stats(
    hours: int = Query(24, description="Stats for the last N hours"),
    db: Session = Depends(get_db_session)
):
    """
    Get webhook processing statistics for monitoring
    """
    try:
        recovery_service = WebhookRecoveryService(db)
        stats = recovery_service.get_webhook_stats(hours)
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting webhook stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting webhook stats: {str(e)}"
        )