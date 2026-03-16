"""
Status API Routes - Enterprise-level status checking
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.premium_status_service import PremiumStatusService

logger = logging.getLogger("payments.status_routes")
router = APIRouter(prefix="/payments", tags=["Status"])


@router.get("/user/{customer_id}/premium-status")
async def get_premium_status(
    customer_id: str = Path(..., description="Customer ID"),
    db: Session = Depends(get_db_session)
):
    """
    Get comprehensive premium status for a customer
    This is the main endpoint used by frontend verification
    """
    try:
        status_service = PremiumStatusService(db)
        status = status_service.get_premium_status(customer_id)
        
        # Log for debugging
        logger.info(f"Premium status check for {customer_id}: {status.get('has_premium', False)}")
        
        return status
        
    except Exception as e:
        logger.error(f"Error checking premium status for {customer_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error checking premium status: {str(e)}"
        )


@router.get("/user/orders/{order_id}/status")
async def get_order_status(
    order_id: str = Path(..., description="Order ID"),
    db: Session = Depends(get_db_session)
):
    """
    Get detailed order status for frontend polling
    Used during purchase flow verification
    """
    try:
        status_service = PremiumStatusService(db)
        order_status = status_service.get_order_status(order_id)
        
        # Log for debugging
        logger.info(f"Order status check for {order_id}: {order_status.get('status', 'unknown')}")
        
        return order_status
        
    except Exception as e:
        logger.error(f"Error checking order status for {order_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error checking order status: {str(e)}"
        )


@router.get("/user/{customer_id}/subscription-details")
async def get_subscription_details(
    customer_id: str = Path(..., description="Customer ID"),
    db: Session = Depends(get_db_session)
):
    """
    Get detailed subscription information for a customer
    """
    try:
        status_service = PremiumStatusService(db)
        status = status_service.get_premium_status(customer_id)
        
        if not status.get("has_premium", False):
            return {
                "has_subscription": False,
                "message": "No active premium subscription found"
            }
        
        subscription_details = status.get("status_details", {}).get("subscription", {})
        entitlement_details = status.get("status_details", {}).get("entitlement", {})
        
        return {
            "has_subscription": True,
            "subscription": subscription_details,
            "entitlement": entitlement_details,
            "expires_at": subscription_details.get("active_until") or entitlement_details.get("active_until"),
            "days_remaining": subscription_details.get("days_remaining") or entitlement_details.get("days_remaining"),
            "auto_renew": subscription_details.get("auto_renew", False)
        }
        
    except Exception as e:
        logger.error(f"Error getting subscription details for {customer_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting subscription details: {str(e)}"
        )