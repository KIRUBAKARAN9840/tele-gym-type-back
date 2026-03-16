"""
Customer Self-Service Payment Recovery
Allow customers to fix their own payment issues
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from ..config.database import get_db_session
from ..models.orders import Order
from ..services.webhook_recovery_service import WebhookRecoveryService
from ..webhooks.revenuecat_handler import now_ist

router = APIRouter(prefix="/recovery", tags=["Payment Recovery"])

@router.post("/check-payment-status/{order_id}")
async def customer_check_payment_status(
    order_id: str,
    customer_id: str,  # From auth middleware
    db: Session = Depends(get_db_session)
):
    """Allow customer to trigger payment status check if they think something's wrong"""
    
    # Find the order
    order = db.query(Order).filter(
        Order.id == order_id,
        Order.customer_id == customer_id  # Security check
    ).first()
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Only allow recovery check if order is recent and pending
    if order.status != "pending":
        return {
            "status": "no_action_needed", 
            "message": f"Order status is already '{order.status}'"
        }
    
    order_age_minutes = (now_ist() - order.created_at).total_seconds() / 60
    
    if order_age_minutes < 5:
        return {
            "status": "too_early",
            "message": "Payment is still processing. Please wait 5-10 minutes.",
            "wait_time_remaining": max(0, 5 - int(order_age_minutes))
        }
    
    if order_age_minutes > 1440:  # 24 hours
        return {
            "status": "too_old",
            "message": "Order is too old for automatic recovery. Please contact support.",
            "support_contact": "support@fittbot.com"
        }
    
    # Trigger recovery check
    try:
        recovery_service = WebhookRecoveryService()
        await recovery_service.recover_order_status(order, db)
        
        # Refresh order from DB
        db.refresh(order)
        
        if order.status == "paid":
            return {
                "status": "recovered",
                "message": "✅ Payment found! Your premium access has been activated.",
                "order_status": order.status
            }
        else:
            return {
                "status": "still_pending", 
                "message": "Payment is still being processed. If you completed payment, please try again in 5 minutes.",
                "order_status": order.status
            }
            
    except Exception as e:
        logger.error(f"Recovery failed for order {order_id}: {e}")
        raise HTTPException(status_code=500, detail="Recovery check failed")


@router.get("/payment-help/{order_id}")
async def get_payment_help(
    order_id: str,
    customer_id: str,  # From auth middleware  
    db: Session = Depends(get_db_session)
):
    """Provide helpful information about payment status"""
    
    order = db.query(Order).filter(
        Order.id == order_id,
        Order.customer_id == customer_id
    ).first()
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order_age_minutes = (now_ist() - order.created_at).total_seconds() / 60
    
    # Different help messages based on order age and status
    if order.status == "pending":
        if order_age_minutes <= 5:
            return {
                "status": "processing",
                "title": "Payment is Processing",
                "message": "Your payment is being confirmed. This usually takes 2-5 minutes.",
                "actions": [
                    "Wait 5 minutes and refresh the app",
                    "Check your Google Play purchase history",
                    "Try the 'Check Payment Status' button"
                ],
                "estimated_completion": "2-5 minutes"
            }
        elif order_age_minutes <= 30:
            return {
                "status": "delayed",
                "title": "Payment Taking Longer Than Usual", 
                "message": "Sometimes payments can take up to 30 minutes to process.",
                "actions": [
                    "Click 'Check Payment Status' to verify",
                    "Check if you were charged in Google Play",
                    "Wait a bit longer - payments can be delayed"
                ],
                "can_retry_check": True
            }
        else:
            return {
                "status": "likely_failed",
                "title": "Payment May Have Failed",
                "message": "Your payment has been pending for over 30 minutes. This usually means the payment didn't complete.",
                "actions": [
                    "Check Google Play purchase history",
                    "Try purchasing again if you weren't charged", 
                    "Contact support if you were charged but don't have access"
                ],
                "support_contact": "support@fittbot.com"
            }
    
    elif order.status == "paid":
        return {
            "status": "completed",
            "title": "Payment Successful",
            "message": "Your payment was successful and premium access is active!",
            "premium_expires": "Check subscription settings for expiry date"
        }
    
    elif order.status == "failed":
        return {
            "status": "failed", 
            "title": "Payment Failed",
            "message": "Your payment could not be processed.",
            "actions": [
                "Check your payment method has sufficient funds",
                "Try purchasing again",
                "Contact support if you were charged"
            ]
        }