"""Subscription status and user access API routes"""

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from typing import Dict, Any, Optional

from ..config.database import get_db_session
from ..models.subscriptions import Subscription
from ..models.entitlements import Entitlement
from ..models.payments import Payment

router = APIRouter(prefix="/subscription", tags=["subscription"])


@router.get("/{user_id}/status")
async def get_subscription_status(
    user_id: str = Path(..., description="User ID to check"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Get user's subscription status and premium access"""
    
    try:
        # Check active subscription
        active_subscription = db.query(Subscription).filter(
            Subscription.customer_id == user_id,
            Subscription.status.in_(['active', 'renewed']),
            Subscription.active_until > datetime.now()
        ).order_by(Subscription.created_at.desc()).first()
        
        # Check entitlements
        active_entitlements = db.query(Entitlement).filter(
            Entitlement.customer_id == user_id,
            Entitlement.status.in_(['pending', 'used']),
            Entitlement.active_until > datetime.now()
        ).all()
        
        # Get latest payment
        latest_payment = db.query(Payment).filter(
            Payment.customer_id == user_id,
            Payment.status == 'captured'
        ).order_by(Payment.created_at.desc()).first()
        
        has_premium_access = active_subscription is not None
        
        result = {
            "user_id": user_id,
            "has_premium_access": has_premium_access,
            "subscription_status": active_subscription.status if active_subscription else "inactive",
            "subscription_expires": active_subscription.active_until.isoformat() if active_subscription else None,
            "product_id": active_subscription.product_id if active_subscription else None,
            "entitlements_count": len(active_entitlements),
            "latest_payment_amount": latest_payment.amount_minor / 100 if latest_payment else 0,
            "latest_payment_date": latest_payment.created_at.isoformat() if latest_payment else None,
            "checked_at": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking subscription status: {str(e)}")


@router.get("/{user_id}/access")
async def check_premium_access(
    user_id: str = Path(..., description="User ID to check"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Simple premium access check for app usage"""
    
    try:
        # Quick check for active subscription
        has_access = db.execute(text("""
            SELECT COUNT(*) > 0 as has_access
            FROM payments.subscriptions 
            WHERE customer_id = :user_id 
            AND status IN ('active', 'renewed')
            AND (active_until IS NULL OR active_until > NOW())
        """), {"user_id": user_id}).scalar()
        
        return {
            "user_id": user_id,
            "has_premium_access": bool(has_access),
            "checked_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking access: {str(e)}")


@router.get("/{user_id}/subscriptions")
async def get_user_subscriptions(
    user_id: str = Path(..., description="User ID to get subscriptions for"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Get user's subscription history"""
    
    try:
        subscriptions = db.query(Subscription).filter(
            Subscription.customer_id == user_id
        ).order_by(Subscription.created_at.desc()).all()
        
        subscription_list = []
        for sub in subscriptions:
            subscription_list.append({
                "id": sub.id,
                "product_id": sub.product_id,
                "status": sub.status,
                "provider": sub.provider,
                "active_from": sub.active_from.isoformat() if sub.active_from else None,
                "active_until": sub.active_until.isoformat() if sub.active_until else None,
                "auto_renew": sub.auto_renew,
                "cancel_reason": sub.cancel_reason,
                "created_at": sub.created_at.isoformat()
            })
        
        return {
            "user_id": user_id,
            "subscriptions": subscription_list,
            "total_subscriptions": len(subscription_list),
            "retrieved_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting subscriptions: {str(e)}")


@router.get("/health")
async def subscription_health():
    """Health check for subscription service"""
    return {
        "service": "subscription_status",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }