"""Production User Premium Management APIs"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, or_
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

logger = logging.getLogger("payments.user_premium")

# Indian Standard Time (IST) timezone
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Get current time in IST"""
    return datetime.now(IST)

from ..config.database import get_db_session
from ..models.subscriptions import Subscription
from ..models.entitlements import Entitlement
from ..models.payments import Payment
from ..models.orders import Order
from ..models.catalog import CatalogProduct

router = APIRouter(prefix="/user_premium", tags=["user_premium"])


# Request/Response Models
class CreateOrderRequest(BaseModel):
    client_id: str
    product_sku: str
    currency: str = "INR"


class PremiumStatusResponse(BaseModel):
    client_id: str
    has_premium: bool
    subscription_status: str
    expires_at: Optional[str]
    features_unlocked: List[str]
    subscription_id: Optional[str]


class FeatureAccessResponse(BaseModel):
    client_id: str
    feature: str
    has_access: bool
    access_type: str
    expires_at: Optional[str]


@router.get("/payments/user/{client_id}/premium-status")
async def get_user_premium_status(
    client_id: str = Path(..., description="Client ID to check"),
    db: Session = Depends(get_db_session)
) -> PremiumStatusResponse:
   
    try:
        # ✅ FIX: Use IST timezone for comparison
        now = now_ist()
        allowed_providers = {"razorpay_pg", "google_play"}

        # Check active subscription with timezone-aware comparison
        active_subscription = db.query(Subscription).filter(
            Subscription.customer_id == client_id,
            Subscription.provider.in_(allowed_providers),
            or_(
                and_(
                    Subscription.status.in_(["active", "renewed"]),
                    Subscription.active_until > now,
                ),
                and_(
                    Subscription.status == "canceled",
                    Subscription.active_until.isnot(None),
                    Subscription.active_until >= now,
                ),
            ),
        ).order_by(Subscription.created_at.desc()).first()

        # ✅ FIX: Verify payment is captured (like Razorpay)
        has_premium = False
        if active_subscription:
            # Check if there's a captured payment for this subscription
            payment_verified = db.query(Payment).filter(
                Payment.customer_id == client_id,
                Payment.provider_payment_id == active_subscription.latest_txn_id,
                Payment.status == "captured"
            ).first() is not None

            has_premium = payment_verified

            logger.info(f"Premium check for {client_id}:")
            logger.info(f"  - Subscription found: {active_subscription.id}")
            logger.info(f"  - Status: {active_subscription.status}")
            logger.info(f"  - Expires: {active_subscription.active_until}")
            logger.info(f"  - Payment verified: {payment_verified}")
            logger.info(f"  - Has premium: {has_premium}")

        print("has premium is", has_premium)

        return PremiumStatusResponse(
            client_id=client_id,
            has_premium=has_premium,
            subscription_status=active_subscription.status if active_subscription else "inactive",
            expires_at=active_subscription.active_until.isoformat() if active_subscription else None,
            features_unlocked=[],
            subscription_id=active_subscription.id if active_subscription else None
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking premium status: {str(e)}")


@router.get("/payments/user/{client_id}/can-access/{feature_name}")
async def check_feature_access(
    client_id: str = Path(..., description="Client ID to check"),
    feature_name: str = Path(..., description="Feature name to check access for"),
    db: Session = Depends(get_db_session)
) -> FeatureAccessResponse:
    """Check if user can access specific premium feature"""
    
    try:
        # Define free features (always accessible)
        free_features = [
            "basic_workouts",
            "basic_nutrition",
            "community_access",
            "profile_management"
        ]
        
        if feature_name in free_features:
            return FeatureAccessResponse(
                client_id=client_id,
                feature=feature_name,
                has_access=True,
                access_type="free",
                expires_at=None
            )
        
        # Check premium features
        active_subscription = db.query(Subscription).filter(
            Subscription.customer_id == client_id,
            Subscription.status.in_(['active', 'renewed']),
            Subscription.active_until > datetime.now()
        ).first()
        
        if active_subscription:
            return FeatureAccessResponse(
                client_id=client_id,
                feature=feature_name,
                has_access=True,
                access_type="premium_subscription",
                expires_at=active_subscription.active_until.isoformat()
            )
        
        # Check trial access (if you have trials)
        # trial_access = check_trial_access(client_id, feature_name)
        
        return FeatureAccessResponse(
            client_id=client_id,
            feature=feature_name,
            has_access=False,
            access_type="none",
            expires_at=None
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking feature access: {str(e)}")


@router.get("/payments/user/{client_id}/entitlements")
async def get_user_entitlements(
    client_id: str = Path(..., description="Client ID to get entitlements for"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Get all user entitlements (gym access, trainer sessions, etc.)"""
    
    try:
        # Get active entitlements
        active_entitlements = db.query(Entitlement).filter(
            Entitlement.customer_id == client_id,
            Entitlement.status.in_(['pending', 'used']),
            Entitlement.active_until > datetime.now()
        ).all()
        
        # Get expired entitlements (last 30 days)
        expired_entitlements = db.query(Entitlement).filter(
            Entitlement.customer_id == client_id,
            Entitlement.status == 'expired',
            Entitlement.active_until > (datetime.now() - timedelta(days=30))
        ).all()
        
        active_list = []
        for ent in active_entitlements:
            active_list.append({
                "id": ent.id,
                "type": ent.entitlement_type,
                "status": ent.status,
                "gym_id": ent.gym_id,
                "trainer_id": ent.trainer_id,
                "scheduled_for": ent.scheduled_for.isoformat() if ent.scheduled_for else None,
                "expires_at": ent.active_until.isoformat() if ent.active_until else None,
                "created_at": ent.created_at.isoformat()
            })
        
        expired_list = []
        for ent in expired_entitlements:
            expired_list.append({
                "id": ent.id,
                "type": ent.entitlement_type,
                "gym_id": ent.gym_id,
                "expired_at": ent.active_until.isoformat() if ent.active_until else None
            })
        
        return {
            "client_id": client_id,
            "active_entitlements": active_list,
            "expired_entitlements": expired_list,
            "total_active": len(active_list),
            "retrieved_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting entitlements: {str(e)}")


@router.post("/user/orders/create")
async def create_pending_order(
    request: CreateOrderRequest,
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Create pending order before RevenueCat purchase"""
    
    try:
        # Get product details
        product = db.query(CatalogProduct).filter(
            CatalogProduct.sku == request.product_sku,
            CatalogProduct.active == True
        ).first()
        
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Generate order ID (using IST)
        ist_now = now_ist()
        order_id = f"ord_{ist_now.strftime('%Y%m%d')}_{request.client_id}_{int(ist_now.timestamp())}"
        
        # Create order
        order = Order(
            id=order_id,
            customer_id=request.client_id,
            currency=request.currency,
            provider="google_play",  # Fixed: Use consistent provider
            gross_amount_minor=product.base_amount_minor,
            status="pending"
        )
        
        db.add(order)
        db.commit()
        db.refresh(order)
        
        return {
            "order_id": order.id,
            "client_id": request.client_id,
            "product_sku": request.product_sku,
            "amount": product.base_amount_minor,
            "currency": request.currency,
            "status": "pending",
            "expires_at": (now_ist() + timedelta(minutes=15)).isoformat(),
            "created_at": order.created_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating order: {str(e)}")


@router.get("/payments/user/orders/{order_id}/status")
async def get_order_status(
    order_id: str = Path(..., description="Order ID to check"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Check order status and processing results"""
    
    try:
        # Get order details
        order = db.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Get associated payment
        payment = db.query(Payment).filter(Payment.order_id == order_id).first()
        
        # Get associated subscription
        subscription = db.query(Subscription).filter(
            Subscription.customer_id == order.customer_id
        ).order_by(Subscription.created_at.desc()).first()
        
        # Count created entitlements
        entitlement_count = db.query(Entitlement).filter(
            Entitlement.customer_id == order.customer_id
        ).count()
        
        return {
            "order_id": order.id,
            "status": order.status,
            "amount": order.gross_amount_minor,
            "currency": order.currency,
            "payment_status": payment.status if payment else "not_found",
            "payment_amount": payment.amount_minor if payment else 0,
            "subscription_created": subscription is not None,
            "subscription_status": subscription.status if subscription else "not_created",
            "entitlements_created": entitlement_count,
            "created_at": order.created_at.isoformat(),
            "updated_at": order.updated_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting order status: {str(e)}")


@router.get("/payments/user/products/available")
async def get_available_products(
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Get all available premium products for purchase"""
    
    try:
        products = db.query(CatalogProduct).filter(
            CatalogProduct.active == True,
            CatalogProduct.item_type == 'app_subscription'
        ).all()
        
        product_list = []
        for product in products:
            # Determine billing period from SKU
            billing_period = "monthly" if "monthly" in product.sku else "yearly"
            
            # Define features based on product
            features = [
                "unlimited_workouts",
                "nutrition_plans",
                "expert_support", 
                "advanced_analytics",
                "priority_booking",
                "exclusive_content"
            ]
            
            product_list.append({
                "sku": product.sku,
                "title": product.title,
                "description": product.description,
                "price": product.base_amount_minor / 100,  # Convert to rupees
                "currency": "INR",
                "billing_period": billing_period,
                "features": features,
                "popular": billing_period == "monthly"  # Mark monthly as popular
            })
        
        return {
            "products": product_list,
            "total_products": len(product_list),
            "currency": "INR",
            "retrieved_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting products: {str(e)}")


# Premium Feature Gate Utility
async def check_premium_access(client_id: str, feature_name: str, db: Session) -> bool:
    """Utility function to check if user has access to premium feature"""
    try:
        # Check active subscription
        active_subscription = db.query(Subscription).filter(
            Subscription.customer_id == client_id,
            Subscription.status.in_(['active', 'renewed']),
            Subscription.active_until > datetime.now()
        ).first()
        
        return active_subscription is not None
    except:
        return False


@router.get("/payments/user/{client_id}/subscription-history")
async def get_subscription_history(
    client_id: str = Path(..., description="Client ID to get history for"),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """Get user's complete subscription and payment history"""
    
    try:
        # Get all subscriptions
        subscriptions = db.query(Subscription).filter(
            Subscription.customer_id == client_id
        ).order_by(Subscription.created_at.desc()).all()
        
        # Get all payments
        payments = db.query(Payment).filter(
            Payment.customer_id == client_id
        ).order_by(Payment.created_at.desc()).all()
        
        subscription_history = []
        for sub in subscriptions:
            subscription_history.append({
                "id": sub.id,
                "product_id": sub.product_id,
                "status": sub.status,
                "amount": 0,  # Get from related payment
                "active_from": sub.active_from.isoformat() if sub.active_from else None,
                "active_until": sub.active_until.isoformat() if sub.active_until else None,
                "auto_renew": sub.auto_renew,
                "created_at": sub.created_at.isoformat()
            })
        
        payment_history = []
        for payment in payments:
            payment_history.append({
                "id": payment.id,
                "amount": payment.amount_minor / 100,
                "currency": payment.currency,
                "status": payment.status,
                "provider": payment.provider,
                "created_at": payment.created_at.isoformat()
            })
        
        return {
            "client_id": client_id,
            "subscriptions": subscription_history,
            "payments": payment_history,
            "total_subscriptions": len(subscription_history),
            "total_payments": len(payment_history),
            "total_spent": sum([p.amount_minor for p in payments]) / 100,
            "retrieved_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting history: {str(e)}")
