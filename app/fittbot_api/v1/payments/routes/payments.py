"""Payment processing routes"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config.database import get_db_session
from ..services.payment_service import PaymentService
from ..services.entitlement_service import EntitlementService
from ..schemas.payments import VerifyPaymentRequest, PaymentResponse

router = APIRouter(prefix="/payments", tags=["Payments"])


@router.post("/verify", response_model=dict)
async def verify_payment(
    payload: VerifyPaymentRequest,
    db: Session = Depends(get_db_session)
):
    """Verify and capture a payment"""
    try:
        with PaymentService(db) as payment_service:
            payment = payment_service.verify_payment(payload)
            
            # Get created entitlements for this order
            order_entitlements = []
            with EntitlementService(db) as ent_service:
                entitlements = ent_service.get_customer_entitlements(
                    customer_id=payment.customer_id
                )
                
                # Filter entitlements created from this order
                for ent in entitlements:
                    if hasattr(ent, 'order_item') and ent.order_item and ent.order_item.order_id == payment.order_id:
                        order_entitlements.append(ent.id)
            
            return {
                "payment_id": payment.id,
                "entitlements": order_entitlements
            }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to verify payment")


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id: str,
    db: Session = Depends(get_db_session)
):
    """Get payment by ID"""
    with PaymentService(db) as payment_service:
        payment = payment_service.get_payment_by_id(payment_id)
        
        if not payment:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        return PaymentResponse.from_orm(payment)


@router.get("/customer/{customer_id}")
async def get_customer_payments(
    customer_id: str,
    limit: int = 10,
    db: Session = Depends(get_db_session)
):
    """Get payments for a customer"""
    with PaymentService(db) as payment_service:
        payments = payment_service.get_payments_by_customer(
            customer_id=customer_id,
            limit=limit
        )
        
        return [PaymentResponse.from_orm(payment) for payment in payments]


@router.post("/{payment_id}/mark-failed")
async def mark_payment_failed(
    payment_id: str,
    reason: str = None,
    db: Session = Depends(get_db_session)
):
    """Mark a payment as failed"""
    with PaymentService(db) as payment_service:
        payment = payment_service.mark_payment_failed(payment_id, reason)
        
        if not payment:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        return {"payment_id": payment.id, "status": payment.status}
    