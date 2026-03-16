"""
Simple working Razorpay system for testing
"""

import json
import logging
import hmac
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
import razorpay

# Simple database connection
from app.models.database import SessionLocal

def get_db_session():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

logger = logging.getLogger("payments.razorpay")

router = APIRouter(prefix="/razorpay_gateway", tags=["razorpay Webhooks"])



# Initialize Razorpay client
from app.config.settings import settings
razorpay_client = razorpay.Client(auth=(settings.razorpay_key_id, settings.razorpay_key_secret))

class CreateSubscriptionRequest(BaseModel):
    customer_id: str
    product_id: str
    customer_email: Optional[str] = None
    customer_name: Optional[str] = None
    customer_contact: Optional[str] = None

class VerifyPaymentRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_subscription_id: str
    razorpay_signature: str

@router.post("/subscriptions/create")
async def create_subscription(request: CreateSubscriptionRequest):
    """Create subscription for your Platinum Plan"""
    try:
        logger.info(f"Creating subscription for: {request.customer_id}")

        # Your Platinum Plan from the image
        plan_id = "plan_RGfiC75bgBctNV"  # From your screenshot

        # Create subscription in Razorpay
        subscription_data = {
            "plan_id": plan_id,
            "customer_notify": 1,
            "total_count": 12,
            "notes": {
                "customer_id": request.customer_id,
                "product_id": request.product_id
            }
        }

        subscription = razorpay_client.subscription.create(subscription_data)

        logger.info(f"✅ Created subscription: {subscription['id']}")

        return {
            "subscription_id": subscription["id"],
            "razorpay_key_id": settings.razorpay_key_id,
            "product_title": "Platinum Plan",
            "amount": 190800,  # Rs.1908 from your plan
            "currency": "INR",
            "prefill": {
                "email": request.customer_email,
                "contact": request.customer_contact,
                "name": request.customer_name
            }
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/subscriptions/verify")
async def verify_subscription(request: VerifyPaymentRequest):
    """Verify payment signature"""
    try:
        logger.info(f"Verifying payment: {request.razorpay_payment_id}")

        # Validate signature
        generated_signature = hmac.new(
            settings.razorpay_key_secret.encode('utf-8'),
            f"{request.razorpay_payment_id}|{request.razorpay_subscription_id}".encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        is_valid = hmac.compare_digest(request.razorpay_signature, generated_signature)

        if not is_valid:
            raise HTTPException(status_code=400, detail="Invalid signature")

        logger.info(f"✅ Payment verified: {request.razorpay_payment_id}")

        return {
            "verified": True,
            "subscription_id": request.razorpay_subscription_id,
            "payment_id": request.razorpay_payment_id,
            "message": "Payment verified successfully"
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/webhook")
async def razorpay_webhook(request: Request):
    """
    RAZORPAY WEBHOOK URL: http://your-domain.com/razorpay/webhook
    """
    try:
        body = await request.body()
        signature = request.headers.get("x-razorpay-signature")

        if not signature:
            raise HTTPException(status_code=400, detail="Missing signature")

        # Verify webhook signature
        expected_signature = hmac.new(
            settings.razorpay_webhook_secret.encode('utf-8'),
            body,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_signature):
            raise HTTPException(status_code=400, detail="Invalid signature")

        # Parse webhook data
        webhook_data = json.loads(body.decode('utf-8'))
        event = webhook_data.get("event")

        logger.info(f"✅ Received webhook: {event}")
        print(f"WEBHOOK RECEIVED: {event}")
        print(f"WEBHOOK DATA: {json.dumps(webhook_data, indent=2)}")

        # Process different events
        if event == "subscription.activated":
            print("🎉 SUBSCRIPTION ACTIVATED!")
            # Here you would update your database

        elif event == "subscription.charged":
            print("💰 SUBSCRIPTION CHARGED!")
            # Here you would record the payment

        elif event == "subscription.cancelled":
            print("❌ SUBSCRIPTION CANCELLED!")
            # Here you would deactivate premium

        return {"status": "accepted", "event": event}

    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "service": "razorpay_simple",
        "webhook_url": "http://your-domain.com/razorpay/webhook",
        "timestamp": datetime.now().isoformat()
    }