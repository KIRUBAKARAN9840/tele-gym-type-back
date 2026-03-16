"""
Multi-step Agreement Acceptance API.
Steps: Terms → Selfie → Signature → OTP
Records acceptance with audit trail for legal compliance.
"""
import time
import random
import boto3
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from app.utils.http_retry import http_get_with_retry
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    Gym,
    GymOwner,
    GymVerificationDocument,
    GymAgreementSteps,
    AccountDetails
)
from app.utils.logging_utils import auth_logger, FittbotHTTPException
from app.utils.otp import generate_otp, async_send_verification_sms
from app.utils.redis_config import get_redis
import os

router = APIRouter(prefix="/agreement_acceptance", tags=["Agreement Acceptance"])

CURRENT_AGREEMENT_VERSION = "1.0"

# S3 Configuration
AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)


# ============ Request/Response Models ============

class StepStatusResponse(BaseModel):
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


class AcceptTermsRequest(BaseModel):
    gym_id: int = Field(..., description="Gym ID")
    accepted_by_name: str = Field(..., min_length=2, max_length=200)


class ConfirmUploadRequest(BaseModel):
    gym_id: int = Field(..., description="Gym ID")
    cdn_url: str = Field(..., description="CDN URL of uploaded file")


class SendOTPRequest(BaseModel):
    gym_id: int = Field(..., description="Gym ID")


class VerifyOTPRequest(BaseModel):
    gym_id: int = Field(..., description="Gym ID")
    otp: str = Field(..., min_length=6, max_length=6, description="6-digit OTP")


# ============ Helper Functions ============

async def get_or_create_steps(gym_id: int, owner_id: int, db: AsyncSession) -> GymAgreementSteps:
    """Get existing steps record or create new one."""
    stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == gym_id)
    result = await db.execute(stmt)
    steps = result.scalar_one_or_none()

    if not steps:
        steps = GymAgreementSteps(
            gym_id=gym_id,
            owner_id=owner_id,
            agreement_version=CURRENT_AGREEMENT_VERSION,
            created_at=datetime.now(timezone.utc)
        )
        db.add(steps)
        await db.commit()
        await db.refresh(steps)

    return steps


def get_current_step(steps: GymAgreementSteps) -> int:
    """Determine current step based on completion status."""
    if steps.all_steps_completed:
        return 5  # All done
    if not steps.terms_accepted:
        return 1
    if not steps.selfie_url:
        return 2
    if not steps.signature_url:
        return 3
    if not steps.otp_verified:
        return 4
    return 5


def generate_presigned_url(gym_id: int, file_type: str, extension: str, content_type: str) -> Dict:
    """Generate S3 presigned POST URL."""
    timestamp = int(time.time() * 1000)

    if file_type == "selfie":
        key = f"agreement_selfies/{gym_id}/selfie_{timestamp}.{extension}"
    else:
        key = f"agreement_signatures/{gym_id}/signature_{timestamp}.{extension}"

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, MAX_FILE_SIZE],
    ]

    presigned = _s3.generate_presigned_post(
        Bucket=BUCKET_NAME,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=600,  # 10 minutes
    )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={timestamp}"

    return {
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": timestamp,
    }


# ============ API Endpoints ============

@router.get("/status/{gym_id}", response_model=StepStatusResponse)
async def get_step_status(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get current agreement step status for resume functionality."""
    try:
        stmt = select(Gym).where(Gym.gym_id == gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND"
            )

        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps:
            return StepStatusResponse(
                status=200,
                message="No agreement steps found",
                data={
                    "gym_id": gym_id,
                    "current_step": 1,
                    "all_steps_completed": False,
                    "steps": {
                        "terms_accepted": False,
                        "selfie_uploaded": False,
                        "signature_uploaded": False,
                        "otp_verified": False
                    }
                }
            )

        current_step = get_current_step(steps)

        return StepStatusResponse(
            status=200,
            message="Agreement status retrieved",
            data={
                "gym_id": gym_id,
                "current_step": current_step,
                "all_steps_completed": steps.all_steps_completed,
                "completed_at": steps.completed_at.isoformat() if steps.completed_at else None,
                "steps": {
                    "terms_accepted": steps.terms_accepted,
                    "terms_accepted_at": steps.terms_accepted_at.isoformat() if steps.terms_accepted_at else None,
                    "selfie_uploaded": bool(steps.selfie_url),
                    "selfie_url": steps.selfie_url,
                    "selfie_captured_at": steps.selfie_captured_at.isoformat() if steps.selfie_captured_at else None,
                    "signature_uploaded": bool(steps.signature_url),
                    "signature_url": steps.signature_url,
                    "signature_captured_at": steps.signature_captured_at.isoformat() if steps.signature_captured_at else None,
                    "otp_verified": steps.otp_verified,
                    "otp_verified_at": steps.otp_verified_at.isoformat() if steps.otp_verified_at else None
                }
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        auth_logger.error(f"Error getting agreement status: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to get agreement status: {str(e)}",
            error_code="AGREEMENT_STATUS_ERROR"
        )


@router.post("/step1/accept-terms", response_model=StepStatusResponse)
async def accept_terms(
    payload: AcceptTermsRequest,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 1: Accept terms and conditions."""
    try:
        stmt = select(Gym).where(Gym.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND"
            )

        steps = await get_or_create_steps(payload.gym_id, gym.owner_id, db)

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        # Update step 1
        steps.terms_accepted = True
        steps.terms_accepted_at = datetime.now(timezone.utc)
        steps.accepted_by_name = payload.accepted_by_name
        steps.accepted_ip = request.client.host if request.client else None
        steps.accepted_user_agent = request.headers.get("user-agent", "")[:500]
        steps.updated_at = datetime.now(timezone.utc)

        await db.commit()


        return StepStatusResponse(
            status=200,
            message="Terms accepted successfully",
            data={
                "gym_id": payload.gym_id,
                "current_step": 2,
                "terms_accepted_at": steps.terms_accepted_at.isoformat()
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error accepting terms: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to accept terms: {str(e)}",
            error_code="TERMS_ACCEPT_ERROR"
        )


@router.get("/step2/selfie-presigned", response_model=StepStatusResponse)
async def get_selfie_presigned(
    gym_id: int,
    extension: str = "jpg",
    db: AsyncSession = Depends(get_async_db)
):
    """Step 2: Get presigned URL for selfie upload."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps or not steps.terms_accepted:
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 1 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        url_data = generate_presigned_url(gym_id, "selfie", extension, "image/jpeg")

        return StepStatusResponse(
            status=200,
            message="Presigned URL generated",
            data=url_data
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        auth_logger.error(f"Error generating selfie presigned URL: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to generate upload URL: {str(e)}",
            error_code="PRESIGNED_URL_ERROR"
        )


@router.post("/step2/confirm-selfie", response_model=StepStatusResponse)
async def confirm_selfie(
    payload: ConfirmUploadRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 2: Confirm selfie upload."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps or not steps.terms_accepted:
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 1 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        steps.selfie_url = payload.cdn_url
        steps.selfie_captured_at = datetime.now(timezone.utc)
        steps.updated_at = datetime.now(timezone.utc)

        await db.commit()



        return StepStatusResponse(
            status=200,
            message="Selfie uploaded successfully",
            data={
                "gym_id": payload.gym_id,
                "current_step": 3,
                "selfie_url": steps.selfie_url,
                "selfie_captured_at": steps.selfie_captured_at.isoformat()
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error confirming selfie: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to confirm selfie: {str(e)}",
            error_code="SELFIE_CONFIRM_ERROR"
        )


@router.get("/step3/signature-presigned", response_model=StepStatusResponse)
async def get_signature_presigned(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 3: Get presigned URL for signature PNG upload."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps or not steps.selfie_url:
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 2 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        url_data = generate_presigned_url(gym_id, "signature", "png", "image/png")

        return StepStatusResponse(
            status=200,
            message="Presigned URL generated",
            data=url_data
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        auth_logger.error(f"Error generating signature presigned URL: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to generate upload URL: {str(e)}",
            error_code="PRESIGNED_URL_ERROR"
        )


@router.post("/step3/confirm-signature", response_model=StepStatusResponse)
async def confirm_signature(
    payload: ConfirmUploadRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 3: Confirm signature upload."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps or not steps.selfie_url:
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 2 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        steps.signature_url = payload.cdn_url
        steps.signature_captured_at = datetime.now(timezone.utc)
        steps.updated_at = datetime.now(timezone.utc)

        await db.commit()


        return StepStatusResponse(
            status=200,
            message="Signature uploaded successfully",
            data={
                "gym_id": payload.gym_id,
                "current_step": 4,
                "signature_url": steps.signature_url,
                "signature_captured_at": steps.signature_captured_at.isoformat()
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error confirming signature: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to confirm signature: {str(e)}",
            error_code="SIGNATURE_CONFIRM_ERROR"
        )


@router.post("/step4/send-otp", response_model=StepStatusResponse)
async def send_otp(
    payload: SendOTPRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 4: Send OTP to owner's registered mobile."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps:
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 3 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        # Get owner's mobile number
        stmt = select(Gym).where(Gym.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym or not gym.owner_id:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym owner not found",
                error_code="OWNER_NOT_FOUND"
            )

        stmt = select(GymOwner).where(GymOwner.owner_id == gym.owner_id)
        result = await db.execute(stmt)
        owner = result.scalar_one_or_none()

        if not owner or not owner.contact_number:
            raise FittbotHTTPException(
                status_code=400,
                detail="Owner mobile number not found",
                error_code="MOBILE_NOT_FOUND"
            )

        mobile = owner.contact_number

        # Generate and store OTP in Redis
        otp = generate_otp()
        redis = await get_redis()
        otp_key = f"agreement_otp:{payload.gym_id}"
        await redis.set(otp_key, otp, ex=300)  # 5 minutes expiry

        # Send SMS
        sms_sent = await async_send_verification_sms(mobile, otp)

        if not sms_sent:
            auth_logger.warning(f"SMS send failed for gym {payload.gym_id}")
            # Continue anyway, OTP is stored in Redis

        # Mask mobile number for response
        masked_mobile = f"******{mobile[-4:]}" if len(mobile) >= 4 else "******"



        return StepStatusResponse(
            status=200,
            message="OTP sent successfully",
            data={
                "gym_id": payload.gym_id,
                "mobile": masked_mobile,
                "expires_in": 300
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        auth_logger.error(f"Error sending OTP: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to send OTP: {str(e)}",
            error_code="OTP_SEND_ERROR"
        )


@router.post("/step4/verify-otp", response_model=StepStatusResponse)
async def verify_otp(
    payload: VerifyOTPRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """Step 4: Verify OTP and complete agreement."""
    try:
        stmt = select(GymAgreementSteps).where(GymAgreementSteps.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        steps = result.scalar_one_or_none()

        if not steps :
            raise FittbotHTTPException(
                status_code=400,
                detail="Complete Step 3 first",
                error_code="STEP_ORDER_ERROR"
            )

        if steps.all_steps_completed:
            raise FittbotHTTPException(
                status_code=400,
                detail="Agreement already completed",
                error_code="AGREEMENT_COMPLETED"
            )

        # Verify OTP from Redis
        redis = await get_redis()
        otp_key = f"agreement_otp:{payload.gym_id}"
        stored_otp = await redis.get(otp_key)

        if not stored_otp or stored_otp != payload.otp:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid or expired OTP",
                error_code="INVALID_OTP"
            )

        # Delete OTP from Redis
        await redis.delete(otp_key)

        # Get owner mobile
        stmt = select(Gym).where(Gym.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        stmt = select(GymOwner).where(GymOwner.owner_id == gym.owner_id)
        result = await db.execute(stmt)
        owner = result.scalar_one_or_none()

        # Complete all steps
        steps.otp_verified = True
        steps.otp_verified_at = datetime.now(timezone.utc)
        steps.otp_mobile = owner.contact_number if owner else None
        steps.all_steps_completed = True
        steps.completed_at = datetime.now(timezone.utc)
        steps.updated_at = datetime.now(timezone.utc)

        # Update gym_verification_documents agreement flag
        stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == payload.gym_id)
        result = await db.execute(stmt)
        verification_doc = result.scalar_one_or_none()

        if verification_doc:
            verification_doc.agreement = True
            verification_doc.updated_at = datetime.now(timezone.utc)
        else:
            new_verification_doc = GymVerificationDocument(
                gym_id=payload.gym_id,
                agreement=True,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            db.add(new_verification_doc)

        await db.commit()



        return StepStatusResponse(
            status=200,
            message="Agreement completed successfully",
            data={
                "gym_id": payload.gym_id,
                "all_steps_completed": True,
                "completed_at": steps.completed_at.isoformat()
            }
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error verifying OTP: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to verify OTP: {str(e)}",
            error_code="OTP_VERIFY_ERROR"
        )


@router.get("/gym-details", response_model=StepStatusResponse)
async def get_gym_details(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """Get gym name, GST number, and PAN number for agreement/invoice purposes."""
    try:
        # Fetch gym details
        stmt = select(Gym).where(Gym.gym_id == gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND"
            )

        # Fetch account details for GST and PAN
        stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
        result = await db.execute(stmt)
        account_details = result.scalar_one_or_none()

        response_data = {
            "gym_name": gym.name,
            "gst_number": account_details.gst_number if account_details else None,
            "pan_number": account_details.pan_number if account_details else None
        }



        return StepStatusResponse(
            status=200,
            message="Gym details retrieved successfully",
            data=response_data
        )

    except FittbotHTTPException:
        raise
    except Exception as e:
        auth_logger.error(f"Error fetching gym details: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to fetch gym details: {str(e)}",
            error_code="GYM_DETAILS_ERROR"
        )


