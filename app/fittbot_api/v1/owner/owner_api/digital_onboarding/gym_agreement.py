"""
API endpoints for gym agreement PDF generation and acceptance.
Handles async PDF generation via Celery, status polling, download URLs, and consent acceptance.
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.settings import settings
from app.models.async_database import get_async_db
from app.models.fittbot_models import GymAgreement, Gym, AccountDetails, GymOwner
from app.tasks.pdf_tasks import generate_agreement_pdf_task
from app.utils.logging_utils import auth_logger, FittbotHTTPException
from app.utils.s3_pdf_utils import s3_presign_get_url

router = APIRouter(prefix="/gym_agreement", tags=["Gym Agreement"])




class PrefillData(BaseModel):
    """Prefill data for agreement PDF generation."""
    # Page 1 - Date and basic info
    day: str = Field(..., description="Day of the agreement")
    month: str = Field(..., description="Month of the agreement")
    year: str = Field(..., description="Year of the agreement")
    gym_name: str = Field(..., description="Gym name")
    location: str = Field(..., description="Gym location")
    gstno: Optional[str] = Field(None, description="GST number")
    pan: Optional[str] = Field(None, description="PAN number")

    # Page 5 - Contact info
    registered_address: Optional[str] = Field(None, description="Registered address")
    operational_address: Optional[str] = Field(None, description="Operational address")
    authorized_person: Optional[str] = Field(None, description="Authorized person name")
    mobile_number: Optional[str] = Field(None, description="Mobile number")
    email: Optional[EmailStr] = Field(None, description="Email address")

    bank_account_holder: Optional[str] = Field(None, description="Bank account holder name")
    bank_name: Optional[str] = Field(None, description="Bank name")
    bank_account_number: Optional[str] = Field(None, description="Bank account number")
    bank_ifsc: Optional[str] = Field(None, description="Bank IFSC code")
    bank_branch: Optional[str] = Field(None, description="Bank branch")
    bank_pan: Optional[str] = Field(None, description="Bank PAN")
    company_pan_number: Optional[str] = Field(None, description="Company PAN number")


class GenerateAgreementRequest(BaseModel):
    """Request to generate a new agreement PDF."""
    gym_id: int = Field(..., description="Gym ID")
    prefill: Optional[PrefillData] = Field(None, description="Optional prefill data. If not provided, will auto-fetch from gym data.")


class GenerateAgreementResponse(BaseModel):
    """Response after queuing agreement generation."""
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


class AgreementStatusResponse(BaseModel):
    """Response for agreement status query."""
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


class AcceptAgreementRequest(BaseModel):
    """Request to accept an agreement with consent."""
    consent: bool = Field(..., description="Must be true to accept")
    typed_name: str = Field(..., min_length=2, max_length=200, description="Typed name for consent")
    selfie_s3_key: Optional[str] = Field(None, description="Optional S3 key for selfie verification")


# ============ Helper Functions ============

async def get_prefill_from_gym(gym_id: int, db: AsyncSession) -> Dict[str, Any]:


    stmt = select(Gym).where(Gym.gym_id == gym_id)
    result = await db.execute(stmt)
    gym = result.scalar_one_or_none()

    if not gym:
        raise FittbotHTTPException(
            status_code=404,
            detail="Gym not found",
            error_code="GYM_NOT_FOUND"
        )

    # Get owner data
    owner = None
    if gym.owner_id:
        stmt = select(GymOwner).where(GymOwner.owner_id == gym.owner_id)
        result = await db.execute(stmt)
        owner = result.scalar_one_or_none()

    # Get account details
    stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
    result = await db.execute(stmt)
    account = result.scalar_one_or_none()

    # Build prefill data
    now = datetime.now()
    prefill = {
        "day": str(now.day),
        "month": now.strftime("%B"),
        "year": str(now.year),
        "gym_name": gym.name or "",
        "location": gym.location or "",
        "gstno": account.gst_number if account else "",
        "pan": "",  # Not stored in current schema
    }

    # Add address if available
    address_parts = []
    if gym.door_no:
        address_parts.append(gym.door_no)
    if gym.building:
        address_parts.append(gym.building)
    if gym.street:
        address_parts.append(gym.street)
    if gym.area:
        address_parts.append(gym.area)
    if gym.city:
        address_parts.append(gym.city)
    if gym.state:
        address_parts.append(gym.state)
    if gym.pincode:
        address_parts.append(gym.pincode)

    full_address = ", ".join(address_parts)
    prefill["registered_address"] = full_address
    prefill["operational_address"] = full_address

    # Add owner info
    if owner:
        prefill["authorized_person"] = owner.name or ""
        prefill["mobile_number"] = owner.contact_number or ""
        prefill["email"] = owner.email or ""

    # Add bank details
    if account:
        prefill["bank_account_holder"] = account.account_holdername or ""
        prefill["bank_name"] = account.bank_name or ""
        prefill["bank_account_number"] = account.account_number or ""
        prefill["bank_ifsc"] = account.account_ifsccode or ""
        prefill["bank_branch"] = account.account_branch or ""

    return prefill, gym.owner_id


# ============ API Endpoints ============

@router.post("/generate", response_model=GenerateAgreementResponse)
async def generate_agreement(
    request: GenerateAgreementRequest,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Generate a new prefilled agreement PDF asynchronously.

    - Creates a GymAgreement record with PENDING status
    - Queues a Celery task to generate and upload the PDF
    - Returns the agreement_id for status polling
    """
    try:
        gym_id = request.gym_id
        owner_id = None

        # Get prefill data
        if request.prefill:
            prefill_data = request.prefill.model_dump()
            # Still get owner_id from gym
            stmt = select(Gym).where(Gym.gym_id == gym_id)
            result = await db.execute(stmt)
            gym = result.scalar_one_or_none()
            if gym:
                owner_id = gym.owner_id
        else:
            # Auto-fetch from gym data
            prefill_data, owner_id = await get_prefill_from_gym(gym_id, db)

        # Create agreement record
        agreement = GymAgreement(
            gym_id=gym_id,
            owner_id=owner_id,
            template_version=settings.pdf_template_version,
            status="PENDING",
            prefill_json=prefill_data,
        )
        db.add(agreement)
        await db.commit()
        await db.refresh(agreement)



        # Queue Celery task
        task = generate_agreement_pdf_task.delay(agreement.agreement_id)

        return GenerateAgreementResponse(
            status=200,
            message="Agreement generation queued",
            data={
                "agreement_id": agreement.agreement_id,
                "status": agreement.status,
                "task_id": task.id
            }
        )

    except FittbotHTTPException:
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error generating agreement: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to generate agreement: {str(e)}",
            error_code="AGREEMENT_GENERATION_ERROR"
        )


@router.get("/status/{agreement_id}", response_model=AgreementStatusResponse)
async def get_agreement_status(
    agreement_id: str,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get the current status of an agreement.

    Poll this endpoint until status is READY or FAILED.
    """
    try:
        stmt = select(GymAgreement).where(GymAgreement.agreement_id == agreement_id)
        result = await db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            raise FittbotHTTPException(
                status_code=404,
                detail="Agreement not found",
                error_code="AGREEMENT_NOT_FOUND"
            )

        return AgreementStatusResponse(
            status=200,
            message="Agreement status retrieved",
            data={
                "agreement_id": agreement.agreement_id,
                "gym_id": agreement.gym_id,
                "status": agreement.status,
                "template_version": agreement.template_version,
                "s3_key_final": agreement.s3_key_final,
                "pdf_sha256": agreement.pdf_sha256,
                "error_message": agreement.error_message,
                "created_at": agreement.created_at.isoformat() if agreement.created_at else None,
                "ready_at": agreement.ready_at.isoformat() if agreement.ready_at else None,
                "accepted_at": agreement.accepted_at.isoformat() if agreement.accepted_at else None,
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


@router.get("/download/{agreement_id}")
async def download_agreement(
    agreement_id: str,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get a presigned download URL for the agreement PDF.

    Only works for agreements with status READY or ACCEPTED.
    """
    try:
        stmt = select(GymAgreement).where(GymAgreement.agreement_id == agreement_id)
        result = await db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            raise FittbotHTTPException(
                status_code=404,
                detail="Agreement not found",
                error_code="AGREEMENT_NOT_FOUND"
            )

        if agreement.status not in ("READY", "ACCEPTED"):
            raise FittbotHTTPException(
                status_code=409,
                detail=f"Agreement not ready for download. Current status: {agreement.status}",
                error_code="AGREEMENT_NOT_READY"
            )

        if not agreement.s3_key_final:
            raise FittbotHTTPException(
                status_code=500,
                detail="Agreement PDF key missing",
                error_code="AGREEMENT_PDF_MISSING"
            )

        # Generate presigned URL
        download_url = s3_presign_get_url(
            agreement.s3_key_final,
            expires=settings.pdf_presign_expires_seconds
        )

        return {
            "status": 200,
            "message": "Download URL generated",
            "data": {
                "download_url": download_url,
                "expires_in": settings.pdf_presign_expires_seconds,
                "agreement_id": agreement.agreement_id,
                "pdf_sha256": agreement.pdf_sha256
            }
        }

    except FittbotHTTPException:
        raise

    except Exception as e:
        auth_logger.error(f"Error generating download URL: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to generate download URL: {str(e)}",
            error_code="DOWNLOAD_URL_ERROR"
        )


@router.post("/accept/{agreement_id}")
async def accept_agreement(
    agreement_id: str,
    payload: AcceptAgreementRequest,
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Accept an agreement with consent.

    Records the typed name, IP address, and user agent for audit purposes.
    Optionally stores a selfie S3 key for additional verification.
    """
    try:
        if not payload.consent:
            raise FittbotHTTPException(
                status_code=400,
                detail="Consent must be true to accept agreement",
                error_code="CONSENT_REQUIRED"
            )

        stmt = select(GymAgreement).where(GymAgreement.agreement_id == agreement_id)
        result = await db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            raise FittbotHTTPException(
                status_code=404,
                detail="Agreement not found",
                error_code="AGREEMENT_NOT_FOUND"
            )

        if agreement.status != "READY":
            raise FittbotHTTPException(
                status_code=409,
                detail=f"Agreement must be READY to accept. Current status: {agreement.status}",
                error_code="AGREEMENT_NOT_READY"
            )

        # Update agreement with acceptance details
        agreement.status = "ACCEPTED"
        agreement.accepted_at = datetime.now(timezone.utc)
        agreement.accepted_by_name = payload.typed_name
        agreement.selfie_s3_key = payload.selfie_s3_key

        # Capture audit information
        agreement.accepted_ip = request.client.host if request.client else None
        agreement.accepted_user_agent = request.headers.get("user-agent")

        await db.commit()



        return {
            "status": 200,
            "message": "Agreement accepted successfully",
            "data": {
                "agreement_id": agreement.agreement_id,
                "status": agreement.status,
                "accepted_at": agreement.accepted_at.isoformat(),
                "accepted_by_name": agreement.accepted_by_name
            }
        }

    except FittbotHTTPException:
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error accepting agreement: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to accept agreement: {str(e)}",
            error_code="ACCEPT_AGREEMENT_ERROR"
        )


@router.get("/gym/{gym_id}/latest", response_model=AgreementStatusResponse)
async def get_latest_agreement(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get the latest agreement for a gym.

    Returns the most recently created agreement regardless of status.
    """
    try:
        stmt = (
            select(GymAgreement)
            .where(GymAgreement.gym_id == gym_id)
            .order_by(GymAgreement.created_at.desc())
            .limit(1)
        )
        result = await db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            return AgreementStatusResponse(
                status=200,
                message="No agreement found for this gym",
                data=None
            )

        return AgreementStatusResponse(
            status=200,
            message="Latest agreement retrieved",
            data={
                "agreement_id": agreement.agreement_id,
                "gym_id": agreement.gym_id,
                "status": agreement.status,
                "template_version": agreement.template_version,
                "s3_key_final": agreement.s3_key_final,
                "pdf_sha256": agreement.pdf_sha256,
                "error_message": agreement.error_message,
                "created_at": agreement.created_at.isoformat() if agreement.created_at else None,
                "ready_at": agreement.ready_at.isoformat() if agreement.ready_at else None,
                "accepted_at": agreement.accepted_at.isoformat() if agreement.accepted_at else None,
            }
        )

    except Exception as e:
        auth_logger.error(f"Error getting latest agreement: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to get latest agreement: {str(e)}",
            error_code="LATEST_AGREEMENT_ERROR"
        )
