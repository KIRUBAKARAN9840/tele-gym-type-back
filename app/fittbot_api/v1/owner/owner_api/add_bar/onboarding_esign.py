"""
Gym Onboarding E-Sign API

Endpoints for sending gym onboarding agreements for digital signing via Leegality.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Gym, GymOwner, GymOnboardingEsign
from app.utils.leegality_client import get_leegality_client, LeegalityError
from app.utils.logging_utils import FittbotHTTPException

logger = logging.getLogger("esign")

router = APIRouter(prefix="/esign", tags=["Gym Onboarding E-Sign"])


# ---------- Request/Response Schemas ----------

class OnboardingEsignRequest(BaseModel):
    """Request payload for sending onboarding agreement."""
    gym_id: int = Field(..., description="Gym ID for the agreement")
    gym_name: str = Field(..., min_length=1, max_length=200, description="Name of the gym")
    location: str = Field(..., min_length=1, max_length=255, description="Gym location")
    gst_no: Optional[str] = Field(None, max_length=20, description="GST number")
    pan: Optional[str] = Field(None, max_length=15, description="PAN number")
    address: str = Field(..., min_length=1, description="Full address")
    authorised_name: str = Field(..., min_length=1, max_length=200, description="Name of authorised signatory")
    mobile: str = Field(..., min_length=10, max_length=15, description="Mobile number of signatory")
    email: EmailStr = Field(..., description="Email of signatory")

    @field_validator("mobile")
    @classmethod
    def validate_mobile(cls, v: str) -> str:
        # Remove any spaces or dashes
        cleaned = v.replace(" ", "").replace("-", "")
        # Remove country code if present
        if cleaned.startswith("+91"):
            cleaned = cleaned[3:]
        elif cleaned.startswith("91") and len(cleaned) > 10:
            cleaned = cleaned[2:]

        if not cleaned.isdigit() or len(cleaned) != 10:
            raise ValueError("Mobile number must be 10 digits")
        return cleaned


class OnboardingEsignResponse(BaseModel):
    """Response after initiating e-sign."""
    status: str
    message: str
    esign_id: int
    document_id: Optional[str] = None
    signing_url: Optional[str] = None
    irn: str


class EsignStatusResponse(BaseModel):
    """Response for e-sign status check."""
    esign_id: int
    status: str
    gym_name: str
    authorised_name: str
    email: str
    signing_url: Optional[str] = None
    signed_pdf_url: Optional[str] = None
    audit_trail_url: Optional[str] = None
    signed_at: Optional[datetime] = None
    created_at: datetime


# ---------- Endpoints ----------

@router.post("/send", response_model=OnboardingEsignResponse)
async def send_onboarding_esign(
    request: OnboardingEsignRequest,
    db: Session = Depends(get_db),
):

    try:
        # Validate gym exists
        gym = db.query(Gym).filter(Gym.gym_id == request.gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        # Get owner
        owner = db.query(GymOwner).filter(GymOwner.owner_id == gym.owner_id).first()
        if not owner:
            raise HTTPException(status_code=404, detail="Gym owner not found")

        # Generate unique internal reference number
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        irn = f"FITT-{request.gym_id}-{timestamp}-{uuid.uuid4().hex[:6].upper()}"
        request_id = str(uuid.uuid4())

        # Create database record first (pending status)
        esign_record = GymOnboardingEsign(
            gym_id=request.gym_id,
            owner_id=owner.owner_id,
            irn=irn,
            gym_name=request.gym_name,
            location=request.location,
            gst_no=request.gst_no or "",
            pan=request.pan or "",
            address=request.address,
            authorised_name=request.authorised_name,
            mobile=request.mobile,
            email=request.email,
            status="pending",
        )
        db.add(esign_record)
        db.flush()  # Get the ID without committing

        # Send to Leegality
        leegality_client = get_leegality_client()

        try:
            result = await leegality_client.send_gym_agreement(
                gym_name=request.gym_name,
                location=request.location,
                gst_no=request.gst_no or "",
                pan=request.pan or "",
                address=request.address,
                authorised_name=request.authorised_name,
                mobile=request.mobile,
                email=request.email,
                internal_reference=irn,
                request_id=request_id,
            )

            # Update record with Leegality response (normalized structure)
            esign_record.document_id = result.get("documentId")
            esign_record.signing_url = result.get("signUrl")
            esign_record.status = "sent"

            db.commit()


            return OnboardingEsignResponse(
                status="success",
                message="E-sign request sent successfully. Signatory will receive signing link.",
                esign_id=esign_record.id,
                document_id=esign_record.document_id,
                signing_url=esign_record.signing_url,
                irn=irn,
            )

        except LeegalityError as e:
            # Update status to failed
            esign_record.status = "failed"
            db.commit()

            logger.error(
                f"Leegality API error",
                extra={"esign_id": esign_record.id, "error": str(e)}
            )
            raise HTTPException(
                status_code=502,
                detail=f"Failed to send document for signing: {str(e)}"
            )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Error sending e-sign request: {e}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="ESIGN_SEND_ERROR"
        )


@router.get("/status/{esign_id}", response_model=EsignStatusResponse)
async def get_esign_status(
    esign_id: int,
    db: Session = Depends(get_db),
):

    try:
        esign = db.query(GymOnboardingEsign).filter(
            GymOnboardingEsign.id == esign_id
        ).first()

        if not esign:
            raise HTTPException(status_code=404, detail="E-sign record not found")

        return EsignStatusResponse(
            esign_id=esign.id,
            status=esign.status,
            gym_name=esign.gym_name,
            authorised_name=esign.authorised_name,
            email=esign.email,
            signing_url=esign.signing_url if esign.status in ["pending", "sent"] else None,
            signed_pdf_url=esign.signed_pdf_url,
            audit_trail_url=esign.audit_trail_url,
            signed_at=esign.signed_at,
            created_at=esign.created_at,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching e-sign status: {e}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="ESIGN_STATUS_ERROR"
        )


@router.get("/gym/{gym_id}")
async def get_gym_esign_history(
    gym_id: int,
    db: Session = Depends(get_db),
):
    """
    Get all e-sign documents for a gym.

    Returns list of all e-sign requests for the specified gym.
    """
    try:
        esigns = db.query(GymOnboardingEsign).filter(
            GymOnboardingEsign.gym_id == gym_id
        ).order_by(GymOnboardingEsign.created_at.desc()).all()

        return {
            "status": 200,
            "gym_id": gym_id,
            "count": len(esigns),
            "documents": [
                {
                    "esign_id": e.id,
                    "status": e.status,
                    "gym_name": e.gym_name,
                    "authorised_name": e.authorised_name,
                    "email": e.email,
                    "irn": e.irn,
                    "document_id": e.document_id,
                    "signing_url": e.signing_url if e.status in ["pending", "sent"] else None,
                    "signed_pdf_url": e.signed_pdf_url,
                    "audit_trail_url": e.audit_trail_url,
                    "signed_at": e.signed_at.isoformat() if e.signed_at else None,
                    "created_at": e.created_at.isoformat() if e.created_at else None,
                }
                for e in esigns
            ]
        }

    except Exception as e:
        logger.exception(f"Error fetching gym e-sign history: {e}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="ESIGN_HISTORY_ERROR"
        )


@router.post("/resend/{esign_id}")
async def resend_esign_notification(
    esign_id: int,
    db: Session = Depends(get_db),
):
    """
    Resend e-sign notification to the signatory.

    Can be used when the signatory didn't receive or lost the original signing link.
    """
    try:
        esign = db.query(GymOnboardingEsign).filter(
            GymOnboardingEsign.id == esign_id
        ).first()

        if not esign:
            raise HTTPException(status_code=404, detail="E-sign record not found")

        if esign.status == "signed":
            raise HTTPException(
                status_code=400,
                detail="Document already signed, cannot resend"
            )

        if esign.status not in ["sent", "pending"]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot resend document with status: {esign.status}"
            )

        # Return existing signing URL - Leegality handles resend notifications
        return {
            "status": "success",
            "message": "Signing link is still valid. Share this URL with the signatory.",
            "esign_id": esign.id,
            "signing_url": esign.signing_url,
            "email": esign.email,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error resending e-sign: {e}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="ESIGN_RESEND_ERROR"
        )
