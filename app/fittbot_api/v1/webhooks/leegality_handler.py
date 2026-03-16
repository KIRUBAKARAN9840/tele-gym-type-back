"""
Leegality Webhook Handler

Handles document signing events from Leegality:
- Document signed (Completed)
- Document expired
- Document rejected

On document signed:
1. Verifies webhook MAC signature
2. Downloads signed PDF from Leegality
3. Uploads to S3
4. Updates database record
"""
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.config.settings import settings
from app.models.database import get_db
from app.models.fittbot_models import GymOnboardingEsign
from app.utils.leegality_client import get_leegality_client, LeegalityError

logger = logging.getLogger("webhooks.leegality")

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])

# Valid Leegality webhook types and document statuses
VALID_WEBHOOK_TYPES = {"Success", "Error"}
VALID_DOCUMENT_STATUSES = {"Draft", "Sent", "Completed"}


def get_s3_client():
    """Get configured S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
    )


def verify_leegality_mac(document_id: str, mac: str) -> bool:
    """
    Verify Leegality webhook MAC signature.

    MAC is computed as: HMAC-SHA1(private_salt, documentId)
    Reference: https://knowledge.leegality.com/settings/account-level/API/how-to-enable-api

    Args:
        document_id: The document ID from webhook
        mac: MAC signature from webhook payload

    Returns:
        True if MAC is valid, False otherwise
    """
    if not settings.leegality_private_salt:
        logger.warning("Private salt not configured, skipping MAC verification")
        return True

    if not mac:
        logger.warning("No MAC provided in webhook payload")
        return False

    try:
        # Compute expected MAC: HMAC-SHA1(private_salt, documentId)
        # Note: Leegality uses SHA1 and only documentId (not documentId + irn)
        message = document_id.encode("utf-8")
        expected_mac = hmac.new(
            settings.leegality_private_salt.encode("utf-8"),
            message,
            hashlib.sha1,
        ).hexdigest()

        return hmac.compare_digest(expected_mac.lower(), mac.lower())
    except Exception as exc:
        logger.error(f"MAC verification error: {exc}")
        return False


def upload_to_s3(
    pdf_content: bytes,
    gym_id: int,
    document_id: str,
    filename: str,
    max_retries: int = 3,
) -> str:
    """
    Upload signed PDF to S3 with retry logic.

    Args:
        pdf_content: PDF file content as bytes
        gym_id: Gym ID for organizing files
        document_id: Leegality document ID
        filename: Name for the uploaded file
        max_retries: Number of retry attempts (default: 3)

    Returns:
        S3 URL of the uploaded file
    """
    s3_client = get_s3_client()
    bucket = settings.esign_s3_bucket

    # Create unique key with gym_id for organization
    key = f"esign-documents/gym-{gym_id}/{document_id}/{filename}"

    last_error = None
    for attempt in range(max_retries):
        try:
            s3_client.upload_fileobj(
                BytesIO(pdf_content),
                bucket,
                key,
                ExtraArgs={
                    "ContentType": "application/pdf",
                    "ContentDisposition": f'attachment; filename="{filename}"',
                }
            )

            # Return S3 URL
            s3_url = f"https://{bucket}.s3.{settings.aws_region}.amazonaws.com/{key}"
            logger.info(f"Uploaded signed PDF to S3: {s3_url}")
            return s3_url

        except ClientError as e:
            last_error = e
            logger.warning(
                f"S3 upload attempt {attempt + 1}/{max_retries} failed: {e}",
                extra={"document_id": document_id, "attempt": attempt + 1}
            )
            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s

    logger.error(f"Failed to upload to S3 after {max_retries} attempts: {last_error}")
    raise last_error


@router.post("/leegality", status_code=200)
async def leegality_webhook(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Handle Leegality webhook events.

    Leegality expects a 2XX response to acknowledge receipt.
    Returns 200 for all cases to prevent unnecessary retries.
    """
    request_id = str(uuid.uuid4())

    try:
        # Get raw body
        body = await request.body()

        logger.info("Leegality webhook received", extra={"request_id": request_id})

        # Parse payload
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            logger.error("Invalid JSON in webhook payload")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        # Extract key fields
        webhook_type = payload.get("webhookType", "")
        document_id = payload.get("documentId", "")
        mac = payload.get("mac", "")
        irn = payload.get("irn", "")
        document_status = payload.get("documentStatus", "")
        request_data = payload.get("request", {})
        messages = payload.get("messages", [])

        # Verify MAC signature (uses HMAC-SHA1 with private_salt and documentId only)
        if not verify_leegality_mac(document_id, mac):
            logger.warning(
                "Invalid Leegality webhook MAC",
                extra={"request_id": request_id, "document_id": document_id}
            )
            raise HTTPException(status_code=401, detail="Invalid MAC signature")

        logger.info(
            "Processing Leegality webhook",
            extra={
                "request_id": request_id,
                "webhook_type": webhook_type,
                "document_status": document_status,
                "document_id": document_id,
                "irn": irn,
                "action": request_data.get("action"),
            }
        )

        # Find the esign record using OR filter for efficiency
        esign_record = db.query(GymOnboardingEsign).filter(
            or_(
                GymOnboardingEsign.irn == irn,
                GymOnboardingEsign.document_id == document_id
            )
        ).first()

        if not esign_record:
            logger.warning(
                "E-sign record not found for webhook",
                extra={"document_id": document_id, "irn": irn}
            )
            # Return 200 to prevent retries - this might be a test or duplicate
            return Response(status_code=200)

        # Update webhook tracking with timezone-aware datetime
        esign_record.webhook_received_at = datetime.now(timezone.utc)
        esign_record.webhook_event_type = f"{webhook_type}:{document_status}:{request_data.get('action', '')}"
        esign_record.webhook_payload = payload

        # Process based on webhook type and status
        if webhook_type == "Error":
            # Handle error webhook
            error_msg = request_data.get("error", "")
            if messages:
                error_msg = "; ".join([m.get("message", "") for m in messages])
            esign_record.status = "failed"
            logger.error(
                f"Document error: {error_msg}",
                extra={"document_id": document_id, "irn": irn}
            )

        elif webhook_type == "Success":
            # Check document status and action
            action = request_data.get("action", "").lower()
            expired = request_data.get("expired", False)

            if expired:
                esign_record.status = "expired"
                logger.info(f"Document expired: {document_id}")

            elif action == "rejected":
                esign_record.status = "rejected"
                rejection_msg = request_data.get("rejectionMessage", "")
                logger.info(f"Document rejected: {document_id}, reason: {rejection_msg}")

            elif action == "signed" or document_status == "Completed":
                # Document was signed - download and upload to S3
                await _handle_document_signed(esign_record, payload, request_id, db)

            elif document_status == "Sent":
                esign_record.status = "sent"
                # Update signing URL if provided
                invitation_url = request_data.get("invitationUrl")
                if invitation_url:
                    esign_record.signing_url = invitation_url
                logger.info(f"Document sent for signing: {document_id}")

            elif document_status == "Draft":
                esign_record.status = "pending"
                logger.info(f"Document in draft: {document_id}")

            else:
                logger.info(
                    "Unhandled webhook status",
                    extra={
                        "document_status": document_status,
                        "action": action,
                        "document_id": document_id,
                    }
                )

        db.commit()

        # Return simple 200 response - Leegality only cares about 2XX status
        return Response(status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error processing Leegality webhook: {e}")
        db.rollback()
        # Return 200 to prevent Leegality retries - error is logged for manual review
        return Response(status_code=200)


async def _handle_document_signed(
    esign_record: GymOnboardingEsign,
    payload: dict,
    request_id: str,
    db: Session,
):
    """
    Handle signed document - download from Leegality and upload to S3.

    Downloads both the signed document and audit trail (if available).
    """
    document_id = esign_record.document_id
    leegality_client = get_leegality_client()

    signed_pdf_url: Optional[str] = None
    audit_trail_url: Optional[str] = None

    try:
        # Download signed PDF from Leegality
        logger.info(f"Downloading signed PDF for document: {document_id}")
        pdf_content = await leegality_client.download_signed_document(
            document_id=document_id,
            request_id=request_id,
        )

        # Generate safe filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_gym_name = "".join(
            c for c in esign_record.gym_name if c.isalnum() or c in (" ", "-", "_")
        ).strip().replace(" ", "_")[:50]
        filename = f"signed_agreement_{safe_gym_name}_{timestamp}.pdf"

        # Upload signed document to S3 (sync function, no await)
        signed_pdf_url = upload_to_s3(
            pdf_content=pdf_content,
            gym_id=esign_record.gym_id,
            document_id=document_id,
            filename=filename,
        )

        logger.info(
            "Signed document uploaded to S3",
            extra={"document_id": document_id, "s3_url": signed_pdf_url}
        )

        # Try to download audit trail (may fail if not yet available)
        try:
            logger.info(f"Downloading audit trail for document: {document_id}")
            audit_trail_content = await leegality_client.download_audit_trail(
                document_id=document_id,
                request_id=request_id,
            )

            audit_filename = f"audit_trail_{safe_gym_name}_{timestamp}.pdf"
            audit_trail_url = upload_to_s3(
                pdf_content=audit_trail_content,
                gym_id=esign_record.gym_id,
                document_id=document_id,
                filename=audit_filename,
            )

            logger.info(
                "Audit trail uploaded to S3",
                extra={"document_id": document_id, "s3_url": audit_trail_url}
            )

        except LeegalityError as audit_error:
            # Audit trail download failed - not critical, continue
            logger.warning(
                f"Could not download audit trail: {audit_error}",
                extra={"document_id": document_id}
            )

        # Update record with success
        esign_record.status = "signed"
        esign_record.signed_at = datetime.now(timezone.utc)
        esign_record.signed_pdf_url = signed_pdf_url

        # Store audit trail URL if available
        if audit_trail_url:
            esign_record.audit_trail_url = audit_trail_url

        request_data = payload.get("request", {})
        sign_type = request_data.get("signType", "")

        logger.info(
            "Document signed and uploaded successfully",
            extra={
                "esign_id": esign_record.id,
                "document_id": document_id,
                "signed_pdf_url": signed_pdf_url,
                "audit_trail_url": audit_trail_url,
                "sign_type": sign_type,
            }
        )

    except LeegalityError as e:
        logger.error(
            f"Leegality API error processing signed document: {e}",
            extra={"document_id": document_id}
        )
        # Mark as signed but store error for debugging
        esign_record.status = "signed"
        esign_record.signed_at = datetime.now(timezone.utc)
        if esign_record.webhook_payload:
            esign_record.webhook_payload["_pdf_download_error"] = str(e)

    except ClientError as e:
        logger.error(
            f"S3 upload error processing signed document: {e}",
            extra={"document_id": document_id}
        )
        # Mark as signed but store error for debugging
        esign_record.status = "signed"
        esign_record.signed_at = datetime.now(timezone.utc)
        if esign_record.webhook_payload:
            esign_record.webhook_payload["_s3_upload_error"] = str(e)

    except Exception as e:
        logger.error(
            f"Unexpected error processing signed document: {e}",
            extra={"document_id": document_id}
        )
        # Still mark as signed even if PDF download/upload fails
        esign_record.status = "signed"
        esign_record.signed_at = datetime.now(timezone.utc)
        # Store error in webhook payload for debugging
        if esign_record.webhook_payload:
            esign_record.webhook_payload["_pdf_error"] = str(e)


@router.get("/leegality/health", status_code=200)
async def leegality_webhook_health():
    """Health check endpoint for Leegality webhook."""
    return {
        "status": "ok",
        "service": "leegality-webhook",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
