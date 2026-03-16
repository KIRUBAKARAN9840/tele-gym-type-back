# app/tasks/pdf_tasks.py
"""
Celery tasks for asynchronous PDF agreement generation.
Handles prefilling PDF templates, uploading to S3, and updating agreement status.
"""
import json
import logging
from datetime import datetime, timezone

from celery import current_task
from sqlalchemy import select

from app.celery_app import celery_app
from app.config.settings import settings
from app.models.database import get_db_sync
from app.models.fittbot_models import GymAgreement
from app.utils.pdf_fill import generate_prefilled_pdf, sha256_bytes, get_s3_key_for_agreement
from app.utils.s3_pdf_utils import s3_upload_bytes
from app.utils.redis_config import get_redis_sync

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def publish_progress(task_id: str, data: dict):
    """Publish progress to Redis pub/sub for SSE streaming."""
    try:
        redis_client = get_redis_sync()
        redis_client.publish(
            f"pdf_task:{task_id}",
            json.dumps(data)
        )
        logger.debug(f"Task {task_id}: Published progress - {data.get('status')} ({data.get('progress', 0)}%)")
    except Exception as e:
        logger.error(f"Task {task_id}: Failed to publish progress - {e}")


def _update_agreement_status(
    agreement_id: str,
    status: str,
    s3_key: str = None,
    pdf_hash: str = None,
    error_message: str = None
):
    """Update agreement status in database synchronously (gevent-compatible)."""
    db = next(get_db_sync())
    try:
        stmt = select(GymAgreement).where(GymAgreement.agreement_id == agreement_id)
        result = db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            logger.error(f"Agreement {agreement_id} not found in database")
            return False

        agreement.status = status
        agreement.updated_at = datetime.now()

        if s3_key:
            agreement.s3_key_final = s3_key

        if pdf_hash:
            agreement.pdf_sha256 = pdf_hash

        if error_message:
            agreement.error_message = error_message[:2000]  # Truncate if too long

        if status == "READY":
            agreement.ready_at = datetime.now(timezone.utc)

        db.commit()
        logger.info(f"Agreement {agreement_id} status updated to {status}")
        return True

    except Exception as e:
        db.rollback()
        logger.error(f"Failed to update agreement {agreement_id}: {e}")
        raise
    finally:
        db.close()


def _get_agreement_prefill(agreement_id: str) -> dict:
    """Get prefill data from agreement record (gevent-compatible)."""
    db = next(get_db_sync())
    try:
        stmt = select(GymAgreement).where(GymAgreement.agreement_id == agreement_id)
        result = db.execute(stmt)
        agreement = result.scalar_one_or_none()

        if not agreement:
            return None

        return {
            "gym_id": agreement.gym_id,
            "prefill_json": agreement.prefill_json or {},
            "template_version": agreement.template_version
        }

    except Exception as e:
        logger.error(f"Failed to get agreement {agreement_id}: {e}")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name="app.tasks.pdf_tasks.generate_agreement_pdf_task", max_retries=3)
def generate_agreement_pdf_task(self, agreement_id: str):
    """
    Generate a prefilled PDF agreement asynchronously.

    This task:
    1. Updates status to GENERATING
    2. Generates the prefilled PDF
    3. Uploads to S3
    4. Updates status to READY with S3 key and hash

    Args:
        agreement_id: UUID of the GymAgreement record

    Returns:
        dict with ok, s3_key, sha256 or error
    """
    task_id = self.request.id
    logger.info(f"Task {task_id}: Starting PDF generation for agreement {agreement_id}")

    try:
        # Update status to GENERATING
        publish_progress(task_id, {
            "status": "progress",
            "progress": 10,
            "message": "Starting PDF generation..."
        })

        _update_agreement_status(agreement_id, "GENERATING")

        # Get agreement data
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Loading agreement data..."
        })

        agreement_data = _get_agreement_prefill(agreement_id)

        if not agreement_data:
            raise ValueError(f"Agreement {agreement_id} not found")

        gym_id = agreement_data["gym_id"]
        prefill = agreement_data["prefill_json"]
        template_version = agreement_data["template_version"]

        # Generate the PDF
        publish_progress(task_id, {
            "status": "progress",
            "progress": 40,
            "message": "Generating prefilled PDF..."
        })

        logger.info(f"Task {task_id}: Generating PDF with prefill data for gym {gym_id}")
        pdf_bytes = generate_prefilled_pdf(prefill, version=template_version)
        pdf_hash = sha256_bytes(pdf_bytes)

        logger.info(f"Task {task_id}: PDF generated, size={len(pdf_bytes)} bytes, hash={pdf_hash[:16]}...")

        # Upload to S3
        publish_progress(task_id, {
            "status": "progress",
            "progress": 70,
            "message": "Uploading to secure storage..."
        })

        s3_key = get_s3_key_for_agreement(gym_id, agreement_id, template_version)
        s3_upload_bytes(s3_key, pdf_bytes, content_type="application/pdf")

        logger.info(f"Task {task_id}: PDF uploaded to S3 key {s3_key}")

        # Update status to READY
        publish_progress(task_id, {
            "status": "progress",
            "progress": 90,
            "message": "Finalizing..."
        })

        _update_agreement_status(
            agreement_id,
            status="READY",
            s3_key=s3_key,
            pdf_hash=pdf_hash
        )

        # Done
        publish_progress(task_id, {
            "status": "completed",
            "progress": 100,
            "message": "PDF ready for download",
            "agreement_id": agreement_id,
            "s3_key": s3_key
        })

        logger.info(f"Task {task_id}: PDF generation completed successfully for agreement {agreement_id}")

        return {
            "ok": True,
            "agreement_id": agreement_id,
            "s3_key": s3_key,
            "sha256": pdf_hash
        }

    except Exception as e:
        logger.error(f"Task {task_id}: PDF generation failed for agreement {agreement_id}: {e}")

        # Try to retry for transient errors
        try:
            self.retry(exc=e, countdown=10)
        except Exception:
            # Max retries exceeded, mark as failed
            try:
                _update_agreement_status(
                    agreement_id,
                    status="FAILED",
                    error_message=str(e)
                )
            except Exception as update_error:
                logger.error(f"Task {task_id}: Failed to update status to FAILED: {update_error}")

            publish_progress(task_id, {
                "status": "failed",
                "progress": 0,
                "message": f"PDF generation failed: {str(e)[:200]}",
                "agreement_id": agreement_id
            })

        return {
            "ok": False,
            "agreement_id": agreement_id,
            "error": str(e)
        }
