# app/routers/gym_profile.py

import time
from typing import Optional

import boto3
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Gym
from app.utils.logging_utils import FittbotHTTPException

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
AVATAR_PREFIX = "Profile_pics/"          # keep existing S3 prefix
AVATAR_MAX_SIZE = 10 * 1024 * 1024        # 1 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)

router = APIRouter(prefix="/gym_profile", tags=["Profile"])


def generate_gym_upload_url(
    gym_id: int,
    extension: str,
    scope: str,
    content_type: str = "image/jpeg",
) -> dict:
    """
    Create a presigned POST for uploading a gym logo/cover to S3.
    """
    if not content_type or not content_type.startswith("image/"):
        raise FittbotHTTPException(
            status_code=400,
            detail="Invalid content type; must start with image/",
            error_code="INVALID_CONTENT_TYPE",
            log_data={"content_type": content_type},
        )

    if not extension:
        raise FittbotHTTPException(
            status_code=400,
            detail="File extension is required",
            error_code="MISSING_EXTENSION",
        )

    if scope not in {"logo", "cover_pic"}:
        raise FittbotHTTPException(
            status_code=422,
            detail="Invalid scope. Use 'logo' or 'cover_pic'.",
            error_code="INVALID_SCOPE",
            log_data={"scope": scope},
        )

    key = f"{AVATAR_PREFIX}gym-{gym_id}_{scope}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, AVATAR_MAX_SIZE],
    ]

    try:
        presigned = _s3.generate_presigned_post(
            Bucket=BUCKET_NAME,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=600,  # 10 minutes
        )
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate upload URL",
            error_code="S3_PRESIGNED_URL_ERROR",
            log_data={"error": repr(e), "key": key},
        )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"

    return {
        "upload": presigned,   # form fields + upload URL
        "cdn_url": cdn_url,    # final file URL (with cache-busting param)
        "version": version,
    }


@router.get("/upload-url")
async def create_upload_url(
    gym_id: int,
    extension: str,
    scope: str,
    content_type: Optional[str] = "image/jpeg",
):
    """
    Get a presigned POST for uploading a gym logo/cover.
    """
    try:
        url_data = generate_gym_upload_url(
            gym_id=gym_id,
            extension=extension,
            scope=scope,
            content_type=content_type or "image/jpeg",
        )
        return {"status": 200, "message": "Upload URL generated", "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error while creating upload URL",
            error_code="UPLOAD_URL_CREATION_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id, "scope": scope},
        )


class ConfirmBody(BaseModel):
    cdn_url: str
    gym_id: int
    scope: str


@router.post("/confirm")
async def confirm_avatar(
    body: ConfirmBody,
    db: Session = Depends(get_db),
):
    """
    Confirm the uploaded image and persist the CDN URL on the gym record.
    """
    try:
        if body.scope not in {"logo", "cover_pic"}:
            raise FittbotHTTPException(
                status_code=422,
                detail="Invalid scope. Use 'logo' or 'cover_pic'.",
                error_code="INVALID_SCOPE",
                log_data={"scope": body.scope},
            )

        gym = db.query(Gym).filter(Gym.gym_id == body.gym_id).first()
        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": body.gym_id},
            )

        if body.scope == "logo":
            gym.logo = body.cdn_url
        else:  # cover_pic
            gym.cover_pic = body.cdn_url

        db.commit()
        db.refresh(gym)

        return {
            "status": 200,
            "message": "Gym image updated successfully",
            "data": {
                "gym_id": gym.gym_id,
                "scope": body.scope,
                "cdn_url": body.cdn_url,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while updating the gym image",
            error_code="GYM_IMAGE_UPDATE_ERROR",
            log_data={"error": repr(e), "gym_id": body.gym_id, "scope": body.scope},
        )
