# app/routers/feed_router.py

import os
import json
import uuid
import boto3
from datetime import datetime
from typing import List, Optional

from botocore.config import Config
from fastapi import APIRouter, Form, Depends, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.models.fittbot_models import Post, PostMedia, FeedInterest
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/feed", tags=["feed"])

def get_ist_time() -> datetime:
    # If you maintain a TZ-aware helper elsewhere, replace this.
    return datetime.now()

AWS_S3_BUCKET         = "fittbot-uploads"
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_config = Config(
    signature_version="s3v4",
    region_name="ap-south-2",
    s3={"addressing_style": "virtual"},
)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=s3_config,
)


def generate_presigned_url(key: str, content_type: str) -> str:
    """
    Generates a presigned PUT URL for S3 uploads.
    All non-2xx errors are wrapped in FittbotHTTPException for centralized logging.
    """
    try:
        return s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": AWS_S3_BUCKET, "Key": key, "ContentType": content_type},
            ExpiresIn=900,
            HttpMethod="PUT",
        )
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate upload URL",
            error_code="S3_PRESIGNED_URL_ERROR",
            log_data={"error": repr(e), "key": key[:50] + "..." if len(key) > 50 else key},
        ) from e


@router.post("/create_presigned_url")
async def create_post_presigned(
    request: Request,
    gym_id: int = Form(...),
    client_id: Optional[int] = Form(None),
    content: Optional[str] = Form(None),
    role: str = Form(...),
    media: str = Form("[]"),
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Creates a Post row in 'uploading' status + presigned URLs for each media item.
    Returns the post_id and a list of presigned URLs to PUT the media to S3.
    """
    try:
        now = get_ist_time()
        post = Post(
            gym_id=gym_id,
            client_id=client_id,
            content=content,
            status="uploading",
            created_at=now,
            updated_at=now,
        )
        db.add(post)
        db.commit()
        db.refresh(post)

        try:
            media_metadata: List[dict] = json.loads(media or "[]")
        except json.JSONDecodeError:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid media format",
                error_code="INVALID_MEDIA_JSON",
                log_data={"gym_id": gym_id, "client_id": client_id},
            )

        presigned = []
        for meta in media_metadata:
            ct = meta.get("type", "image/jpeg")
            extension = meta.get("extension")
            if not extension:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Missing file extension for media item",
                    error_code="MISSING_FILE_EXTENSION",
                    log_data={"gym_id": gym_id, "client_id": client_id},
                )

            key = f"post_uploads/{uuid.uuid4()}.{extension}"
            url = generate_presigned_url(key, ct)

            # Keep original logic: only exact 'audio' maps to audio; everything else as 'image'
            file_type = "audio" if ct == "audio" else "image"

            db.add(
                PostMedia(
                    post_id=post.post_id,
                    file_name=key,
                    file_type=file_type,
                    file_path="",
                    status="uploading",
                    created_at=now,
                )
            )

            presigned.append(
                {
                    "file_key": key,
                    "upload_url": url,
                    "content_type": ct,
                    "file_type": file_type,
                }
            )

        db.commit()

        # Invalidate cached posts list for this gym (best-effort)
        redis_key = f"gym:{gym_id}:posts"
        try:
            if await redis.exists(redis_key):
                await redis.delete(redis_key)
        except Exception:
            # Cache invalidation failure should not break the API
            pass

        return {
            "status": 200,
            "message": "Upload your files with PUT; call /finalise when done.",
            "data": {"post_id": post.post_id, "presigned_urls": presigned},
        }

    except FittbotHTTPException:
        # Already logged by our centralized exception class
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error occurred while creating presigned URLs",
            error_code="CREATE_PRESIGNED_URL_ERROR",
            log_data={
                "error": repr(e),
                "gym_id": gym_id,
                "client_id_masked": (str(client_id)[:6] + "****") if client_id and len(str(client_id)) > 6 else "****",
            },
        ) from e


@router.get("/check_interest")
async def check_feed_interest(
    client_id: int,
    db: Session = Depends(get_db),
):

    try:
        print(f"[FEED_INTEREST] check_interest called with client_id: {client_id}")

        existing = db.query(FeedInterest).filter(
            FeedInterest.client_id == client_id
        ).first()

        print(f"[FEED_INTEREST] existing record: {existing}")

        if not existing:
            # No row found - create one with feed_interest=0 and show modal
            print(f"[FEED_INTEREST] No record found, creating new entry for client_id: {client_id}")
            new_record = FeedInterest(
                client_id=client_id,
                feed_interest=0,
                created_at=get_ist_time(),
                updated_at=get_ist_time(),
            )
            db.add(new_record)
            db.commit()
            print(f"[FEED_INTEREST] Created new record, returning show_modal: True")
            return {
                "status": 200,
                "message": "No record found, created new entry",
                "data": {"show_modal": True}
            }

        # Row exists - check feed_interest value
        print(f"[FEED_INTEREST] Record exists with feed_interest: {existing.feed_interest}")
        if existing.feed_interest == 1:
            print(f"[FEED_INTEREST] feed_interest is 1, returning show_modal: False")
            return {
                "status": 200,
                "message": "Client has already dismissed the modal",
                "data": {"show_modal": False}
            }

        # feed_interest is 0 - show modal
        print(f"[FEED_INTEREST] feed_interest is 0, returning show_modal: True")
        return {
            "status": 200,
            "message": "Modal should be shown",
            "data": {"show_modal": True}
        }

    except Exception as e:
        print(f"[FEED_INTEREST] ERROR: {repr(e)}")
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error checking feed interest",
            error_code="CHECK_FEED_INTEREST_ERROR",
            log_data={"error": repr(e), "client_id": client_id},
        ) from e


class SetFeedInterestRequest(BaseModel):
    client_id: int


@router.post("/set_interest")
async def set_feed_interest(
    request: SetFeedInterestRequest,
    db: Session = Depends(get_db),
):
    """
    Set feed_interest to 1 for this client (user dismissed the modal).
    This prevents the modal from showing again.
    """
    try:
        client_id = request.client_id
        existing = db.query(FeedInterest).filter(
            FeedInterest.client_id == client_id
        ).first()

        if not existing:
            # Create new record with feed_interest=1
            new_record = FeedInterest(
                client_id=client_id,
                feed_interest=1,
                created_at=get_ist_time(),
                updated_at=get_ist_time(),
            )
            db.add(new_record)
        else:
            # Update existing record
            existing.feed_interest = 1
            existing.updated_at = get_ist_time()

        db.commit()

        return {
            "status": 200,
            "message": "Feed interest updated successfully",
            "data": {"feed_interest": 1}
        }

    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Error setting feed interest",
            error_code="SET_FEED_INTEREST_ERROR",
            log_data={"error": repr(e), "client_id": client_id},
        ) from e
