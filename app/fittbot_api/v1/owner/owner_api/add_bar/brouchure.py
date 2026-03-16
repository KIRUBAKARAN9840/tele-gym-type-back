# app/routers/gym_brochures.py

import os
import time
import uuid
import boto3
import json
from fastapi import Form, UploadFile, File
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Brochures
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
AVATAR_MAX_SIZE = 10 * 1024 * 1024  # 1 MB

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

_s3 = boto3.client("s3", region_name=AWS_REGION)
router = APIRouter(prefix="/gym_brochures", tags=["Profile"])


class MediaItem(BaseModel):
    type: str
    fileName: str
    contentType: str
    extension: str


class PresignedUrlRequest(BaseModel):
    gym_id: int
    brochure_id: Optional[int] = None
    media: List[MediaItem]


class PresignedUrlResponse(BaseModel):
    upload_url: dict
    cdn_url: str
    content_type: str
    brochure_id: int


class ConfirmBody(BaseModel):
    cdn_url: str
    gym_id: int
    brouchure_id: int


def _generate_presigned(unique_filename: str, content_type: str) -> dict:
    """
    Generate an S3 presigned POST for a brochure asset with size and content-type constraints.
    """
    if not content_type or not content_type.startswith("image/"):
        raise FittbotHTTPException(
            status_code=400,
            detail="Invalid content type; must start with image/",
            error_code="INVALID_CONTENT_TYPE",
            log_data={"content_type": content_type},
        )

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, AVATAR_MAX_SIZE],
    ]

    try:
        presigned = _s3.generate_presigned_post(
            Bucket=BUCKET_NAME,
            Key=unique_filename,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=600,
        )
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate S3 presigned POST",
            error_code="S3_PRESIGNED_POST_ERROR",
            log_data={"error": repr(e), "key": unique_filename},
        ) from e

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    return presigned


@router.post("/presigned-urls")
async def get_brochure_presigned_urls(
    body: PresignedUrlRequest,
    db: Session = Depends(get_db),
):
    """
    For each media item, create a brochure placeholder row and return a presigned POST + CDN url.
    """
    try:
        if not body.media:
            raise FittbotHTTPException(
                status_code=400,
                detail="No media provided",
                error_code="NO_MEDIA_ITEMS",
                log_data={"gym_id": body.gym_id},
            )

        presigned_urls: List[PresignedUrlResponse] = []

        for media_item in body.media:
            # Validate inputs
            if not media_item.extension:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="File extension is required",
                    error_code="MISSING_FILE_EXTENSION",
                    log_data={"fileName": media_item.fileName},
                )
            if not media_item.contentType:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Content type is required",
                    error_code="MISSING_CONTENT_TYPE",
                    log_data={"fileName": media_item.fileName},
                )

            # Key layout: brochures/<gym_id>/<uuid>.<extension>
            unique_filename = f"brochures/{body.gym_id}/{uuid.uuid4()}.{media_item.extension}"

            # Generate presigned form
            presigned = _generate_presigned(unique_filename, media_item.contentType)

            # Versioned CDN URL for cache-busting
            version = int(time.time() * 1000)
            cdn_url = f"{presigned['url']}{unique_filename}?v={version}"

            # Create brochure placeholder (empty pic_url until confirm)
            brochure = Brochures(gym_id=body.gym_id, pic_url="")
            try:
                db.add(brochure)
                db.commit()
                db.refresh(brochure)
            except Exception as e:
                db.rollback()
                raise FittbotHTTPException(
                    status_code=500,
                    detail="Database error while creating brochure record",
                    error_code="BROCHURE_DB_CREATE_ERROR",
                    log_data={"error": repr(e), "gym_id": body.gym_id},
                ) from e

            presigned_urls.append(
                PresignedUrlResponse(
                    upload_url=presigned,
                    cdn_url=cdn_url,
                    content_type=media_item.contentType,
                    brochure_id=brochure.brouchre_id,  # keep field name as in existing schema
                )
            )

        return {
            "status": 200,
            "message": "Presigned URLs generated successfully",
            "data": {"presigned_urls": [p.dict() for p in presigned_urls]},
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate presigned URLs",
            error_code="BROCHURE_PRESIGNED_ERROR",
            log_data={"error": repr(e), "gym_id": body.gym_id if body else None},
        ) from e


@router.post("/confirm")
async def confirm_brochure_upload(
    body: ConfirmBody,
    db: Session = Depends(get_db),
):
    """
    Confirm a brochure upload by saving the final CDN URL on the brochure record.
    """
    try:
        brochure = (
            db.query(Brochures)
            .filter(
                Brochures.brouchre_id == body.brouchure_id,
                Brochures.gym_id == body.gym_id,
            )
            .first()
        )
        if not brochure:
            raise FittbotHTTPException(
                status_code=404,
                detail="Brochure not found",
                error_code="BROCHURE_NOT_FOUND",
                log_data={"gym_id": body.gym_id, "brouchure_id": body.brouchure_id},
            )

        brochure.pic_url = body.cdn_url
        try:
            db.commit()
            db.refresh(brochure)
        except Exception as e:
            db.rollback()
            raise FittbotHTTPException(
                status_code=500,
                detail="Database error while confirming brochure upload",
                error_code="BROCHURE_CONFIRM_DB_ERROR",
                log_data={
                    "error": repr(e),
                    "gym_id": body.gym_id,
                    "brouchure_id": body.brouchure_id,
                },
            ) from e

        return {"status": 200, "message": "Brochure updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while confirming brochure upload",
            error_code="BROCHURE_CONFIRM_ERROR",
            log_data={
                "error": repr(e),
                "gym_id": getattr(body, "gym_id", None),
                "brouchure_id": getattr(body, "brouchure_id", None),
            },
        ) from e


@router.delete("/delete_brochure")
async def delete_brochure(
    brochure_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete a brochure record by id.
    """
    try:
        brochure = (
            db.query(Brochures).filter(Brochures.brouchre_id == brochure_id).first()
        )
        if not brochure:
            raise FittbotHTTPException(
                status_code=404,
                detail="Brochure not found",
                error_code="BROCHURE_NOT_FOUND",
                log_data={"brouchure_id": brochure_id},
            )

        try:
            db.delete(brochure)
            db.commit()
        except Exception as e:
            db.rollback()
            raise FittbotHTTPException(
                status_code=500,
                detail="Database error while deleting brochure",
                error_code="BROCHURE_DELETE_DB_ERROR",
                log_data={"error": repr(e), "brouchure_id": brochure_id},
            ) from e

        return {"status": 200, "message": "Brochure deleted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while deleting brochure",
            error_code="BROCHURE_DELETE_ERROR",
            log_data={"error": repr(e), "brouchure_id": brochure_id},
        ) from e


@router.get("/get-gym-brochures")
async def get_gym_brouchres(gym_id: int, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):
    try:
        redis_key = f"gym{gym_id}:brochures"
        # cached_data = await redis.get(redis_key)
 
        # if cached_data:
        #     data = json.loads(cached_data)
        #     data["images"] = [img.replace("uploads\\", "https://fittbot.com/avatars/").replace("uploads/", "https://fittbot.com/avatars/") for img in data.get("images", [])]
        #     return {
        #         "status": 200,
        #         "message": "Data retrieved successfully",
        #         "data": data
        #     }
 
        brouchres = db.query(Brochures).filter(Brochures.gym_id == gym_id).all()
 
        if not brouchres:
            return {
                "status": 200,
                "message": "There is no data found for this gym",
                "data": []
            }
 
        #updated_images = [img.replace("uploads\\", "https://fittbot.com/avatars/").replace("uploads/", "https://fittbot.com/avatars/") for img in raw_images]
       
        brouchure_data=[]
       
        for b in brouchres:        
            brouchre = {
                "brouchre_id": b.brouchre_id,
                "gym_id": b.gym_id,
                "images": b.pic_url
            }
            brouchure_data.append(brouchre)
 
 
        await redis.set(redis_key, json.dumps(brouchure_data), ex=86400)
 
        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": brouchure_data
        }
 
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
 
@router.post("/update-gym-brochures")
async def update_brochures( 
    gym_id: int = Form(...),
    brochure_id: Optional[int] = Form(None),
    media: str = Form("[]"),  
    file: Optional[List[UploadFile]] = File(None), 
    db: Session = Depends(get_db), 
    redis: Redis = Depends(get_redis)
):
    try:
        redis_key = f"gym{gym_id}:brochures"
        file_paths = json.loads(media) if media else []

        if file:
            for f in file:
                ext = os.path.splitext(f.filename)[1]
                filename = f"{uuid.uuid4()}{ext}"
                file_path = os.path.join(UPLOAD_DIR, filename)

                with open(file_path, "wb") as buffer:
                    buffer.write(await f.read())

                file_paths.append(file_path)

        if brochure_id:
            db_brochure = db.query(Brochures).filter_by(brouchre_id=brochure_id, gym_id=gym_id).first()
        else:
            db_brochure = None

        if db_brochure:
            db_brochure.images = json.dumps(file_paths)
        else:
            db_brochure = Brochures(
                gym_id=gym_id,
                images=json.dumps(file_paths)
            )
            db.add(db_brochure)

        db.commit()

        if await redis.exists(redis_key):
            await redis.delete(redis_key)

        return {
            "status": 200,
            "message": "Brochure updated successfully",
            "media": file_paths,
            "brochure_id": db_brochure.brouchre_id
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
  