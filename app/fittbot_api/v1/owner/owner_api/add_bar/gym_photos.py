# app/fittbot_api/v1/owner/owner_api/add_bar/gym_photos.py

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
from app.models.fittbot_models import GymPhoto
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PHOTO_MAX_SIZE = 10 * 1024 * 1024  # 5 MB for gym photos

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

_s3 = boto3.client("s3", region_name=AWS_REGION)
router = APIRouter(prefix="/gym_photos", tags=["Gym Photos"])

# Valid area types for gym photos
VALID_AREA_TYPES = ["entrance", "cardio", "weight", "locker", "reception", "other"]


class MediaItem(BaseModel):
    type: str
    fileName: str
    contentType: str
    extension: str
    area_type: str  # New field for gym photos


class PresignedUrlRequest(BaseModel):
    gym_id: int
    media: List[MediaItem]


class PresignedUrlResponse(BaseModel):
    upload_url: dict
    cdn_url: str
    content_type: str
    photo_id: int


class ConfirmBody(BaseModel):
    cdn_url: str
    gym_id: int
    photo_id: int
    area_type: str


class RegistrationMediaItem(BaseModel):
    type: str
    fileName: str
    contentType: str
    extension: str
    area_type: str
    

class RegistrationPresignedUrlRequest(BaseModel):
    owner_contact: str  # Use owner contact as temporary identifier
    gym_index: int  # Which gym this photo belongs to (0, 1, 2...)
    gym_name: str  # Gym name for reference
    media: List[RegistrationMediaItem]


class RegistrationConfirmBody(BaseModel):
    cdn_url: str
    owner_contact: str
    gym_index: int  # Which gym this photo belongs to
    photo_id: str  # Will be UUID string for temp photos
    area_type: str


def _generate_presigned(unique_filename: str, content_type: str) -> dict:
    """
    Generate an S3 presigned POST for a gym photo asset with size and content-type constraints.
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
        ["content-length-range", 1, PHOTO_MAX_SIZE],
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
async def get_gym_photos_presigned_urls(
    body: PresignedUrlRequest,
    db: Session = Depends(get_db),
):
    """
    For each media item, create a gym photo placeholder row and return a presigned POST + CDN url.
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
            if not media_item.area_type or media_item.area_type not in VALID_AREA_TYPES:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"Invalid area_type. Must be one of: {VALID_AREA_TYPES}",
                    error_code="INVALID_AREA_TYPE",
                    log_data={"area_type": media_item.area_type},
                )

            # Check if photo already exists for this area (allow only one photo per area per gym)
            # existing_photo = (
            #     db.query(GymPhoto)
            #     .filter(
            #         GymPhoto.gym_id == body.gym_id,
            #         GymPhoto.area_type == media_item.area_type
            #     )
            #     .first()
            # )
            
            # if existing_photo:
            #     raise FittbotHTTPException(
            #         status_code=400,
            #         detail=f"Photo already exists for area type: {media_item.area_type}",
            #         error_code="AREA_PHOTO_EXISTS",
            #         log_data={
            #             "gym_id": body.gym_id,
            #             "area_type": media_item.area_type,
            #             "existing_photo_id": existing_photo.photo_id
            #         },
            #     )

            # Key layout: gym_photos/<gym_id>/<area_type>/<uuid>.<extension>
            unique_filename = f"gym_photos/{body.gym_id}/{media_item.area_type}/{uuid.uuid4()}.{media_item.extension}"

            # Generate presigned form
            presigned = _generate_presigned(unique_filename, media_item.contentType)

            # Versioned CDN URL for cache-busting
            version = int(time.time() * 1000)
            cdn_url = f"{presigned['url']}{unique_filename}?v={version}"

            # Create gym photo placeholder (empty image_url until confirm)
            gym_photo = GymPhoto(
                gym_id=body.gym_id,
                area_type=media_item.area_type,
                image_url="",
                file_name=media_item.fileName
            )
            try:
                db.add(gym_photo)
                db.commit()
                db.refresh(gym_photo)
            except Exception as e:
                db.rollback()
                raise FittbotHTTPException(
                    status_code=500,
                    detail="Database error while creating gym photo record",
                    error_code="GYM_PHOTO_DB_CREATE_ERROR",
                    log_data={"error": repr(e), "gym_id": body.gym_id},
                ) from e

            presigned_urls.append(
                PresignedUrlResponse(
                    upload_url=presigned,
                    cdn_url=cdn_url,
                    content_type=media_item.contentType,
                    photo_id=gym_photo.photo_id,
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
            error_code="GYM_PHOTO_PRESIGNED_ERROR",
            log_data={"error": repr(e), "gym_id": body.gym_id if body else None},
        ) from e


@router.post("/confirm")
async def confirm_gym_photo_upload(
    body: ConfirmBody,
    db: Session = Depends(get_db),
):
    """
    Confirm a gym photo upload by saving the final CDN URL on the gym photo record.
    """
    try:
        gym_photo = (
            db.query(GymPhoto)
            .filter(
                GymPhoto.photo_id == body.photo_id,
                GymPhoto.gym_id == body.gym_id,
                GymPhoto.area_type == body.area_type,
            )
            .first()
        )
        if not gym_photo:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym photo not found",
                error_code="GYM_PHOTO_NOT_FOUND",
                log_data={
                    "gym_id": body.gym_id,
                    "photo_id": body.photo_id,
                    "area_type": body.area_type
                },
            )

        gym_photo.image_url = body.cdn_url
        try:
            db.commit()
            db.refresh(gym_photo)
            
            # Clear cache for this gym
            redis_key = f"gym{body.gym_id}:photos"
            redis = await get_redis()
            await redis.delete(redis_key)
            
        except Exception as e:
            db.rollback()
            raise FittbotHTTPException(
                status_code=500,
                detail="Database error while confirming gym photo upload",
                error_code="GYM_PHOTO_CONFIRM_DB_ERROR",
                log_data={
                    "error": repr(e),
                    "gym_id": body.gym_id,
                    "photo_id": body.photo_id,
                },
            ) from e

        return {"status": 200, "message": "Gym photo updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while confirming gym photo upload",
            error_code="GYM_PHOTO_CONFIRM_ERROR",
            log_data={
                "error": repr(e),
                "gym_id": getattr(body, "gym_id", None),
                "photo_id": getattr(body, "photo_id", None),
            },
        ) from e


@router.delete("/delete_photo")
async def delete_gym_photo(
    photo_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete a gym photo record by id.
    """
    try:
        gym_photo = (
            db.query(GymPhoto).filter(GymPhoto.photo_id == photo_id).first()
        )
        if not gym_photo:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym photo not found",
                error_code="GYM_PHOTO_NOT_FOUND",
                log_data={"photo_id": photo_id},
            )

        gym_id = gym_photo.gym_id  # Save for cache clearing
        
        try:
            db.delete(gym_photo)
            db.commit()
            
            # Clear cache for this gym
            redis_key = f"gym{gym_id}:photos"
            redis = await get_redis()
            await redis.delete(redis_key)
            
        except Exception as e:
            db.rollback()
            raise FittbotHTTPException(
                status_code=500,
                detail="Database error while deleting gym photo",
                error_code="GYM_PHOTO_DELETE_DB_ERROR",
                log_data={"error": repr(e), "photo_id": photo_id},
            ) from e

        return {"status": 200, "message": "Gym photo deleted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while deleting gym photo",
            error_code="GYM_PHOTO_DELETE_ERROR",
            log_data={"error": repr(e), "photo_id": photo_id},
        ) from e


@router.get("/get-gym-photos")
async def get_gym_photos(
    gym_id: int, 
    db: Session = Depends(get_db), 
    redis: Redis = Depends(get_redis)
):
    """
    Get all gym photos for a specific gym, organized by area type.
    """
    try:
        redis_key = f"gym{gym_id}:photos"
        
        # Try to get from cache first
        # cached_data = await redis.get(redis_key)
        # if cached_data:
        #     return {
        #         "status": 200,
        #         "message": "Data retrieved successfully from cache",
        #         "data": json.loads(cached_data)
        #     }

        gym_photos = (
            db.query(GymPhoto)
            .filter(GymPhoto.gym_id == gym_id)
            .filter(GymPhoto.image_url != "")  # Only return confirmed photos
            .all()
        )

        if not gym_photos:
            return {
                "status": 200,
                "message": "No gym photos found for this gym",
                "data": []
            }

        photo_data = []
        for photo in gym_photos:
            photo_item = {
                "photo_id": photo.photo_id,
                "gym_id": photo.gym_id,
                "area_type": photo.area_type,
                "image_url": photo.image_url,
                "file_name": photo.file_name,
                "created_at": photo.created_at.isoformat() if photo.created_at else None,
                "updated_at": photo.updated_at.isoformat() if photo.updated_at else None
            }
            photo_data.append(photo_item)

        # Cache the result
        await redis.set(redis_key, json.dumps(photo_data), ex=86400)  # Cache for 24 hours

        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": photo_data
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.put("/update_photo")
async def update_gym_photo(
    photo_id: int,
    area_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Update gym photo details (currently supports area_type updates).
    """
    try:
        gym_photo = (
            db.query(GymPhoto).filter(GymPhoto.photo_id == photo_id).first()
        )
        if not gym_photo:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym photo not found",
                error_code="GYM_PHOTO_NOT_FOUND",
                log_data={"photo_id": photo_id},
            )

        if area_type:
            if area_type not in VALID_AREA_TYPES:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"Invalid area_type. Must be one of: {VALID_AREA_TYPES}",
                    error_code="INVALID_AREA_TYPE",
                    log_data={"area_type": area_type},
                )
            
            # Check if another photo already exists for this area type
            existing_photo = (
                db.query(GymPhoto)
                .filter(
                    GymPhoto.gym_id == gym_photo.gym_id,
                    GymPhoto.area_type == area_type,
                    GymPhoto.photo_id != photo_id
                )
                .first()
            )
            
            if existing_photo:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"Another photo already exists for area type: {area_type}",
                    error_code="AREA_PHOTO_EXISTS",
                    log_data={
                        "gym_id": gym_photo.gym_id,
                        "area_type": area_type,
                        "existing_photo_id": existing_photo.photo_id
                    },
                )
            
            gym_photo.area_type = area_type

        try:
            db.commit()
            db.refresh(gym_photo)
            
            # Clear cache for this gym
            redis_key = f"gym{gym_photo.gym_id}:photos"
            redis = await get_redis()
            await redis.delete(redis_key)
            
        except Exception as e:
            db.rollback()
            raise FittbotHTTPException(
                status_code=500,
                detail="Database error while updating gym photo",
                error_code="GYM_PHOTO_UPDATE_DB_ERROR",
                log_data={"error": repr(e), "photo_id": photo_id},
            ) from e

        return {
            "status": 200, 
            "message": "Gym photo updated successfully",
            "data": {
                "photo_id": gym_photo.photo_id,
                "area_type": gym_photo.area_type,
                "image_url": gym_photo.image_url
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while updating gym photo",
            error_code="GYM_PHOTO_UPDATE_ERROR",
            log_data={"error": repr(e), "photo_id": photo_id},
        ) from e


@router.post("/registration-presigned-urls")
async def get_registration_photos_presigned_urls(
    body: RegistrationPresignedUrlRequest,
    redis: Redis = Depends(get_redis),
):
    """
    Generate presigned URLs for photo uploads during gym owner registration.
    Stores temp photo data in Redis with owner_contact as key.
    """
    try:
        if not body.media:
            raise FittbotHTTPException(
                status_code=400,
                detail="No media provided",
                error_code="NO_MEDIA_ITEMS",
                log_data={"owner_contact": body.owner_contact},
            )

        presigned_urls = []

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
            if not media_item.area_type or media_item.area_type not in VALID_AREA_TYPES:
                raise FittbotHTTPException(
                    status_code=400,
                    detail=f"Invalid area_type. Must be one of: {VALID_AREA_TYPES}",
                    error_code="INVALID_AREA_TYPE",
                    log_data={"area_type": media_item.area_type},
                )

            # Generate unique photo ID for tracking
            photo_id = str(uuid.uuid4())
            
            # Key layout: registration_photos/<owner_contact>/gym_<gym_index>/<area_type>/<photo_id>.<extension>
            unique_filename = f"registration_photos/{body.owner_contact}/gym_{body.gym_index}/{media_item.area_type}/{photo_id}.{media_item.extension}"

            # Generate presigned form
            presigned = _generate_presigned(unique_filename, media_item.contentType)

            # Versioned CDN URL for cache-busting
            version = int(time.time() * 1000)
            cdn_url = f"{presigned['url']}{unique_filename}?v={version}"

            # Store temp photo data in Redis (expires in 24 hours)
            temp_photo_data = {
                "photo_id": photo_id,
                "owner_contact": body.owner_contact,
                "gym_index": body.gym_index,
                "gym_name": body.gym_name,
                "area_type": media_item.area_type,
                "file_name": media_item.fileName,
                "content_type": media_item.contentType,
                "cdn_url": "",  # Will be set on confirm
                "created_at": time.time()
            }
            
            redis_key = f"temp_photo:{body.owner_contact}:gym_{body.gym_index}:{photo_id}"
            await redis.set(redis_key, json.dumps(temp_photo_data), ex=86400)  # 24 hours
            print(f"Stored registration photo in Redis with key: {redis_key}")
            print(f"Photo data: {temp_photo_data}")

            presigned_urls.append({
                "upload_url": presigned,
                "cdn_url": cdn_url,
                "content_type": media_item.contentType,
                "photo_id": photo_id,
            })

        return {
            "status": 200,
            "message": "Registration presigned URLs generated successfully",
            "data": {"presigned_urls": presigned_urls},
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate registration presigned URLs",
            error_code="REGISTRATION_PHOTO_PRESIGNED_ERROR",
            log_data={"error": repr(e), "owner_contact": body.owner_contact if body else None},
        ) from e


@router.post("/registration-confirm")
async def confirm_registration_photo_upload(
    body: RegistrationConfirmBody,
    redis: Redis = Depends(get_redis),
):
    """
    Confirm a registration photo upload by updating the temp photo data in Redis.
    """
    try:
        redis_key = f"temp_photo:{body.owner_contact}:gym_{body.gym_index}:{body.photo_id}"
        print(f"Confirming registration photo with key: {redis_key}")
        
        # Get temp photo data from Redis
        temp_data = await redis.get(redis_key)
        print(f"Found temp data in Redis: {temp_data}")
        if not temp_data:
            raise FittbotHTTPException(
                status_code=404,
                detail="Registration photo not found or expired",
                error_code="REGISTRATION_PHOTO_NOT_FOUND",
                log_data={
                    "owner_contact": body.owner_contact,
                    "photo_id": body.photo_id,
                    "area_type": body.area_type
                },
            )

        photo_data = json.loads(temp_data)
        
        # Verify area_type matches
        if photo_data["area_type"] != body.area_type:
            raise FittbotHTTPException(
                status_code=400,
                detail="Area type mismatch",
                error_code="AREA_TYPE_MISMATCH",
                log_data={
                    "expected": photo_data["area_type"],
                    "received": body.area_type
                },
            )

        # Update CDN URL
        print(f"Updating CDN URL from '{photo_data.get('cdn_url', '')}' to '{body.cdn_url}'")
        photo_data["cdn_url"] = body.cdn_url
        photo_data["confirmed_at"] = time.time()
        
        # Save updated data back to Redis
        await redis.set(redis_key, json.dumps(photo_data), ex=86400)  # 24 hours
        print(f"Updated Redis data: {photo_data}")

        return {
            "status": 200, 
            "message": "Registration photo confirmed successfully",
            "data": {
                "photo_id": body.photo_id,
                "cdn_url": body.cdn_url,
                "area_type": body.area_type
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while confirming registration photo upload",
            error_code="REGISTRATION_PHOTO_CONFIRM_ERROR",
            log_data={
                "error": repr(e),
                "owner_contact": getattr(body, "owner_contact", None),
                "photo_id": getattr(body, "photo_id", None),
            },
        ) from e


@router.delete("/registration-cleanup")
async def cleanup_registration_photo(
    owner_contact: str,
    photo_id: str,
    gym_index: int,
    redis: Redis = Depends(get_redis)
):
    """
    Clean up a registration photo from Redis when user removes it from frontend.
    """
    try:
        redis_key = f"temp_photo:{owner_contact}:gym_{gym_index}:{photo_id}"
        print(f"Cleaning up registration photo with key: {redis_key}")
        
        # Check if the key exists
        temp_data = await redis.get(redis_key)
        if not temp_data:
            return {
                "status": 200,
                "message": "Photo was already removed or expired",
                "data": {"cleaned": False}
            }
        
        # Delete the Redis key
        result = await redis.delete(redis_key)
        print(f"Redis cleanup result for {redis_key}: {result}")
        
        return {
            "status": 200,
            "message": "Registration photo cleaned up successfully",
            "data": {"cleaned": result > 0}
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to cleanup registration photo",
            error_code="REGISTRATION_PHOTO_CLEANUP_ERROR",
            log_data={
                "error": repr(e), 
                "owner_contact": owner_contact,
                "photo_id": photo_id,
                "gym_index": gym_index
            },
        ) from e


class RegistrationDeleteBody(BaseModel):
    owner_contact: str
    gym_index: int
    photo_id: str


@router.delete("/redis_delete")
async def redis_delete_registration_photo(
    body: RegistrationDeleteBody,
    redis: Redis = Depends(get_redis),
):
    """
    Delete a specific registration photo from Redis using the same data structure as /registration-confirm.
    This endpoint accepts the same parameters that were used during photo confirmation.
    """
    try:
        # Construct Redis key using the same pattern as registration-confirm
        redis_key = f"temp_photo:{body.owner_contact}:gym_{body.gym_index}:{body.photo_id}"
        print(f"[redis_delete] Attempting to delete Redis key: {redis_key}")

        # Get temp photo data from Redis before deleting (to verify and return info)
        temp_data = await redis.get(redis_key)

        if not temp_data:
            print(f"[redis_delete] Key not found or already expired: {redis_key}")
            raise FittbotHTTPException(
                status_code=404,
                detail="Registration photo not found or already deleted",
                error_code="REGISTRATION_PHOTO_NOT_FOUND",
                log_data={
                    "owner_contact": body.owner_contact,
                    "gym_index": body.gym_index,
                    "photo_id": body.photo_id,
                    "redis_key": redis_key
                },
            )

        # Parse the data to verify area_type matches
        # photo_data = json.loads(temp_data)
        # print(f"[redis_delete] Found photo data: {photo_data}")

        # if photo_data.get("area_type") != body.area_type:
        #     print(f"[redis_delete] Area type mismatch - Expected: {photo_data.get('area_type')}, Got: {body.area_type}")
        #     raise FittbotHTTPException(
        #         status_code=400,
        #         detail="Area type mismatch",
        #         error_code="AREA_TYPE_MISMATCH",
        #         log_data={
        #             "expected_area_type": photo_data.get("area_type"),
        #             "received_area_type": body.area_type,
        #             "photo_id": body.photo_id
        #         },
        #     )

        delete_result = await redis.delete(redis_key)
        print(f"[redis_delete] Delete result for {redis_key}: {delete_result}")

        return {
            "status": 200,
            "message": "Registration photo deleted from Redis successfully"
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while deleting registration photo from Redis",
            error_code="REDIS_DELETE_ERROR",
            log_data={
                "error": repr(e),
                "owner_contact": getattr(body, "owner_contact", None),
                "gym_index": getattr(body, "gym_index", None),
                "photo_id": getattr(body, "photo_id", None),
            },
        ) from e


@router.get("/registration-photos/{owner_contact}")
async def get_registration_photos(
    owner_contact: str,
    gym_index: Optional[int] = None,  # Get photos for specific gym or all gyms
    redis: Redis = Depends(get_redis)
):
    """
    Get all registration photos for an owner contact, optionally filtered by gym index.
    """
    try:
        # Get all temp photos for this owner (and specific gym if specified)
        if gym_index is not None:
            pattern = f"temp_photo:{owner_contact}:gym_{gym_index}:*"
        else:
            pattern = f"temp_photo:{owner_contact}:*"
            
        keys = await redis.keys(pattern)
        
        if not keys:
            return {
                "status": 200,
                "message": "No registration photos found",
                "data": {
                    "gyms": {},
                    "total_photos": 0
                }
            }

        # Group photos by gym_index and area_type
        gyms_photos = {}
        total_photos = 0
        
        for key in keys:
            photo_data = await redis.get(key)
            if photo_data:
                photo_info = json.loads(photo_data)
                if photo_info.get("cdn_url"):  # Only confirmed photos
                    gym_idx = photo_info.get("gym_index", 0)
                    gym_name = photo_info.get("gym_name", f"Gym {gym_idx + 1}")
                    area_type = photo_info["area_type"]
                    
                    if gym_idx not in gyms_photos:
                        gyms_photos[gym_idx] = {
                            "gym_name": gym_name,
                            "photos_by_area": {},
                            "photo_count": 0
                        }
                    
                    if area_type not in gyms_photos[gym_idx]["photos_by_area"]:
                        gyms_photos[gym_idx]["photos_by_area"][area_type] = []
                    
                    gyms_photos[gym_idx]["photos_by_area"][area_type].append(photo_info["cdn_url"])
                    gyms_photos[gym_idx]["photo_count"] += 1
                    total_photos += 1

        return {
            "status": 200,
            "message": "Registration photos retrieved successfully",
            "data": {
                "gyms": gyms_photos,
                "total_photos": total_photos,
                "total_gyms": len(gyms_photos)
            }
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to retrieve registration photos",
            error_code="REGISTRATION_PHOTOS_GET_ERROR",
            log_data={"error": repr(e), "owner_contact": owner_contact},
        ) from e
    
