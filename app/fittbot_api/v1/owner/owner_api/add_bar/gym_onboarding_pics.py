import time
import boto3
import json

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import GymOnboardingPics
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PHOTO_MAX_SIZE = 5 * 1024 * 1024  # 10 MB for onboarding photos

_s3 = boto3.client("s3", region_name=AWS_REGION)
router = APIRouter(prefix="/gym_onboarding_pics", tags=["Gym Onboarding Pics"])

# Valid column names for gym onboarding photos
VALID_COLUMN_NAMES = [
    "machinery_1",
    "machinery_2",
    "treadmill_area",
    "cardio_area",
    "dumbell_area",
    "reception_area"
]


class SavePhotoRequest(BaseModel):
    gym_id: int
    column_name: str
    cdn_url: str


def generate_onboarding_upload_url(gym_id: int, column_name: str, extension: str, content_type: str = "image/jpeg"):
    """
    Create a browser POST policy for direct S3 upload + return CDN URL.
    """
    if not content_type.startswith("image/"):
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
            error_code="MISSING_FILE_EXTENSION",
            log_data={"gym_id": gym_id},
        )

    if column_name not in VALID_COLUMN_NAMES:
        raise FittbotHTTPException(
            status_code=400,
            detail=f"Invalid column_name. Must be one of: {VALID_COLUMN_NAMES}",
            error_code="INVALID_COLUMN_NAME",
            log_data={"column_name": column_name},
        )

    # Key layout: gym_onboarding_pics/<gym_id>/<column_name>.<extension>
    key = f"gym_onboarding_pics/{gym_id}/{column_name}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, PHOTO_MAX_SIZE],
    ]

    try:
        presigned = _s3.generate_presigned_post(
            Bucket=BUCKET_NAME,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=600,
        )
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate presigned upload form",
            error_code="S3_PRESIGN_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id, "key": key},
        )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"

    return {
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": version,
    }


@router.get("/upload-url")
async def get_upload_url(gym_id: int, scope: str, extension: str):

    try:
        url_data = generate_onboarding_upload_url(gym_id, scope, extension)
        return {"status": 200, "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="ONBOARDING_UPLOAD_URL_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id, "column_name": scope, "extension": extension},
        )


@router.post("/confirm")
async def save_onboarding_photo(
    body: SavePhotoRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Save the photo URL to the gym_onboarding_pics table for the specified column.
    Creates a new record if one doesn't exist for the gym_id.
    """
    try:
        # Validate column_name
        if body.column_name not in VALID_COLUMN_NAMES:
            raise FittbotHTTPException(
                status_code=400,
                detail=f"Invalid column_name. Must be one of: {VALID_COLUMN_NAMES}",
                error_code="INVALID_COLUMN_NAME",
                log_data={"column_name": body.column_name},
            )

        # Check if record exists for this gym_id
        existing_record = db.query(GymOnboardingPics).filter(
            GymOnboardingPics.gym_id == body.gym_id
        ).first()

        if existing_record:
            # Update the specific column
            setattr(existing_record, body.column_name, body.cdn_url)
            db.commit()
            db.refresh(existing_record)
        else:
            # Create new record with the specified column
            new_record = GymOnboardingPics(gym_id=body.gym_id)
            setattr(new_record, body.column_name, body.cdn_url)
            db.add(new_record)
            db.commit()
            db.refresh(new_record)

        # Clear cache for this gym
        redis_key = f"gym{body.gym_id}:onboarding_pics"
        await redis.delete(redis_key)

        return {
            "status": 200,
            "message": "Photo saved successfully",
            "data": {
                "gym_id": body.gym_id,
                "column_name": body.column_name,
                "url": body.cdn_url
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save photo",
            error_code="SAVE_PHOTO_ERROR",
            log_data={"error": repr(e), "gym_id": body.gym_id},
        ) from e


SAMPLE_BASE_URL = "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/Gym_studios_sample"

# Title mapping for each column
COLUMN_TITLES = {
    "machinery_1": "Machinery Area",
    "machinery_2": "Static Bench Area",
    "treadmill_area": "Treadmill Area",
    "cardio_area": "Cardio Area",
    "dumbell_area": "Dumbell Area",
    "reception_area": "Reception Area"
}


@router.get("/get")
async def get_onboarding_photos(
    gym_id:int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):

    try:
        redis_key = f"gym{gym_id}:onboarding_pics"


        cached_data = await redis.get(redis_key)
        if cached_data:
            return {
                "status": 200,
                "message": "Data retrieved successfully from cache",
                "data": json.loads(cached_data)
            }

        # Query from database
        record = db.query(GymOnboardingPics).filter(
            GymOnboardingPics.gym_id == gym_id
        ).first()

        # Build response with all columns - use sample URLs for missing ones
        photos = []
        for idx, column_name in enumerate(VALID_COLUMN_NAMES, start=1):
            if record:
                url = getattr(record, column_name, None)
            else:
                url = None

            if url:
                # Real uploaded photo
                photos.append({
                    "id": idx,
                    "key": column_name,
                    "title": COLUMN_TITLES.get(column_name, column_name),
                    "image_url": url,
                    "is_sample": False
                })
            else:
                # Use sample image
                sample_url = f"{SAMPLE_BASE_URL}/{column_name}.webp"
                photos.append({
                    "id": idx,
                    "key": column_name,
                    "title": COLUMN_TITLES.get(column_name, column_name),
                    "image_url": sample_url,
                    "is_sample": True
                })

        response_data = {
            "gym_id": gym_id,
            "photos": photos
        }

        await redis.set(redis_key, json.dumps(response_data), ex=86400)

        return {
            "status": 200,
            "data": response_data
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.delete("/delete")
async def delete_onboarding_photo(
    gym_id: int,
    column_name: str,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):

    try:
        # Validate column_name
        if column_name not in VALID_COLUMN_NAMES:
            raise FittbotHTTPException(
                status_code=400,
                detail=f"Invalid column_name. Must be one of: {VALID_COLUMN_NAMES}",
                error_code="INVALID_COLUMN_NAME",
                log_data={"column_name": column_name},
            )

        # Find the record
        record = db.query(GymOnboardingPics).filter(
            GymOnboardingPics.gym_id == gym_id
        ).first()

        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="No onboarding photos found for this gym",
                error_code="RECORD_NOT_FOUND",
                log_data={"gym_id": gym_id},
            )

        # Set the column to None
        setattr(record, column_name, None)
        db.commit()

        # Clear cache
        redis_key = f"gym{gym_id}:onboarding_pics"
        await redis.delete(redis_key)

        return {
            "status": 200,
            "message": f"Photo deleted successfully from {column_name}",
            "data": {
                "gym_id": gym_id,
                "column_name": column_name
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete photo",
            error_code="DELETE_PHOTO_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id},
        ) from e
