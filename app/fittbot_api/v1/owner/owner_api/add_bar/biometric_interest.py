import time
import boto3

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import BiometricModal
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PHOTO_MAX_SIZE = 5 * 1024 * 1024  # 5 MB for biometric photos

_s3 = boto3.client("s3", region_name=AWS_REGION)
router = APIRouter(prefix="/biometric_interest", tags=["Biometric Interest"])

# Valid column names for biometric photos
VALID_COLUMN_NAMES = [
    "pic_1",
    "pic_2",
    "pic_3",
    "pic_4",
    "pic_5",
    "pic_6"
]


class SaveBiometricPhotoRequest(BaseModel):
    gym_id: int
    column_name: str
    cdn_url: str


def generate_biometric_upload_url(gym_id: int, column_name: str, extension: str, content_type: str = "image/jpeg"):

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

    # Key layout: biometric_modal/<gym_id>/<column_name>.<extension>
    key = f"biometric_modal/{gym_id}/{column_name}.{extension}"
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


@router.get("/get")
async def get_biometric_interest(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        stmt = select(BiometricModal).where(BiometricModal.gym_id == gym_id)
        result = await db.execute(stmt)
        record = result.scalars().first()

        if record:
            return {
                "status": 200,
                "interest": record.interest,

            }
        else:
            return {
                "status": 200,
                "interest": False,

            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/get_pics")
async def get_biometric_pics(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        stmt = select(BiometricModal).where(BiometricModal.gym_id == gym_id)
        result = await db.execute(stmt)
        record = result.scalars().first()

        if record:
            pics = []
            for col in VALID_COLUMN_NAMES:
                url = getattr(record, col, None)
                if url:
                    pics.append({"key": col, "url": url})

            return {
                "status": 200,
                "data": {
                    "gym_id": gym_id,
                    "pics": pics
                }
            }
        else:
            return {
                "status": 200,
                "data": {
                    "gym_id": gym_id,
                    "pics": []
                }
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/upload-url")
async def get_upload_url(gym_id: int, scope: str, extension: str):

    try:
        url_data = generate_biometric_upload_url(gym_id, scope, extension)
        return {"status": 200, "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="BIOMETRIC_UPLOAD_URL_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id, "column_name": scope, "extension": extension},
        )


@router.post("/confirm")
async def save_biometric_photo(
    body: SaveBiometricPhotoRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):

    try:

        if body.column_name not in VALID_COLUMN_NAMES:
            raise FittbotHTTPException(
                status_code=400,
                detail=f"Invalid column_name. Must be one of: {VALID_COLUMN_NAMES}",
                error_code="INVALID_COLUMN_NAME",
                log_data={"column_name": body.column_name},
            )

        stmt = select(BiometricModal).where(BiometricModal.gym_id == body.gym_id)
        result = await db.execute(stmt)
        existing_record = result.scalars().first()

        if existing_record:
            setattr(existing_record, body.column_name, body.cdn_url)
            existing_record.interest = True
            await db.commit()
        else:
            new_record = BiometricModal(gym_id=body.gym_id, interest=True)
            setattr(new_record, body.column_name, body.cdn_url)
            db.add(new_record)
            await db.commit()


        redis_key = f"gym{body.gym_id}:biometric_interest"
        await redis.delete(redis_key)

        return {
            "status": 200,
            "message": "Photo saved successfully",
            "data": {
                "gym_id": body.gym_id,
                "column_name": body.column_name,
                "url": body.cdn_url,
                "interest": True
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to save photo",
            error_code="SAVE_BIOMETRIC_PHOTO_ERROR",
            log_data={"error": repr(e), "gym_id": body.gym_id},
        ) from e


@router.delete("/delete")
async def delete_biometric_photo(
    gym_id: int,
    column_name: str,
    db: AsyncSession = Depends(get_async_db),
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
        stmt = select(BiometricModal).where(BiometricModal.gym_id == gym_id)
        result = await db.execute(stmt)
        record = result.scalars().first()

        if not record:
            raise FittbotHTTPException(
                status_code=404,
                detail="No biometric record found for this gym",
                error_code="RECORD_NOT_FOUND",
                log_data={"gym_id": gym_id},
            )

        # Set the column to None
        setattr(record, column_name, None)
        await db.commit()

        # Clear cache
        redis_key = f"gym{gym_id}:biometric_interest"
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
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete photo",
            error_code="DELETE_BIOMETRIC_PHOTO_ERROR",
            log_data={"error": repr(e), "gym_id": gym_id},
        ) from e
