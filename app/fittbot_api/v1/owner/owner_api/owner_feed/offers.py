# app/routers/gym_offer_router.py

import time
import json
import boto3
from datetime import datetime
from typing import Optional, List, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.models.fittbot_models import GymOffer, Gym
from app.utils.logging_utils import FittbotHTTPException

# ────── S3 CONFIG ──────
AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PREFIX = "Offers_Pics/"
MAX_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)

CACHE_TTL_SECONDS = 86_400  # 24h


def _b2s(v: Any) -> Any:
    """Decode bytes from redis into str if needed."""
    return v.decode() if isinstance(v, (bytes, bytearray)) else v


def _offer_cache_key(gym_id: int) -> str:
    return f"gym:{gym_id}:offers"


def _gen_presigned(
    gym_id: int,
    offer_id: int,
    extension: str,
    content_type: str = "image/jpeg",
):
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

    key = f"{PREFIX}gym-{gym_id}-offer-{offer_id}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, MAX_SIZE_BYTES],
    ]

    try:
        presigned = _s3.generate_presigned_post(
            Bucket=BUCKET_NAME,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=600,  # seconds
        )
        presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
        cdn_url = f"{presigned['url']}{key}?v={version}"
        return presigned, cdn_url
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate presigned URL",
            error_code="S3_PRESIGNED_URL_ERROR",
            log_data={"gym_id": gym_id, "offer_id": offer_id, "error": repr(e)},
        )


router = APIRouter(prefix="/gym_offers", tags=["offers"])


# ---------- 1) ADD OFFER ----------
class AddOfferRequest(BaseModel):
    gym_id: int
    title: str
    description: str
    validity: str
    discount: float
    category: str
    code: str
    extension: str
    content_type: str = "image/jpeg"
    # optional fields your model may support
    subdescription: Optional[str] = None
    tag: Optional[str] = None

    @field_validator("validity")
    @classmethod
    def _valid_validity(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v)
            return v
        except Exception:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid validity format; must be ISO8601 (e.g., 2025-01-31T23:59:59)",
                error_code="INVALID_VALIDITY",
                log_data={"value": v[:50]},
            )


@router.post("/add_offer")
async def add_offer(
    request: AddOfferRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        gym = db.query(Gym).filter(Gym.gym_id == request.gym_id).first()
        if not gym:
            raise FittbotHTTPException(
                status_code=400,
                detail="There is no gym for this gym_id",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": request.gym_id},
            )

        offer = GymOffer(
            gym_id=request.gym_id,
            title=request.title,
            description=request.description,
            validity=datetime.fromisoformat(request.validity),
            discount=request.discount,
            category=request.category,
            code=request.code,
            image_url="",  # will be set after upload confirm
            # optional:
            subdescription=request.subdescription,
            tag=request.tag,
        )
        db.add(offer)
        db.commit()
        db.refresh(offer)

        presigned, cdn_url = _gen_presigned(
            gym_id=request.gym_id,
            offer_id=offer.id,
            extension=request.extension,
            content_type=request.content_type,
        )

        await redis.delete(_offer_cache_key(request.gym_id))

        return {
            "status": 200,
            "message": "Offer created – upload URL generated",
            "data": {
                "offer_id": offer.id,
                "presigned": presigned,
                "cdn_url": cdn_url,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unexpected error while creating offer",
            error_code="ADD_OFFER_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(e)},
        )


# ---------- 2) CONFIRM IMAGE ----------
class OfferConfirmBody(BaseModel):
    offer_id: int
    cdn_url: str


@router.post("/confirm_offer_image")
async def confirm_offer_image(
    body: OfferConfirmBody,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        offer = db.query(GymOffer).filter(GymOffer.id == body.offer_id).first()
        if not offer:
            raise FittbotHTTPException(
                status_code=404,
                detail="Offer not found",
                error_code="OFFER_NOT_FOUND",
                log_data={"offer_id": body.offer_id},
            )

        offer.image_url = body.cdn_url
        db.commit()
        db.refresh(offer)

        await redis.delete(_offer_cache_key(offer.gym_id))

        return {
            "status": 200,
            "message": "Offer image saved",
            "data": offer.image_url,
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unexpected error while confirming image",
            error_code="CONFIRM_OFFER_IMAGE_ERROR",
            log_data={"offer_id": body.offer_id, "error": repr(e)},
        )


# ---------- 3) UPDATE OFFER (+ optional new image) ----------
class UpdateOfferRequest(BaseModel):
    offer_id: int
    gym_id: int
    title: Optional[str] = None
    description: Optional[str] = None
    validity: Optional[str] = None
    discount: Optional[float] = None
    category: Optional[str] = None
    code: Optional[str] = None
    # optional fields supported by the model
    subdescription: Optional[str] = None
    tag: Optional[str] = None

    # image update (optional)
    extension: Optional[str] = Field(default=None)  # provide to upload a new image
    content_type: Optional[str] = "image/jpeg"

    @field_validator("validity")
    @classmethod
    def _valid_validity(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v)
            return v
        except Exception:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid validity format; must be ISO8601",
                error_code="INVALID_VALIDITY",
                log_data={"value": v[:50]},
            )


@router.post("/update_offer")
async def update_offer(
    request: UpdateOfferRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        offer = db.query(GymOffer).filter(GymOffer.id == request.offer_id).first()
        if not offer:
            raise FittbotHTTPException(
                status_code=404,
                detail="Offer not found",
                error_code="OFFER_NOT_FOUND",
                log_data={"offer_id": request.offer_id},
            )

        if offer.gym_id != request.gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Offer does not belong to this gym",
                error_code="GYM_OWNERSHIP_MISMATCH",
                log_data={"offer_id": request.offer_id, "gym_id": request.gym_id},
            )

        # ---- text / numeric field updates ----
        updates = request.model_dump(exclude_unset=True)
        for field, val in updates.items():
            if field in ("offer_id", "gym_id", "extension", "content_type"):
                continue
            if field == "validity" and val is not None:
                offer.validity = datetime.fromisoformat(val)
            elif val is not None:
                setattr(offer, field, val)

        db.commit()
        db.refresh(offer)

        # ---- optional image replacement ----
        presigned_info = None
        if request.extension:  # frontend wants to upload a new image
            presigned, cdn_url = _gen_presigned(
                gym_id=request.gym_id,
                offer_id=offer.id,
                extension=request.extension,
                content_type=request.content_type or "image/jpeg",
            )
            presigned_info = {
                "offer_id": offer.id,
                "presigned": presigned,
                "cdn_url": cdn_url,
            }

        # ---- purge cache ----
        await redis.delete(_offer_cache_key(request.gym_id))

        return {
            "status": 200,
            "message": "Offer updated" + (" – upload URL generated" if presigned_info else ""),
            "data": presigned_info,
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unexpected error while updating offer",
            error_code="UPDATE_OFFER_ERROR",
            log_data={"offer_id": request.offer_id, "gym_id": request.gym_id, "error": repr(e)},
        )


# ---------- 4) GET OFFERS ----------
@router.get("/get_offer")
async def get_offers(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        cache_key = _offer_cache_key(gym_id)
        raw = await redis.get(cache_key)
        if raw:
            try:
                data: List[dict] = json.loads(_b2s(raw))
                data.sort(key=lambda x: x.get("id", 0), reverse=True)
                return {"status": 200, "data": data}
            except json.JSONDecodeError:
                # fall through to rebuild
                pass

        offs = (
            db.query(GymOffer)
            .filter(GymOffer.gym_id == gym_id)
            .order_by(GymOffer.id.desc())
            .all()
        )

        data = [
            {
                "id": o.id,
                "gym_id": o.gym_id,
                "title": o.title,
                "subdescription": getattr(o, "subdescription", None),
                "description": o.description,
                "validity": o.validity.isoformat() if o.validity else None,
                "discount": float(o.discount) if o.discount is not None else 0.0,
                "category": o.category,
                "tag": getattr(o, "tag", None),
                "code": o.code,
                "image_url": o.image_url,
            }
            for o in offs
        ]

        await redis.set(cache_key, json.dumps(data), ex=CACHE_TTL_SECONDS)
        return {"status": 200, "data": data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch offers",
            error_code="FETCH_OFFERS_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


# ---------- 5) DELETE OFFER ----------
@router.delete("/delete_offer")
async def delete_offer(
    gym_id: int,
    offer_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        off = db.query(GymOffer).filter(GymOffer.id == offer_id).first()
        if not off:
            raise FittbotHTTPException(
                status_code=404,
                detail="Offer not found",
                error_code="OFFER_NOT_FOUND",
                log_data={"offer_id": offer_id},
            )

        if off.gym_id != gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Offer does not belong to this gym",
                error_code="GYM_OWNERSHIP_MISMATCH",
                log_data={"offer_id": offer_id, "gym_id": gym_id},
            )

        db.delete(off)
        db.commit()

        await redis.delete(_offer_cache_key(gym_id))

        return {"status": 200, "message": "Offer deleted successfully"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete offer",
            error_code="DELETE_OFFER_ERROR",
            log_data={"offer_id": offer_id, "gym_id": gym_id, "error": repr(e)},
        )
