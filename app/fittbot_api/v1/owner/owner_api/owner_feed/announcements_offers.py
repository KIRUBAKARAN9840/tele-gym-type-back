# app/routers/gym_feed.py

from datetime import datetime
import json
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, field_validator
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import GymAnnouncement, GymOffer
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/gym_feed", tags=["announcements"])

CACHE_TTL_SECONDS = 86_400  # 24h


# ----------------------------- Helpers ----------------------------- #

def _b2s(v: Any) -> Any:
    """Decode bytes from redis into str when needed."""
    return v.decode() if isinstance(v, (bytes, bytearray)) else v


def _ann_cache_key(gym_id: int) -> str:
    return f"gym:{gym_id}:announcements"


def _offer_cache_key(gym_id: int) -> str:
    return f"gym:{gym_id}:offers"


# -------------------------- Announcements -------------------------- #

class AddAnnouncementRequest(BaseModel):
    gym_id: int
    title: str
    description: str
    datetime: str  # ISO8601 string from client
    priority: str

    @field_validator("datetime")
    @classmethod
    def _validate_dt(cls, v: str) -> str:
        try:
            # Handle 'Z' suffix (UTC timezone) by replacing it with '+00:00'
            # Also handle milliseconds
            normalized = v.replace('Z', '+00:00')
            datetime.fromisoformat(normalized)
            return v
        except Exception:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid datetime format; must be ISO8601 (e.g., 2025-01-31T10:15:00)",
                error_code="INVALID_DATETIME",
                log_data={"value": v[:50]},
            )


@router.post("/add_announcement")
async def add_announcement(
    request: AddAnnouncementRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        ann = GymAnnouncement(
            gym_id=request.gym_id,
            title=request.title,
            description=request.description,
            priority=request.priority,
            datetime=datetime.fromisoformat(request.datetime.replace('Z', '+00:00')),
        )
        db.add(ann)
        db.commit()
        db.refresh(ann)

        # Invalidate cache
        await redis.delete(_ann_cache_key(request.gym_id))

        return {"status": 200, "message": "Announcement added successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to add announcement",
            error_code="ADD_ANNOUNCEMENT_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(e)},
        )


@router.get("/get_announcements")
async def get_announcements(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        cache_key = _ann_cache_key(gym_id)
        raw = await redis.get(cache_key)
        if raw:
            try:
                data: List[dict] = json.loads(_b2s(raw))
                # Ensure newest first
                data.sort(key=lambda x: x.get("datetime", ""), reverse=True)
                return {"status": 200, "data": data}
            except json.JSONDecodeError:
                # fall through to rebuild cache
                pass

        anns = (
            db.query(GymAnnouncement)
            .filter(GymAnnouncement.gym_id == gym_id)
            .order_by(GymAnnouncement.datetime.desc())
            .all()
        )

        data = [
            {
                "id": a.id,
                "gym_id": a.gym_id,
                "title": a.title,
                "description": a.description,
                "priority": a.priority,
                "datetime": a.datetime.isoformat() if a.datetime else None,
            }
            for a in anns
        ]

        await redis.set(cache_key, json.dumps(data), ex=CACHE_TTL_SECONDS)
        return {"status": 200, "data": data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch announcements",
            error_code="FETCH_ANNOUNCEMENTS_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


class UpdateAnnouncementRequest(BaseModel):
    announcement_id: int
    gym_id: int
    title: str
    description: str
    datetime: str  # ISO8601
    priority: str

    @field_validator("datetime")
    @classmethod
    def _validate_dt(cls, v: str) -> str:
        try:
            # Handle 'Z' suffix (UTC timezone) by replacing it with '+00:00'
            # Also handle milliseconds
            normalized = v.replace('Z', '+00:00')
            datetime.fromisoformat(normalized)
            return v
        except Exception:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid datetime format; must be ISO8601",
                error_code="INVALID_DATETIME",
                log_data={"value": v[:50]},
            )


@router.post("/update_announcement")
async def update_announcement(
    request: UpdateAnnouncementRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        ann = (
            db.query(GymAnnouncement)
            .filter(GymAnnouncement.id == request.announcement_id)
            .first()
        )
        if not ann:
            raise FittbotHTTPException(
                status_code=404,
                detail="Announcement not found",
                error_code="ANNOUNCEMENT_NOT_FOUND",
                log_data={"announcement_id": request.announcement_id},
            )

        # Optional safety: prevent cross-gym edits
        if ann.gym_id != request.gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Announcement does not belong to this gym",
                error_code="GYM_OWNERSHIP_MISMATCH",
                log_data={"announcement_id": request.announcement_id, "gym_id": request.gym_id},
            )

        ann.title = request.title
        ann.description = request.description
        ann.datetime = datetime.fromisoformat(request.datetime.replace('Z', '+00:00'))
        ann.priority = request.priority

        db.commit()
        db.refresh(ann)

        await redis.delete(_ann_cache_key(request.gym_id))

        return {"status": 200, "message": "Announcement updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update announcement",
            error_code="UPDATE_ANNOUNCEMENT_ERROR",
            log_data={"announcement_id": request.announcement_id, "gym_id": request.gym_id, "error": repr(e)},
        )


@router.delete("/delete_announcement")
async def delete_announcement(
    gym_id: int,
    announcement_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        ann = (
            db.query(GymAnnouncement)
            .filter(GymAnnouncement.id == announcement_id)
            .first()
        )
        if not ann:
            raise FittbotHTTPException(
                status_code=404,
                detail="Announcement not found",
                error_code="ANNOUNCEMENT_NOT_FOUND",
                log_data={"announcement_id": announcement_id},
            )

        if ann.gym_id != gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Announcement does not belong to this gym",
                error_code="GYM_OWNERSHIP_MISMATCH",
                log_data={"announcement_id": announcement_id, "gym_id": gym_id},
            )

        db.delete(ann)
        db.commit()

        await redis.delete(_ann_cache_key(gym_id))

        return {"status": 200, "message": "Announcement deleted successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete announcement",
            error_code="DELETE_ANNOUNCEMENT_ERROR",
            log_data={"announcement_id": announcement_id, "gym_id": gym_id, "error": repr(e)},
        )


# ------------------------------ Offers ------------------------------ #

class AddOfferRequest(BaseModel):
    gym_id: int
    title: str
    subdescription: str
    description: str
    validity: str       # ISO date/time
    discount: float
    category: str
    tag: str
    code: str

    @field_validator("validity")
    @classmethod
    def _validate_validity(cls, v: str) -> str:
        try:
            # Handle 'Z' suffix (UTC timezone) by replacing it with '+00:00'
            # Also handle milliseconds
            normalized = v.replace('Z', '+00:00')
            datetime.fromisoformat(normalized)
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
        off = GymOffer(
            gym_id=request.gym_id,
            title=request.title,
            subdescription=request.subdescription,
            description=request.description,
            validity=datetime.fromisoformat(request.validity.replace('Z', '+00:00')),
            discount=request.discount,
            category=request.category,
            tag=request.tag,
            code=request.code,
        )
        db.add(off)
        db.commit()
        db.refresh(off)

        await redis.delete(_offer_cache_key(request.gym_id))

        return {"status": 200, "message": "Offer added successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to add offer",
            error_code="ADD_OFFER_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(e)},
        )


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
                data.sort(key=lambda x: x.get("validity", ""), reverse=True)
                return {"status": 200, "data": data}
            except json.JSONDecodeError:
                pass  # rebuild

        offs = (
            db.query(GymOffer)
            .filter(GymOffer.gym_id == gym_id)
            .order_by(GymOffer.validity.desc())
            .all()
        )

        data = [
            {
                "id": o.id,
                "gym_id": o.gym_id,
                "title": o.title,
                "subdescription": o.subdescription,
                "description": o.description,
                "validity": o.validity.isoformat() if o.validity else None,
                "discount": float(o.discount) if o.discount is not None else 0.0,
                "category": o.category,
                "tag": o.tag,
                "code": o.code,
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


class UpdateOfferRequest(BaseModel):
    offer_id: int
    gym_id: int
    title: Optional[str] = None
    subdescription: Optional[str] = None
    description: Optional[str] = None
    validity: Optional[str] = None  # ISO8601
    discount: Optional[float] = None
    category: Optional[str] = None
    tag: Optional[str] = None
    code: Optional[str] = None

    @field_validator("validity")
    @classmethod
    def _validate_validity(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        try:
            # Handle 'Z' suffix (UTC timezone) by replacing it with '+00:00'
            # Also handle milliseconds
            normalized = v.replace('Z', '+00:00')
            datetime.fromisoformat(normalized)
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
        off = db.query(GymOffer).filter(GymOffer.id == request.offer_id).first()
        if not off:
            raise FittbotHTTPException(
                status_code=404,
                detail="Offer not found",
                error_code="OFFER_NOT_FOUND",
                log_data={"offer_id": request.offer_id},
            )

        if off.gym_id != request.gym_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Offer does not belong to this gym",
                error_code="GYM_OWNERSHIP_MISMATCH",
                log_data={"offer_id": request.offer_id, "gym_id": request.gym_id},
            )

        updates = request.model_dump(exclude_unset=True)
        for field, val in updates.items():
            if field in ("offer_id", "gym_id"):
                continue
            if field == "validity" and val is not None:
                off.validity = datetime.fromisoformat(val.replace('Z', '+00:00'))
            elif val is not None:
                setattr(off, field, val)

        db.commit()
        db.refresh(off)

        await redis.delete(_offer_cache_key(request.gym_id))

        return {"status": 200, "message": "Offer updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update offer",
            error_code="UPDATE_OFFER_ERROR",
            log_data={"offer_id": request.offer_id, "gym_id": request.gym_id, "error": repr(e)},
        )


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
