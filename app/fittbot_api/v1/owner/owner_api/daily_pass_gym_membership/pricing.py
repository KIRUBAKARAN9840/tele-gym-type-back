# app.py - Daily Pass Pricing API
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional
import asyncio
from app.models.dailypass_models import (
    DailyPassPricing,
    get_dailypass_session,
    new_id,
)
from app.models.fittbot_models import Gym, NewOffer
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.utils.redis_config import get_redis

# Dailypass cache keys (must match gym_studios.py)
DAILYPASS_HASH_KEY = "hash:dailypass:pricing"
DAILYPASS_LOW_SET_KEY = "set:dailypass:low49"
DAILYPASS_ENABLED_SET_KEY = "set:dailypass:enabled"
DAILYPASS_REFRESH_KEY = "dailypass:last_refresh"


async def _invalidate_dailypass_cache():
    """Clear all dailypass cache keys to force re-hydration on next client request."""
    try:
        redis = await get_redis()
        await redis.delete(
            DAILYPASS_HASH_KEY,
            DAILYPASS_LOW_SET_KEY,
            DAILYPASS_ENABLED_SET_KEY,
            DAILYPASS_REFRESH_KEY
        )
    except Exception:
        # Log but don't fail - cache will eventually expire via TTL
        pass


def invalidate_dailypass_cache():
    """Sync wrapper to invalidate dailypass cache."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_invalidate_dailypass_cache())
        else:
            loop.run_until_complete(_invalidate_dailypass_cache())
    except RuntimeError:
        # No event loop, create new one
        asyncio.run(_invalidate_dailypass_cache())

router = APIRouter(prefix="/fittbot_gym_price", tags=["daily pass pricing"])


class PriceResponse(BaseModel):
    status: int = 200
    dailypass_enabled: Optional[bool] = False
    gym_id: str
    price: float  # price in rupees (for frontend display)
    price_minor: int  # price in paisa (for backend processing)
    discount_price: Optional[int] = None  # Discounted price in rupees
    discount_percentage: Optional[float] = None  # Discount percentage
    intro_offer_activated: Optional[bool] = False  # Whether ₹49 intro offer is activated


class UpdatePriceRequest(BaseModel):
    gym_id: str 
    price: int
    dailypass_enabled: Optional[bool]
    discount_price: Optional[int] 
    discount_percentage: Optional[float] 


class UpdatePriceResponse(BaseModel):
    status: int = 200
    message: str
    gym_id: str
    price: float
    price_minor: int


@router.get("/api/daily-pass-price", response_model=PriceResponse, status_code=200)
async def get_daily_pass_price(
    gym_id: str = Query(..., description="Gym ID"),
    db: Session = Depends(get_db),
):

    dbs = None
    try:
        dbs = get_dailypass_session()

        # Check if gym has activated the ₹49 intro offer
        intro_offer = db.query(NewOffer).filter(NewOffer.gym_id == int(gym_id)).first()
        intro_offer_activated = intro_offer.dailypass if intro_offer else False

        dailypass_enabled = db.query(Gym).filter(Gym.gym_id == str(gym_id)).first()
        if dailypass_enabled.dailypass:
           

            stmt = select(DailyPassPricing).where(DailyPassPricing.gym_id == str(gym_id))
            pricing = dbs.execute(stmt).scalar_one_or_none()

            if not pricing:
                # Return default price if not set
                return PriceResponse(
                    status=200,
                    dailypass_enabled = True,
                    gym_id=str(gym_id),
                    price=0.0,
                    price_minor=0,
                    discount_price=0,
                    discount_percentage=0,
                    intro_offer_activated=intro_offer_activated
                )

            # Convert minor units (paisa) to rupees
            price_rupees = (pricing.price or 0) / 100.0

            # Handle discount_percentage = 0 case: discount_price should equal price
            if pricing.discount_percentage == 0 or pricing.discount_percentage is None:
                discount_price_rupees = price_rupees
                discount_percentage_value = 0
            else:
                discount_price_rupees = pricing.discount_price * 0.01 if pricing.discount_price else price_rupees
                discount_percentage_value = pricing.discount_percentage

            return PriceResponse(
                status=200,
                dailypass_enabled = True,
                gym_id=str(gym_id),
                price=price_rupees,
                price_minor=pricing.price,
                discount_price=discount_price_rupees,
                discount_percentage=discount_percentage_value,
                intro_offer_activated=intro_offer_activated
            )

        else:
            return PriceResponse(
                status=200,
                dailypass_enabled = False,
                gym_id=str(gym_id),
                price=0.0,
                price_minor=0,
                discount_price=0,
                discount_percentage=0,
                intro_offer_activated=intro_offer_activated
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching price: {str(e)}")
    finally:
        if dbs:
            try:
                dbs.close()
            except Exception:
                pass


@router.post("/api/daily-pass-price", response_model=UpdatePriceResponse, status_code=200)
async def update_daily_pass_price(
    request: UpdatePriceRequest,
    db: Session = Depends(get_db),
):

    dbs = None
    try:
        dbs = get_dailypass_session()

        if not request.dailypass_enabled:
            gym = db.query(Gym).filter(Gym.gym_id == str(request.gym_id)).first()
            if gym:
                gym.dailypass = False
                db.commit()
            # Invalidate dailypass cache when disabled
            await _invalidate_dailypass_cache()
            return UpdatePriceResponse(
                status=200,
                message="Daily pass feature disabled",
                gym_id=str(request.gym_id),
                price=0.0,
                price_minor=0.0,
                discount_price=0,
                discount_percentage=0
            )

        price_minor = int(request.price * 100)

        # Handle discount_percentage = 0: set discount_price = price
        if request.discount_percentage == 0 or request.discount_percentage is None:
            discount_price_minor = price_minor
            discount_percentage_value = 0
        else:
            discount_price_minor = int(request.discount_price * 100) if request.discount_price else price_minor
            discount_percentage_value = request.discount_percentage

        # Check if pricing already exists
        stmt = select(DailyPassPricing).where(DailyPassPricing.gym_id == str(request.gym_id))
        existing_pricing = dbs.execute(stmt).scalar_one_or_none()

        if existing_pricing:
            existing_pricing.price = price_minor
            existing_pricing.discount_price = discount_price_minor
            existing_pricing.discount_percentage = discount_percentage_value
            dbs.commit()
            dbs.refresh(existing_pricing)

            dailypass=db.query(Gym).filter(Gym.gym_id == str(request.gym_id)).first()
            if dailypass:
                dailypass.dailypass = True
                db.commit()

            # Invalidate dailypass cache after price update
            await _invalidate_dailypass_cache()

            return UpdatePriceResponse(
                status=200,
                message="Daily pass price updated successfully",
                gym_id=str(request.gym_id),
                price=request.price,
                price_minor=price_minor
            )
        else:
            # Create new pricing entry
            new_pricing = DailyPassPricing(
                id=new_id("dpp"),
                gym_id=str(request.gym_id),
                price=price_minor,
                discount_price=discount_price_minor,
                discount_percentage=discount_percentage_value
            )
            dbs.add(new_pricing)
            dbs.commit()
            dbs.refresh(new_pricing)
            dailypass=db.query(Gym).filter(Gym.gym_id == str(request.gym_id)).first()
            if dailypass:
                dailypass.dailypass = True
                db.commit()

            # Invalidate dailypass cache after new price set
            await _invalidate_dailypass_cache()

            return UpdatePriceResponse(
                status=200,
                message="Daily pass price set successfully",
                gym_id=str(request.gym_id),
                price=request.price,
                price_minor=price_minor
            )

    except Exception as e:
        if dbs:
            dbs.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating price: {str(e)}")
    finally:
        if dbs:
            try:
                dbs.close()
            except Exception:
                pass
