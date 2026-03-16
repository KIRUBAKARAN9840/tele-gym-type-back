# owneroffer.py - New Offer Activation API
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from app.models.fittbot_models import NewOffer
from sqlalchemy.orm import Session
from app.models.database import get_db

router = APIRouter(prefix="/offerprice", tags=["offer pricing"])


class ActivateOfferRequest(BaseModel):
    gym_id: str
    mode: str  # "dailypass" or "session"


class ActivateOfferResponse(BaseModel):
    status: int = 200
    message: str
    gym_id: str
    mode: str


class OffersStatusResponse(BaseModel):
    status: int = 200
    gym_id: str
    dailypass_activated: bool = False
    session_activated: bool = False


@router.post("/api/activate-offer", response_model=ActivateOfferResponse, status_code=200)
async def activate_new_offer(
    request: ActivateOfferRequest,
    db: Session = Depends(get_db),
):
    """
    Activate the ₹49 special offer for first 50 new users.
    This sets the dailypass or session flag in the NewOffer table.
    """
    try:
        # Validate mode
        if request.mode not in ["dailypass", "session"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid mode. Must be 'dailypass' or 'session'"
            )

        # Check if entry exists for this gym
        existing_offer = db.query(NewOffer).filter(
            NewOffer.gym_id == int(request.gym_id)
        ).first()

        if existing_offer:
            # Update existing entry
            if request.mode == "dailypass":
                existing_offer.dailypass = True
            elif request.mode == "session":
                existing_offer.session = True
            db.commit()
            db.refresh(existing_offer)
        else:
            # Create new entry
            new_offer = NewOffer(
                gym_id=int(request.gym_id),
                dailypass=request.mode == "dailypass",
                session=request.mode == "session"
            )
            db.add(new_offer)
            db.commit()
            db.refresh(new_offer)

        price = "₹49" if request.mode == "dailypass" else "₹99"
        return ActivateOfferResponse(
            status=200,
            message=f"{price} {request.mode} offer activated for first 50 new users!",
            gym_id=request.gym_id,
            mode=request.mode
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error activating offer: {str(e)}"
        )


@router.get("/api/offers-status", response_model=OffersStatusResponse, status_code=200)
async def get_offers_status(
    gym_id: str = Query(..., description="Gym ID"),
    db: Session = Depends(get_db),
):

    try:
       
        offer = db.query(NewOffer).filter(
            NewOffer.gym_id == int(gym_id)
        ).first()

        if offer:
            return OffersStatusResponse(
                status=200,
                gym_id=gym_id,
                dailypass_activated=offer.dailypass or False,
                session_activated=offer.session or False
            )
        else:
            return OffersStatusResponse(
                status=200,
                gym_id=gym_id,
                dailypass_activated=False,
                session_activated=False
            )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching offer status: {str(e)}"
        )
