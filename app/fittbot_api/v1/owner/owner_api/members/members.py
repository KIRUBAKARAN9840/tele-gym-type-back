# app/routers/owner_members.py

import json
from datetime import date
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.models.fittbot_models import AboutToExpire, FittbotGymMembership, Client, Gym, AccountDetails, GymPlans, EstimateDiscount
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from datetime import timedelta
from pydantic import BaseModel

router = APIRouter(prefix="/owner/members", tags=["Gymowner"])


def _decode_cached(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return value


def _serialize_invoice(inv: AboutToExpire) -> Dict[str, Any]:
    return {
        "expiry_id": inv.expiry_id,
        "client_id": inv.client_id,
        "gym_id": inv.gym_id,
        "client_name": inv.client_name,
        "gym_name": inv.gym_name,
        "gym_logo": inv.gym_logo,
        "gym_contact": inv.gym_contact,
        "gym_location": inv.gym_location,
        "plan_id": inv.plan_id,
        "plan_description": inv.plan_description,
        "fees": inv.fees,
        "discount": inv.discount,
        "discounted_fees": inv.discounted_fees,
        "due_date": str(inv.due_date) if getattr(inv, "due_date", None) else None,
        "invoice_number": inv.invoice_number,
        "client_contact": inv.client_contact,
        "bank_details": inv.bank_details,
        "ifsc_code": inv.ifsc_code,
        "account_holder_name": inv.account_holder_name,
        "paid": inv.paid,
        "mail_send": inv.mail_status,
        "expired": inv.expired,
        "email": inv.email,
    }


@router.get("/all_old")
async def get_members_data(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        # basic input validation
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        response: Dict[str, Any] = {}

 
        invoice_key = f"gym:{gym_id}:invoice_data"
        cached_invoice = await redis.get(invoice_key)

        if cached_invoice:
            try:
                response["invoice_data"] = json.loads(_decode_cached(cached_invoice))
            except json.JSONDecodeError:
                response["invoice_data"] = None

        if not response.get("invoice_data"):
            sent_invoices: List[AboutToExpire] = (
                db.query(AboutToExpire)
                .filter(
                    AboutToExpire.gym_id == gym_id,
                    AboutToExpire.mail_status.is_(True),
                    AboutToExpire.expired.is_(False),
                )
                .all()
            )
            unsent_invoices: List[AboutToExpire] = (
                db.query(AboutToExpire)
                .filter(
                    AboutToExpire.gym_id == gym_id,
                    AboutToExpire.mail_status.is_(False),
                    AboutToExpire.expired.is_(False),
                )
                .all()
            )

            invoice_data = {
                "send": [_serialize_invoice(inv) for inv in sent_invoices],
                "unsend": [_serialize_invoice(inv) for inv in unsent_invoices],
            }

            response["invoice_data"] = invoice_data
            await redis.set(invoice_key, json.dumps(invoice_data), ex=86400)

        # ---------- Unpaid (expired) invoices ----------
        unpaid_key = f"gym:{gym_id}:unpaid_members"
        cached_unpaid = await redis.get(unpaid_key)

        if cached_unpaid:
            try:
                response["unpaid_data"] = json.loads(_decode_cached(cached_unpaid))
            except json.JSONDecodeError:
                response["unpaid_data"] = None

        if not response.get("unpaid_data"):
            unpaid_sent_invoices: List[AboutToExpire] = (
                db.query(AboutToExpire)
                .filter(
                    AboutToExpire.gym_id == gym_id,
                    AboutToExpire.mail_status.is_(True),
                    AboutToExpire.expired.is_(True),
                )
                .all()
            )
            unpaid_unsent_invoices: List[AboutToExpire] = (
                db.query(AboutToExpire)
                .filter(
                    AboutToExpire.gym_id == gym_id,
                    AboutToExpire.mail_status.is_(False),
                    AboutToExpire.expired.is_(True),
                )
                .all()
            )

            unpaid_data = {
                "send": [_serialize_invoice(inv) for inv in unpaid_sent_invoices],
                "unsend": [_serialize_invoice(inv) for inv in unpaid_unsent_invoices],
            }

            response["unpaid_data"] = unpaid_data
            await redis.set(unpaid_key, json.dumps(unpaid_data), ex=86400)

        return {
            "status": 200,
            "message": "Data listed successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch members/invoices data",
            error_code="MEMBERS_DATA_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


def _serialize_membership(membership, client, gym, account, gym_client, plan) -> Dict[str, Any]:
    """Serialize membership data from FittbotGymMembership table"""
    return {
        "expiry_id": membership.id,
        "client_id": int(membership.client_id) if membership.client_id else None,
        "gym_id": int(membership.gym_id) if membership.gym_id else None,
        "client_name": client.name if client else None,
        "gym_name": gym.name if gym else None,
        "gym_logo": gym.logo if gym else None,
        "gym_contact": gym.contact_number if gym else None,
        "gym_location": gym.location if gym else None,
        "plan_id": membership.plan_id,
        "plan_description": plan.plans if plan else None,
        "fees": float(membership.amount) if membership.amount else 0.0,
        "discount": 0,
        "discounted_fees": float(membership.amount) if membership.amount else 0.0,
        "due_date": str(membership.expires_at) if membership.expires_at else None,
        "invoice_number": None,  # Not available in FittbotGymMembership
        "client_contact": client.contact if client else None,
        "bank_details": account.account_number if account else None,
        "ifsc_code": account.account_ifsccode if account else None,
        "account_holder_name": account.account_holdername if account else None,
        "paid": False,  # Calculate based on remaining
        "mail_send": False,  # Default to False
        "expired": membership.expires_at < date.today() if membership.expires_at else False,
        "email": client.email if client else None,
    }


@router.get("/all")
async def get_members_data_v2(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    New version that fetches data from FittbotGymMembership instead of AboutToExpire table.
    Uses date-based Redis keys and same response structure as /all endpoint.
    """
    try:
        # Basic input validation
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        today = date.today()
        five_days_later = today + timedelta(days=5)

        response: Dict[str, Any] = {}

        # ---------- Invoice data (about to expire, not yet expired) ----------
        invoice_key = f"gym:{gym_id}:invoice_data:{today.strftime('%Y-%m-%d')}"
        cached_invoice = await redis.get(invoice_key)

        if cached_invoice:
            try:
                response["invoice_data"] = json.loads(_decode_cached(cached_invoice))
            except json.JSONDecodeError:
                response["invoice_data"] = None

        if not response.get("invoice_data"):
            # Get gym and account details once
            gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
            account = db.query(AccountDetails).filter(AccountDetails.gym_id == gym_id).first()

            # Query memberships expiring within next 5 days (not expired yet, exclude upcoming)
            invoice_memberships = (
                db.query(FittbotGymMembership, Client, GymPlans)
                .select_from(FittbotGymMembership)
                .join(Client, Client.client_id == FittbotGymMembership.client_id)
                .join(GymPlans, GymPlans.id == FittbotGymMembership.plan_id, isouter=True)
                .filter(
                    FittbotGymMembership.gym_id == str(gym_id),
                    FittbotGymMembership.status != "upcoming",
                    FittbotGymMembership.expires_at > today,
                    FittbotGymMembership.expires_at <= five_days_later
                )
                .all()
            )

            # For this new version, we'll treat all as "unsend" (mail_send=False)
            # since we don't have mail_status tracking in FittbotGymMembership
            invoice_data = {
                "send": [],  # Empty for now, would need separate tracking
                "unsend": [
                    _serialize_membership(membership, client, gym, account, None, plan)
                    for membership, client, plan in invoice_memberships
                ],
            }

            response["invoice_data"] = invoice_data
            await redis.set(invoice_key, json.dumps(invoice_data), ex=86400)

        # ---------- Unpaid (expired) invoices ----------
        unpaid_key = f"gym:{gym_id}:unpaid_members:{today.strftime('%Y-%m-%d')}"
        cached_unpaid = await redis.get(unpaid_key)

        if cached_unpaid:
            try:
                response["unpaid_data"] = json.loads(_decode_cached(cached_unpaid))
            except json.JSONDecodeError:
                response["unpaid_data"] = None

        if not response.get("unpaid_data"):
            # Get gym and account details if not already fetched
            if 'gym' not in locals():
                gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
                account = db.query(AccountDetails).filter(AccountDetails.gym_id == gym_id).first()

            # Update expired memberships status
            db.query(FittbotGymMembership).filter(
                FittbotGymMembership.gym_id == str(gym_id),
                FittbotGymMembership.expires_at < today,
                FittbotGymMembership.status == "active"
            ).update({"status": "expired"}, synchronize_session=False)
            db.commit()


            unpaid_memberships = (
                db.query(FittbotGymMembership, Client, GymPlans)
                .select_from(FittbotGymMembership)
                .join(Client, Client.client_id == FittbotGymMembership.client_id)
                .join(GymPlans, GymPlans.id == FittbotGymMembership.plan_id, isouter=True)
                .filter(
                    FittbotGymMembership.gym_id == str(gym_id),
                    FittbotGymMembership.status != "upcoming",
                    FittbotGymMembership.expires_at < today
                )
                .all()
            )

            unpaid_data = {
                "send": [],  # Empty for now, would need separate tracking
                "unsend": [
                    _serialize_membership(membership, client, gym, account, None, plan)
                    for membership, client, plan in unpaid_memberships
                ],
            }

            response["unpaid_data"] = unpaid_data
            await redis.set(unpaid_key, json.dumps(unpaid_data), ex=86400)


        print("response:", response)

        return {
            "status": 200,
            "message": "Data listed successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch members/invoices data",
            error_code="MEMBERS_DATA_V2_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


class DiscountRequest(BaseModel):
    membership_id: int
    discount_amount: float


@router.post("/add_discount")
async def add_membership_discount(
    request: DiscountRequest,
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Add or update discount for a membership.
    Clears all related Redis cache keys after update.
    """
    try:
        # Validate membership exists
        membership = db.query(FittbotGymMembership).filter(
            FittbotGymMembership.id == request.membership_id,
            FittbotGymMembership.gym_id == str(gym_id)
        ).first()

        if not membership:
            raise FittbotHTTPException(
                status_code=404,
                detail="Membership not found",
                error_code="MEMBERSHIP_NOT_FOUND",
                log_data={"membership_id": request.membership_id, "gym_id": gym_id},
            )

        # Check if discount already exists for this membership
        existing_discount = db.query(EstimateDiscount).filter(
            EstimateDiscount.membership_id == request.membership_id
        ).first()

        if existing_discount:
            # Update existing discount
            existing_discount.discount_amount = request.discount_amount
            existing_discount.updated_at = date.today()
        else:
            # Create new discount entry
            new_discount = EstimateDiscount(
                membership_id=request.membership_id,
                discount_amount=request.discount_amount
            )
            db.add(new_discount)

        db.commit()

        # Clear all related Redis cache keys
        patterns = [
            f"gym:{gym_id}:invoice_data:*",
            f"gym:{gym_id}:unpaid_members:*",
            f"gym:{gym_id}:about_to_expire:*",
            f"gym:{gym_id}:expired:*"
        ]

        for pattern in patterns:
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
                print(f"Deleted Redis keys matching pattern: {pattern}")

        return {
            "status": 200,
            "message": "Discount added/updated successfully",
            "data": {
                "membership_id": request.membership_id,
                "discount_amount": float(request.discount_amount)
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to add/update discount",
            error_code="DISCOUNT_UPDATE_ERROR",
            log_data={"membership_id": request.membership_id, "error": repr(e)},
        )
