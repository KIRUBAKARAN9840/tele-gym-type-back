"""
Manual Client Registration API
For CRM-style manual client entry without Fittbot app connection
"""

from __future__ import annotations

import logging
import uuid
import boto3
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis
from pydantic import BaseModel, Field, validator
from sqlalchemy import select, or_, and_, func
from sqlalchemy.ext.asyncio import AsyncSession


AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
S3_ENDPOINT = f"https://s3.{AWS_REGION}.amazonaws.com"
_s3 = boto3.client("s3", region_name=AWS_REGION, endpoint_url=S3_ENDPOINT)

from app.models.async_database import get_async_db
from app.utils.redis_config import get_redis
from app.models.fittbot_models import (
    Client,
    ClientGym,
    ManualClient,
    ManualFeeHistory,
    GymPlans,
    GymBatches,
    FittbotGymMembership,
    FeesReceipt,
    Gym,
    AccountDetails,
    GymMonthlyData,
)

logger = logging.getLogger("owner.manual_registration")

router = APIRouter(prefix="/owner/gym/manual", tags=["Manual Registration"])

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(IST)


def _today_ist() -> date:
    return _now_ist().date()



class LookupResponse(BaseModel):
    status: int
    exists: bool
    source: Optional[str] = None  # "manual_clients" or "clients"
    message: str
    client_data: Optional[dict] = None


class ManualClientRequest(BaseModel):
    gym_id: int = Field(..., gt=0)

    # Required
    name: str = Field(..., min_length=1, max_length=100)
    contact: str = Field(..., min_length=10, max_length=15)

    # Optional Personal
    email: Optional[str] = None
    gender: Optional[str] = None
    date_of_birth: Optional[date] = None

    # Optional Physical
    height: Optional[float] = None
    weight: Optional[float] = None
    bmi: Optional[float] = None
    goal: Optional[str] = None  # weight_gain, weight_loss, body_recomposition

    # Membership
    admission_number: Optional[str] = None
    batch_id: Optional[int] = None
    plan_id: Optional[int] = None
    joined_at: Optional[date] = None
    expires_at: Optional[date] = None

    # Fees
    admission_fee: Optional[float] = 0
    monthly_fee: Optional[float] = 0
    discount_amount: Optional[float] = 0
    total_paid: Optional[float] = 0
    balance_due: Optional[float] = 0
    payment_method: Optional[str] = None

    # Notes
    notes: Optional[str] = None

    # Profile Photo
    dp: Optional[str] = None  # S3 URL for client photo

    @validator('contact')
    def validate_contact(cls, v):
        # Remove any non-digit characters
        cleaned = ''.join(filter(str.isdigit, v))
        if len(cleaned) < 10:
            raise ValueError('Contact must be at least 10 digits')
        return cleaned


class ManualClientResponse(BaseModel):
    status: int
    message: str
    client_id: Optional[int] = None
    client_data: Optional[dict] = None



def normalize_contact(contact: str) -> str:
    """Normalize contact number by removing non-digits"""
    if not contact:
        return ""
    return ''.join(filter(str.isdigit, str(contact).strip()))


def calculate_age(dob: date) -> int:
    """Calculate age from date of birth"""
    if not dob:
        return None
    today = _today_ist()
    age = today.year - dob.year
    if (today.month, today.day) < (dob.month, dob.day):
        age -= 1
    return age



@router.get("/lookup", response_model=LookupResponse)
async def lookup_client_by_contact(
    contact: str = Query(..., min_length=10, description="Client contact number"),
    gym_id: int = Query(..., gt=0, description="Gym ID"),
    db: AsyncSession = Depends(get_async_db)
):

    normalized_contact = normalize_contact(contact)

    if len(normalized_contact) < 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Contact must be at least 10 digits"
        )

    try:
       
        manual_result = await db.execute(
            select(ManualClient).where(
                and_(
                    ManualClient.gym_id == gym_id,
                    ManualClient.contact == normalized_contact
                )
            )
        )
        manual_client: Optional[ManualClient] = manual_result.scalars().first()

        if manual_client:
            return LookupResponse(
                status=200,
                exists=True,
                source="manual_clients",
                message=f"Client '{manual_client.name}' is already registered (Manual Entry)",
                client_data={
                    "id": manual_client.id,
                    "name": manual_client.name,
                    "contact": manual_client.contact,
                    "email": manual_client.email,
                    "status": manual_client.status,
                    "joined_at": str(manual_client.joined_at) if manual_client.joined_at else None,
                    "expires_at": str(manual_client.expires_at) if manual_client.expires_at else None,
                    "entry_type": "manual"
                }
            )

        # Check in clients table (Fittbot app clients)
        client_result = await db.execute(
            select(Client).where(
                and_(
                    Client.gym_id == gym_id,
                    Client.contact == normalized_contact
                )
            )
        )
        client: Optional[Client] = client_result.scalars().first()

        if client:
            return LookupResponse(
                status=200,
                exists=True,
                source="clients",
                message=f"Client '{client.name}' is already registered (Fittbot App)",
                client_data={
                    "id": client.client_id,
                    "name": client.name,
                    "contact": client.contact,
                    "email": client.email,
                    "status": client.status,
                    "joined_date": str(client.joined_date) if client.joined_date else None,
                    "entry_type": "qr"
                }
            )

        # Client doesn't exist
        return LookupResponse(
            status=200,
            exists=False,
            source=None,
            message="Client not found. You can add as new client.",
            client_data=None
        )

    except Exception as e:
        logger.error(f"Error looking up client: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to lookup client: {str(e)}"
        )


@router.post("/add_client", response_model=ManualClientResponse)
async def add_manual_client(
    payload: ManualClientRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):

    normalized_contact = normalize_contact(payload.contact)

    try:
       
        existing_manual = await db.execute(
            select(ManualClient).where(
                and_(
                    ManualClient.gym_id == payload.gym_id,
                    ManualClient.contact == normalized_contact
                )
            )
        )
        if existing_manual.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Client with this contact already exists in manual clients"
            )

        # Check if client already exists in clients table
        existing_client = await db.execute(
            select(Client).where(
                and_(
                    Client.gym_id == payload.gym_id,
                    Client.contact == normalized_contact
                )
            )
        )
        if existing_client.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Client with this contact already exists (registered via Fittbot app)"
            )

        # Calculate age if DOB provided
        age = None
        if payload.date_of_birth:
            age = calculate_age(payload.date_of_birth)

        # Calculate BMI if height and weight provided
        bmi = payload.bmi
        if payload.height and payload.weight and not bmi:
            height_m = payload.height / 100  # Convert cm to m
            bmi = round(payload.weight / (height_m ** 2), 2)

        # Set default joined_at to today if not provided
        joined_at = payload.joined_at or _today_ist()

        # Auto-generate admission number if not provided
        admission_number = payload.admission_number.strip() if payload.admission_number else None
        if not admission_number:
            # Get gym name prefix (first 2 characters)
            gym_result = await db.execute(select(Gym).where(Gym.gym_id == payload.gym_id))
            gym = gym_result.scalars().first()

            if gym and gym.name and gym.name.strip():
                prefix = gym.name.strip().upper()[:2]
            else:
                prefix = "MC"  # Manual Client fallback

            # Find max running number from ClientGym (same pattern as owner.py)
            prefix_pattern = f"{prefix}-{payload.gym_id}-%"
            existing_ids_result = await db.execute(
                select(ClientGym.gym_client_id).where(
                    ClientGym.gym_id == payload.gym_id,
                    ClientGym.gym_client_id.like(prefix_pattern)
                )
            )
            existing_ids = existing_ids_result.scalars().all()

            max_number = 0
            for id_value in existing_ids:
                try:
                    parts = id_value.split('-')
                    if len(parts) == 3:
                        num = int(parts[2])
                        max_number = max(max_number, num)
                except (ValueError, IndexError):
                    continue

            running_number = max_number + 1
            admission_number = f"{prefix}-{payload.gym_id}-{running_number}"
            #logger.info(f"Auto-generated admission number from ClientGym max: {admission_number}")

        # Create new manual client
        new_client = ManualClient(
            gym_id=payload.gym_id,
            name=payload.name.strip(),
            contact=normalized_contact,
            email=payload.email.strip() if payload.email else None,
            gender=payload.gender,
            date_of_birth=payload.date_of_birth,
            age=age,
            height=payload.height,
            weight=payload.weight,
            bmi=bmi,
            goal=payload.goal,
            admission_number=admission_number,
            batch_id=payload.batch_id,
            plan_id=payload.plan_id,
            joined_at=joined_at,
            expires_at=payload.expires_at,
            admission_fee=payload.admission_fee or 0,
            monthly_fee=payload.monthly_fee or 0,
            total_paid=payload.total_paid or 0,
            balance_due=payload.balance_due or 0,
            last_payment_date=_today_ist() if payload.total_paid and payload.total_paid > 0 else None,
            status="active",
            notes=payload.notes.strip() if payload.notes else None,
            entry_type="manual",
            dp=payload.dp,
        )

 
        db.add(new_client)
        await db.flush()

        # Add to ClientGym for unified admission number tracking (use negative ID for manual clients)
        client_gym_record = ClientGym(
            client_id=-new_client.id,  # Negative ID to differentiate manual clients
            gym_id=payload.gym_id,
            gym_client_id=admission_number,
            admission_number=admission_number
        )
        db.add(client_gym_record)
        #logger.info(f"Added ClientGym record for manual client: client_id={-new_client.id}, admission_number={admission_number}")

        if payload.total_paid and payload.total_paid > 0:
       
            if payload.admission_fee and payload.admission_fee > 0:
                admission_record = ManualFeeHistory(
                    manual_client_id=new_client.id,
                    gym_id=payload.gym_id,
                    amount=payload.admission_fee,
                    payment_method=payload.payment_method,
                    payment_date=_today_ist(),
                    type="admission",
                    notes="Initial admission fee"
                )
                db.add(admission_record)
   
            membership_amount = payload.total_paid - (payload.admission_fee or 0)
            if membership_amount > 0:
                membership_record = ManualFeeHistory(
                    manual_client_id=new_client.id,
                    gym_id=payload.gym_id,
                    amount=membership_amount,
                    payment_method=payload.payment_method,
                    payment_date=_today_ist(),
                    type="monthly",
                    notes="Initial membership fee"
                )
                db.add(membership_record)


        if payload.plan_id:
            total_amount = (payload.monthly_fee or 0) + (payload.admission_fee or 0) - (payload.discount_amount or 0)

            new_membership = FittbotGymMembership(
                gym_id=str(payload.gym_id),
                client_id=f"manual_{new_client.id}",
                plan_id=payload.plan_id,
                type="normal",
                amount=total_amount,
                purchased_at=_now_ist(),
                status="active",
                joined_at=joined_at,
                expires_at=payload.expires_at,
            )
            db.add(new_membership)
            #logger.info(f"FittbotGymMembership created for manual client {new_client.id}")

    
            gym_result = await db.execute(select(Gym).where(Gym.gym_id == payload.gym_id))
            gym = gym_result.scalars().first()

     
            account_result = await db.execute(select(AccountDetails).where(AccountDetails.gym_id == payload.gym_id))
            account = account_result.scalars().first()

            # Fetch plan details
            plan_result = await db.execute(select(GymPlans).where(GymPlans.id == payload.plan_id))
            plan = plan_result.scalars().first()

            if gym and plan:
                discount_pct = payload.discount_amount or 0
                fees_after_discount = total_amount

                new_receipt = FeesReceipt(
                    client_id=None,  # Not a regular client
                    manual_client_id=new_client.id,  # Manual client reference
                    gym_id=payload.gym_id,
                    client_name=new_client.name,
                    gym_name=gym.name,
                    gym_logo=gym.logo,
                    gym_contact=gym.contact_number or "",
                    gym_location=gym.location,
                    plan_id=payload.plan_id,
                    plan_description=plan.plans,
                    fees=plan.amount,
                    discount=discount_pct,
                    discounted_fees=fees_after_discount,
                    due_date=payload.expires_at,
                    invoice_number=None,  # Will be set after flush
                    client_contact=new_client.contact,
                    bank_details=account.account_number if account else "",
                    ifsc_code=account.account_ifsccode if account else "",
                    account_holder_name=account.account_holdername if account else "",
                    invoice_date=_today_ist(),
                    payment_method=payload.payment_method,
                    gst_number=account.gst_number if account else "",
                    bank_name=account.bank_name if account else "",
                    branch=account.account_branch if account else "",
                    client_email=new_client.email,
                    mail_status=False,
                    payment_date=joined_at,
                    payment_reference_number=None,
                    created_at=_now_ist(),
                    update_at=_now_ist(),
                    gst_percentage=None,
                    gst_type=None,
                    total_amount=total_amount,
                    fees_type="New Registration"
                )
                db.add(new_receipt)
                await db.flush()

                # Generate invoice number
                receipt_count_result = await db.execute(
                    select(FeesReceipt).where(FeesReceipt.gym_id == payload.gym_id)
                )
                gym_receipt_count = len(receipt_count_result.scalars().all())
                location_prefix = (gym.location[:3].upper() if gym.location else "GYM")
                new_receipt.invoice_number = f"{location_prefix}-{gym.gym_id}-{gym_receipt_count}"

                #logger.info(f"FeesReceipt created for manual client {new_client.id}, invoice: {new_receipt.invoice_number}")

        await db.commit()
        await db.refresh(new_client)

        #logger.info(f"Manual client added: {new_client.id} - {new_client.name} for gym {payload.gym_id}")

        try:
            today = date.today()
            await redis.delete(
                f"gym:{payload.gym_id}:members",
                f"gym:{payload.gym_id}:all_clients",
                f"gym:{payload.gym_id}:trainers",
                f"gym:{payload.gym_id}:all_diets",
                f"gym:{payload.gym_id}:all_workouts",
                f"gym:{payload.gym_id}:attendance:{today.strftime('%Y-%m-%d')}"
            )
        except Exception as cache_err:
            logger.warning(f"[manual_add_client] Failed to clear cache for gym {payload.gym_id}: {cache_err}")

        return ManualClientResponse(
            status=201,
            message=f"Client '{new_client.name}' added successfully",
            client_id=new_client.id,
            client_data={
                "id": new_client.id,
                "name": new_client.name,
                "contact": new_client.contact,
                "email": new_client.email,
                "status": new_client.status,
                "joined_at": str(new_client.joined_at) if new_client.joined_at else None,
                "expires_at": str(new_client.expires_at) if new_client.expires_at else None,
                "balance_due": new_client.balance_due,
                "entry_type": "manual"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error adding manual client: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add client: {str(e)}"
        )




@router.get("/clients")
async def get_manual_clients(
    gym_id: int = Query(..., gt=0, description="Gym ID"),
    status_filter: Optional[str] = Query(None, description="Filter by status: active, inactive, expired"),
    db: AsyncSession = Depends(get_async_db)
):
   
    try:
        query = select(ManualClient).where(ManualClient.gym_id == gym_id)

        if status_filter:
            query = query.where(ManualClient.status == status_filter)

        query = query.order_by(ManualClient.id.desc())

        result = await db.execute(query)
        clients = result.scalars().all()

        client_data = []
        for client in clients:
            client_data.append({
                "id": client.id,
                "name": client.name,
                "contact": client.contact,
                "email": client.email,
                "gender": client.gender,
                "admission_number": client.admission_number,
                "plan_id": client.plan_id,
                "batch_id": client.batch_id,
                "joined_at": str(client.joined_at) if client.joined_at else None,
                "expires_at": str(client.expires_at) if client.expires_at else None,
                "status": client.status,
                "balance_due": client.balance_due,
                "total_paid": client.total_paid,
                "notes": client.notes,
                "dp": client.dp,
                "entry_type": "manual"
            })

        return {
            "status": 200,
            "message": "Clients retrieved successfully",
            "count": len(client_data),
            "clients": client_data
        }

    except Exception as e:
        logger.error(f"Error fetching manual clients: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch clients: {str(e)}"
        )


class ManualUpdateFeeRequest(BaseModel):
    """Request model for updating fee status of manual client"""
    manual_client_id: int = Field(..., gt=0)
    gym_id: int = Field(..., gt=0)
    plan_id: int = Field(..., gt=0)
    batch_id: Optional[int] = None

    # Fee details
    fees: float = Field(..., ge=0)  # Plan amount
    total_amount: float = Field(..., ge=0)  # Amount after discount
    discount_amount: Optional[float] = 0
    payment_method: Optional[str] = None

    # Dates
    joined_at: Optional[date] = None
    expires_at: Optional[date] = None


@router.post("/update_fee_status")
async def update_manual_client_fee_status(
    payload: ManualUpdateFeeRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):

    try:
        #logger.info(f"[manual_update_fee_status] Received payload: {payload.dict()}")

        result = await db.execute(
            select(ManualClient).where(ManualClient.id == payload.manual_client_id)
        )
        client = result.scalars().first()

        if not client:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Manual client not found"
            )

        if client.gym_id != payload.gym_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Client does not belong to this gym"
            )

        #logger.info(f"[manual_update_fee_status] Found client: {client.name} (id={client.id})")

        # 2. Get plan details
        plan_result = await db.execute(
            select(GymPlans).where(GymPlans.id == payload.plan_id)
        )
        plan = plan_result.scalars().first()

        if not plan:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Plan not found"
            )

        # 3. Set dates
        joined_at = payload.joined_at or _today_ist()
        expires_at = payload.expires_at

        # Calculate expiry if not provided (based on plan duration)
        if not expires_at and plan.duration:
            from dateutil.relativedelta import relativedelta
            expires_at = joined_at + relativedelta(months=int(plan.duration))

        # 4. Update ManualClient record
        client.plan_id = payload.plan_id
        if payload.batch_id:
            client.batch_id = payload.batch_id
        client.joined_at = joined_at
        client.expires_at = expires_at
        client.status = "active"
        client.total_paid = (client.total_paid or 0) + payload.total_amount
        client.last_payment_date = _today_ist()

        #logger.info(f"[manual_update_fee_status] Updated client: joined_at={joined_at}, expires_at={expires_at}")

        # 5. Create ManualFeeHistory record
        fee_history = ManualFeeHistory(
            manual_client_id=client.id,
            gym_id=payload.gym_id,
            amount=payload.total_amount,
            payment_method=payload.payment_method,
            payment_date=_today_ist(),
            type="monthly",
            notes=f"Renewal - {plan.plans}"
        )
        db.add(fee_history)
        #logger.info(f"[manual_update_fee_status] Created ManualFeeHistory record")

        # 6. Update GymMonthlyData (gym income)
        month_tag = datetime.now().strftime("%Y-%m")
        monthly_result = await db.execute(
            select(GymMonthlyData).where(
                GymMonthlyData.gym_id == payload.gym_id,
                GymMonthlyData.month_year.like(f"{month_tag}%")
            )
        )
        monthly_rec = monthly_result.scalars().first()

        if monthly_rec:
            monthly_rec.income = (monthly_rec.income or 0) + payload.fees
            #logger.info(f"[manual_update_fee_status] Updated GymMonthlyData income +{payload.fees}")
        else:
            new_monthly = GymMonthlyData(
                gym_id=payload.gym_id,
                month_year=datetime.now().strftime("%Y-%m-%d"),
                income=payload.fees,
                expenditure=0,
                new_entrants=0
            )
            db.add(new_monthly)
            #logger.info(f"[manual_update_fee_status] Created GymMonthlyData record")

        # 7. Get gym and account details for receipt
        gym_result = await db.execute(select(Gym).where(Gym.gym_id == payload.gym_id))
        gym = gym_result.scalars().first()

        account_result = await db.execute(
            select(AccountDetails).where(AccountDetails.gym_id == payload.gym_id)
        )
        account = account_result.scalars().first()

        # 8. Create/Update FittbotGymMembership
        manual_client_id_str = f"manual_{client.id}"

        membership_result = await db.execute(
            select(FittbotGymMembership).where(
                FittbotGymMembership.client_id == manual_client_id_str,
                FittbotGymMembership.gym_id == str(payload.gym_id)
            ).order_by(FittbotGymMembership.id.desc())
        )
        existing_membership = membership_result.scalars().first()

        # Mark old membership as inactive if exists
        if existing_membership:
            existing_membership.status = "inactive"
            #logger.info(f"[manual_update_fee_status] Marked old FittbotGymMembership id={existing_membership.id} as inactive")

        # Always create a new membership record for each renewal/fee update
        new_membership = FittbotGymMembership(
            gym_id=str(payload.gym_id),
            client_id=manual_client_id_str,
            plan_id=payload.plan_id,
            type="normal",
            amount=payload.total_amount,
            purchased_at=_now_ist(),
            status="active",
            joined_at=joined_at,
            expires_at=expires_at,
        )
        db.add(new_membership)
        #logger.info(f"[manual_update_fee_status] Created new FittbotGymMembership")

        # 9. Create FeesReceipt
        if gym and plan:
            new_receipt = FeesReceipt(
                client_id=None,  # Not a regular client
                manual_client_id=client.id,
                gym_id=payload.gym_id,
                client_name=client.name,
                gym_name=gym.name,
                gym_logo=gym.logo,
                gym_contact=gym.contact_number or "",
                gym_location=gym.location,
                plan_id=payload.plan_id,
                plan_description=plan.plans,
                fees=plan.amount,
                discount=payload.discount_amount or 0,
                discounted_fees=payload.total_amount,
                due_date=expires_at,
                invoice_number=None,
                client_contact=client.contact,
                bank_details=account.account_number if account else "",
                ifsc_code=account.account_ifsccode if account else "",
                account_holder_name=account.account_holdername if account else "",
                invoice_date=_today_ist(),
                payment_method=payload.payment_method,
                gst_number=account.gst_number if account else "",
                bank_name=account.bank_name if account else "",
                branch=account.account_branch if account else "",
                client_email=client.email,
                mail_status=False,
                payment_date=joined_at,
                payment_reference_number=None,
                created_at=_now_ist(),
                update_at=_now_ist(),
                gst_percentage=None,
                gst_type=None,
                total_amount=payload.total_amount,
                fees_type="Renewal"
            )
            db.add(new_receipt)
            await db.flush()

            # Generate invoice number
            receipt_count_result = await db.execute(
                select(func.count()).select_from(FeesReceipt).where(FeesReceipt.gym_id == payload.gym_id)
            )
            gym_receipt_count = receipt_count_result.scalar() or 0
            location_prefix = (gym.location[:3].upper() if gym.location else "GYM")
            new_receipt.invoice_number = f"{location_prefix}-{gym.gym_id}-{gym_receipt_count}"

            #logger.info(f"[manual_update_fee_status] Created FeesReceipt invoice={new_receipt.invoice_number}")

        # 10. Commit all changes
        await db.commit()
        await db.refresh(client)

        #logger.info(f"[manual_update_fee_status] Successfully updated fee status for manual client {client.id}")

        try:
            await redis.delete(f"gym:{payload.gym_id}:members")
        except Exception as cache_err:
            logger.warning(f"[manual_update_fee_status] Failed to clear members cache for gym {payload.gym_id}: {cache_err}")

        return {
            "status": 200,
            "message": "Fee status updated successfully",
            "client_data": {
                "id": client.id,
                "name": client.name,
                "plan_id": client.plan_id,
                "joined_at": str(client.joined_at) if client.joined_at else None,
                "expires_at": str(client.expires_at) if client.expires_at else None,
                "total_paid": client.total_paid,
                "status": client.status
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"[manual_update_fee_status] Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update fee status: {str(e)}"
        )




class DPUploadRequest(BaseModel):
    gym_id: int = Field(..., gt=0)


@router.post("/dp-upload-url")
async def get_dp_upload_url(payload: DPUploadRequest):

    try:

        unique_id = str(uuid.uuid4())
        filename = f"manual_clients/dp/{payload.gym_id}/{unique_id}.jpg"

        # Generate presigned PUT URL
        presigned_url = _s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": BUCKET_NAME,
                "Key": filename,
                "ContentType": "image/jpeg",
            },
            ExpiresIn=600,
        )

        s3_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{filename}"

        return {
            "status": 200,
            "presigned_url": presigned_url,
            "s3_url": s3_url,
        }

    except Exception as e:
        logger.error(f"Error generating DP upload URL: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate upload URL: {str(e)}"
        )
