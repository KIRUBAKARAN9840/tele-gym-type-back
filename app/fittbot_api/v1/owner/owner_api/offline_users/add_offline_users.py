from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime, date, timedelta
from typing import Optional, List
import calendar
import random
import string
import traceback

from app.models.database import get_db
from app.models.fittbot_models import (
    Client, ClientGym, OldGymData, Gym, GymOwner, GymPlans,
    FittbotGymMembership, GymFees, FeeHistory, GymMonthlyData,
    FeesReceipt, AccountDetails, ClientScheduler, RewardGym,
    LeaderboardOverall, ClientNextXp, GymJoinRequest, GymImportData
)
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from sqlalchemy import func, desc


router = APIRouter(prefix="/offline_users", tags=["Offline Users"])


class AddOfflineUserRequest(BaseModel):
    mobile_number:int
    gym_id: int
    batch: Optional[str]=None
    admission_fees: Optional[float] = 0
    fees: Optional[float] = 0  # Final fees amount (no discount calculation needed)
    joining_date: Optional[date] = None  # Default to today if not provided
    expiry_date: Optional[date] = None  # If not provided, calculate from duration_months
    duration_months: Optional[int] = None  # Used to calculate expiry if expiry_date not provided
    client_id: Optional[int]=None
    uuid: Optional[str]=None



def _add_months_safe(base: date, months: int) -> date:
    """Add months to a date, handling month-end edge cases."""
    months = int(months or 0)
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = (month_index % 12) + 1
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(base.day, max_day))


def _pick_next_reward(ladder: List[RewardGym], current_xp: int):
    for tier in ladder:
        if tier.xp > current_xp:
            return tier
    return None


@router.post("/add")
async def add_offline_user(
    request: AddOfflineUserRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):

    try:
        gym_id = request.gym_id
        if request.client_id:
            client_id = request.client_id
        elif request.uuid:
            client= db.query(Client).filter(Client.uuid_client==request.uuid).first()
            client_id=client.client_id

        mobile_number = str(request.mobile_number)
        admission_fees = request.admission_fees if request.admission_fees else 0
        fees = request.fees if request.fees else 0
        today_date = date.today()

        # Check for import data using mobile_number and gym_id
        import_data = db.query(GymImportData).filter(
            GymImportData.gym_id == gym_id,
            GymImportData.client_contact == mobile_number
        ).first()


        import_data_expiry = None
        if import_data and getattr(import_data, "expires_at", None):
            expiry_value = import_data.expires_at
            if isinstance(expiry_value, date):
                import_data_expiry = expiry_value
            else:
                try:
                    import_data_expiry = datetime.strptime(str(expiry_value)[:10], "%Y-%m-%d").date()
                except ValueError:
                    import_data_expiry = None

        # Parse import data joined_at if available
        import_data_joined_at = None
        if import_data and getattr(import_data, "joined_at", None):
            joined_value = import_data.joined_at
            if isinstance(joined_value, date):
                import_data_joined_at = joined_value
            else:
                try:
                    import_data_joined_at = datetime.strptime(str(joined_value)[:10], "%Y-%m-%d").date()
                except ValueError:
                    import_data_joined_at = None

        print(f"[add_offline_user] Import data found: {import_data is not None}")
        if import_data:
            print(f"[add_offline_user] Import expiry: {import_data_expiry}, Import joined: {import_data_joined_at}")

        # Determine joining_date: request > import_data > today
        if request.joining_date:
            joining_date = request.joining_date
        elif import_data_joined_at:
            joining_date = import_data_joined_at
        else:
            joining_date = today_date

        # Determine expiry_date: request > import_data > duration_months > default 1 month
        if request.expiry_date:
            expiry_date = request.expiry_date
        elif import_data_expiry and import_data_expiry >= today_date:
            # Use import expiry if valid (not expired)
            expiry_date = import_data_expiry
        elif request.duration_months:
            expiry_date = _add_months_safe(joining_date, request.duration_months)
        else:
            # Default to 1 month if nothing provided
            expiry_date = _add_months_safe(joining_date, 1)

        # Fixed values as per requirements
        if request.batch== "gym_membership":
            batch_type = 366
        elif request.batch=="personal_training":
            batch_type=367
        else:
            batch_type=366

        training_type = 146

        print(f"[add_offline_user] Processing client_id={client_id}, gym_id={gym_id}, mobile={mobile_number}")
        print(f"[add_offline_user] joining_date={joining_date}, expiry_date={expiry_date}")
        print(f"[add_offline_user] admission_fees={admission_fees}, fees={fees}")

        cursor = b'0'
        pattern = f"gym:{gym_id}:*"
        while cursor:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                await redis.delete(*keys)

        # Get the existing client
        existing_client = db.query(Client).filter(Client.client_id == client_id).first()

        if not existing_client:
            raise HTTPException(status_code=404, detail="Client not found")

        print(f"[add_offline_user] Found client: {existing_client.name}")

        # Handle existing gym assignment - save old data only if old gym exists
        if existing_client.gym_id is not None:
            # Check if the old gym exists before saving old data
            old_gym_exists = db.query(Gym).filter(Gym.gym_id == existing_client.gym_id).first()

            if old_gym_exists:
                admission_num = db.query(ClientGym).filter(
                    ClientGym.client_id == existing_client.client_id
                ).first()

                # Get expires_at from FittbotGymMembership (latest joined_at)
                latest_membership = (
                    db.query(FittbotGymMembership)
                    .filter(
                        FittbotGymMembership.client_id == str(existing_client.client_id),
                        FittbotGymMembership.gym_id == str(existing_client.gym_id)
                    )
                    .order_by(desc(FittbotGymMembership.joined_at))
                    .first()
                )
                expires_at_value = latest_membership.expires_at if latest_membership else None
                starts_at_value = latest_membership.joined_at if (latest_membership and latest_membership.joined_at) else None

                old_row_data = {
                    "gym_client_id": admission_num.gym_client_id if admission_num and admission_num.gym_client_id else None,
                    "gym_id": existing_client.gym_id,
                    "name": existing_client.name,
                    "profile": existing_client.profile,
                    "location": existing_client.location,
                    "email": existing_client.email,
                    "contact": existing_client.contact,
                    "lifestyle": existing_client.lifestyle,
                    "medical_issues": existing_client.medical_issues,
                    "batch_id": existing_client.batch_id,
                    "training_id": existing_client.training_id,
                    "age": existing_client.age,
                    "goals": existing_client.goals,
                    "gender": existing_client.gender,
                    "height": existing_client.height,
                    "weight": existing_client.weight,
                    "bmi": existing_client.bmi,
                    "joined_date": existing_client.joined_date,
                    "status": existing_client.status,
                    "dob": existing_client.dob,
                    "admission_number": admission_num.admission_number if admission_num else None,
                    "starts_at": starts_at_value,
                    "expires_at": expires_at_value,
                }
                db.add(OldGymData(**old_row_data))
            else:
                print(f"[add_offline_user] Skipping old data save - gym_id {existing_client.gym_id} doesn't exist")

        # Generate gym_client_id
        gym_record = db.query(Gym).filter(Gym.gym_id == gym_id).first()
        if gym_record and gym_record.name and gym_record.name.strip():
            first_two = gym_record.name.strip().upper()[:2]
        else:
            first_two = ''.join(random.choices(string.ascii_uppercase, k=2))

        # Check for existing old client data
        existing_old_client = db.query(OldGymData).filter(
            OldGymData.gym_id == gym_id,
            OldGymData.contact == existing_client.contact
        ).first()

        if existing_old_client:
            gym_client_id = existing_old_client.gym_client_id
            admission_number = existing_old_client.admission_number
            db.delete(existing_old_client)
        else:
            try:
                prefix_pattern = f"{first_two}-{gym_id}-%"
                existing_ids = db.query(ClientGym.gym_client_id).filter(
                    ClientGym.gym_id == gym_id,
                    ClientGym.gym_client_id.like(prefix_pattern)
                ).all()

                max_number = 0
                for (id_value,) in existing_ids:
                    try:
                        parts = id_value.split('-')
                        if len(parts) == 3:
                            num = int(parts[2])
                            max_number = max(max_number, num)
                    except (ValueError, IndexError):
                        continue

                running_number = max_number + 1
                gym_client_id = f"{first_two}-{gym_id}-{running_number}"
            except Exception as e:
                print(f"[add_offline_user] Error generating gym_client_id: {str(e)}")
                gym_client_id = None
            admission_number = None

        # Check if ClientGym mapping already exists
        gym_client = db.query(ClientGym).filter(
            ClientGym.client_id == existing_client.client_id,
            ClientGym.gym_id == gym_id
        ).first()

        if not gym_client:
            new_data = ClientGym(
                client_id=existing_client.client_id,
                gym_client_id=gym_client_id,
                gym_id=gym_id,
                admission_number=admission_number
            )
            db.add(new_data)

        # Update client core attributes
        existing_client.gym_id = gym_id
        existing_client.batch_id = batch_type
        existing_client.training_id = training_type
        existing_client.expiry = "joining_date"  # Default expiry mode
        existing_client.status = "active"
        existing_client.access = True

 
        if admission_fees > 0:
            new_admission_fee = FeeHistory(
                gym_id=gym_id,
                client_id=existing_client.client_id,
                type="admission",
                fees_paid=admission_fees,
                payment_date=date.today()
            )
            db.add(new_admission_fee)

        # Record regular fee history
        new_fee_history = FeeHistory(
            gym_id=gym_id,
            client_id=existing_client.client_id,
            type="fees",
            fees_paid=fees,  # Use the fees from payload
            payment_date=date.today()
        )
        db.add(new_fee_history)

        # Update monthly data
        current_month = datetime.now().strftime("%Y-%m")
        existing_record = db.query(GymMonthlyData).filter(
            GymMonthlyData.gym_id == gym_id,
            GymMonthlyData.month_year.like(f"{current_month}%")
        ).first()

        total_income = fees + admission_fees  # Both fees and admission fees count as income
        if existing_record:
            existing_record.income += total_income
            existing_record.new_entrants += 1
        else:
            new_record = GymMonthlyData(
                gym_id=gym_id,
                month_year=datetime.now().strftime("%Y-%m-%d"),
                income=total_income,
                expenditure=0,
                new_entrants=1
            )
            db.add(new_record)

        # Handle client scheduler
        scheduler = db.query(ClientScheduler).filter(
            ClientScheduler.client_id == existing_client.client_id
        ).first()
        if scheduler:
            db.delete(scheduler)
        db.add(ClientScheduler(
            gym_id=gym_id,
            client_id=existing_client.client_id
        ))

        # Clear additional Redis caches
        cache_keys = [
            f"gym:{gym_id}:collection",
            f"gym:{gym_id}:monthly_data",
            f"gym:{gym_id}:analysis",
            f"gym:{gym_id}:hourlyagg",
            f"gym:{gym_id}:members",
            f"gym:{gym_id}:new_clients",
            f"gym:{gym_id}:pendingClients",
            f"{existing_client.client_id}:fees",
            f"gym:{gym_id}:clientdata"
        ]
        for key in cache_keys:
            if await redis.exists(key):
                await redis.delete(key)

        # Create fee receipts
        gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
        gym_owner = db.query(GymOwner).filter(GymOwner.owner_id == gym.owner_id).first()
        account = db.query(AccountDetails).filter(AccountDetails.gym_id == gym_id).first()

        # Main fee receipt
        new_receipt = FeesReceipt(
            client_id=existing_client.client_id,
            gym_id=gym_id,
            client_name=existing_client.name,
            gym_name=gym.name,
            gym_logo=gym.logo,
            gym_contact=gym_owner.contact_number if gym_owner else "",
            gym_location=gym.location,
            plan_id=training_type,
            plan_description="Offline Membership",
            fees=fees,
            discount=0,
            discounted_fees=fees,
            due_date=expiry_date,
            invoice_number=None,
            client_contact=existing_client.contact,
            bank_details=account.account_number if account else "",
            ifsc_code=account.account_ifsccode if account else "",
            account_holder_name=account.account_holdername if account else "",
            invoice_date=datetime.now().date(),
            payment_method="offline",
            gst_number=account.gst_number if account else "",
            gst_type="",
            gst_percentage=0,
            total_amount=fees,
            client_email=existing_client.email,
            mail_status=False,
            payment_date=joining_date,
            payment_reference_number=None,
            fees_type="normal",
            created_at=datetime.now(),
            update_at=datetime.now()
        )

        # Admission receipt if applicable
        if admission_fees > 0:
            admission_receipt = FeesReceipt(
                client_id=existing_client.client_id,
                gym_id=gym_id,
                client_name=existing_client.name,
                gym_name=gym.name,
                gym_logo=gym.logo,
                gym_contact=gym_owner.contact_number if gym_owner else "",
                gym_location=gym.location,
                plan_id=training_type,
                plan_description="Admission Fees",
                fees=admission_fees,
                discount=0,
                discounted_fees=admission_fees,
                due_date=expiry_date,
                invoice_number=None,
                client_contact=existing_client.contact,
                bank_details=account.account_number if account else "",
                ifsc_code=account.account_ifsccode if account else "",
                account_holder_name=account.account_holdername if account else "",
                invoice_date=datetime.now().date(),
                payment_method="offline",
                gst_number=account.gst_number if account else "",
                gst_type="",
                gst_percentage=0,
                total_amount=admission_fees,
                client_email=existing_client.email,
                mail_status=False,
                payment_date=joining_date,
                payment_reference_number=None,
                fees_type="admission",
                created_at=datetime.now(),
                update_at=datetime.now()
            )
            db.add(admission_receipt)

        db.add(new_receipt)
        db.flush()

        # Generate invoice numbers
        if admission_fees > 0:
            gym_receipt_count = db.query(FeesReceipt).filter(FeesReceipt.gym_id == gym_id).count()
            admission_receipt.invoice_number = f"{gym.location[:3].upper()}-{gym.gym_id}-{gym_receipt_count - 1}"
            new_receipt.invoice_number = f"{gym.location[:3].upper()}-{gym.gym_id}-{gym_receipt_count}"
        else:
            gym_receipt_count = db.query(FeesReceipt).filter(FeesReceipt.gym_id == gym_id).count()
            new_receipt.invoice_number = f"{gym.location[:3].upper()}-{gym.gym_id}-{gym_receipt_count}"

        # Clear fee receipt cache
        pattern = f"gym{gym_id}:feesReceipt:*"
        async for key in redis.scan_iter(match=pattern):
            await redis.delete(key)

        # Handle rewards
        ladder = (
            db.query(RewardGym)
            .filter(RewardGym.gym_id == gym_id)
            .order_by(RewardGym.xp.asc())
            .all()
        )

        reward = db.query(LeaderboardOverall).filter(
            LeaderboardOverall.client_id == existing_client.client_id
        ).first()

        if reward:
            cur_xp = reward.xp
            tier = _pick_next_reward(ladder, cur_xp)
            next_xp = tier.xp if tier else 0
            next_gift = tier.gift if tier else None

            db.query(ClientNextXp).filter(
                ClientNextXp.client_id == existing_client.client_id
            ).delete(synchronize_session=False)

            db.add(ClientNextXp(
                client_id=existing_client.client_id,
                next_xp=next_xp,
                gift=next_gift,
            ))

        # Update or create GymFees record
        gym_fees_record = db.query(GymFees).filter(
            GymFees.client_id == existing_client.client_id
        ).first()

        if gym_fees_record:
            gym_fees_record.start_date = joining_date
            gym_fees_record.end_date = expiry_date
        else:
            db.add(GymFees(
                client_id=existing_client.client_id,
                start_date=joining_date,
                end_date=expiry_date
            ))

        # Determine old_client flag:
        # If joining_date is given and is in current month -> old_client = False
        # Otherwise -> old_client = True
        current_month = datetime.now().month
        current_year = datetime.now().year
        is_current_month = (
            request.joining_date is not None
            and joining_date.month == current_month
            and joining_date.year == current_year
        )
        old_client_flag = not is_current_month

        # Create FittbotGymMembership entry with type "normal"
        normal_entry = FittbotGymMembership(
            gym_id=str(gym_id),
            client_id=str(existing_client.client_id),
            plan_id=training_type,
            type="normal",
            amount=fees,  # Use fees from payload
            status="active",
            purchased_at=datetime.now(),
            joined_at=joining_date,
            expires_at=expiry_date,
            old_client=old_client_flag
        )
        db.add(normal_entry)

        # Add admission fees entry in FittbotGymMembership if applicable
        if admission_fees > 0:
            admission_entry = FittbotGymMembership(
                gym_id=str(gym_id),
                client_id=str(existing_client.client_id),
                plan_id=training_type,
                type="admission_fees",
                amount=admission_fees,
                status="active",
                purchased_at=datetime.now(),
                joined_at=joining_date,
                expires_at=expiry_date,
                old_client=old_client_flag
            )
            db.add(admission_entry)

        # Commit all changes
        db.commit()

        # Update GymJoinRequest status to "onboarded" if exists
        join_request = db.query(GymJoinRequest).filter(
            GymJoinRequest.gym_id == gym_id,
            GymJoinRequest.client_id == existing_client.client_id,
            GymJoinRequest.status=='pending'

        ).order_by(GymJoinRequest.id.desc()).first()

        if join_request:
            join_request.status = "onboarded"
            join_request.updated_at = datetime.now()
            db.commit()
            print(f"[add_offline_user] Updated GymJoinRequest status to 'onboarded' for client_id={existing_client.client_id}, gym_id={gym_id}")

        # Delete import data after successful addition (same as original add_client_data)
        if import_data:
            db.delete(import_data)
            db.commit()
            print(f"[add_offline_user] Deleted GymImportData for mobile={mobile_number}, gym_id={gym_id}")

        return {
            "status": 200,
            "message": "Offline user added to the Gym successfully.",
            "data": {
                "client_id": existing_client.client_id,
                "gym_id": gym_id,
                "joining_date": str(joining_date),
                "expiry_date": str(expiry_date),
                "batch_type": batch_type,
                "training_type": training_type
            }
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        print(f"[add_offline_user] Error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error adding offline user: {str(e)}")
