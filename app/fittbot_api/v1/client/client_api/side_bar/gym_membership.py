# app/api/v1/client/show_client_qr.py

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Depends
from sqlalchemy import desc
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from app.models.database import get_db
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import OldGymData,ClientGym,Client, Gym, GymPlans, GymFees, FittbotGymMembership, GymJoinRequest
from app.fittbot_api.v1.payments.models.entitlements import Entitlement
from app.fittbot_api.v1.payments.models.orders import OrderItem
from app.fittbot_api.v1.client.client_api.home.gym_studios import smart_round_price, calculate_nutritional_plan, calculate_fittbot_plan_offer
from app.config.pricing import get_markup_multiplier
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

router = APIRouter(prefix="/client_qr", tags=["Client QR"])


@router.get("/show")
async def show_client_qr(client_id: int, db: Session = Depends(get_db)):
    try:
        # Keep original query style/logic
        client = db.query(Client).filter(Client.client_id == client_id).one()

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="User not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        gym_name = None
        gym_id = None

        if client.gym_id:
            gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).one()
            gym_name = gym.name
            gym_id = gym.gym_id

        # Original logic preserved (no encryption applied)
        encrypted_uuid = str(client.uuid_client)

        membership_records = (
            db.query(FittbotGymMembership)
            .filter(
                FittbotGymMembership.client_id == str(client_id),
                FittbotGymMembership.status.in_(["upcoming", "active","paused"])
            )
            .order_by(FittbotGymMembership.purchased_at.desc())
            .all()
        )

        # Build membership cards list
        membership_cards = []
        for record in membership_records:
            # Skip admission_fees entries
            if record.type == "admission_fees":
                continue

            # Get gym name from gym_id
            gym = db.query(Gym).filter(Gym.gym_id == int(record.gym_id)).first()
            gym_name_for_card = gym.name if gym else "Unknown Gym"

            # Get plan details if plan_id exists
            plan_amount = None
            duration = None
            plan_bonus = None
            plan_bonus_type = None
            pause = None
            pause_type = None
            avail_pause = False  # Initialize to False

            if record.plan_id:
                plan = db.query(GymPlans).filter(GymPlans.id == record.plan_id).first()
                if plan:
                    plan_amount = plan.amount
                    duration = plan.duration
                    plan_bonus = plan.bonus
                    plan_bonus_type = plan.bonus_type
                    pause = plan.pause if hasattr(plan, 'pause') else None
                    pause_type = plan.pause_type if hasattr(plan, 'pause_type') else None

                    # Check if plan has pause feature (pause value exists and is not None/empty)
                    if pause is not None and str(pause).strip() != "" and str(pause).strip().lower() != "false":
                        avail_pause = True
                else:
                    plan_amount = None
                    duration = None
                    plan_bonus = None
                    plan_bonus_type = None
            else:
                plan_bonus = None
                plan_bonus_type = None


            pause_available = True if ((record.pause =='0' or record.pause == "False" or record.pause == "" or  record.pause is None) and avail_pause) else False

            # Continue is available if pause was taken (pause = "taken")
            continue_available = True if (record.pause and str(record.pause).lower() == "taken") else False

            # Get actual paid amount from OrderItem via entitlement_id
            actual_amount = None
            if record.entitlement_id:
                entitlement = db.query(Entitlement).filter(Entitlement.id == record.entitlement_id).first()
                if entitlement and entitlement.order_item_id:
                    order_item = db.query(OrderItem).filter(OrderItem.id == entitlement.order_item_id).first()
                    if order_item:
                        actual_amount = order_item.unit_price_minor // 100  # Convert paisa to rupees

            # Fallback to plan amount with smart rounding if OrderItem not found
            if actual_amount is None and plan_amount:
                actual_amount = smart_round_price(plan_amount * get_markup_multiplier())

            nutritional_plan = calculate_nutritional_plan(duration) if duration else None
            fittbot_offer = calculate_fittbot_plan_offer(gym_plan_duration=duration) if duration else None

            membership_cards.append({
                "membership_id": record.id,
                "gym_id": record.gym_id,
                "gym_name": gym_name_for_card,
                "amount": actual_amount,
                "duration": duration if record.plan_id else None,
                "purchased_at": record.purchased_at.isoformat() if record.purchased_at else None,
                "type": record.type,
                "status": record.status,
                "entitlement_id": record.entitlement_id,
                "expires_at": record.expires_at.isoformat() if record.expires_at else None,
                "bonus": plan_bonus,
                "bonus_type": plan_bonus_type,
                "pause_available": pause_available,  # True if can pause, False if already paused
                "pause": pause,  # Pause duration from plan (e.g., 5)
                "pause_type": pause_type,
                "continue_available": continue_available,  # Pause type from plan (e.g., "days", "months")
                "nutritional_plan": nutritional_plan,
                "fittbot_plan_offer": fittbot_offer,
            })

        if membership_records:
            membership_type = membership_records[0].type
            membership_status = membership_records[0].status
        else:
            membership_type = "normal"
            membership_status = None


        row = (
            db.query(
                GymPlans.plans.label("plan_name"),
                GymPlans.amount,
                GymPlans.duration,
                GymPlans.bonus,
                GymPlans.bonus_type,
                Client.joined_date,
            )
            .join(Client, GymPlans.id == Client.training_id)
            .filter(Client.client_id == client_id)
            .first()
        )

        if not row:
            plans = {}
        else:
            plan_name, amount, duration, bonus, bonus_type, joined_date = row
            days = db.query(GymFees).filter(GymFees.client_id == client_id).first()
            expiration_date = days.end_date
            days_left = (expiration_date - date.today()).days
            expiry = days_left if days_left > 0 else 0

            plans = {
                "plan_name": plan_name,
                "amount": amount,
                "duration": duration,
                "joined_date": joined_date,
                "expiry": expiry,
                "bonus": bonus,
                "bonus_type": bonus_type,
            }


        pending_request = db.query(GymJoinRequest).filter(
            GymJoinRequest.client_id == client_id,
            GymJoinRequest.status == "pending"
        ).first()

        sent_request = None
        if pending_request:
            request_gym = db.query(Gym).filter(Gym.gym_id == pending_request.gym_id).first()
            sent_request = {
                "id": pending_request.id,
                "gym_name": request_gym.name if request_gym else None,
                "sent_at": pending_request.created_at.isoformat() if pending_request.created_at else None
            }

        response = {
            "profile": client.profile,
            "name": client.name,
            "client_id": client.client_id,
            "contact": client.contact,
            "gender": client.gender,
            "uuid": encrypted_uuid,
            "gym_id": gym_id,
            "gym_name": gym_name,
            "plans": plans,
            "type": membership_type,
            "membership_status": membership_status,
            "membership_cards": membership_cards,
            "sent_request": sent_request,
        }

        return {
            "status": 200,
            "message": "Data retrived successfully",
            "data": response,
        }

    except FittbotHTTPException:
        # Pass through structured errors
        raise
    except Exception as e:
        # Normalize unexpected errors to your standard format; logic above remains unchanged
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occured : {str(e)}",
            error_code="SHOW_CLIENT_QR_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


@router.post("/cancel_membership")
async def cancel_membership(client_id:int,membership_id: int,redis: Redis = Depends(get_redis), db: Session = Depends(get_db)):
    try:
        admission_num=db.query(ClientGym).filter(ClientGym.client_id==client_id).first()
        client=db.query(Client).filter(Client.client_id==client_id).first()

        latest_membership = (
            db.query(FittbotGymMembership)
            .filter(
                FittbotGymMembership.client_id == str(client_id),
                FittbotGymMembership.gym_id == str(client.gym_id)
            )
            .order_by(desc(FittbotGymMembership.joined_at))
            .first()
        )
        expires_at_value = latest_membership.expires_at if latest_membership else None
        starts_at_value = latest_membership.joined_at if (latest_membership and latest_membership.joined_at) else None

        old_row_data = {
        "gym_client_id":  admission_num.gym_client_id if admission_num and admission_num.gym_client_id else None,
        "client_id":client_id,
        "gym_id":         client.gym_id,
        "name":           client.name,
        "profile":        client.profile,
        "location":       client.location,
        "email":          client.email,
        "contact":        client.contact,
        "lifestyle":      client.lifestyle,
        "medical_issues": client.medical_issues,
        "batch_id":       client.batch_id,
        "training_id":    client.training_id,
        "age":            client.age,
        "goals":          client.goals,
        "gender":         client.gender,
        "height":         client.height,
        "weight":         client.weight,
        "bmi":            client.bmi,
        "joined_date":    client.joined_date,
        "status":         client.status,
        "dob":            client.dob,
        "admission_number": admission_num.admission_number if admission_num else None,
        "starts_at":      starts_at_value,
        "expires_at":     expires_at_value,
        }

        db.add(OldGymData(**old_row_data))


        membership = db.query(FittbotGymMembership).filter(FittbotGymMembership.id == membership_id).first()
        client_gym_id=db.query(Client).filter(Client.client_id==client_id).first()

        if client_gym_id:
            gym_id=client_gym_id.gym_id
            redis_key = f"gym:{gym_id}:clientdata"

            if await redis.exists(redis_key):
                await redis.delete(redis_key)
            client_gym_id.gym_id=None

        if not membership:
            raise FittbotHTTPException(
                status_code=404,
                detail="Membership not found",
                error_code="MEMBERSHIP_NOT_FOUND",
                log_data={"membership_id": membership_id},
            )

        membership.status = "expired"

        # Also expire all admission_fees type memberships for this client (single UPDATE query)
        try:
            db.query(FittbotGymMembership).filter(
                FittbotGymMembership.client_id == str(client_id),
                FittbotGymMembership.type == "admission_fees"
            ).update({"status": "expired"}, synchronize_session=False)
        except Exception:
            pass  # Continue even if this fails

        db.commit()



        redis_key = f"gym:{gym_id}:clientdata"
        return {
            "status": 200,
            "message": "Membership cancelled successfully",

        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="CANCEL_MEMBERSHIP_ERROR",
            log_data={"membership_id": membership_id, "error": str(e)},
        )


class PauseMembershipRequest(BaseModel):
    client_id: int
    membership_id: int
    pause: int  
    pause_type: str  


class ContinueMembershipRequest(BaseModel):
    client_id: int
    membership_id: int


@router.post("/pause_membership")
async def pause_membership(
    request: PauseMembershipRequest,
    redis: Redis = Depends(get_redis),
    db: Session = Depends(get_db)
):

    try:
        client_id = request.client_id
        membership_id = request.membership_id
        pause_duration = request.pause
        pause_type = request.pause_type

        # Get the membership
        membership = db.query(FittbotGymMembership).filter(
            FittbotGymMembership.id == membership_id,
            FittbotGymMembership.client_id == str(client_id)
        ).first()

        if not membership:
            raise FittbotHTTPException(
                status_code=404,
                detail="Membership not found",
                error_code="MEMBERSHIP_NOT_FOUND",
                log_data={"membership_id": membership_id, "client_id": client_id},
            )

        # Check if pause is already taken
        if membership.pause and membership.pause.lower() in ["taken", "continued"]:
            raise FittbotHTTPException(
                status_code=400,
                detail="Membership pause already used",
                error_code="PAUSE_ALREADY_USED",
                log_data={"membership_id": membership_id, "pause_status": membership.pause},
            )

        # Get the client
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        # Store gym_id for redis cleanup
        gym_id = client.gym_id

        # Remove gym_id from client table
        if gym_id:
            redis_key = f"gym:{gym_id}:clientdata"
            if await redis.exists(redis_key):
                await redis.delete(redis_key)
            client.gym_id = None

        # Calculate new expiry date

        print("membership.expires_at",membership.expires_at)
        print("membership.expires_at",membership.joined_at)
        
        if membership.expires_at:
            current_expiry = membership.expires_at
        else:
            current_expiry = date.today()

        # Add pause duration to expires_at
        if pause_type.lower() in ["day", "days"]:
            new_expiry = current_expiry + timedelta(days=pause_duration)
        elif pause_type.lower() in ["month", "months"]:
            new_expiry = current_expiry + relativedelta(months=pause_duration)
        elif pause_type.lower() in ["year", "years"]:
            new_expiry = current_expiry + relativedelta(years=pause_duration)
        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid pause_type. Must be 'days', 'months', or 'years'",
                error_code="INVALID_PAUSE_TYPE",
                log_data={"pause_type": pause_type},
            )
        print("new_expiry",new_expiry)

        # Update membership
        membership.pause = "taken"
        membership.pause_at = date.today()  # Store the date when pause was taken
        membership.expires_at = new_expiry
        membership.status = "paused"  # Optional: add paused status

        db.commit()

        return {
            "status": 200,
            "message": "Membership paused successfully",
            "data": {
                "membership_id": membership_id,
                "pause_duration": pause_duration,
                "pause_type": pause_type,
                "old_expiry": current_expiry.isoformat(),
                "new_expiry": new_expiry.isoformat(),
                "pause_status": "taken"
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="PAUSE_MEMBERSHIP_ERROR",
            log_data={"membership_id": request.membership_id, "client_id": request.client_id, "error": str(e)},
        )


@router.post("/continue_membership")
async def continue_membership(
    request: ContinueMembershipRequest,
    redis: Redis = Depends(get_redis),
    db: Session = Depends(get_db)
):
    """
    Continue a paused membership:
    1. Check pause_at date and calculate actual days used
    2. Get plan's pause period
    3. Calculate unused pause days
    4. Reduce expires_at by unused days
    5. Change pause status to "continued"
    6. Add gym_id back to client table
    7. Change membership status to active
    """
    try:
        client_id = request.client_id
        membership_id = request.membership_id

        # Get the membership
        membership = db.query(FittbotGymMembership).filter(
            FittbotGymMembership.id == membership_id,
            FittbotGymMembership.client_id == str(client_id)
        ).first()

        if not membership:
            raise FittbotHTTPException(
                status_code=404,
                detail="Membership not found",
                error_code="MEMBERSHIP_NOT_FOUND",
                log_data={"membership_id": membership_id, "client_id": client_id},
            )

        # Check if membership was paused
        if not membership.pause or membership.pause.lower() != "taken":
            raise FittbotHTTPException(
                status_code=400,
                detail="Membership is not paused",
                error_code="MEMBERSHIP_NOT_PAUSED",
                log_data={"membership_id": membership_id, "pause_status": membership.pause},
            )

        # Check if pause_at date exists
        if not membership.pause_at:
            raise FittbotHTTPException(
                status_code=400,
                detail="Pause date not found for this membership",
                error_code="PAUSE_DATE_MISSING",
                log_data={"membership_id": membership_id},
            )

        # Get the plan to retrieve pause duration
        if not membership.plan_id:
            raise FittbotHTTPException(
                status_code=400,
                detail="Plan ID not found for this membership",
                error_code="PLAN_ID_MISSING",
                log_data={"membership_id": membership_id},
            )

        plan = db.query(GymPlans).filter(GymPlans.id == membership.plan_id).first()
        if not plan:
            raise FittbotHTTPException(
                status_code=404,
                detail="Plan not found",
                error_code="PLAN_NOT_FOUND",
                log_data={"plan_id": membership.plan_id},
            )

        # Get pause duration from plan
        plan_pause_duration = plan.pause if hasattr(plan, 'pause') else None
        plan_pause_type = plan.pause_type if hasattr(plan, 'pause_type') else None

        if not plan_pause_duration or not plan_pause_type:
            raise FittbotHTTPException(
                status_code=400,
                detail="Plan does not have pause configuration",
                error_code="PLAN_PAUSE_NOT_CONFIGURED",
                log_data={"plan_id": membership.plan_id},
            )

        # Calculate actual days used during pause
        today = date.today()
        pause_start_date = membership.pause_at
        days_actually_used = (today - pause_start_date).days

        # Convert plan pause duration to days for comparison
        if plan_pause_type.lower() in ["day", "days"]:
            total_pause_days_allowed = int(plan_pause_duration)
        elif plan_pause_type.lower() in ["month", "months"]:
            total_pause_days_allowed = int(plan_pause_duration) * 30  # Approximate
        elif plan_pause_type.lower() in ["year", "years"]:
            total_pause_days_allowed = int(plan_pause_duration) * 365  # Approximate
        else:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid pause_type in plan",
                error_code="INVALID_PLAN_PAUSE_TYPE",
                log_data={"pause_type": plan_pause_type},
            )

        # Check if user used less time than allowed
        if days_actually_used < total_pause_days_allowed:
            # Calculate unused pause days
            unused_pause_days = total_pause_days_allowed - days_actually_used

            # Reduce expires_at by unused days
            if membership.expires_at:
                new_expiry = membership.expires_at - timedelta(days=unused_pause_days)
            else:
                new_expiry = None
        else:
            # User used all or more than allowed pause time, no adjustment needed
            unused_pause_days = 0
            new_expiry = membership.expires_at

        # Check if membership has expired
        if new_expiry and new_expiry < today:
            raise FittbotHTTPException(
                status_code=400,
                detail="Membership has expired and cannot be continued",
                error_code="MEMBERSHIP_EXPIRED",
                log_data={
                    "membership_id": membership_id,
                    "expires_at": new_expiry.isoformat(),
                    "today": today.isoformat()
                },
            )

        # Get the client
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        # Get gym_id from membership
        gym_id = int(membership.gym_id)

        # Clear redis cache for the gym
        redis_key = f"gym:{gym_id}:clientdata"
        if await redis.exists(redis_key):
            await redis.delete(redis_key)

        # Add gym_id back to client table
        client.gym_id = gym_id

        # Update membership status
        membership.pause = "continued"
        membership.status = "active"
        membership.expires_at = new_expiry
        membership.resume_at = today  # Store resume date

        db.commit()

        return {
            "status": 200,
            "message": "Membership continued successfully",
            "data": {
                "membership_id": membership_id,
                "gym_id": gym_id,
                "pause_status": "continued",
                "membership_status": "active",
                "pause_start_date": pause_start_date.isoformat(),
                "resume_date": today.isoformat(),
                "days_actually_used": days_actually_used,
                "total_pause_days_allowed": total_pause_days_allowed,
                "unused_pause_days": unused_pause_days,
                "adjusted_expires_at": new_expiry.isoformat() if new_expiry else None
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="CONTINUE_MEMBERSHIP_ERROR",
            log_data={"membership_id": request.membership_id, "client_id": request.client_id, "error": str(e)},
        )


@router.delete("/delete_request")
async def delete_join_request(id: int, db: Session = Depends(get_db)):
    try:
        # Find the join request by id
        join_request = db.query(GymJoinRequest).filter(GymJoinRequest.id == id).first()

        if not join_request:
            raise FittbotHTTPException(
                status_code=404,
                detail="Join request not found",
                error_code="JOIN_REQUEST_NOT_FOUND",
                log_data={"id": id},
            )

        # Delete the request
        db.delete(join_request)
        db.commit()

        return {
            "status": 200,
            "message": "Join request deleted successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}",
            error_code="DELETE_JOIN_REQUEST_ERROR",
            log_data={"id": id, "error": str(e)},
        )

