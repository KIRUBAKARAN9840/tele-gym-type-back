from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, desc, or_, select, distinct, func
from app.models.async_database import get_async_db
from app.models.telecaller_models import GymDatabase, Telecaller, GymAssignment, GymCallLogs, ConvertedStatus
from app.telecaller.dependencies import get_current_telecaller
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date, timedelta
import pytz

router = APIRouter()

class CallLogForm(BaseModel):
    interest_level: Optional[str] = None  # "High", "Medium", "Low"
    total_members: Optional[int] = None
    new_contact_number: Optional[str] = None
    feature_explained: Optional[bool] = False
    remarks: str  # Mandatory field

class ConvertedStatusData(BaseModel):
    document_uploaded: Optional[bool] = False
    membership_plan_created: Optional[bool] = False
    session_created: Optional[bool] = False
    daily_pass_created: Optional[bool] = False
    gym_studio_images_uploaded: Optional[bool] = False
    agreement_signed: Optional[bool] = False
    biometric_required: Optional[bool] = False
    registered_place: Optional[str] = None  # "GYM" or "OTHERS"

class UpdateGymStatus(BaseModel):
    gym_id: int
    status: str  # "follow_up", "converted", "rejected", "no_response"
    call_form: CallLogForm
    converted_status: Optional[ConvertedStatusData] = None
    follow_up_date: Optional[datetime] = None
    assigned_telecaller_id: Optional[int] = None  # For assigning follow-up to another telecaller

@router.get("/gyms")
async def get_gyms_by_status(
    status: str = Query(..., description="Filter gyms by status: pending, follow_up, converted, rejected"),
    target_date_filter: Optional[str] = Query(None, description="Filter by target date: today, this_week, this_month, custom"),
    target_start_date: Optional[date] = Query(None, description="Target start date"),
    target_end_date: Optional[date] = Query(None, description="Target end date"),
    follow_up_filter: Optional[str] = Query(None, description="Filter by follow-up date: today, this_week, overdue, custom"),
    follow_up_start_date: Optional[date] = Query(None, description="Follow-up start date"),
    follow_up_end_date: Optional[date] = Query(None, description="Follow-up end date"),
    converted_filter: Optional[str] = Query(None, description="Filter by converted date: today, this_week, this_month, custom"),
    converted_start_date: Optional[date] = Query(None, description="Converted start date"),
    converted_end_date: Optional[date] = Query(None, description="Converted end date"),
    verification_complete: Optional[str] = Query(None, description="Filter converted gyms by verification completion: true, false"),
    rejected_filter: Optional[str] = Query(None, description="Filter by rejected date: today, this_week, this_month, custom"),
    rejected_start_date: Optional[date] = Query(None, description="Rejected start date"),
    rejected_end_date: Optional[date] = Query(None, description="Rejected end date"),
    no_response_filter: Optional[str] = Query(None, description="Filter by no response date: today, this_week, this_month, custom"),
    no_response_start_date: Optional[date] = Query(None, description="No response start date"),
    no_response_end_date: Optional[date] = Query(None, description="No response end date"),
    out_of_service_filter: Optional[str] = Query(None, description="Filter by out of service date: today, this_week, this_month, custom"),
    out_of_service_start_date: Optional[date] = Query(None, description="Out of service start date"),
    out_of_service_end_date: Optional[date] = Query(None, description="Out of service end date"),
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """Get gyms assigned to the telecaller filtered by their latest call status"""
    try:

        # Define IST timezone
        ist_tz = pytz.timezone('Asia/Kolkata')

        # Validate status
        valid_statuses = ["pending", "follow_up", "converted", "rejected", "no_response", "out_of_service", "delegated"]
        if status not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}"
            )



        if status == "delegated":
            try:
                # Use a subquery to get the latest call log for each gym in ONE query
                latest_log_subq = select(
                    GymCallLogs.gym_id,
                    func.max(GymCallLogs.created_at).label('max_created')
                ).group_by(GymCallLogs.gym_id).subquery()

                # Join to get only the latest logs that match our criteria
                result = await db.execute(
                    select(GymCallLogs.gym_id).select_from(GymCallLogs).join(
                        latest_log_subq,
                        and_(
                            GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                            GymCallLogs.created_at == latest_log_subq.c.max_created
                        )
                    ).where(
                        and_(
                            GymCallLogs.telecaller_id == telecaller.id,
                            GymCallLogs.call_status == "delegated",
                            GymCallLogs.assigned_telecaller_id != telecaller.id,
                            GymCallLogs.assigned_telecaller_id.isnot(None)
                        )
                    ).distinct()
                )
                gym_ids = [log[0] for log in result.all()]
            except Exception as e:
                raise
        elif status == "follow_up":
            try:
                # For follow_up status, get my assigned gyms PLUS gyms where follow-ups were assigned to me
                result = await db.execute(
                    select(distinct(GymAssignment.gym_id)).where(
                        and_(
                            GymAssignment.telecaller_id == telecaller.id,
                            GymAssignment.status == "active"
                        )
                    )
                )
                assignments = result.all()
                assigned_gym_ids = [a[0] for a in assignments]

                # Also get gyms where follow-ups were assigned to me by others (status is "delegated" in DB)
                result = await db.execute(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.assigned_telecaller_id == telecaller.id,  # Assigned to me
                            GymCallLogs.telecaller_id != telecaller.id,  # But created by someone else
                            GymCallLogs.call_status == "delegated"  # Status is delegated
                        )
                    ).distinct()
                )
                delegated_to_me = result.all()
                delegated_gym_ids = [log[0] for log in delegated_to_me]


                result = await db.execute(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.telecaller_id == telecaller.id,
                            GymCallLogs.call_status == "follow_up"
                        )
                    ).distinct()
                )
                my_latest_logs = result.all()
                my_log_gym_ids = [log[0] for log in my_latest_logs]

                # Use a subquery to get the latest call log for each gym in ONE query
                latest_log_subq = select(
                    GymCallLogs.gym_id,
                    func.max(GymCallLogs.created_at).label('max_created')
                ).group_by(GymCallLogs.gym_id).subquery()

                # Get gym_ids where MY delegated log is still the latest log
                result = await db.execute(
                    select(GymCallLogs.gym_id).select_from(GymCallLogs).join(
                        latest_log_subq,
                        and_(
                            GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                            GymCallLogs.created_at == latest_log_subq.c.max_created
                        )
                    ).where(
                        and_(
                            GymCallLogs.telecaller_id == telecaller.id,
                            GymCallLogs.call_status == "delegated",
                            GymCallLogs.assigned_telecaller_id != telecaller.id,
                            GymCallLogs.assigned_telecaller_id.isnot(None)
                        )
                    ).distinct()
                )
                delegated_by_me_gym_ids = [log[0] for log in result.all()]

                # Combine all gym_ids, but exclude ones where I've delegated and it's still the latest status
                gym_ids = list(set(assigned_gym_ids + delegated_gym_ids + my_log_gym_ids))
                gym_ids = [gym_id for gym_id in gym_ids if gym_id not in delegated_by_me_gym_ids]
            except Exception as e:
                raise
        else:
            try:
                result = await db.execute(
                    select(distinct(GymAssignment.gym_id)).where(
                        and_(
                            GymAssignment.telecaller_id == telecaller.id,
                            GymAssignment.status == "active"
                        )
                    )
                )
                assignments = result.all()
                assigned_gym_ids = [a[0] for a in assignments]

                # Use a subquery to get the latest call log for each gym in ONE query
                latest_log_subq = select(
                    GymCallLogs.gym_id,
                    func.max(GymCallLogs.created_at).label('max_created')
                ).group_by(GymCallLogs.gym_id).subquery()

                # Get gym_ids where the latest log meets delegation criteria
                result = await db.execute(
                    select(GymCallLogs.gym_id).select_from(GymCallLogs).join(
                        latest_log_subq,
                        and_(
                            GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                            GymCallLogs.created_at == latest_log_subq.c.max_created
                        )
                    ).where(
                        and_(
                            GymCallLogs.telecaller_id == telecaller.id,
                            GymCallLogs.call_status == "delegated",
                            GymCallLogs.assigned_telecaller_id != telecaller.id,
                            GymCallLogs.assigned_telecaller_id.isnot(None)
                        )
                    ).distinct()
                )
                delegated_gym_ids = [log[0] for log in result.all()]

                # Also get gyms where I have created the latest call log (even if not assigned to me)
                result = await db.execute(
                    select(GymCallLogs.gym_id).where(
                        and_(
                            GymCallLogs.telecaller_id == telecaller.id,
                            GymCallLogs.call_status.in_(["pending", "contacted", "interested", "not_interested", "converted", "rejected", "no_response", "out_of_service"])
                        )
                    ).distinct()
                )
                my_latest_logs = result.all()
                my_log_gym_ids = [log[0] for log in my_latest_logs]

                # Combine and exclude delegated gyms
                gym_ids = list(set(assigned_gym_ids + my_log_gym_ids))
                gym_ids = [gym_id for gym_id in gym_ids if gym_id not in delegated_gym_ids]
            except Exception as e:
                raise


        if not gym_ids:
            return {"gyms": []}

        gyms = []

        # For each gym, get the latest call log to determine current status
        for gym_id in gym_ids:
            # Get gym details
            result = await db.execute(
                select(GymDatabase).where(GymDatabase.id == gym_id)
                .limit(1)
            )
            gym = result.scalar_one_or_none()
            if not gym:
                continue

            # For delegated status, get the latest log I created that was delegated
            # For other statuses, get logs assigned to me (excluding ones I delegated to others)
            if status == "delegated":
                result = await db.execute(
                    select(GymCallLogs).where(
                        and_(
                            GymCallLogs.gym_id == gym_id,
                            GymCallLogs.telecaller_id == telecaller.id,  # I created it
                            GymCallLogs.call_status == "delegated",  # Status is delegated
                            GymCallLogs.assigned_telecaller_id != telecaller.id,  # Assigned to someone else
                            GymCallLogs.assigned_telecaller_id.isnot(None)
                        )
                    ).order_by(desc(GymCallLogs.created_at))
                    .limit(1)
                )
                latest_log = result.scalar_one_or_none()
            else:
                result = await db.execute(
                    select(GymCallLogs).where(
                        and_(
                            GymCallLogs.gym_id == gym_id,
                            or_(
                                # Case 1: I created this log and it's NOT delegated to someone else
                                and_(
                                    GymCallLogs.telecaller_id == telecaller.id,
                                    GymCallLogs.call_status != "delegated"  # Not delegated status
                                ),
                                # Case 2: This log was delegated TO me (assigned_telecaller_id == my id)
                                # This means I am the assignee, so I should see the latest delegated log
                                and_(
                                    GymCallLogs.assigned_telecaller_id == telecaller.id,
                                    GymCallLogs.telecaller_id != telecaller.id,  # Created by someone else
                                    GymCallLogs.call_status == "delegated"  # Status is delegated
                                )
                            )
                        )
                    ).order_by(desc(GymCallLogs.created_at))
                    .limit(1)
                )
                latest_log = result.scalar_one_or_none()

            # Get current assignment to get target date
            result = await db.execute(
                select(GymAssignment).where(
                    and_(
                        GymAssignment.gym_id == gym_id,
                        GymAssignment.telecaller_id == telecaller.id,
                        GymAssignment.status == "active"
                    )
                )
                .limit(1)
            )
            current_assignment = result.scalar_one_or_none()

            # Determine current status
            if latest_log:
                current_status = latest_log.call_status
                # Map the database statuses to our five status types
                if current_status in ["pending", "contacted", "interested", "not_interested"]:
                    mapped_status = "pending"
                elif current_status == "follow_up" or current_status == "follow_up_required":
                    mapped_status = "follow_up"
                elif current_status == "delegated":
                    # "delegated" status - map based on who is viewing
                    if latest_log.assigned_telecaller_id == telecaller.id:
                        # I am the assignee, treat as follow_up in my tabs
                        mapped_status = "follow_up"
                    elif latest_log.telecaller_id == telecaller.id:
                        # I am the creator who delegated it, treat as delegated
                        mapped_status = "delegated"
                    else:
                        # Not relevant to me
                        mapped_status = "delegated"
                elif current_status == "converted":
                    mapped_status = "converted"
                elif current_status == "rejected":
                    mapped_status = "rejected"
                elif current_status == "no_response":
                    mapped_status = "no_response"
                elif current_status == "out_of_service":
                    mapped_status = "out_of_service"
                else:
                    mapped_status = "pending"  # Default to pending for any other status
            else:
                # No call logs yet, status is pending
                mapped_status = "pending"


            # Only include if it matches the requested status
            if mapped_status != status:
                continue


            # Datetimes are now stored in IST, handle both naive and timezone-aware
            if latest_log and latest_log.created_at:
                if latest_log.created_at.tzinfo is None:
                    created_at_ist = ist_tz.localize(latest_log.created_at)
                else:
                    created_at_ist = latest_log.created_at.astimezone(ist_tz)
                last_call_date = created_at_ist.isoformat()
            else:
                last_call_date = None

            if latest_log and latest_log.follow_up_date:
                if latest_log.follow_up_date.tzinfo is None:
                    follow_up_date_ist = ist_tz.localize(latest_log.follow_up_date)
                else:
                    follow_up_date_ist = latest_log.follow_up_date.astimezone(ist_tz)
                follow_up_date = follow_up_date_ist.isoformat()
            else:
                follow_up_date = None

            # Format target date if available
            target_date = None
            if current_assignment and current_assignment.target_date:
                # Keep as date object for filtering
                gym_target_date = current_assignment.target_date
                # For frontend, format as string
                target_date = current_assignment.target_date.isoformat()
            else:
                gym_target_date = None

            # Prepare last call details
            last_call_details = None
            if latest_log:
                    # Get converted status if available
                    result = await db.execute(
                        select(ConvertedStatus).where(
                            and_(
                                ConvertedStatus.gym_id == gym_id,
                                ConvertedStatus.telecaller_id == telecaller.id
                            )
                        )
                        .order_by(desc(ConvertedStatus.created_at))
                        .limit(1)
                    )
                    converted_status = result.scalar_one_or_none()

                    last_call_details = {
                        "interest_level": latest_log.interest_level,
                        "total_members": latest_log.total_members,
                        "new_contact_number": latest_log.new_contact_number,
                        "feature_explained": latest_log.feature_explained,
                        "remarks": latest_log.remarks,
                        "converted_status": {
                            "document_uploaded": converted_status.document_uploaded if converted_status else False,
                            "membership_plan_created": converted_status.membership_plan_created if converted_status else False,
                            "session_created": converted_status.session_created if converted_status else False,
                            "daily_pass_created": converted_status.daily_pass_created if converted_status else False,
                            "gym_studio_images_uploaded": converted_status.gym_studio_images_uploaded if converted_status else False,
                            "agreement_signed": converted_status.agreement_signed if converted_status else False,
                            "biometric_required": converted_status.biometric_required if converted_status else False,
                            "registered_place": converted_status.registered_place if converted_status else None,
                        } if converted_status else None
                    }

            # Get assigned telecaller info if delegated
            assigned_telecaller = None
            if latest_log and latest_log.assigned_telecaller_id:
                result = await db.execute(
                    select(Telecaller).where(Telecaller.id == latest_log.assigned_telecaller_id)
                    .limit(1)
                )
                assigned_t = result.scalar_one_or_none()
                if assigned_t:
                    assigned_telecaller = {
                        "id": assigned_t.id,
                        "name": assigned_t.name
                    }


            result = await db.execute(
                select(GymCallLogs).where(
                    GymCallLogs.gym_id == gym_id
                ).order_by(desc(GymCallLogs.created_at))
                .limit(1)
            )
            most_recent_call_log = result.scalar_one_or_none()

            # Use new_contact_number from most recent call log if available, otherwise fall back to gym.contact_phone
            contact_number = None
            contact_number_source = None  # 'call_logs' or 'database'
            if most_recent_call_log and most_recent_call_log.new_contact_number:
                contact_number = most_recent_call_log.new_contact_number
                contact_number_source = 'call_logs'
            else:
                contact_number = getattr(gym, 'contact_phone', None)
                contact_number_source = 'database'

            gym_data = {
                "gym_id": gym.id,
                "gym_name": gym.gym_name,
                "contact_person": getattr(gym, 'contact_person', None),
                "contact_number": contact_number,
                "contact_number_source": contact_number_source,
                "city": getattr(gym, 'city', None),
                "address": getattr(gym, 'address', None),
                "call_status": mapped_status,
                "last_call_date": last_call_date,
                "follow_up_date": follow_up_date,
                "target_date": target_date,
                "gym_target_date": gym_target_date,  # Add raw date for filtering
                "notes": latest_log.remarks if latest_log else None,
                "last_call_details": last_call_details,
                "assigned_telecaller": assigned_telecaller  # For delegated follow-ups
            }

            # Add any additional gym fields (except contact_number and contact_phone to preserve our logic)
            for column in gym.__table__.columns:
                if column.name not in gym_data and column.name not in ['contact_number', 'contact_phone']:
                    gym_data[column.name] = getattr(gym, column.name, None)

            gyms.append(gym_data)

        # Apply target date filter if specified
        if target_date_filter and target_date_filter != "all" and status == "pending":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("gym_target_date"):
                    # Use the gym_target_date directly since it's already a date object
                    target_date = gym["gym_target_date"]

                    if target_date_filter == "today" and target_date == today:
                        filtered_gyms.append(gym)
                    elif target_date_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= target_date <= week_end:
                            filtered_gyms.append(gym)
                    elif target_date_filter == "this_month":
                        if target_date.month == today.month and target_date.year == today.year:
                            filtered_gyms.append(gym)
                    elif target_date_filter == "custom" and target_start_date and target_end_date:
                        if target_start_date <= target_date <= target_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Apply follow-up date filter if specified
        if follow_up_filter and follow_up_filter != "all" and status == "follow_up":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("follow_up_date"):
                    # Parse the follow-up date from ISO string
                    follow_up_date_str = gym["follow_up_date"]
                    if isinstance(follow_up_date_str, str):
                        try:
                            if 'T' in follow_up_date_str:
                                follow_up_date = datetime.fromisoformat(follow_up_date_str.replace('Z', '+00:00')).date()
                            else:
                                follow_up_date = datetime.strptime(follow_up_date_str, '%Y-%m-%d').date()
                        except:
                            follow_up_date = datetime.fromisoformat(follow_up_date_str).date()
                    else:
                        follow_up_date = follow_up_date_str.date() if isinstance(follow_up_date_str, datetime) else follow_up_date_str

                    if follow_up_filter == "today" and follow_up_date == today:
                        filtered_gyms.append(gym)
                    elif follow_up_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= follow_up_date <= week_end:
                            filtered_gyms.append(gym)
                    elif follow_up_filter == "overdue" and follow_up_date < today:
                        filtered_gyms.append(gym)
                    elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                        if follow_up_start_date <= follow_up_date <= follow_up_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Apply converted date filter if specified
        if converted_filter and converted_filter != "all" and status == "converted":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("last_call_date"):
                    # Parse the last call date (which is the converted date for converted gyms)
                    converted_date_str = gym["last_call_date"]
                    if isinstance(converted_date_str, str):
                        try:
                            if 'T' in converted_date_str:
                                converted_date = datetime.fromisoformat(converted_date_str.replace('Z', '+00:00')).date()
                            else:
                                converted_date = datetime.strptime(converted_date_str, '%Y-%m-%d').date()
                        except:
                            converted_date = datetime.fromisoformat(converted_date_str).date()
                    else:
                        converted_date = converted_date_str.date() if isinstance(converted_date_str, datetime) else converted_date_str

                    if converted_filter == "today" and converted_date == today:
                        filtered_gyms.append(gym)
                    elif converted_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= converted_date <= week_end:
                            filtered_gyms.append(gym)
                    elif converted_filter == "this_month":
                        if converted_date.month == today.month and converted_date.year == today.year:
                            filtered_gyms.append(gym)
                    elif converted_filter == "custom" and converted_start_date and converted_end_date:
                        if converted_start_date <= converted_date <= converted_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Apply verification complete filter for converted gyms
        if verification_complete and status == "converted":
            filtered_gyms = []
            for gym in gyms:
                if gym.get("last_call_details") and gym["last_call_details"].get("converted_status"):
                    converted_status = gym["last_call_details"]["converted_status"]
                    # Check if all verification items are complete
                    all_complete = (converted_status.get("document_uploaded") and
                        converted_status.get("membership_plan_created") and
                        converted_status.get("session_created") and
                        converted_status.get("daily_pass_created") and
                        converted_status.get("gym_studio_images_uploaded") and
                        converted_status.get("agreement_signed"))

                    # Filter based on the verification_complete parameter
                    if verification_complete == "true" and all_complete:
                        filtered_gyms.append(gym)
                    elif verification_complete == "false" and not all_complete:
                        filtered_gyms.append(gym)
                else:
                    # If no converted_status exists, treat as not complete
                    if verification_complete == "false":
                        filtered_gyms.append(gym)

            # Apply the filter - even if no gyms match, return empty list
            gyms = filtered_gyms

        # Apply rejected date filter if specified
        if rejected_filter and rejected_filter != "all" and status == "rejected":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("last_call_date"):
                    # Parse the last call date (which is the rejected date for rejected gyms)
                    rejected_date_str = gym["last_call_date"]
                    if isinstance(rejected_date_str, str):
                        try:
                            if 'T' in rejected_date_str:
                                rejected_date = datetime.fromisoformat(rejected_date_str.replace('Z', '+00:00')).date()
                            else:
                                rejected_date = datetime.strptime(rejected_date_str, '%Y-%m-%d').date()
                        except:
                            rejected_date = datetime.fromisoformat(rejected_date_str).date()
                    else:
                        rejected_date = rejected_date_str.date() if isinstance(rejected_date_str, datetime) else rejected_date_str

                    if rejected_filter == "today" and rejected_date == today:
                        filtered_gyms.append(gym)
                    elif rejected_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= rejected_date <= week_end:
                            filtered_gyms.append(gym)
                    elif rejected_filter == "this_month":
                        if rejected_date.month == today.month and rejected_date.year == today.year:
                            filtered_gyms.append(gym)
                    elif rejected_filter == "custom" and rejected_start_date and rejected_end_date:
                        if rejected_start_date <= rejected_date <= rejected_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Apply no response date filter if specified
        if no_response_filter and no_response_filter != "all" and status == "no_response":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("last_call_date"):
                    # Parse the last call date (which is the no response date for no response gyms)
                    no_response_date_str = gym["last_call_date"]
                    if isinstance(no_response_date_str, str):
                        try:
                            if 'T' in no_response_date_str:
                                no_response_date = datetime.fromisoformat(no_response_date_str.replace('Z', '+00:00')).date()
                            else:
                                no_response_date = datetime.strptime(no_response_date_str, '%Y-%m-%d').date()
                        except:
                            no_response_date = datetime.fromisoformat(no_response_date_str).date()
                    else:
                        no_response_date = no_response_date_str.date() if isinstance(no_response_date_str, datetime) else no_response_date_str

                    if no_response_filter == "today" and no_response_date == today:
                        filtered_gyms.append(gym)
                    elif no_response_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= no_response_date <= week_end:
                            filtered_gyms.append(gym)
                    elif no_response_filter == "this_month":
                        if no_response_date.month == today.month and no_response_date.year == today.year:
                            filtered_gyms.append(gym)
                    elif no_response_filter == "custom" and no_response_start_date and no_response_end_date:
                        if no_response_start_date <= no_response_date <= no_response_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Apply out of service date filter if specified
        if out_of_service_filter and out_of_service_filter != "all" and status == "out_of_service":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_gyms = []

            for gym in gyms:
                if gym.get("last_call_date"):
                    # Parse the last call date (which is the out of service date for out of service gyms)
                    out_of_service_date_str = gym["last_call_date"]
                    if isinstance(out_of_service_date_str, str):
                        try:
                            if 'T' in out_of_service_date_str:
                                out_of_service_date = datetime.fromisoformat(out_of_service_date_str.replace('Z', '+00:00')).date()
                            else:
                                out_of_service_date = datetime.strptime(out_of_service_date_str, '%Y-%m-%d').date()
                        except:
                            out_of_service_date = datetime.fromisoformat(out_of_service_date_str).date()
                    else:
                        out_of_service_date = out_of_service_date_str.date() if isinstance(out_of_service_date_str, datetime) else out_of_service_date_str

                    if out_of_service_filter == "today" and out_of_service_date == today:
                        filtered_gyms.append(gym)
                    elif out_of_service_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= out_of_service_date <= week_end:
                            filtered_gyms.append(gym)
                    elif out_of_service_filter == "this_month":
                        if out_of_service_date.month == today.month and out_of_service_date.year == today.year:
                            filtered_gyms.append(gym)
                    elif out_of_service_filter == "custom" and out_of_service_start_date and out_of_service_end_date:
                        if out_of_service_start_date <= out_of_service_date <= out_of_service_end_date:
                            filtered_gyms.append(gym)

            # Always apply the filter when a valid filter is specified
            gyms = filtered_gyms

        # Sort by last call date (newest first). Items without last_call_date will be at the end
        gyms.sort(key=lambda x: x["last_call_date"] or datetime(1970, 1, 1), reverse=True)


        return {"gyms": gyms}

    except Exception as e:
        # Re-raise the exception so it gets properly handled
        raise

@router.post("/update-gym-status")
async def update_gym_status(
    data: UpdateGymStatus,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """Update the status of a gym and create a call log entry"""
    try:
        # Validate status
        valid_statuses = ["follow_up", "converted", "rejected", "no_response", "interested", "not_interested","out_of_service", "delegated"]
        if data.status not in valid_statuses:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"}
            )

        # Verify gym is assigned to telecaller OR delegated to telecaller
        result = await db.execute(
            select(GymAssignment).where(
                and_(
                    GymAssignment.gym_id == data.gym_id,
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active"
                )
            )
            .limit(1)
        )
        assignment = result.scalar_one_or_none()

        # Also check if this gym was delegated to this telecaller
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.gym_id == data.gym_id,
                    GymCallLogs.assigned_telecaller_id == telecaller.id,
                    GymCallLogs.call_status == "delegated"
                )
            )
            .order_by(desc(GymCallLogs.created_at))
            .limit(1)
        )
        delegated_log = result.scalar_one_or_none()

        if not assignment and not delegated_log:
            return JSONResponse(
                status_code=404,
                content={"detail": "Gym not found or not assigned to you"}
            )

        # Validate that remarks is mandatory
        if not data.call_form.remarks:
            return JSONResponse(
                status_code=400,
                content={"detail": "Remarks are required"}
            )

        # Create a call log entry using IST time
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = datetime.now(ist_tz)

        # Convert follow_up_date to IST for storage if provided
        follow_up_date_ist = None
        if data.follow_up_date:
            if isinstance(data.follow_up_date, datetime):
                # If it's already a datetime, assume it's in IST
                if data.follow_up_date.tzinfo is None:
                    # Naive datetime, treat as IST
                    follow_up_date_ist = ist_tz.localize(data.follow_up_date)
                else:
                    # Already timezone aware, convert to IST
                    follow_up_date_ist = data.follow_up_date.astimezone(ist_tz)
            else:
                # If it's a date, combine with time (start of day)
                follow_up_date_ist = ist_tz.localize(datetime.combine(data.follow_up_date, datetime.min.time()))

        # Calculate followup_alert time: 15 minutes before follow_up_date (in IST)
        followup_alert_ist = None
        if follow_up_date_ist and data.status in ['follow_up', 'follow_up_required']:
            # Calculate alert time: 15 minutes before follow-up
            from datetime import timedelta
            followup_alert_ist = follow_up_date_ist - timedelta(minutes=15)

        # Handle assigned_telecaller_id - default to self if not provided
        assigned_telecaller_id = data.assigned_telecaller_id if data.assigned_telecaller_id else None

        # Validate assigned telecaller is active (if assigning to someone else)
        if assigned_telecaller_id and assigned_telecaller_id != telecaller.id:
            result = await db.execute(
                select(Telecaller).where(
                    and_(
                        Telecaller.id == assigned_telecaller_id,
                        Telecaller.status == "active"
                    )
                )
            )
            assigned_telecaller = result.scalar_one_or_none()
            if not assigned_telecaller:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "Invalid telecaller assignment. Telecaller must be active."}
                )

        # Determine the actual call_status to save
        # If assigning to someone else with follow_up status, save as "delegated"
        call_status_to_save = data.status
        if assigned_telecaller_id and assigned_telecaller_id != telecaller.id and data.status == "follow_up":
            call_status_to_save = "delegated"

        # Log the incoming new_contact_number for debugging

        # Handle new_contact_number: if not provided or empty, preserve from last call log
        final_contact_number = data.call_form.new_contact_number
        if not final_contact_number or (isinstance(final_contact_number, str) and final_contact_number.strip() == ""):
            # Get the most recent call log for this gym that has a new_contact_number
            result = await db.execute(
                select(GymCallLogs).where(
                    and_(
                        GymCallLogs.gym_id == data.gym_id,
                        GymCallLogs.new_contact_number.isnot(None),
                        GymCallLogs.new_contact_number != ""
                    )
                ).order_by(desc(GymCallLogs.created_at))
                .limit(1)
            )
            last_log_with_number = result.scalar_one_or_none()

            if last_log_with_number:
                final_contact_number = last_log_with_number.new_contact_number
            else:
                final_contact_number = None


        call_log = GymCallLogs(
            gym_id=data.gym_id,
            telecaller_id=telecaller.id,
            manager_id=telecaller.manager_id,
            call_status=call_status_to_save,
            remarks=data.call_form.remarks,
            follow_up_date=follow_up_date_ist,
            followup_alert=followup_alert_ist,
            interest_level=data.call_form.interest_level,
            total_members=data.call_form.total_members,
            new_contact_number=final_contact_number,
            feature_explained=data.call_form.feature_explained,
            created_at=ist_now,
            assigned_telecaller_id=assigned_telecaller_id
        )

        db.add(call_log)
        await db.commit()
        await db.refresh(call_log)

        # If status is converted, create/update converted status entry
        if data.status == "converted" and data.converted_status:
            # Check if converted status already exists for this telecaller-gym combination
            result = await db.execute(
                select(ConvertedStatus).where(
                    and_(
                        ConvertedStatus.telecaller_id == telecaller.id,
                        ConvertedStatus.gym_id == data.gym_id
                    )
                )
                .order_by(desc(ConvertedStatus.created_at))
                .limit(1)
            )
            existing_converted = result.scalar_one_or_none()

            if existing_converted:
                # Update existing record
                existing_converted.gym_call_log_id = call_log.id
                existing_converted.document_uploaded = data.converted_status.document_uploaded
                existing_converted.membership_plan_created = data.converted_status.membership_plan_created
                existing_converted.session_created = data.converted_status.session_created
                existing_converted.daily_pass_created = data.converted_status.daily_pass_created
                existing_converted.gym_studio_images_uploaded = data.converted_status.gym_studio_images_uploaded
                existing_converted.agreement_signed = data.converted_status.agreement_signed
                existing_converted.registered_place = data.converted_status.registered_place
                existing_converted.updated_at = ist_now
            else:
                # Create new record
                converted_status = ConvertedStatus(
                    telecaller_id=telecaller.id,
                    gym_id=data.gym_id,
                    gym_call_log_id=call_log.id,
                    document_uploaded=data.converted_status.document_uploaded,
                    membership_plan_created=data.converted_status.membership_plan_created,
                    session_created=data.converted_status.session_created,
                    daily_pass_created=data.converted_status.daily_pass_created,
                    gym_studio_images_uploaded=data.converted_status.gym_studio_images_uploaded,
                    agreement_signed=data.converted_status.agreement_signed,
                    biometric_required=data.converted_status.biometric_required,
                    registered_place=data.converted_status.registered_place
                )
                db.add(converted_status)

            await db.commit()

        return JSONResponse(
            status_code=200,
            content={
                "message": "Gym status updated successfully",
                "log_id": call_log.id,
                "is_converted": data.status == "converted"
            }
        )

    except Exception as e:
        await db.rollback()
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )

@router.get("/gym/{gym_id}/follow-up-history")
async def get_gym_follow_up_history(
    gym_id: int,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get the complete follow-up history for a gym.
    This endpoint shows ALL follow-up entries from gym_call_logs table.
    No delegation or assignment rules apply - simply query by gym_id.
    """
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')
    try:
        # Get ALL follow-up call logs for this gym from gym_call_logs table
        # Only include follow-up related statuses
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.gym_id == gym_id,
                    GymCallLogs.call_status.in_(['follow_up', 'follow_up_required', 'delegated'])
                )
            ).order_by(desc(GymCallLogs.created_at))
        )
        call_logs = result.scalars().all()

        # Format the response
        history = []
        for log in call_logs:
            # The timestamp is already IST time stored as UTC, so we add IST timezone
            if log.created_at:
                created_at_utc = pytz.UTC.localize(log.created_at)
                created_at_ist = created_at_utc.astimezone(ist_tz)
            else:
                created_at_ist = None

            if log.follow_up_date:
                follow_up_utc = pytz.UTC.localize(log.follow_up_date)
                follow_up_date_ist = follow_up_utc.astimezone(ist_tz)
            else:
                follow_up_date_ist = None

            # Get telecaller info who created this log
            result = await db.execute(
                select(Telecaller).where(Telecaller.id == log.telecaller_id).limit(1)
            )
            creator_tc = result.scalar_one_or_none()

            # Get assigned telecaller info if this was delegated
            assigned_tc_info = None
            if log.assigned_telecaller_id:
                result = await db.execute(
                    select(Telecaller).where(Telecaller.id == log.assigned_telecaller_id).limit(1)
                )
                assigned_tc = result.scalar_one_or_none()
                if assigned_tc:
                    assigned_tc_info = {
                        "id": assigned_tc.id,
                        "name": assigned_tc.name
                    }

            history.append({
                "log_id": log.id,
                "call_status": log.call_status,
                "remarks": log.remarks,
                "follow_up_date": follow_up_date_ist.isoformat() if follow_up_date_ist else None,
                "created_at": created_at_ist.isoformat() if created_at_ist else None,
                "created_by": {
                    "id": creator_tc.id,
                    "name": creator_tc.name
                } if creator_tc else None,
                "assigned_to": assigned_tc_info,
                "manager_id": log.manager_id
            })

        return JSONResponse(
            status_code=200,
            content={"history": history}
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )

@router.get("/gym/{gym_id}/converted-status")
async def get_converted_status(
    gym_id: int,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
    ):
    """Get converted status details for a gym"""
    try:
        # Verify gym is assigned to telecaller OR delegated to telecaller
        result = await db.execute(
            select(GymAssignment).where(
                and_(
                    GymAssignment.gym_id == gym_id,
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active"
                )
            )
            .limit(1)
        )
        assignment = result.scalar_one_or_none()

        # Also check if this gym was ever delegated to this telecaller
        # Check if there's any call log where I was the assignee (assigned_telecaller_id == my id)
        # and it was created by someone else (telecaller_id != my id)
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.gym_id == gym_id,
                    GymCallLogs.assigned_telecaller_id == telecaller.id,
                    GymCallLogs.telecaller_id != telecaller.id
                )
            )
            .order_by(desc(GymCallLogs.created_at))
            .limit(1)
        )
        delegated_to_me = result.scalar_one_or_none()

        if not assignment and not delegated_to_me:
            return JSONResponse(
                status_code=404,
                content={"detail": "Gym not found or not assigned to you"}
            )

        # Get converted status for the CURRENT telecaller only
        # Each telecaller has their own conversion status record for proper attribution
        result = await db.execute(
            select(ConvertedStatus).where(
                and_(
                    ConvertedStatus.telecaller_id == telecaller.id,
                    ConvertedStatus.gym_id == gym_id
                )
            )
            .order_by(desc(ConvertedStatus.created_at))
            .limit(1)
        )
        converted_status = result.scalar_one_or_none()

        if not converted_status:
            return JSONResponse(
                status_code=404,
                content={"detail": "No converted status found for this gym"}
            )

        return JSONResponse(
            status_code=200,
            content={
                "id": converted_status.id,
                "document_uploaded": converted_status.document_uploaded,
                "membership_plan_created": converted_status.membership_plan_created,
                "session_created": converted_status.session_created,
                "daily_pass_created": converted_status.daily_pass_created,
                "gym_studio_images_uploaded": converted_status.gym_studio_images_uploaded,
                "agreement_signed": converted_status.agreement_signed,
                "biometric_required": converted_status.biometric_required,
                "registered_place": converted_status.registered_place,
                "created_at": converted_status.created_at.isoformat() if converted_status.created_at else None,
                "updated_at": converted_status.updated_at.isoformat() if converted_status.updated_at else None
            }
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )

@router.put("/gym/{gym_id}/converted-status")
async def update_converted_status(
    gym_id: int,
    data: ConvertedStatusData,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
    ):
    """Update converted status details for a gym"""
    try:
        # Verify gym is assigned to telecaller OR delegated to telecaller
        result = await db.execute(
            select(GymAssignment).where(
                and_(
                    GymAssignment.gym_id == gym_id,
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active"
                )
            )
            .limit(1)
        )
        assignment = result.scalar_one_or_none()

        # Also check if this gym was ever delegated to this telecaller
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.gym_id == gym_id,
                    GymCallLogs.assigned_telecaller_id == telecaller.id,
                    GymCallLogs.telecaller_id != telecaller.id
                )
            )
            .order_by(desc(GymCallLogs.created_at))
            .limit(1)
        )
        delegated_to_me = result.scalar_one_or_none()

        if not assignment and not delegated_to_me:
            return JSONResponse(
                status_code=404,
                content={"detail": "Gym not found or not assigned to you"}
            )

        # Get or create converted status for the CURRENT telecaller
        # IMPORTANT: Each telecaller should have their own ConvertedStatus record
        # regardless of delegation history. This ensures proper attribution of
        # conversion work to the telecaller actually performing it.
        result = await db.execute(
            select(ConvertedStatus).where(
                and_(
                    ConvertedStatus.telecaller_id == telecaller.id,
                    ConvertedStatus.gym_id == gym_id
                )
            )
            .order_by(desc(ConvertedStatus.created_at))
            .limit(1)
        )
        converted_status = result.scalar_one_or_none()

        if converted_status:
            # Update existing record for current telecaller
            converted_status.document_uploaded = data.document_uploaded
            converted_status.membership_plan_created = data.membership_plan_created
            converted_status.session_created = data.session_created
            converted_status.daily_pass_created = data.daily_pass_created
            converted_status.gym_studio_images_uploaded = data.gym_studio_images_uploaded
            converted_status.agreement_signed = data.agreement_signed
            converted_status.biometric_required = data.biometric_required
            converted_status.registered_place = data.registered_place
            converted_status.updated_at = datetime.utcnow()
        else:
            # Create new record
            converted_status = ConvertedStatus(
                telecaller_id=telecaller.id,
                gym_id=gym_id,
                document_uploaded=data.document_uploaded,
                membership_plan_created=data.membership_plan_created,
                session_created=data.session_created,
                daily_pass_created=data.daily_pass_created,
                gym_studio_images_uploaded=data.gym_studio_images_uploaded,
                agreement_signed=data.agreement_signed,
                biometric_required=data.biometric_required,
                registered_place=data.registered_place
            )
            db.add(converted_status)

        await db.commit()
        await db.refresh(converted_status)

        return JSONResponse(
            status_code=200,
            content={"message": "Converted status updated successfully"}
        )

    except Exception as e:
        await db.rollback()
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )

# Pydantic model for updating follow-up alert
class UpdateFollowUpAlertRequest(BaseModel):
    log_id: int
    gym_id: int

@router.get("/follow-up-alerts")
async def get_follow_up_alerts(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        # Get current time in IST
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = datetime.now(ist_tz)

        # Get all active assignments for this telecaller
        result = await db.execute(
            select(distinct(GymAssignment.gym_id)).where(
                and_(
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active"
                )
            )
        )
        assigned_gym_ids_result = result.all()
        assigned_gym_ids = [g[0] for g in assigned_gym_ids_result]

        if not assigned_gym_ids:
            return {"alerts": []}

        # Get the latest call log for each gym that has a follow-up alert due
        # We need to check if the latest status is follow_up and alert time has passed
        alerts = []
        for gym_id in assigned_gym_ids:
            # Get the latest call log for this gym
            result = await db.execute(
                select(GymCallLogs).where(
                    and_(
                        GymCallLogs.gym_id == gym_id,
                        GymCallLogs.telecaller_id == telecaller.id
                    )
                ).order_by(desc(GymCallLogs.created_at))
                .limit(1)
            )
            latest_log = result.scalar_one_or_none()

            if not latest_log:
                continue

            # Check if latest status is follow_up and alert is due
            # followup_alert is stored in IST, compare directly with ist_now
            # Handle both timezone-aware and naive datetimes (naive = IST)
            alert_time = latest_log.followup_alert
            if alert_time and alert_time.tzinfo is None:
                # Naive datetime, treat as IST
                alert_time = ist_tz.localize(alert_time)

            if (latest_log.call_status in ['follow_up', 'follow_up_required'] and
                alert_time and
                alert_time <= ist_now):

                # Get gym details
                result = await db.execute(
                    select(GymDatabase).where(GymDatabase.id == gym_id)
                    .limit(1)
                )
                gym = result.scalar_one_or_none()
                if not gym:
                    continue

                # Alert time is already in IST, format with timezone info
                alert_time_ist = alert_time

                # Follow-up time is already in IST, format with timezone info
                follow_up_time_ist = latest_log.follow_up_date
                # If datetime is naive (no timezone), assume it's IST and add timezone
                if follow_up_time_ist and follow_up_time_ist.tzinfo is None:
                    follow_up_time_ist = ist_tz.localize(follow_up_time_ist)

                # Determine phone number: Check gym_call_logs for new_contact_number first
                # Get the most recent call log entry for this gym_id across all telecallers
                result = await db.execute(
                    select(GymCallLogs).where(
                        GymCallLogs.gym_id == gym_id
                    ).order_by(desc(GymCallLogs.created_at))
                    .limit(1)
                )
                most_recent_call_log_for_alert = result.scalar_one_or_none()

                # Use new_contact_number from most recent call log if available, otherwise fall back to gym.contact_phone
                phone_number = None
                phone_source = None  # 'call_logs' or 'database'
                if most_recent_call_log_for_alert and most_recent_call_log_for_alert.new_contact_number:
                    phone_number = most_recent_call_log_for_alert.new_contact_number
                    phone_source = 'call_logs'
                else:
                    phone_number = gym.contact_phone
                    phone_source = 'database'

                alerts.append({
                    "log_id": latest_log.id,
                    "gym_id": gym_id,
                    "gym_name": gym.gym_name or f"Gym {gym_id}",
                    "contact_person": gym.contact_person,
                    "phone": phone_number,
                    "phone_source": phone_source,
                    "alert_time": alert_time_ist.isoformat() if alert_time_ist else None,
                    "follow_up_time": follow_up_time_ist.isoformat() if follow_up_time_ist else None,
                    "remarks": latest_log.remarks
                })

        # Sort by alert time (oldest first) to handle multiple missed follow-ups
        alerts.sort(key=lambda x: x["alert_time"])

        return {"alerts": alerts}

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )


# ============================================================================
# OPTIMIZED FOLLOW-UP ALERTS ENDPOINT - Single query, no N+1 problem
# ============================================================================

@router.get("/follow-up-alerts-optimized")
async def get_follow_up_alerts_optimized(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):

    try:
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = datetime.now(ist_tz)

        # Subquery to get the latest call log for each gym
        latest_log_subq = select(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).where(
            GymCallLogs.telecaller_id == telecaller.id
        ).group_by(GymCallLogs.gym_id).subquery()

        # Single query with JOINs to get all alert data at once
        query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymCallLogs.id.label('log_id'),
            GymCallLogs.call_status,
            GymCallLogs.followup_alert,
            GymCallLogs.follow_up_date,
            GymCallLogs.remarks
        ).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).join(
            latest_log_subq,
            and_(
                GymAssignment.gym_id == latest_log_subq.c.gym_id,
                GymAssignment.telecaller_id == telecaller.id,
                GymAssignment.status == "active"
            )
        ).join(
            GymCallLogs,
            and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created,
                GymCallLogs.telecaller_id == telecaller.id
            )
        ).where(
            and_(
                GymCallLogs.call_status.in_(['follow_up', 'follow_up_required']),
                GymCallLogs.followup_alert.isnot(None)
            )
        )

        result = await db.execute(query)
        rows = result.all()

        alerts = []
        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, log_id, call_status,
             followup_alert, follow_up_date, remarks) = row

            # Handle timezone for alert time
            alert_time = followup_alert
            if alert_time and alert_time.tzinfo is None:
                alert_time = ist_tz.localize(alert_time)

            # Check if alert is due (filter at application level for datetime comparison)
            if not alert_time or alert_time > ist_now:
                continue

            # Handle timezone for follow-up time
            follow_up_time_ist = follow_up_date
            if follow_up_time_ist and follow_up_time_ist.tzinfo is None:
                follow_up_time_ist = ist_tz.localize(follow_up_time_ist)

            # For new_contact_number, check if latest log has it
            phone_number = contact_phone
            phone_source = 'database'

            alerts.append({
                "log_id": log_id,
                "gym_id": gym_id,
                "gym_name": gym_name or f"Gym {gym_id}",
                "contact_person": contact_person,
                "phone": phone_number,
                "phone_source": phone_source,
                "alert_time": alert_time.isoformat() if alert_time else None,
                "follow_up_time": follow_up_time_ist.isoformat() if follow_up_time_ist else None,
                "remarks": remarks
            })

        # Sort by alert time (oldest first)
        alerts.sort(key=lambda x: x["alert_time"])

        return {"alerts": alerts}

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )


@router.put("/follow-up-alert/snooze")
async def snooze_follow_up_alert(
    request_data: UpdateFollowUpAlertRequest,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
    ):

    try:
        # Get the call log
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.id == request_data.log_id,
                    GymCallLogs.gym_id == request_data.gym_id,
                    GymCallLogs.telecaller_id == telecaller.id
                )
            )
            .limit(1)
        )
        call_log = result.scalar_one_or_none()

        if not call_log:
            return JSONResponse(
                status_code=404,
                content={"detail": "Call log not found"}
            )

        # Only snooze if the current status is still follow_up
        if call_log.call_status not in ['follow_up', 'follow_up_required']:
            return JSONResponse(
                status_code=400,
                content={"detail": "Cannot snooze alert for non follow-up status"}
            )

        # Get current time in IST
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = datetime.now(ist_tz)

        # Calculate new alert time: add 30 minutes to current time (in IST)
        new_alert_time_ist = ist_now + timedelta(minutes=30)

        # Update the followup_alert (stored in IST)
        call_log.followup_alert = new_alert_time_ist
        await db.commit()
        await db.refresh(call_log)

        return JSONResponse(
            status_code=200,
            content={
                "message": "Alert snoozed successfully",
                "new_alert_time": new_alert_time_ist.isoformat()
            }
        )

    except Exception as e:
        await db.rollback()
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )


class BulkSnoozeRequest(BaseModel):
    log_ids: List[int]

@router.put("/follow-up-alert/bulk-snooze")
async def bulk_snooze_follow_up_alerts(
    request_data: BulkSnoozeRequest,
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    try:
        if not request_data.log_ids:
            return JSONResponse(status_code=400, content={"detail": "No log IDs provided"})

        # Fetch all matching call logs in one query
        result = await db.execute(
            select(GymCallLogs).where(
                and_(
                    GymCallLogs.id.in_(request_data.log_ids),
                    GymCallLogs.telecaller_id == telecaller.id,
                    GymCallLogs.call_status.in_(['follow_up', 'follow_up_required'])
                )
            )
        )
        call_logs = result.scalars().all()

        if not call_logs:
            return JSONResponse(status_code=404, content={"detail": "No matching follow-up logs found"})

        # Calculate new alert time once
        ist_tz = pytz.timezone('Asia/Kolkata')
        new_alert_time_ist = datetime.now(ist_tz) + timedelta(minutes=30)

        # Update all logs
        snoozed_ids = []
        for log in call_logs:
            log.followup_alert = new_alert_time_ist
            snoozed_ids.append(log.id)

        await db.commit()

        return JSONResponse(
            status_code=200
        )

    except Exception as e:
        await db.rollback()
        return JSONResponse(status_code=500, content={"detail": f"Internal server error: {str(e)}"})


@router.get("/team-telecallers")
async def get_team_telecallers(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
    ):
    """Get list of all active telecallers (for delegation)"""
    try:
        result = await db.execute(
            select(Telecaller).where(Telecaller.status == "active")
        )
        telecallers = result.scalars().all()

        return {
            "telecallers": [
                {
                    "id": t.id,
                    "name": t.name,
                    "mobile_number": t.mobile_number,
                    "language_known": t.language_known,
                    "is_me": t.id == telecaller.id
                }
                for t in telecallers
            ]
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )


# ============================================================================
# OPTIMIZED ENDPOINTS - Single query, no N+1 problem, no blocking loops
# ============================================================================

class GymListItemOptimized(BaseModel):
    gym_id: int
    gym_name: Optional[str] = None
    contact_person: Optional[str] = None
    contact_number: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    target_date: Optional[str] = None
    assigned_at: Optional[str] = None
    days_since_assigned: Optional[int] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    isprime: Optional[int] = None
    location: Optional[str] = None

class CursorPaginatedResponse(BaseModel):
    gyms: List[GymListItemOptimized]
    next_cursor: Optional[str] = None  # Encoded cursor for next page
    has_more: bool
    page_size: int
    # Include total count optionally (can be expensive for large datasets)
    total_count: Optional[int] = None


@router.get("/pending-gyms-optimized", response_model=CursorPaginatedResponse)
async def get_pending_gyms_optimized(
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination (from previous response)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count (slower for large datasets)"),
    target_date_filter: Optional[str] = Query(None, description="Filter by target date: today, this_week, this_month, custom, overdue"),
    target_start_date: Optional[date] = Query(None, description="Target start date for custom filter"),
    target_end_date: Optional[date] = Query(None, description="Target end date for custom filter"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    sort_by: Optional[str] = Query("target_date", description="Sort by: target_date, assigned_at, gym_name"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc, desc"),
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):

    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        # Decode cursor if provided
        last_target_date = None
        last_assigned_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                # Decode the base64 cursor
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_target_date_str = cursor_data.get("target_date")
                last_assigned_at_str = cursor_data.get("assigned_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_target_date_str:
                    last_target_date = date.fromisoformat(last_target_date_str)
                if last_assigned_at_str:
                    last_assigned_at = datetime.fromisoformat(last_assigned_at_str)
            except Exception as e:
                pass

        # Determine sort column
        sort_column = GymAssignment.target_date if sort_by == "target_date" else GymAssignment.assigned_at if sort_by == "assigned_at" else GymDatabase.gym_name

        # Build the base query with all filters
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymAssignment.target_date,
            GymAssignment.assigned_at
        ).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).where(
            and_(
                GymAssignment.telecaller_id == telecaller.id,
                GymAssignment.status == "active",
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        GymCallLogs.telecaller_id == telecaller.id
                    )
                )
            )
        )

        # Apply target date filter at database level
        if target_date_filter and target_date_filter != "all":
            if target_date_filter == "today":
                base_query = base_query.where(GymAssignment.target_date == today)
            elif target_date_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                base_query = base_query.where(GymAssignment.target_date.between(week_start, week_end))
            elif target_date_filter == "this_month":
                base_query = base_query.where(
                    and_(
                        func.extract('year', GymAssignment.target_date) == today.year,
                        func.extract('month', GymAssignment.target_date) == today.month
                    )
                )
            elif target_date_filter == "overdue":
                base_query = base_query.where(GymAssignment.target_date < today)
            elif target_date_filter == "custom" and target_start_date and target_end_date:
                base_query = base_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))

        # Apply search filter at database level
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION: Apply WHERE conditions instead of OFFSET
        # This is much faster than OFFSET for large datasets
        if cursor and (last_target_date is not None or last_assigned_at is not None or last_gym_name is not None):
            if sort_by == "target_date":
                if last_target_date is not None:
                    if sort_order == "desc":
                        # For DESC: get rows where target_date < last_target_date
                        # OR (target_date = last_target_date AND gym_id < last_gym_id)
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date < last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        # For ASC: get rows where target_date > last_target_date
                        # OR (target_date = last_target_date AND gym_id > last_gym_id)
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date > last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "assigned_at":
                if last_assigned_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at < last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at > last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting at database level
        # Always include gym_id as secondary sort for stable pagination
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply cursor-based pagination: Use LIMIT + 1 to check if there are more results
        paginated_query = base_query.limit(page_size + 1)

        # Execute the main query (single query)
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]  # Remove the extra row used for checking

        # Process results and build response
        gyms = []
        next_cursor_data = None

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, target_date, assigned_at) = row

            # Calculate days since assigned (lightweight calculation)
            days_since_assigned = None
            if assigned_at:
                if assigned_at.tzinfo is None:
                    assigned_at_ist = ist_tz.localize(assigned_at)
                else:
                    assigned_at_ist = assigned_at.astimezone(ist_tz)
                days_since_assigned = (ist_tz.localize(datetime.now()) - assigned_at_ist).days

            gym_item = GymListItemOptimized(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=contact_phone,
                city=city,
                address=address,
                target_date=target_date.isoformat() if target_date else None,
                assigned_at=assigned_at.isoformat() if assigned_at else None,
                days_since_assigned=days_since_assigned,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location
            )
            gyms.append(gym_item)

            # Store the last row for next cursor
            next_cursor_data = {
                "target_date": target_date.isoformat() if target_date else None,
                "assigned_at": assigned_at.isoformat() if assigned_at else None,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        # Get total count only if requested (this is an extra query)
        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymAssignment
            ).join(
                GymDatabase,
                GymAssignment.gym_id == GymDatabase.id
            ).where(
                and_(
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active",
                    ~GymAssignment.gym_id.in_(
                        select(GymCallLogs.gym_id).where(
                            GymCallLogs.telecaller_id == telecaller.id
                        )
                    )
                )
            )

            # Apply same filters to count query
            if target_date_filter and target_date_filter != "all":
                if target_date_filter == "today":
                    count_query = count_query.where(GymAssignment.target_date == today)
                elif target_date_filter == "this_week":
                    week_start = today - timedelta(days=today.weekday())
                    week_end = week_start + timedelta(days=6)
                    count_query = count_query.where(GymAssignment.target_date.between(week_start, week_end))
                elif target_date_filter == "this_month":
                    count_query = count_query.where(
                        and_(
                            func.extract('year', GymAssignment.target_date) == today.year,
                            func.extract('month', GymAssignment.target_date) == today.month
                        )
                    )
                elif target_date_filter == "overdue":
                    count_query = count_query.where(GymAssignment.target_date < today)
                elif target_date_filter == "custom" and target_start_date and target_end_date:
                    count_query = count_query.where(GymAssignment.target_date.between(target_start_date, target_end_date))

            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return CursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/today-gyms-optimized", response_model=CursorPaginatedResponse)
async def get_today_gyms_optimized(
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination (from previous response)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count (slower for large datasets)"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    sort_by: Optional[str] = Query("target_date", description="Sort by: target_date, assigned_at, gym_name"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc, desc"),
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """
    OPTIMIZED: Get gyms for Today's call (Today tab) with CURSOR-BASED pagination.

    A gym qualifies for Today tab if:
    1. It exists in gym_assignments with telecaller_id = current user AND status = 'active'
    2. AND gym_id does NOT exist in gym_call_logs for that telecaller
    3. AND gym_assignments.target_date = today's date

    Uses a SINGLE optimized query with cursor-based pagination for maximum efficiency.
    No blocking loops - all processing done at database level.
    """
    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        # Decode cursor if provided
        last_target_date = None
        last_assigned_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_target_date_str = cursor_data.get("target_date")
                last_assigned_at_str = cursor_data.get("assigned_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_target_date_str:
                    last_target_date = date.fromisoformat(last_target_date_str)
                if last_assigned_at_str:
                    last_assigned_at = datetime.fromisoformat(last_assigned_at_str)
            except Exception as e:
                pass

        # Determine sort column
        sort_column = GymAssignment.target_date if sort_by == "target_date" else GymAssignment.assigned_at if sort_by == "assigned_at" else GymDatabase.gym_name

        # Build the base query
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymAssignment.target_date,
            GymAssignment.assigned_at
        ).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).where(
            and_(
                GymAssignment.telecaller_id == telecaller.id,
                GymAssignment.status == "active",
                GymAssignment.target_date == today,
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        GymCallLogs.telecaller_id == telecaller.id
                    )
                )
            )
        )

        # Apply search filter at database level
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION: Apply WHERE conditions instead of OFFSET
        if cursor and (last_target_date is not None or last_assigned_at is not None or last_gym_name is not None):
            if sort_by == "target_date":
                if last_target_date is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date < last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymAssignment.target_date > last_target_date,
                                and_(
                                    GymAssignment.target_date == last_target_date,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "assigned_at":
                if last_assigned_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at < last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymAssignment.assigned_at > last_assigned_at,
                                and_(
                                    GymAssignment.assigned_at == last_assigned_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting at database level
        # Always include gym_id as secondary sort for stable pagination
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply cursor-based pagination: Use LIMIT + 1 to check if there are more results
        paginated_query = base_query.limit(page_size + 1)

        # Execute the main query (single query)
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]

        # Process results
        gyms = []
        next_cursor_data = None

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, target_date, assigned_at) = row

            # Calculate days since assigned
            days_since_assigned = None
            if assigned_at:
                if assigned_at.tzinfo is None:
                    assigned_at_ist = ist_tz.localize(assigned_at)
                else:
                    assigned_at_ist = assigned_at.astimezone(ist_tz)
                days_since_assigned = (ist_tz.localize(datetime.now()) - assigned_at_ist).days

            gym_item = GymListItemOptimized(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=contact_phone,
                city=city,
                address=address,
                target_date=target_date.isoformat() if target_date else None,
                assigned_at=assigned_at.isoformat() if assigned_at else None,
                days_since_assigned=days_since_assigned,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location
            )
            gyms.append(gym_item)

            # Store the last row for next cursor
            next_cursor_data = {
                "target_date": target_date.isoformat() if target_date else None,
                "assigned_at": assigned_at.isoformat() if assigned_at else None,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        # Get total count only if requested
        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymAssignment
            ).join(
                GymDatabase,
                GymAssignment.gym_id == GymDatabase.id
            ).where(
                and_(
                    GymAssignment.telecaller_id == telecaller.id,
                    GymAssignment.status == "active",
                    GymAssignment.target_date == today,
                    ~GymAssignment.gym_id.in_(
                        select(GymCallLogs.gym_id).where(
                            GymCallLogs.telecaller_id == telecaller.id
                        )
                    )
                )
            )

            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return CursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pending-count-optimized")
async def get_pending_count_optimized(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):

    try:
        query = select(func.count()).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).where(
            and_(
                GymAssignment.telecaller_id == telecaller.id,
                GymAssignment.status == "active",
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        GymCallLogs.telecaller_id == telecaller.id
                    )
                )
            )
        )

        result = await db.execute(query)
        count = result.scalar() or 0

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/today-count-optimized")
async def get_today_count_optimized(
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """
    OPTIMIZED: Get the count of today's gyms (useful for badges/counters).
    Uses a single COUNT query for efficiency.
    """
    try:
        today = date.today()

        query = select(func.count()).select_from(
            GymAssignment
        ).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).where(
            and_(
                GymAssignment.telecaller_id == telecaller.id,
                GymAssignment.status == "active",
                GymAssignment.target_date == today,
                ~GymAssignment.gym_id.in_(
                    select(GymCallLogs.gym_id).where(
                        GymCallLogs.telecaller_id == telecaller.id
                    )
                )
            )
        )

        result = await db.execute(query)
        count = result.scalar() or 0

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ============================================================================
# FOLLOW-UP TAB OPTIMIZED ENDPOINT
# ============================================================================

class FollowUpGymListItemOptimized(BaseModel):
    gym_id: int
    gym_name: Optional[str] = None
    contact_person: Optional[str] = None
    contact_number: Optional[str] = None
    phone_number_source: Optional[str] = None  # 'call_log' or 'gym_database'
    city: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    zone: Optional[str] = None
    isprime: Optional[int] = None
    location: Optional[str] = None
    follow_up_date: Optional[str] = None  # From gym_call_logs
    last_call_date: Optional[str] = None  # From gym_call_logs.created_at
    interest_level: Optional[str] = None
    remarks: Optional[str] = None
    is_delegated: bool = False  # True if this was delegated to me by someone else
    delegated_by: Optional[str] = None  # Name of telecaller who delegated this to me


class FollowUpCursorPaginatedResponse(BaseModel):
    gyms: List[FollowUpGymListItemOptimized]
    next_cursor: Optional[str] = None
    has_more: bool
    page_size: int
    total_count: Optional[int] = None


@router.get("/follow-up-gyms-optimized", response_model=FollowUpCursorPaginatedResponse)
async def get_follow_up_gyms_optimized(
    cursor: Optional[str] = Query(None, description="Encoded cursor for pagination (from previous response)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    include_total_count: bool = Query(False, description="Include total count (slower for large datasets)"),
    follow_up_filter: Optional[str] = Query(None, description="Filter by follow-up date: today, this_week, this_month, overdue, custom"),
    follow_up_start_date: Optional[date] = Query(None, description="Follow-up start date for custom filter"),
    follow_up_end_date: Optional[date] = Query(None, description="Follow-up end date for custom filter"),
    search_query: Optional[str] = Query(None, description="Search by gym name or contact person"),
    sort_by: Optional[str] = Query("follow_up_date", description="Sort by: follow_up_date, last_call_date, gym_name"),
    sort_order: Optional[str] = Query("asc", description="Sort order: asc, desc"),
    telecaller: Telecaller = Depends(get_current_telecaller),
    db: AsyncSession = Depends(get_async_db)
):
    """
    OPTIMIZED: Get gyms for Follow-Up tab with CURSOR-BASED pagination.

    A gym appears in the Follow-Up tab if EITHER condition is met:
    1. The LATEST gym_call_logs entry for that gym has call_status = 'follow_up' for the given telecaller
    2. The LATEST gym_call_logs entry has call_status = 'delegated' AND assigned_telecaller_id = current telecaller

    Only considers the LATEST entry per gym_id from gym_call_logs table.
    Uses a SINGLE optimized query with cursor-based pagination.
    """
    try:
        import base64
        import json
        from urllib.parse import unquote, quote

        ist_tz = pytz.timezone('Asia/Kolkata')
        today = date.today()

        last_follow_up_date = None
        last_created_at = None
        last_gym_id = None
        last_gym_name = None

        if cursor:
            try:
                cursor_data = json.loads(base64.b64decode(unquote(cursor)))
                last_follow_up_date_str = cursor_data.get("follow_up_date")
                last_created_at_str = cursor_data.get("created_at")
                last_gym_id = cursor_data.get("gym_id")
                last_gym_name = cursor_data.get("gym_name")

                if last_follow_up_date_str:
                    last_follow_up_date = datetime.fromisoformat(last_follow_up_date_str)
                if last_created_at_str:
                    last_created_at = datetime.fromisoformat(last_created_at_str)
            except Exception as e:
                pass

        # Determine sort column
        if sort_by == "follow_up_date":
            sort_column = GymCallLogs.follow_up_date
        elif sort_by == "last_call_date":
            sort_column = GymCallLogs.created_at
        else:  # gym_name
            sort_column = GymDatabase.gym_name

        # ============================================================================
        # MAIN QUERY: Get latest call logs per gym that qualify for follow-up
        # ============================================================================
        # A gym qualifies if:
        # 1. Latest log has call_status='follow_up' AND telecaller_id = current user
        # 2. Latest log has call_status='delegated' AND assigned_telecaller_id = current user

        # First, get the latest log for each gym
        latest_log_subq = select(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).group_by(GymCallLogs.gym_id).subquery()

        # Build the main query with JOINs
        base_query = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.city,
            GymDatabase.address,
            GymDatabase.area,
            GymDatabase.zone,
            GymDatabase.isprime,
            GymDatabase.location,
            GymCallLogs.follow_up_date,
            GymCallLogs.created_at,
            GymCallLogs.interest_level,
            GymCallLogs.remarks,
            GymCallLogs.telecaller_id.label('log_telecaller_id'),
            GymCallLogs.assigned_telecaller_id,
            GymCallLogs.new_contact_number  # Add new_contact_number for phone resolution logic
        ).select_from(
            GymDatabase
        ).join(
            GymCallLogs,
            GymDatabase.id == GymCallLogs.gym_id
        ).join(
            latest_log_subq,
            and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created
            )
        ).where(
            or_(
                # Condition 1: My own follow-up
                and_(
                    GymCallLogs.telecaller_id == telecaller.id,
                    GymCallLogs.call_status == 'follow_up'
                ),
                # Condition 2: Delegated to me by someone else
                and_(
                    GymCallLogs.assigned_telecaller_id == telecaller.id,
                    GymCallLogs.call_status == 'delegated',
                    GymCallLogs.telecaller_id != telecaller.id  # Created by someone else
                )
            )
        )

        # Apply follow-up date filter at database level
        if follow_up_filter and follow_up_filter != "all":
            if follow_up_filter == "today":
                base_query = base_query.where(
                    func.date(GymCallLogs.follow_up_date) == today
                )
            elif follow_up_filter == "this_week":
                week_start = today - timedelta(days=today.weekday())
                week_end = week_start + timedelta(days=6)
                base_query = base_query.where(
                    func.date(GymCallLogs.follow_up_date).between(week_start, week_end)
                )
            elif follow_up_filter == "this_month":
                base_query = base_query.where(
                    and_(
                        func.extract('year', GymCallLogs.follow_up_date) == today.year,
                        func.extract('month', GymCallLogs.follow_up_date) == today.month
                    )
                )
            elif follow_up_filter == "overdue":
                base_query = base_query.where(
                    func.date(GymCallLogs.follow_up_date) < today
                )
            elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                base_query = base_query.where(
                    func.date(GymCallLogs.follow_up_date).between(follow_up_start_date, follow_up_end_date)
                )

        # Apply search filter at database level
        if search_query:
            search_pattern = f"%{search_query}%"
            base_query = base_query.where(
                or_(
                    GymDatabase.gym_name.ilike(search_pattern),
                    GymDatabase.contact_person.ilike(search_pattern)
                )
            )

        # CURSOR-BASED PAGINATION: Apply WHERE conditions instead of OFFSET
        if cursor and (last_follow_up_date is not None or last_created_at is not None or last_gym_name is not None):
            if sort_by == "follow_up_date":
                if last_follow_up_date is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.follow_up_date < last_follow_up_date,
                                and_(
                                    GymCallLogs.follow_up_date == last_follow_up_date,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.follow_up_date > last_follow_up_date,
                                and_(
                                    GymCallLogs.follow_up_date == last_follow_up_date,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "last_call_date":
                if last_created_at is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at < last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id < last_gym_id if last_gym_id else True
                                )
                            )
                        )
                    else:
                        base_query = base_query.where(
                            or_(
                                GymCallLogs.created_at > last_created_at,
                                and_(
                                    GymCallLogs.created_at == last_created_at,
                                    GymDatabase.id > last_gym_id if last_gym_id else True
                                )
                            )
                        )
            elif sort_by == "gym_name":
                if last_gym_name is not None:
                    if sort_order == "desc":
                        base_query = base_query.where(GymDatabase.gym_name < last_gym_name)
                    else:
                        base_query = base_query.where(GymDatabase.gym_name > last_gym_name)

        # Apply sorting at database level
        # Always include gym_id as secondary sort for stable pagination
        if sort_order == "desc":
            base_query = base_query.order_by(desc(sort_column), desc(GymDatabase.id))
        else:
            base_query = base_query.order_by(sort_column, GymDatabase.id)

        # Apply cursor-based pagination: Use LIMIT + 1 to check if there are more results
        paginated_query = base_query.limit(page_size + 1)

        # Execute the main query (single query)
        result = await db.execute(paginated_query)
        rows = result.all()

        # Check if there are more results
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]

        # Process results
        gyms = []
        next_cursor_data = None

        # Get telecaller names for delegated items (batch query for efficiency)
        delegated_telecaller_ids = list(set([
            row.log_telecaller_id for row in rows
            if row.log_telecaller_id != telecaller.id
        ]))

        delegated_by_names = {}
        if delegated_telecaller_ids:
            result_names = await db.execute(
                select(Telecaller.id, Telecaller.name).where(
                    Telecaller.id.in_(delegated_telecaller_ids)
                )
            )
            delegated_by_names = {row[0]: row[1] for row in result_names.all()}

        for row in rows:
            (gym_id, gym_name, contact_person, contact_phone, city, address,
             area, zone, isprime, location, follow_up_date, created_at,
             interest_level, remarks, log_telecaller_id, assigned_telecaller_id, new_contact_number) = row

            # Determine if this was delegated to me
            is_delegated = (log_telecaller_id != telecaller.id and assigned_telecaller_id == telecaller.id)
            delegated_by = delegated_by_names.get(log_telecaller_id) if is_delegated else None

            # Phone number resolution logic:
            # Priority 1: new_contact_number from the latest call log (if not null/empty)
            # Priority 2: contact_phone from gym_database
            final_contact_number = contact_phone
            phone_number_source = "gym_database"  # Default source
            if new_contact_number and new_contact_number.strip():
                final_contact_number = new_contact_number.strip()
                phone_number_source = "call_log"

            # Format follow_up_date
            follow_up_date_str = None
            if follow_up_date:
                if follow_up_date.tzinfo is None:
                    follow_up_date_ist = ist_tz.localize(follow_up_date)
                else:
                    follow_up_date_ist = follow_up_date.astimezone(ist_tz)
                follow_up_date_str = follow_up_date_ist.isoformat()

            # Format created_at (last_call_date)
            last_call_date_str = None
            if created_at:
                if created_at.tzinfo is None:
                    created_at_ist = ist_tz.localize(created_at)
                else:
                    created_at_ist = created_at.astimezone(ist_tz)
                last_call_date_str = created_at_ist.isoformat()

            gym_item = FollowUpGymListItemOptimized(
                gym_id=gym_id,
                gym_name=gym_name,
                contact_person=contact_person,
                contact_number=final_contact_number,
                phone_number_source=phone_number_source,
                city=city,
                address=address,
                area=area,
                zone=zone,
                isprime=isprime,
                location=location,
                follow_up_date=follow_up_date_str,
                last_call_date=last_call_date_str,
                interest_level=interest_level,
                remarks=remarks,
                is_delegated=is_delegated,
                delegated_by=delegated_by
            )
            gyms.append(gym_item)

            # Store the last row for next cursor
            next_cursor_data = {
                "follow_up_date": follow_up_date_str,
                "created_at": last_call_date_str,
                "gym_id": gym_id,
                "gym_name": gym_name
            }

        # Encode next cursor
        next_cursor = None
        if has_more and next_cursor_data:
            cursor_json = json.dumps(next_cursor_data)
            next_cursor = quote(base64.b64encode(cursor_json.encode()).decode())

        total_count = None
        if include_total_count:
            count_query = select(func.count(GymDatabase.id)).select_from(
                GymDatabase
            ).join(
                GymCallLogs,
                GymDatabase.id == GymCallLogs.gym_id
            ).join(
                latest_log_subq,
                and_(
                    GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                    GymCallLogs.created_at == latest_log_subq.c.max_created
                )
            ).where(
                or_(
                    and_(
                        GymCallLogs.telecaller_id == telecaller.id,
                        GymCallLogs.call_status == 'follow_up'
                    ),
                    and_(
                        GymCallLogs.assigned_telecaller_id == telecaller.id,
                        GymCallLogs.call_status == 'delegated',
                        GymCallLogs.telecaller_id != telecaller.id
                    )
                )
            )

            # Apply follow-up date filter
            if follow_up_filter and follow_up_filter != "all":
                if follow_up_filter == "today":
                    count_query = count_query.where(
                        func.date(GymCallLogs.follow_up_date) == today
                    )
                elif follow_up_filter == "this_week":
                    week_start = today - timedelta(days=today.weekday())
                    week_end = week_start + timedelta(days=6)
                    count_query = count_query.where(
                        func.date(GymCallLogs.follow_up_date).between(week_start, week_end)
                    )
                elif follow_up_filter == "this_month":
                    count_query = count_query.where(
                        and_(
                            func.extract('year', GymCallLogs.follow_up_date) == today.year,
                            func.extract('month', GymCallLogs.follow_up_date) == today.month
                        )
                    )
                elif follow_up_filter == "overdue":
                    count_query = count_query.where(
                        func.date(GymCallLogs.follow_up_date) < today
                    )
                elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                    count_query = count_query.where(
                        func.date(GymCallLogs.follow_up_date).between(follow_up_start_date, follow_up_end_date)
                    )

            if search_query:
                search_pattern = f"%{search_query}%"
                count_query = count_query.where(
                    or_(
                        GymDatabase.gym_name.ilike(search_pattern),
                        GymDatabase.contact_person.ilike(search_pattern)
                    )
                )

            count_result = await db.execute(count_query)
            total_count = count_result.scalar() or 0

        return FollowUpCursorPaginatedResponse(
            gyms=gyms,
            next_cursor=next_cursor,
            has_more=has_more,
            page_size=page_size,
            total_count=total_count
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")