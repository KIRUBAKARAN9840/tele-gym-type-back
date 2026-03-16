from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.marketingmodels import Attendance, Executives, Managers, ManagerAttendance
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from sqlalchemy import and_, or_, desc, func
import pytz

router = APIRouter(tags=["Attendance"], prefix="/marketing/attendance")

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Pydantic models for request/response
class PunchInRequest(BaseModel):
    employee_id: str
    manager_id: str
    employee_name: str
    manager_name: str
    gym_name: str
    gym_address: Optional[str] = None
    punchin_location: Optional[dict] = None

class PunchOutRequest(BaseModel):
    attendance_id: int
    punchout_location: Optional[dict] = None

class AttendanceResponse(BaseModel):
    id: int
    employee_id: str
    manager_id: str
    employee_name: str
    manager_name: str
    gym_name: str
    gym_address: Optional[str]
    punchin_time: Optional[datetime]
    punchin_location: Optional[dict]
    punchout_time: Optional[datetime]
    punchout_location: Optional[dict]
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# Helper function to convert UTC datetime to IST (naive)
def convert_utc_to_ist(utc_dt):
    """Convert UTC datetime to IST and return naive ISO format string (without timezone offset)
    This allows JavaScript's new Date() to interpret it as local time"""
    if not utc_dt:
        return None
    # Localize to UTC, then convert to IST
    utc_dt_aware = pytz.UTC.localize(utc_dt) if utc_dt.tzinfo is None else utc_dt
    ist_dt = utc_dt_aware.astimezone(IST)
    # Return without timezone info (naive), so JavaScript interprets as local time
    return ist_dt.replace(tzinfo=None).isoformat()

# Helper function to calculate duration
def calculate_duration(punchin_time, punchout_time):
    if punchin_time and punchout_time:
        duration = punchout_time - punchin_time
        hours = duration.total_seconds() / 3600
        return round(hours, 2)
    return None

@router.post("/punchin", response_model=AttendanceResponse)
async def punch_in(request: PunchInRequest, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Punch in for BDE at a gym
    """
    try:
        # Check if employee already has an active punch-in for any gym
        active_attendance = db.query(Attendance).filter(
            and_(
                Attendance.employee_id == request.employee_id,
                Attendance.status == "Active"
            )
        ).first()

        if active_attendance:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Already punched in at another gym. Please punch out first."
            )

        # Fetch actual names from database
        actual_employee_name = None
        actual_manager_name = None
        actual_manager_id = None

        # Get employee details from Executives table
        executive = db.query(Executives).filter(Executives.id == request.employee_id).first()
        if executive:
            actual_employee_name = executive.name
            # Always use the manager_id from the executive record (this is the correct relationship)
            actual_manager_id = str(executive.manager_id)
        else:
            # Fallback if executive not found
            actual_employee_name = request.employee_name or "Executive"
            actual_manager_id = request.manager_id or "1"

        # Get manager name from Managers table using the manager_id from executives table
        if actual_manager_id and actual_manager_id != "1":
            manager = db.query(Managers).filter(Managers.id == actual_manager_id).first()
            if manager:
                actual_manager_name = manager.name
            else:
                actual_manager_name = request.manager_name or "Manager"
        else:
            actual_manager_name = request.manager_name or "Manager"

        # Create new attendance record with actual names
        attendance = Attendance(
            employee_id=request.employee_id,
            manager_id=actual_manager_id,  # Use the correct manager_id from executives table
            employee_name=actual_employee_name,
            manager_name=actual_manager_name,
            gym_name=request.gym_name,
            gym_address=request.gym_address,
            punchin_time=datetime.now(),
            punchin_location=request.punchin_location,
            status="Active"
        )

        db.add(attendance)
        db.commit()
        db.refresh(attendance)

        return attendance

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to punch in: {str(e)}"
        )

@router.post("/punchout", response_model=AttendanceResponse)
async def punch_out(request: PunchOutRequest, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Punch out for BDE
    """
    try:
        # Get the attendance record
        attendance = db.query(Attendance).filter(
            and_(
                Attendance.id == request.attendance_id,
                Attendance.status == "Active"
            )
        ).first()

        if not attendance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Active attendance record not found"
            )

        # Update punch out details
        attendance.punchout_time = datetime.now()
        attendance.punchout_location = request.punchout_location
        attendance.status = "Completed"

        db.commit()
        db.refresh(attendance)

        return attendance

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to punch out: {str(e)}"
        )

@router.get("/active/{employee_id}", response_model=Optional[AttendanceResponse])
async def get_active_attendance(employee_id: str, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get active attendance record for an employee
    """
    try:
        attendance = db.query(Attendance).filter(
            and_(
                Attendance.employee_id == employee_id,
                Attendance.status == "Active"
            )
        ).first()

        return attendance

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active attendance: {str(e)}"
        )

@router.get("/manager/{manager_id}", response_model=list[AttendanceResponse])
async def get_manager_attendance(
    manager_id: str,
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    status_filter: Optional[str] = None,
    date_filter: Optional[str] = None
):
    """
    Get attendance records for a manager's team
    """
    try:
        query = db.query(Attendance).filter(Attendance.manager_id == manager_id)

        if status_filter and status_filter in ["Active", "Completed"]:
            query = query.filter(Attendance.status == status_filter)

        if date_filter:
            try:
                filter_date = datetime.strptime(date_filter, "%Y-%m-%d").date()
                next_day = filter_date.replace(day=filter_date.day + 1) if filter_date.day < 31 else filter_date.replace(month=filter_date.month + 1, day=1)
                query = query.filter(
                    and_(
                        Attendance.created_at >= filter_date,
                        Attendance.created_at < next_day
                    )
                )
            except ValueError:
                pass  # Invalid date format, ignore filter

        attendance_records = query.order_by(desc(Attendance.created_at)).all()

        return attendance_records

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get manager attendance: {str(e)}"
        )

@router.get("/employee/{employee_id}", response_model=list[AttendanceResponse])
async def get_employee_attendance(
    employee_id: str,
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    limit: int = 50,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    Get attendance history for an employee
    """
    try:
        query = db.query(Attendance).filter(Attendance.employee_id == employee_id)

        # Add date filtering if provided
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
                query = query.filter(Attendance.created_at >= start_datetime)
            except ValueError:
                pass  # Invalid date format, ignore filter

        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
                # Add one day to make it inclusive
                from datetime import timedelta
                end_datetime = end_datetime + timedelta(days=1)
                query = query.filter(Attendance.created_at < end_datetime)
            except ValueError:
                pass  # Invalid date format, ignore filter

        attendance_records = query.order_by(desc(Attendance.created_at)).offset(offset).limit(limit).all()

        return attendance_records

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get employee attendance: {str(e)}"
        )

@router.get("/{attendance_id}/duration")
async def get_attendance_duration(attendance_id: int, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Calculate duration between punch-in and punch-out
    """
    try:
        attendance = db.query(Attendance).filter(Attendance.id == attendance_id).first()

        if not attendance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Attendance record not found"
            )

        duration = calculate_duration(attendance.punchin_time, attendance.punchout_time)

        return {
            "attendance_id": attendance_id,
            "duration_hours": duration,
            "punchin_time": attendance.punchin_time,
            "punchout_time": attendance.punchout_time
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate duration: {str(e)}"
        )

@router.get("/stats/manager/{manager_id}")
async def get_manager_attendance_stats(manager_id: str, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get attendance statistics for a manager's team
    """
    try:
        # Get all BDEs under this manager
        bdes = db.query(Executives).filter(Executives.manager_id == manager_id).all()

        bde_ids = [str(bde.id) for bde in bdes]

        # Get attendance records for the team
        attendance_records = db.query(Attendance).filter(
            Attendance.employee_id.in_(bde_ids)
        ).all()

        # Calculate statistics
        total_records = len(attendance_records)
        active_records = len([a for a in attendance_records if a.status == "Active"])
        completed_records = len([a for a in attendance_records if a.status == "Completed"])

        # Calculate total hours for completed records
        total_hours = 0
        for attendance in attendance_records:
            if attendance.punchin_time and attendance.punchout_time:
                duration = calculate_duration(attendance.punchin_time, attendance.punchout_time)
                if duration:
                    total_hours += duration

        return {
            "total_bdes": len(bdes),
            "total_attendance_records": total_records,
            "active_attendance": active_records,
            "completed_attendance": completed_records,
            "total_hours_worked": round(total_hours, 2),
            "average_hours_per_visit": round(total_hours / completed_records, 2) if completed_records > 0 else 0
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get attendance stats: {str(e)}"
        )

@router.get("/team/current/{manager_id}")
async def get_team_current_attendance(manager_id: str, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get all attendance records for the current day for all team members of a manager
    """
    try:
        # Get current date start and end
        today = datetime.now().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())

        # Get all BDEs under this manager with their executive info
        bdes = db.query(Executives).filter(Executives.manager_id == manager_id).all()

        team_attendance = []

        for bde in bdes:
            # Get all attendance records for today for this BDE
            today_attendance = db.query(Attendance).filter(
                and_(
                    Attendance.employee_id == str(bde.id),
                    Attendance.created_at >= start_of_day,
                    Attendance.created_at <= end_of_day
                )
            ).order_by(Attendance.punchin_time.desc()).all()

            bde_info = {
                "bde_id": str(bde.id),
                "bde_name": bde.name,
                "bde_email": bde.email,
                "bde_phone": bde.contact,
                "today_records": []
            }

            # Process each attendance record for today
            for attendance in today_attendance:
                # Calculate duration differently for active vs completed records
                if attendance.status == "Active":
                    duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, datetime.now())
                    is_active = True
                else:
                    duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, attendance.punchout_time)
                    is_active = False

                attendance_record = {
                    "attendance_id": attendance.id,
                    "gym_name": attendance.gym_name,
                    "gym_address": attendance.gym_address,
                    "punchin_time": attendance.punchin_time,
                    "punchout_time":attendance.punchout_time,
                    "punchin_location": attendance.punchin_location,
                    "punchout_location": attendance.punchout_location,
                    "duration_minutes": duration_minutes,
                    "status": attendance.status,
                    "is_active": is_active
                }

                bde_info["today_records"].append(attendance_record)

            # Check if BDE has any active attendance
            bde_info["has_active_attendance"] = any(record["is_active"] for record in bde_info["today_records"])
            bde_info["total_today_records"] = len(bde_info["today_records"])

            team_attendance.append(bde_info)

        # Sort: BDEs with active attendance first, then by name
        team_attendance.sort(key=lambda x: (not x["has_active_attendance"], x["bde_name"]))

        # Calculate statistics
        total_records = sum(len(bde["today_records"]) for bde in team_attendance)
        active_members = len([bde for bde in team_attendance if bde["has_active_attendance"]])
        active_sessions = len([record for bde in team_attendance for record in bde["today_records"] if record["is_active"]])

        return {
            "manager_id": manager_id,
            "date": today.strftime("%Y-%m-%d"),
            "total_team_members": len(team_attendance),
            "active_members": active_members,
            "active_sessions": active_sessions,
            "total_daily_records": total_records,
            "team_attendance": team_attendance
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get team daily attendance: {str(e)}"
        )


def calculate_duration_in_minutes(punchin_time, current_time):
    """Calculate duration in minutes between punchin and current time"""
    if not punchin_time:
        return 0

    duration = current_time - punchin_time
    return int(duration.total_seconds() / 60)

# Manager Attendance Models
class ManagerPunchInRequest(BaseModel):
    employee_id: str  # BDM employee ID
    employee_name: str  # BDM employee name
    gym_name: str
    gym_address: Optional[str] = None
    punchin_location: Optional[dict] = None

class ManagerPunchOutRequest(BaseModel):
    attendance_id: int
    punchout_location: Optional[dict] = None

class ManagerAttendanceResponse(BaseModel):
    id: int
    employee_id: str
    employee_name: str
    gym_name: str
    gym_address: Optional[str]
    punchin_time: Optional[datetime]
    punchin_location: Optional[dict]
    punchout_time: Optional[datetime]
    punchout_location: Optional[dict]
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


@router.post("/manager/punchin", response_model=ManagerAttendanceResponse)
async def manager_punch_in(request: ManagerPunchInRequest, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Punch In for Manager/BDM
    Creates a new attendance record for manager with gym details
    """
    try:
        # Check if manager has already punched in today and not punched out
        today = datetime.now().date()
        active_attendance = db.query(ManagerAttendance).filter(
            and_(
                ManagerAttendance.employee_id == request.employee_id,
                ManagerAttendance.status == "Active",
                func.date(ManagerAttendance.punchin_time) == today
            )
        ).first()

        if active_attendance:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You have already punched in today. Please punch out first."
            )

        # Get user's actual name from database if not provided or using generic fallback
        actual_employee_name = request.employee_name
        if not actual_employee_name or actual_employee_name in ["User", "BDM User", "Temporary"]:
            # Fetch from Managers table (since BDM users are Managers)
            from app.models.marketingmodels import Managers
            user = db.query(Managers).filter(Managers.id == int(request.employee_id)).first()
            if user and user.name:
                actual_employee_name = user.name
            else:
                # Fallback to fetching from Executives if not found in Managers
                from app.models.marketingmodels import Executives
                user = db.query(Executives).filter(Executives.id == int(request.employee_id)).first()
                if user and user.name:
                    actual_employee_name = user.name

        # Create new manager attendance record
        manager_attendance = ManagerAttendance(
            employee_id=request.employee_id,
            employee_name=actual_employee_name or request.employee_name,
            gym_name=request.gym_name,
            gym_address=request.gym_address,
            punchin_time=datetime.now(),
            punchin_location=request.punchin_location,
            status="Active"
        )

        db.add(manager_attendance)
        db.commit()
        db.refresh(manager_attendance)

        return manager_attendance

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to punch in: {str(e)}"
        )

@router.post("/manager/punchout", response_model=ManagerAttendanceResponse)
async def manager_punch_out(request: ManagerPunchOutRequest, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Punch Out for Manager/BDM
    Updates the existing attendance record with punch out time and location
    """
    try:
        # Get the active attendance record
        attendance = db.query(ManagerAttendance).filter(
            ManagerAttendance.id == request.attendance_id
        ).first()

        if not attendance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Attendance record not found"
            )

        if attendance.status != "Active":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="This attendance record is already completed"
            )

        if attendance.punchout_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You have already punched out from this gym"
            )

        # Update punch out details
        attendance.punchout_time = datetime.now()
        attendance.punchout_location = request.punchout_location
        attendance.status = "Completed"
        attendance.updated_at = datetime.now()

        db.commit()
        db.refresh(attendance)

        return attendance

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to punch out: {str(e)}"
        )

@router.get("/manager/active/{employee_id}", response_model=Optional[ManagerAttendanceResponse])
async def get_manager_active_attendance(employee_id: str, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get active attendance for a manager
    Returns the current active attendance record if the manager is punched in
    """
    try:
        today = datetime.now().date()
        active_attendance = db.query(ManagerAttendance).filter(
            and_(
                ManagerAttendance.employee_id == employee_id,
                ManagerAttendance.status == "Active",
                func.date(ManagerAttendance.punchin_time) == today
            )
        ).first()

        return active_attendance

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active attendance: {str(e)}"
        )

@router.get("/manager/attendance/{employee_id}", response_model=list[ManagerAttendanceResponse])
async def get_manager_attendance_history(employee_id: str, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get attendance history for a manager
    Returns all attendance records for the specified manager
    """
    try:
        # Get attendance records for the manager
        attendance_records = db.query(ManagerAttendance).filter(
            ManagerAttendance.employee_id == employee_id
        ).order_by(desc(ManagerAttendance.created_at)).all()

        return attendance_records

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get attendance history: {str(e)}"
        )

@router.get("/manager/{attendance_id}/duration")
async def get_manager_attendance_duration(attendance_id: int, user_id: int, role: str, db: Session = Depends(get_db)):
    """
    Get duration for a specific manager attendance record
    Returns the duration in hours between punch in and punch out
    """
    try:
        attendance = db.query(ManagerAttendance).filter(
            ManagerAttendance.id == attendance_id
        ).first()

        if not attendance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Attendance record not found"
            )

        duration = calculate_duration(attendance.punchin_time, attendance.punchout_time)

        return {
            "attendance_id": attendance_id,
            "punchin_time": attendance.punchin_time,
            "punchout_time": attendance.punchout_time,
            "duration_hours": duration,
            "status": attendance.status
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate duration: {str(e)}"
        )

@router.get("/get_bdes/{user_id}/{date}")
async def get_bde_attendance(
    user_id: int,
    date: str,
    db: Session = Depends(get_db)
):
    """
    Get attendance records for a specific BDE for a given date - same format as /team/current/
    """
    try:
        employee_id = str(user_id)

        # Parse the date from client
        try:
            selected_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use 'YYYY-MM-DD'."
            )

        # Get date start and end
        start_of_day = datetime.combine(selected_date, datetime.min.time())
        end_of_day = datetime.combine(selected_date, datetime.max.time())

        # Get BDE info from Executives table
        bde = db.query(Executives).filter(Executives.id == user_id).first()

        if not bde:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="BDE not found"
            )

        # Get all attendance records for today for this BDE
        today_attendance = db.query(Attendance).filter(
            and_(
                Attendance.employee_id == employee_id,
                Attendance.created_at >= start_of_day,
                Attendance.created_at <= end_of_day
            )
        ).order_by(Attendance.punchin_time.desc()).all()

        bde_info = {
            "bde_id": str(bde.id),
            "bde_name": bde.name,
            "bde_email": bde.email,
            "bde_phone": bde.contact,
            "today_records": []
        }

        # Process each attendance record for today
        for attendance in today_attendance:
            # Calculate duration differently for active vs completed records
            if attendance.status == "Active":
                duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, datetime.now())
                is_active = True
            else:
                duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, attendance.punchout_time)
                is_active = False

            attendance_record = {
                "attendance_id": attendance.id,
                "gym_name": attendance.gym_name,
                "gym_address": attendance.gym_address,
                "punchin_time": attendance.punchin_time,
                "punchout_time": attendance.punchout_time,
                "punchin_location": attendance.punchin_location,
                "punchout_location": attendance.punchout_location,
                "duration_minutes": duration_minutes,
                "status": attendance.status,
                "is_active": is_active
            }

            bde_info["today_records"].append(attendance_record)

        # Check if BDE has any active attendance
        bde_info["has_active_attendance"] = any(record["is_active"] for record in bde_info["today_records"])
        bde_info["total_today_records"] = len(bde_info["today_records"])

        team_attendance = [bde_info]

        # Calculate statistics
        total_records = len(bde_info["today_records"])
        active_members = 1 if bde_info["has_active_attendance"] else 0
        active_sessions = len([record for record in bde_info["today_records"] if record["is_active"]])

        profile_data = {
            "user_id": bde.id,
            "manager_id": bde.manager_id,
            "name": bde.name,
            "email": bde.email,
            "contact": bde.contact,
            "profile": bde.profile,
            "role": bde.role
        }

        return {
            "user_id": user_id,
            "date": selected_date.strftime("%Y-%m-%d"),
            "profile_data": profile_data,
            "total_team_members": 1,
            "active_members": active_members,
            "active_sessions": active_sessions,
            "total_daily_records": total_records,
            "team_attendance": team_attendance
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get attendance: {str(e)}"
        )

@router.get("/get_bdms/{user_id}/{date}")
async def get_bdms_attendance(
    user_id: int,
    date: str,
    db: Session = Depends(get_db)
):
 
    try:
        employee_id = str(user_id)

        # Parse the date from client
        try:
            selected_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use 'YYYY-MM-DD'."
            )

        # Get date start and end
        start_of_day = datetime.combine(selected_date, datetime.min.time())
        end_of_day = datetime.combine(selected_date, datetime.max.time())

        # Get BDMS info from Managers table
        bdms = db.query(Managers).filter(Managers.id == user_id).first()

        if not bdms:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="BDMS not found"
            )

        # Get all attendance records for today for this BDMS
        today_attendance = db.query(ManagerAttendance).filter(
            and_(
                ManagerAttendance.employee_id == employee_id,
                ManagerAttendance.created_at >= start_of_day,
                ManagerAttendance.created_at <= end_of_day
            )
        ).order_by(ManagerAttendance.punchin_time.desc()).all()

        bdms_info = {
            "bdms_id": str(bdms.id),
            "bdms_name": bdms.name,
            "bdms_email": bdms.email,
            "bdms_phone": bdms.contact,
            "today_records": []
        }

        # Process each attendance record for today
        for attendance in today_attendance:
            # Calculate duration differently for active vs completed records
            if attendance.status == "Active":
                duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, datetime.now())
                is_active = True
            else:
                duration_minutes = calculate_duration_in_minutes(attendance.punchin_time, attendance.punchout_time)
                is_active = False

            attendance_record = {
                "attendance_id": attendance.id,
                "gym_name": attendance.gym_name,
                "gym_address": attendance.gym_address,
                "punchin_time": attendance.punchin_time,
                "punchout_time": attendance.punchout_time,
                "punchin_location": attendance.punchin_location,
                "punchout_location": attendance.punchout_location,
                "duration_minutes": duration_minutes,
                "status": attendance.status,
                "is_active": is_active
            }

            bdms_info["today_records"].append(attendance_record)

        # Check if BDMS has any active attendance
        bdms_info["has_active_attendance"] = any(record["is_active"] for record in bdms_info["today_records"])
        bdms_info["total_today_records"] = len(bdms_info["today_records"])

        team_attendance = [bdms_info]

        # Calculate statistics
        total_records = len(bdms_info["today_records"])
        active_members = 1 if bdms_info["has_active_attendance"] else 0
        active_sessions = len([record for record in bdms_info["today_records"] if record["is_active"]])

        profile_data = {
            "user_id": bdms.id,
            "name": bdms.name,
            "email": bdms.email,
            "contact": bdms.contact,
            "profile": bdms.profile,
            "role": bdms.role
        }

        return {
            "user_id": user_id,
            "date": selected_date.strftime("%Y-%m-%d"),
            "profile_data": profile_data,
            "total_team_members": 1,
            "active_members": active_members,
            "active_sessions": active_sessions,
            "total_daily_records": total_records,
            "team_attendance": team_attendance
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get attendance: {str(e)}"
        )


