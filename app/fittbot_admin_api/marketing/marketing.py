from fastapi import APIRouter, Depends, HTTPException, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.marketingmodels import Executives, Managers, GymVisits, GymDatabase, Attendance, ManagerAttendance
from app.models.async_database import get_async_db
from sqlalchemy import func, case, distinct, select
import logging
from datetime import datetime, timedelta
import pytz

router = APIRouter(prefix="/api/admin/marketing", tags=["Admin Marketing"])

logger = logging.getLogger(__name__)

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Helper function for BDE times - subtract 5:30 hours
def convert_bde_time(db_dt):
    """Subtract 5:30 hours from database time for BDEs
    Database already has IST time, so we need to subtract the offset"""
    if not db_dt:
        return None
    # Subtract 5 hours and 30 minutes
    adjusted_dt = db_dt - timedelta(hours=5, minutes=30)
    return adjusted_dt.isoformat()

# Helper function for BDM times - return as-is
def convert_bdm_time(db_dt):
    """Return database time as-is for BDMs"""
    if not db_dt:
        return None
    return db_dt.isoformat()




@router.get("/stats")
async def get_marketing_stats(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):

    try:
        # Debug log to check cookies
        cookies = request.cookies
        access_token = cookies.get("access_token")
        logger.info(f"[MARKETING-STATS] Request received with cookies: {list(cookies.keys())}")
        logger.info(f"[MARKETING-STATS] Access token present: {bool(access_token)}")
        logger.info(f"[MARKETING-STATS] Access token (first 20 chars): {access_token[:20] if access_token else 'None'}")

        # Get total BDEs count from Executives table
        total_bdes_stmt = select(func.count(Executives.id))
        total_bdes_result = await db.execute(total_bdes_stmt)
        total_bdes = total_bdes_result.scalar() or 0
        logger.info(f"[MARKETING-STATS] Total BDEs found: {total_bdes}")

        # Get total BDMs count from Managers table
        total_bdms_stmt = select(func.count(Managers.id))
        total_bdms_result = await db.execute(total_bdms_stmt)
        total_bdms = total_bdms_result.scalar() or 0
        logger.info(f"[MARKETING-STATS] Total BDMs found: {total_bdms}")

        # Get active counts
        active_bdes_stmt = select(func.count(Executives.id)).where(Executives.status == 'active')
        active_bdes_result = await db.execute(active_bdes_stmt)
        active_bdes = active_bdes_result.scalar() or 0

        active_bdms_stmt = select(func.count(Managers.id)).where(Managers.status == 'active')
        active_bdms_result = await db.execute(active_bdms_stmt)
        active_bdms = active_bdms_result.scalar() or 0

        logger.info(f"[MARKETING-STATS] Active BDEs: {active_bdes}, Active BDMs: {active_bdms}")

        return {
            "status": 200,
            "message": "Marketing stats fetched successfully",
            "data": {
                "bdes": {
                    "total": total_bdes,
                    "active": active_bdes,
                    "inactive": total_bdes - active_bdes
                },
                "bdms": {
                    "total": total_bdms,
                    "active": active_bdms,
                    "inactive": total_bdms - active_bdms
                }
            }
        }

    except Exception as e:
        logger.error(f"[MARKETING-STATS] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch marketing stats: {str(e)}")


@router.get("/bdes")
async def get_bdes_list(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    time_filter: str = Query("all", description="Filter by time: all, today, week")
):
    """
    Get list of all BDEs with their performance stats
    Columns: Rank, Name, Total Assigned, Total Visited, Total Converted, Conversion Ratio
    Time filters: all (all time), today, week (this week)
    Requires admin authentication via cookies
    """
    try:
        # Debug log to check cookies
        cookies = request.cookies
        access_token = cookies.get("access_token")

        date_filter = None
        if time_filter == "today":
            date_filter = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"[BDEs-LIST] Filtering for today: {date_filter}")
        elif time_filter == "week":
            date_filter = datetime.now() - timedelta(days=7)
            logger.info(f"[BDEs-LIST] Filtering for last 7 days: {date_filter}")

        # Get all executives from Executives table
        executives_stmt = select(Executives)
        executives_result = await db.execute(executives_stmt)
        executives = executives_result.scalars().all()
        logger.info(f"[BDEs-LIST] Found {len(executives)} executives")

        bdes_stats = []

        for executive in executives:
            # Base query filter
            base_filter = [GymVisits.user_id == executive.id]

            # Add time filter if specified
            if date_filter:
                base_filter.append(GymVisits.updated_at >= date_filter)

            # Total Assigned: Count of all gym_visits for this executive
            total_assigned_stmt = select(func.count(GymVisits.id)).where(*base_filter)
            total_assigned_result = await db.execute(total_assigned_stmt)
            total_assigned = total_assigned_result.scalar() or 0

            # Total Visited: Count where attendance_selfie is not null
            visited_filter = base_filter + [
                GymVisits.attendance_selfie.isnot(None),
                GymVisits.attendance_selfie != ''
            ]
            total_visited_stmt = select(func.count(GymVisits.id)).where(*visited_filter)
            total_visited_result = await db.execute(total_visited_stmt)
            total_visited = total_visited_result.scalar() or 0

            # Total Converted: Count where final_status = 'converted'
            converted_filter = base_filter + [GymVisits.final_status == 'converted']
            total_converted_stmt = select(func.count(GymVisits.id)).where(*converted_filter)
            total_converted_result = await db.execute(total_converted_stmt)
            total_converted = total_converted_result.scalar() or 0

            # Conversion Ratio: total_converted / total_visited (avoid division by zero)
            conversion_ratio = round((total_converted / total_visited * 100), 2) if total_visited > 0 else 0.0

            bdes_stats.append({
                'id': executive.id,
                'name': executive.name,
                'email': executive.email,
                'contact': executive.contact,
                'total_assigned': total_assigned,
                'total_visited': total_visited,
                'total_converted': total_converted,
                'conversion_ratio': conversion_ratio
            })

        bdes_stats.sort(key=lambda x: (
            -x['total_converted'],
            -x['conversion_ratio'],  # Higher conversion ratio first (negative for descending)
               # Higher conversions (negative for descending)
            x['total_visited'],       # Lower visits (positive for ascending)
            x['name'].lower()         # Alphabetical (positive for ascending)
        ))

        # Add rank to each BDE
        for rank, bde in enumerate(bdes_stats, start=1):
            bde['rank'] = rank

        logger.info(f"[BDEs-LIST] Ranked {len(bdes_stats)} BDEs successfully")

        return {
            "status": 200,
            "message": "BDEs list fetched successfully",
            "data": bdes_stats
        }

    except Exception as e:
        logger.error(f"[BDEs-LIST] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch BDEs list: {str(e)}")


@router.get("/bdms")
async def get_bdms_list(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    time_filter: str = Query("all", description="Filter by time: all, today, week")
):

    try:
        # Debug log to check cookies
        cookies = request.cookies
        access_token = cookies.get("access_token")
        logger.info(f"[BDMs-LIST] Request received with cookies: {list(cookies.keys())}")
        logger.info(f"[BDMs-LIST] Access token present: {bool(access_token)}")
        logger.info(f"[BDMs-LIST] Time filter: {time_filter}")

        # Calculate date filter based on time_filter
        date_filter = None
        if time_filter == "today":
            date_filter = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"[BDMs-LIST] Filtering for today: {date_filter}")
        elif time_filter == "week":
            date_filter = datetime.now() - timedelta(days=7)
            logger.info(f"[BDMs-LIST] Filtering for last 7 days: {date_filter}")

        # Get all managers from Managers table
        managers_stmt = select(Managers)
        managers_result = await db.execute(managers_stmt)
        managers = managers_result.scalars().all()
        logger.info(f"[BDMs-LIST] Found {len(managers)} managers")

        bdms_stats = []

        for manager in managers:
            # Get all executives (team members) under this manager
            team_members_stmt = select(Executives).where(Executives.manager_id == manager.id)
            team_members_result = await db.execute(team_members_stmt)
            team_members = team_members_result.scalars().all()

            team_member_ids = [tm.id for tm in team_members]
            logger.info(f"[BDMs-LIST] Manager {manager.name} has {len(team_member_ids)} team members")

            # Initialize counters
            team_assigned = 0
            team_visited = 0
            team_converted = 0

            # Calculate team stats (from all team members' GymVisits)
            if team_member_ids:
                # Base filter for team visits
                team_base_filter = [GymVisits.user_id.in_(team_member_ids)]
                if date_filter:
                    team_base_filter.append(GymVisits.updated_at >= date_filter)

                team_assigned_stmt = select(func.count(GymVisits.id)).where(*team_base_filter)
                team_assigned_result = await db.execute(team_assigned_stmt)
                team_assigned = team_assigned_result.scalar() or 0

                team_visited_filter = team_base_filter + [
                    GymVisits.attendance_selfie.isnot(None),
                    GymVisits.attendance_selfie != ''
                ]
                team_visited_stmt = select(func.count(GymVisits.id)).where(*team_visited_filter)
                team_visited_result = await db.execute(team_visited_stmt)
                team_visited = team_visited_result.scalar() or 0

                team_converted_filter = team_base_filter + [GymVisits.final_status == 'converted']
                team_converted_stmt = select(func.count(GymVisits.id)).where(*team_converted_filter)
                team_converted_result = await db.execute(team_converted_stmt)
                team_converted = team_converted_result.scalar() or 0

            # Calculate self-assigned conversions for this manager
            # Get gyms where self_assigned=True for this manager
            self_assigned_gyms_stmt = select(GymDatabase.id).where(
                GymDatabase.submitted_by_manager == manager.id,
                GymDatabase.self_assigned == True
            )
            self_assigned_gyms_result = await db.execute(self_assigned_gyms_stmt)
            self_assigned_gyms = self_assigned_gyms_result.scalars().all()

            self_assigned_gym_ids = list(self_assigned_gyms)

            # Count self-assigned visits and conversions
            self_assigned_total = 0
            self_assigned_visited = 0
            self_assigned_converted = 0

            if self_assigned_gym_ids:
                # Base filter for self-assigned visits
                self_base_filter = [GymVisits.gym_id.in_(self_assigned_gym_ids)]
                if date_filter:
                    self_base_filter.append(GymVisits.updated_at >= date_filter)

                # Total self-assigned
                self_assigned_total_stmt = select(func.count(GymVisits.id)).where(*self_base_filter)
                self_assigned_total_result = await db.execute(self_assigned_total_stmt)
                self_assigned_total = self_assigned_total_result.scalar() or 0

                # Self-assigned visited
                self_visited_filter = self_base_filter + [
                    GymVisits.attendance_selfie.isnot(None),
                    GymVisits.attendance_selfie != ''
                ]
                self_assigned_visited_stmt = select(func.count(GymVisits.id)).where(*self_visited_filter)
                self_assigned_visited_result = await db.execute(self_assigned_visited_stmt)
                self_assigned_visited = self_assigned_visited_result.scalar() or 0

                # Self-assigned converted
                self_converted_filter = self_base_filter + [GymVisits.final_status == 'converted']
                self_assigned_converted_stmt = select(func.count(GymVisits.id)).where(*self_converted_filter)
                self_assigned_converted_result = await db.execute(self_assigned_converted_stmt)
                self_assigned_converted = self_assigned_converted_result.scalar() or 0

            # Add self-assigned to team totals
            total_assigned = team_assigned + self_assigned_total
            total_visited = team_visited + self_assigned_visited
            total_converted = team_converted + self_assigned_converted

            # Conversion Ratio: total_converted / total_visited (avoid division by zero)
            conversion_ratio = round((total_converted / total_visited * 100), 2) if total_visited > 0 else 0.0

            logger.info(
                f"[BDMs-LIST] Manager {manager.name}: "
                f"Team(A:{team_assigned},V:{team_visited},C:{team_converted}) + "
                f"Self(A:{self_assigned_total},V:{self_assigned_visited},C:{self_assigned_converted}) = "
                f"Total(A:{total_assigned},V:{total_visited},C:{total_converted})"
            )

            bdms_stats.append({
                'id': manager.id,
                'name': manager.name,
                'email': manager.email,
                'contact': manager.contact,
                'team_size': len(team_member_ids),
                'total_assigned': total_assigned,
                'total_visited': total_visited,
                'total_converted': total_converted,
                'self_converted': self_assigned_converted,
                'conversion_ratio': conversion_ratio
            })

        logger.info(f"[BDMs-LIST] Calculated stats for {len(bdms_stats)} BDMs")

        # Ranking logic (same as BDEs):
        # 1. Higher conversion ratio first
        # 2. If same ratio, higher conversions with lower visits (more efficient)
        # 3. If still same, alphabetical order by name
        bdms_stats.sort(key=lambda x: (
            -x['total_converted'],
            -x['conversion_ratio'],  # Higher conversion ratio first (negative for descending)
                # Higher conversions (negative for descending)
            x['total_visited'],       # Lower visits (positive for ascending)
            x['name'].lower()         # Alphabetical (positive for ascending)
        ))

        # Add rank to each BDM
        for rank, bdm in enumerate(bdms_stats, start=1):
            bdm['rank'] = rank

        logger.info(f"[BDMs-LIST] Ranked {len(bdms_stats)} BDMs successfully")

        return {
            "status": 200,
            "message": "BDMs list fetched successfully",
            "data": bdms_stats
        }

    except Exception as e:
        logger.error(f"[BDMs-LIST] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch BDMs list: {str(e)}")


@router.get("/gym-visits/summary")
async def get_gym_visits_summary(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    time_filter: str = Query("today", description="Filter by time: all, today"),
    manager_id: int = Query(None, description="Filter by manager ID (only for 'all' time filter)")
):
    """
    Get gym visits summary stats
    - Assigned: Total gym visits (based on filter)
    - Visited: Gym visits with attendance_selfie
    - Converted: Gym visits with final_status='converted'

    Time filters:
    - today: assigned_on = today for assigned, updated_at = today for visited/converted
    - all: all time stats

    Manager filter (only with 'all' time):
    - Filter to show only that manager's team stats
    """
    try:
        logger.info(f"[GYM-VISITS-SUMMARY] Time filter: {time_filter}, Manager ID: {manager_id}")

        # Calculate date filter
        today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Get executive IDs based on manager filter (works for both today and all time)
        executive_ids = None
        if manager_id:
            # Get all executives under this manager
            executives_stmt = select(Executives.id).where(Executives.manager_id == manager_id)
            executives_result = await db.execute(executives_stmt)
            executives = executives_result.scalars().all()
            executive_ids = list(executives)
            logger.info(f"[GYM-VISITS-SUMMARY] Manager {manager_id} has {len(executive_ids)} team members")

        # Base filters
        base_filter = []
        if executive_ids is not None:
            base_filter.append(GymVisits.user_id.in_(executive_ids))

        if time_filter == "today":
            # For today: assigned_on = today
            assigned_filter = base_filter + [
                func.date(GymVisits.assigned_on) == today_date.date()
            ]
            # For visited/converted: updated_at = today
            updated_filter = base_filter + [
                func.date(GymVisits.updated_at) == today_date.date()
            ]
        else:  # all time
            assigned_filter = base_filter.copy()
            updated_filter = base_filter.copy()

        # Count Assigned
        assigned_count_stmt = select(func.count(GymVisits.id)).where(*assigned_filter)
        assigned_count_result = await db.execute(assigned_count_stmt)
        assigned_count = assigned_count_result.scalar() or 0

        # Count Visited (has attendance_selfie)
        visited_filter = updated_filter + [
            GymVisits.attendance_selfie.isnot(None),
            GymVisits.attendance_selfie != ''
        ]
        visited_count_stmt = select(func.count(GymVisits.id)).where(*visited_filter)
        visited_count_result = await db.execute(visited_count_stmt)
        visited_count = visited_count_result.scalar() or 0

        # Count Converted (final_status = 'converted')
        converted_filter = updated_filter + [
            GymVisits.final_status == 'converted'
        ]
        converted_count_stmt = select(func.count(GymVisits.id)).where(*converted_filter)
        converted_count_result = await db.execute(converted_count_stmt)
        converted_count = converted_count_result.scalar() or 0

        logger.info(f"[GYM-VISITS-SUMMARY] Stats - Assigned: {assigned_count}, Visited: {visited_count}, Converted: {converted_count}")

        return {
            "status": 200,
            "message": "Gym visits summary fetched successfully",
            "data": {
                "assigned": assigned_count,
                "visited": visited_count,
                "converted": converted_count
            }
        }

    except Exception as e:
        logger.error(f"[GYM-VISITS-SUMMARY] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch gym visits summary: {str(e)}")


@router.get("/gym-visits/list")
async def get_gym_visits_list(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    filter_type: str = Query("all", description="Filter type: all, assigned, visited, converted"),
    time_filter: str = Query("today", description="Time filter: today, all"),
    manager_id: int = Query(None, description="Filter by manager ID")
):
    """
    Get filtered list of gym visits
    - filter_type: all, assigned (all assigned), visited (has attendance_selfie), converted (final_status='converted')
    - time_filter: today, all
    - manager_id: Filter by manager's team
    """
    try:
        logger.info(f"[GYM-VISITS-LIST] Filter type: {filter_type}, Time: {time_filter}, Manager: {manager_id}")

        # Calculate date filter
        today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Get executive IDs based on manager filter
        executive_ids = None
        if manager_id:
            executives_stmt = select(Executives.id).where(Executives.manager_id == manager_id)
            executives_result = await db.execute(executives_stmt)
            executives = executives_result.scalars().all()
            executive_ids = list(executives)
            logger.info(f"[GYM-VISITS-LIST] Manager {manager_id} has {len(executive_ids)} team members")

        # Base query with joins
        stmt = select(
            GymVisits.id,
            GymVisits.gym_name,
            GymVisits.gym_address,
            GymVisits.assigned_on,
            GymVisits.final_status,
            GymVisits.attendance_selfie,
            Executives.name.label('bde_name'),
            Managers.name.label('bdm_name')
        ).join(
            Executives, GymVisits.user_id == Executives.id
        ).outerjoin(
            Managers, Executives.manager_id == Managers.id
        )

        # Apply manager filter
        if executive_ids is not None:
            stmt = stmt.where(GymVisits.user_id.in_(executive_ids))

        # Apply time filter based on filter_type
        # For "visited" and "converted", use updated_at (when action happened)
        # For "assigned" and "all", use assigned_on (when gym was assigned)
        if time_filter == "today":
            if filter_type in ["visited", "converted"]:
                stmt = stmt.where(func.date(GymVisits.updated_at) == today_date.date())
            else:  # "assigned" or "all"
                stmt = stmt.where(func.date(GymVisits.assigned_on) == today_date.date())

        # Apply filter type
        if filter_type == "visited":
            stmt = stmt.where(
                GymVisits.attendance_selfie.isnot(None),
                GymVisits.attendance_selfie != ''
            )
        elif filter_type == "converted":
            stmt = stmt.where(GymVisits.final_status == 'converted')
        # For "assigned" and "all", no additional filter needed

        # Execute query
        result = await db.execute(stmt)
        visits = result.all()

        # Format results
        result_list = []
        for visit in visits:
            result_list.append({
                "id": visit.id,
                "gym_name": visit.gym_name,
                "gym_address": visit.gym_address,
                "assigned_on": visit.assigned_on.isoformat() if visit.assigned_on else None,
                "final_status": visit.final_status,
                "bde_name": visit.bde_name,
                "bdm_name": visit.bdm_name
            })

        logger.info(f"[GYM-VISITS-LIST] Found {len(result_list)} gym visits")

        return {
            "status": 200,
            "message": "Gym visits list fetched successfully",
            "data": result_list
        }

    except Exception as e:
        logger.error(f"[GYM-VISITS-LIST] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch gym visits list: {str(e)}")


@router.get("/attendance/stats")
async def get_attendance_stats(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get attendance statistics for today
    - BDEs: Count of unique BDEs with at least one punch-in today
    - BDMs: Count of unique BDMs with at least one punch-in today
    """
    try:
        logger.info("[ATTENDANCE-STATS] Fetching attendance stats...")

        # Get today's date - NO timezone conversion
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Count unique BDEs with at least one punch-in today
        bdes_count_stmt = select(func.count(distinct(Attendance.employee_id))).where(
            Attendance.punchin_time >= today_start,
            Attendance.punchin_time <= today_end
        )
        bdes_count_result = await db.execute(bdes_count_stmt)
        bdes_count = bdes_count_result.scalar() or 0

        # Count unique BDMs with at least one punch-in today
        bdms_count_stmt = select(func.count(distinct(ManagerAttendance.employee_id))).where(
            ManagerAttendance.punchin_time >= today_start,
            ManagerAttendance.punchin_time <= today_end
        )
        bdms_count_result = await db.execute(bdms_count_stmt)
        bdms_count = bdms_count_result.scalar() or 0

        logger.info(f"[ATTENDANCE-STATS] BDEs with punch-in: {bdes_count}, BDMs with punch-in: {bdms_count}")

        return {
            "status": 200,
            "message": "Attendance stats fetched successfully",
            "data": {
                "bdes": bdes_count,
                "bdms": bdms_count
            }
        }

    except Exception as e:
        logger.error(f"[ATTENDANCE-STATS] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch attendance stats: {str(e)}")


@router.get("/attendance/today")
async def get_today_attendance(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    role: str = Query("BDE", description="Role filter: BDE or BDM"),
    start_date: str = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date filter (YYYY-MM-DD)")
):
    """
    Get attendance list for BDEs or BDMs
    Supports date range filtering via start_date and end_date parameters
    Returns: employee_id, employee_name, total_punch_entries, latest_punchin_time,
             latest_punchout_time, latest_punchin_address
    """
    try:
        logger.info(f"[ATTENDANCE-TODAY] Fetching attendance for role: {role}, start_date: {start_date}, end_date: {end_date}")

        # Determine date range - NO timezone conversion, use DB values as-is
        today_start = None
        today_end = None

        if start_date and end_date:
            # Use provided date range without timezone conversion
            today_start = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.info(f"[ATTENDANCE-TODAY] Date range: {today_start} to {today_end}")
        else:
            logger.info(f"[ATTENDANCE-TODAY] Fetching all-time attendance data")

        # Select the appropriate table and model based on role
        if role.upper() == "BDE":
            AttendanceTable = Attendance
            EmployeeTable = Executives
        elif role.upper() == "BDM":
            AttendanceTable = ManagerAttendance
            EmployeeTable = Managers
        else:
            raise HTTPException(status_code=400, detail="Invalid role. Must be BDE or BDM")

        # Get unique employee IDs with punch-ins in the specified date range
        # If no date range provided (all time), get all employees with any attendance
        if start_date and end_date:
            employee_ids_stmt = select(distinct(AttendanceTable.employee_id)).where(
                AttendanceTable.punchin_time >= today_start,
                AttendanceTable.punchin_time <= today_end
            )
        else:
            # For "all time", get all employees with at least one punch-in record
            employee_ids_stmt = select(distinct(AttendanceTable.employee_id))

        employee_ids_result = await db.execute(employee_ids_stmt)
        employee_ids_tuples = employee_ids_result.all()
        employee_ids = [emp[0] for emp in employee_ids_tuples]
        logger.info(f"[ATTENDANCE-TODAY] Found {len(employee_ids)} {role}s with attendance in the specified range")

        attendance_list = []

        for emp_id in employee_ids:
            # Get employee name
            employee_stmt = select(EmployeeTable).filter(EmployeeTable.id == emp_id)
            employee_result = await db.execute(employee_stmt)
            employee = employee_result.scalar_one_or_none()
            if not employee:
                continue

            # Build query for attendance records
            if start_date and end_date:
                # Get attendance records in the specified date range
                records_stmt = select(AttendanceTable).filter(
                    AttendanceTable.employee_id == emp_id,
                    AttendanceTable.punchin_time >= today_start,
                    AttendanceTable.punchin_time <= today_end
                )
            else:
                # Get all attendance records (all time)
                records_stmt = select(AttendanceTable).filter(
                    AttendanceTable.employee_id == emp_id
                )

            records_result = await db.execute(records_stmt)
            today_records = records_result.scalars().all()

            # Count total punch entries (only count if both punch in and out exist)
            total_entries = sum(1 for record in today_records if record.punchin_time and record.punchout_time)

            # Get latest punch in record
            latest_punchin_stmt = records_stmt.order_by(AttendanceTable.punchin_time.desc()).limit(1)
            latest_punchin_result = await db.execute(latest_punchin_stmt)
            latest_record = latest_punchin_result.scalar_one_or_none()

            if latest_record:
                # Extract address from punchin_location
                punchin_address = "-"
                if latest_record.punchin_location:
                    if isinstance(latest_record.punchin_location, dict):
                        punchin_address = latest_record.punchin_location.get('address', '-')
                    elif isinstance(latest_record.punchin_location, str):
                        punchin_address = latest_record.punchin_location



                attendance_list.append({
                    "employee_id": emp_id,
                    "employee_name": employee.name,
                    "total_punch_entries": total_entries,
                    "latest_punchin_time": latest_record.punchin_time.strftime("%Y-%m-%d %H:%M:%S") if latest_record.punchin_time else None,
                    "latest_punchout_time": latest_record.punchout_time.strftime("%Y-%m-%d %H:%M:%S") if latest_record.punchout_time else None,
                    "latest_punchin_address": punchin_address
                })

        logger.info(f"[ATTENDANCE-TODAY] Returning {len(attendance_list)} attendance records")

        return {
            "status": 200,
            "message": "Attendance data fetched successfully",
            "data": attendance_list
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"[ATTENDANCE-TODAY] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch today's attendance: {str(e)}")


@router.get("/attendance/employee/{employee_id}")
async def get_employee_attendance_history(
    request: Request,
    employee_id: int,
    db: AsyncSession = Depends(get_async_db),
    role: str = Query("BDE", description="Role filter: BDE or BDM"),
    start_date: str = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date filter (YYYY-MM-DD)")
):
    """
    Get attendance history for a specific employee
    Returns all punch in/out records with locations
    """
    try:
        logger.info(f"[ATTENDANCE-HISTORY] Fetching history for employee {employee_id}, role: {role}")

        # Select the appropriate table based on role
        if role.upper() == "BDE":
            AttendanceTable = Attendance
        elif role.upper() == "BDM":
            AttendanceTable = ManagerAttendance
        else:
            raise HTTPException(status_code=400, detail="Invalid role. Must be BDE or BDM")

        # Build query
        stmt = select(AttendanceTable).filter(AttendanceTable.employee_id == employee_id)

        # Apply date filters if provided - NO timezone conversion, use DB values as-is
        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"[ATTENDANCE-HISTORY] Filtering from: {start_dt}")
            stmt = stmt.where(AttendanceTable.punchin_time >= start_dt)

        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.info(f"[ATTENDANCE-HISTORY] Filtering to: {end_dt}")
            stmt = stmt.where(AttendanceTable.punchin_time <= end_dt)

        # Order by punch in time descending (most recent first)
        stmt = stmt.order_by(AttendanceTable.punchin_time.desc())

        result = await db.execute(stmt)
        records = result.scalars().all()

        logger.info(f"[ATTENDANCE-HISTORY] Found {len(records)} attendance records")

        # Format results - Send DB values as strings
        result_list = []
        for record in records:

            result_list.append({
                "id": record.id,
                "employee_id": record.employee_id,
                "punchin_time": record.punchin_time.strftime("%Y-%m-%d %H:%M:%S") if record.punchin_time else None,
                "punchout_time": record.punchout_time.strftime("%Y-%m-%d %H:%M:%S") if record.punchout_time else None,
                "punchin_location": record.punchin_location,
                "punchout_location": record.punchout_location,
                "created_at": record.created_at.strftime("%Y-%m-%d %H:%M:%S") if record.created_at else None
            })

        return {
            "status": 200,
            "message": "Employee attendance history fetched successfully",
            "data": result_list
        }

    except HTTPException as he:
        raise he
    except ValueError as ve:
        logger.error(f"[ATTENDANCE-HISTORY] Date parsing error: {str(ve)}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"[ATTENDANCE-HISTORY] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch employee attendance history: {str(e)}")


@router.get("/attendance/export")
async def export_attendance_data(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
    month: int = Query(None, description="Month (1-12)"),
    year: int = Query(None, description="Year (e.g., 2024)")
):
    """
    Export attendance data for all employees (BDEs and BDMs) with monthly filter
    Returns: summary data and daily detailed records
    """
    try:
        logger.info(f"[ATTENDANCE-EXPORT] Exporting attendance data for month: {month}, year: {year}")

        # If no month/year provided, use current month
        now_ist = datetime.now(IST)
        if not month or not year:
            month = now_ist.month
            year = now_ist.year

        # Calculate date range for the selected month
        start_date_ist = IST.localize(datetime(year, month, 1, 0, 0, 0))
        if month == 12:
            end_date_ist = IST.localize(datetime(year + 1, 1, 1, 0, 0, 0)) - timedelta(seconds=1)
        else:
            end_date_ist = IST.localize(datetime(year, month + 1, 1, 0, 0, 0)) - timedelta(seconds=1)

        logger.info(f"[ATTENDANCE-EXPORT] Date range: {start_date_ist} to {end_date_ist}")

        summary_data = []
        daily_data = []
        daily_summary_data = []

        # Process BDEs
        bde_start = start_date_ist.replace(tzinfo=None)
        bde_end = end_date_ist.replace(tzinfo=None)

        bde_employee_ids_stmt = select(distinct(Attendance.employee_id)).where(
            Attendance.punchin_time >= bde_start,
            Attendance.punchin_time <= bde_end
        )
        bde_employee_ids_result = await db.execute(bde_employee_ids_stmt)
        bde_employee_ids_tuples = bde_employee_ids_result.all()

        for emp_id_tuple in bde_employee_ids_tuples:
            emp_id = emp_id_tuple[0]
            employee_stmt = select(Executives).filter(Executives.id == emp_id)
            employee_result = await db.execute(employee_stmt)
            employee = employee_result.scalar_one_or_none()
            if not employee:
                continue

            # Get all records for this BDE in the month
            records_stmt = select(Attendance).filter(
                Attendance.employee_id == emp_id,
                Attendance.punchin_time >= bde_start,
                Attendance.punchin_time <= bde_end
            ).order_by(Attendance.punchin_time.asc())
            records_result = await db.execute(records_stmt)
            records = records_result.scalars().all()

            # Calculate total punch records (only completed ones)
            total_punch_records = sum(1 for record in records if record.punchin_time and record.punchout_time)

            # Calculate total duration (only for completed records)
            total_duration_minutes = 0

            # Dictionary to track daily summaries for this employee
            daily_summaries = {}

            # Process each record for daily data
            for record in records:
                if record.punchin_time and record.punchout_time:
                    # For BDEs: times are in IST, but we need to subtract 5:30 offset
                    punchin_adjusted = record.punchin_time - timedelta(hours=5, minutes=30)
                    punchout_adjusted = record.punchout_time - timedelta(hours=5, minutes=30)
                    duration_seconds = (punchout_adjusted - punchin_adjusted).total_seconds()
                    duration_minutes = int(duration_seconds / 60)
                    total_duration_minutes += duration_minutes

                    # Format duration for this record
                    record_hours = duration_minutes // 60
                    record_mins = duration_minutes % 60
                    record_duration = f"{record_hours}h {record_mins}m" if record_hours > 0 else f"{record_mins}m"

                    # Extract addresses
                    punchin_address = "-"
                    if record.punchin_location:
                        if isinstance(record.punchin_location, dict):
                            punchin_address = record.punchin_location.get('address', '-')
                        elif isinstance(record.punchin_location, str):
                            punchin_address = record.punchin_location

                    punchout_address = "-"
                    if record.punchout_location:
                        if isinstance(record.punchout_location, dict):
                            punchout_address = record.punchout_location.get('address', '-')
                        elif isinstance(record.punchout_location, str):
                            punchout_address = record.punchout_location

                    # Add to daily data
                    daily_data.append({
                        "date": punchin_adjusted.strftime("%Y-%m-%d"),
                        "name": employee.name,
                        "role": "BDE",
                        "punchin_time": punchin_adjusted.strftime("%Y-%m-%d %H:%M:%S"),
                        "punchout_time": punchout_adjusted.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": record_duration,
                        "punchin_address": punchin_address,
                        "punchout_address": punchout_address
                    })

                    # Track daily summary
                    date_key = punchin_adjusted.strftime("%Y-%m-%d")
                    if date_key not in daily_summaries:
                        daily_summaries[date_key] = {
                            "punch_count": 0,
                            "total_minutes": 0
                        }
                    daily_summaries[date_key]["punch_count"] += 1
                    daily_summaries[date_key]["total_minutes"] += duration_minutes

            # Add daily summaries to daily_summary_data
            for date_key, summary in daily_summaries.items():
                hours = summary["total_minutes"] // 60
                mins = summary["total_minutes"] % 60
                duration_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

                daily_summary_data.append({
                    "date": date_key,
                    "name": employee.name,
                    "role": "BDE",
                    "total_punch_records": summary["punch_count"],
                    "total_duration": duration_str
                })

            # Format total duration
            hours = total_duration_minutes // 60
            mins = total_duration_minutes % 60
            total_duration = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

            summary_data.append({
                "date": f"{year}-{month:02d}",
                "name": employee.name,
                "role": "BDE",
                "total_punch_records": total_punch_records,
                "total_duration": total_duration,
                "total_duration_minutes": total_duration_minutes  # For sorting
            })

        # Process BDMs
        bdm_start = start_date_ist.astimezone(pytz.UTC).replace(tzinfo=None)
        bdm_end = end_date_ist.astimezone(pytz.UTC).replace(tzinfo=None)

        bdm_employee_ids_stmt = select(distinct(ManagerAttendance.employee_id)).where(
            ManagerAttendance.punchin_time >= bdm_start,
            ManagerAttendance.punchin_time <= bdm_end
        )
        bdm_employee_ids_result = await db.execute(bdm_employee_ids_stmt)
        bdm_employee_ids_tuples = bdm_employee_ids_result.all()

        for emp_id_tuple in bdm_employee_ids_tuples:
            emp_id = emp_id_tuple[0]
            employee_stmt = select(Managers).filter(Managers.id == emp_id)
            employee_result = await db.execute(employee_stmt)
            employee = employee_result.scalar_one_or_none()
            if not employee:
                continue

            # Get all records for this BDM in the month
            records_stmt = select(ManagerAttendance).filter(
                ManagerAttendance.employee_id == emp_id,
                ManagerAttendance.punchin_time >= bdm_start,
                ManagerAttendance.punchin_time <= bdm_end
            ).order_by(ManagerAttendance.punchin_time.asc())
            records_result = await db.execute(records_stmt)
            records = records_result.scalars().all()

            # Calculate total punch records (only completed ones)
            total_punch_records = sum(1 for record in records if record.punchin_time and record.punchout_time)

            # Calculate total duration (only for completed records)
            total_duration_minutes = 0

            # Dictionary to track daily summaries for this employee
            daily_summaries = {}

            # Process each record for daily data
            for record in records:
                if record.punchin_time and record.punchout_time:
                    # For BDMs: times are already in UTC, return as-is
                    duration_seconds = (record.punchout_time - record.punchin_time).total_seconds()
                    duration_minutes = int(duration_seconds / 60)
                    total_duration_minutes += duration_minutes

                    # Format duration for this record
                    record_hours = duration_minutes // 60
                    record_mins = duration_minutes % 60
                    record_duration = f"{record_hours}h {record_mins}m" if record_hours > 0 else f"{record_mins}m"

                    # Extract addresses
                    punchin_address = "-"
                    if record.punchin_location:
                        if isinstance(record.punchin_location, dict):
                            punchin_address = record.punchin_location.get('address', '-')
                        elif isinstance(record.punchin_location, str):
                            punchin_address = record.punchin_location

                    punchout_address = "-"
                    if record.punchout_location:
                        if isinstance(record.punchout_location, dict):
                            punchout_address = record.punchout_location.get('address', '-')
                        elif isinstance(record.punchout_location, str):
                            punchout_address = record.punchout_location

                    # Add to daily data
                    daily_data.append({
                        "date": record.punchin_time.strftime("%Y-%m-%d"),
                        "name": employee.name,
                        "role": "BDM",
                        "punchin_time": record.punchin_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "punchout_time": record.punchout_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": record_duration,
                        "punchin_address": punchin_address,
                        "punchout_address": punchout_address
                    })

                    # Track daily summary
                    date_key = record.punchin_time.strftime("%Y-%m-%d")
                    if date_key not in daily_summaries:
                        daily_summaries[date_key] = {
                            "punch_count": 0,
                            "total_minutes": 0
                        }
                    daily_summaries[date_key]["punch_count"] += 1
                    daily_summaries[date_key]["total_minutes"] += duration_minutes

            # Add daily summaries to daily_summary_data
            for date_key, summary in daily_summaries.items():
                hours = summary["total_minutes"] // 60
                mins = summary["total_minutes"] % 60
                duration_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

                daily_summary_data.append({
                    "date": date_key,
                    "name": employee.name,
                    "role": "BDM",
                    "total_punch_records": summary["punch_count"],
                    "total_duration": duration_str
                })

            # Format total duration
            hours = total_duration_minutes // 60
            mins = total_duration_minutes % 60
            total_duration = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

            summary_data.append({
                "date": f"{year}-{month:02d}",
                "name": employee.name,
                "role": "BDM",
                "total_punch_records": total_punch_records,
                "total_duration": total_duration,
                "total_duration_minutes": total_duration_minutes  # For sorting
            })

        # Sort summary by total duration (descending)
        summary_data.sort(key=lambda x: x["total_duration_minutes"], reverse=True)

        # Remove the sorting field before returning
        for item in summary_data:
            del item["total_duration_minutes"]

        # Sort daily data by date and name
        daily_data.sort(key=lambda x: (x["date"], x["name"], x["punchin_time"]))

        # Sort daily summary by date and name
        daily_summary_data.sort(key=lambda x: (x["date"], x["name"]))

        logger.info(f"[ATTENDANCE-EXPORT] Returning {len(summary_data)} summary records, {len(daily_summary_data)} daily summary records, and {len(daily_data)} daily records")

        return {
            "status": 200,
            "message": "Attendance export data fetched successfully",
            "data": {
                "summary": summary_data,
                "daily_summary": daily_summary_data,
                "daily": daily_data
            }
        }

    except Exception as e:
        logger.error(f"[ATTENDANCE-EXPORT] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export attendance data: {str(e)}")
