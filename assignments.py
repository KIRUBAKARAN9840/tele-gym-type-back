from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, not_, desc
from app.models.database import get_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymAssignmentHistory, GymCallLogs, ConvertedStatus,GymDatabase
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date, timedelta
import logging
import pytz

logger = logging.getLogger(__name__)

router = APIRouter()

class TelecallerResponse(BaseModel):
    id: int
    name: str
    mobile_number: str
    status: str
    verified: bool
    created_at: datetime

class GymInfo(BaseModel):
    id: int
    gym_name: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    address: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    zone: Optional[str] = None
    approval_status: Optional[str] = None
    location: Optional[str] = None
    verified: Optional[bool] = None

class TelecallerAssignment(BaseModel):
    telecaller_id: int
    gym_id: int
    target_date: Optional[date] = None

class BulkTelecallerAssignment(BaseModel):
    telecaller_id: int
    gym_ids: List[int]
    target_date: Optional[date] = None

class UnassignGym(BaseModel):
    gym_id: int

class ReassignGym(BaseModel):
    gym_id: int
    from_telecaller_id: int
    to_telecaller_id: int

@router.get("/telecallers")
async def get_telecallers(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get all telecallers under the manager"""
    telecallers = db.query(Telecaller).filter(
        Telecaller.manager_id == manager.id
    ).offset(skip).limit(limit).all()

    return {
        "telecallers": [
            TelecallerResponse(
                id=t.id,
                name=t.name,
                mobile_number=t.mobile_number,
                status=t.status,
                verified=t.verified,
                created_at=t.created_at
            )
            for t in telecallers
        ]
    }

@router.post("/assign-gym")
async def assign_gym(
    assignment: TelecallerAssignment,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Assign a gym to a telecaller"""
    # Verify telecaller belongs to manager
    telecaller = db.query(Telecaller).filter(
        Telecaller.id == assignment.telecaller_id,
        Telecaller.manager_id == manager.id
    ).first()

    if not telecaller:
        raise HTTPException(
            status_code=404,
            detail="Telecaller not found or not under your management"
        )

    # Check if gym exists
    gym = db.query(GymDatabase).filter(
        GymDatabase.id == assignment.gym_id
    ).first()

    if not gym:
        raise HTTPException(status_code=404, detail="Gym not found")

    # Check if gym is already assigned
    existing_assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == assignment.gym_id,
        GymAssignment.status == "active"
    ).first()

    if existing_assignment:
        raise HTTPException(
            status_code=400,
            detail="Gym is already assigned to another telecaller"
        )

    # Check if there's an inactive assignment for the same gym and telecaller
    inactive_assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == assignment.gym_id,
        GymAssignment.telecaller_id == assignment.telecaller_id,
        GymAssignment.status == "inactive"
    ).first()

    if inactive_assignment:
        # Reactivate the inactive assignment
        inactive_assignment.status = "active"
        inactive_assignment.manager_id = manager.id
        inactive_assignment.target_date = assignment.target_date
        assignment_record = inactive_assignment
    else:
        # Create new assignment
        assignment_record = GymAssignment(
            gym_id=assignment.gym_id,
            telecaller_id=assignment.telecaller_id,
            manager_id=manager.id,
            target_date=assignment.target_date,
            status="active"
        )
        db.add(assignment_record)

    # Create history record
    history = GymAssignmentHistory(
        gym_id=assignment.gym_id,
        telecaller_id=assignment.telecaller_id,
        manager_id=manager.id,
        action="assigned",
        remarks=f"Gym assigned to {telecaller.name}"
    )
    db.add(history)

    db.commit()

    return {"message": "Gym assigned successfully"}

@router.post("/bulk-assign-gyms")
async def bulk_assign_gyms(
    assignment: BulkTelecallerAssignment,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Assign multiple gyms to a telecaller"""
    # Verify telecaller belongs to manager
    telecaller = db.query(Telecaller).filter(
        Telecaller.id == assignment.telecaller_id,
        Telecaller.manager_id == manager.id
    ).first()

    if not telecaller:
        raise HTTPException(
            status_code=404,
            detail="Telecaller not found or not under your management"
        )

    # Check if gyms exist
    gyms = db.query(GymDatabase).filter(
        GymDatabase.id.in_(assignment.gym_ids)
    ).all()

    if len(gyms) != len(assignment.gym_ids):
        raise HTTPException(
            status_code=404,
            detail="One or more gyms not found"
        )

    # Check if gyms are already assigned
    existing_assignments = db.query(GymAssignment).filter(
        GymAssignment.gym_id.in_(assignment.gym_ids),
        GymAssignment.status == "active"
    ).all()

    if existing_assignments:
        gym_ids = [str(a.gym_id) for a in existing_assignments]
        raise HTTPException(
            status_code=400,
            detail=f"Gyms {', '.join(gym_ids)} are already assigned to other telecallers"
        )

    # Process each gym
    assigned_count = 0
    for gym_id in assignment.gym_ids:
        # Check for inactive assignment
        inactive_assignment = db.query(GymAssignment).filter(
            GymAssignment.gym_id == gym_id,
            GymAssignment.telecaller_id == assignment.telecaller_id,
            GymAssignment.status == "inactive"
        ).first()

        if inactive_assignment:
            # Reactivate
            inactive_assignment.status = "active"
            inactive_assignment.manager_id = manager.id
            inactive_assignment.target_date = assignment.target_date
        else:
            # Create new assignment
            assignment_record = GymAssignment(
                gym_id=gym_id,
                telecaller_id=assignment.telecaller_id,
                manager_id=manager.id,
                target_date=assignment.target_date,
                status="active"
            )
            db.add(assignment_record)

        # Create history record
        history = GymAssignmentHistory(
            gym_id=gym_id,
            telecaller_id=assignment.telecaller_id,
            manager_id=manager.id,
            action="assigned",
            remarks=f"Gym assigned to {telecaller.name}"
        )
        db.add(history)
        assigned_count += 1

    db.commit()

    return {"message": f"{assigned_count} gyms assigned successfully"}

@router.post("/unassign-gym")
async def unassign_gym(
    data: UnassignGym,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Unassign a gym from telecaller"""
    # Get active assignment
    assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == data.gym_id,
        GymAssignment.status == "active"
    ).first()

    if not assignment:
        raise HTTPException(status_code=404, detail="No active assignment found")

    # Verify assignment belongs to manager
    if assignment.manager_id != manager.id:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to unassign this gym"
        )

    # Check the gym's current call status
    # Get the latest call log for this gym
    latest_call_log = db.query(GymCallLogs).filter(
        GymCallLogs.gym_id == data.gym_id,
        GymCallLogs.telecaller_id == assignment.telecaller_id
    ).order_by(GymCallLogs.created_at.desc()).first()

    if latest_call_log:
        # Only allow unassignment if the status is pending or no_response
        allowed_statuses = ["pending", "no_response"]
        if latest_call_log.call_status not in allowed_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot unassign gym. Current status is '{latest_call_log.call_status}'. Only gyms with status 'pending' or 'no_response' can be unassigned."
            )

    # Get telecaller for history
    telecaller = db.query(Telecaller).filter(
        Telecaller.id == assignment.telecaller_id
    ).first()

    # Update assignment status
    assignment.status = "inactive"

    # Create history record
    history = GymAssignmentHistory(
        gym_id=data.gym_id,
        telecaller_id=assignment.telecaller_id,
        manager_id=manager.id,
        action="unassigned",
        remarks=f"Gym unassigned from {telecaller.name if telecaller else 'unknown'}"
    )
    db.add(history)

    db.commit()

    return {"message": "Gym unassigned successfully"}

@router.post("/reassign-gym")
async def reassign_gym(
    data: ReassignGym,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Reassign a gym from one telecaller to another"""
    # Verify both telecallers belong to manager
    from_telecaller = db.query(Telecaller).filter(
        Telecaller.id == data.from_telecaller_id,
        Telecaller.manager_id == manager.id
    ).first()

    to_telecaller = db.query(Telecaller).filter(
        Telecaller.id == data.to_telecaller_id,
        Telecaller.manager_id == manager.id
    ).first()

    if not from_telecaller or not to_telecaller:
        raise HTTPException(
            status_code=404,
            detail="One or both telecallers not found"
        )

    # Get current assignment
    assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == data.gym_id,
        GymAssignment.telecaller_id == data.from_telecaller_id,
        GymAssignment.status == "active"
    ).first()

    if not assignment:
        raise HTTPException(
            status_code=404,
            detail="No active assignment found for the given telecaller"
        )

    # Verify the current manager is the one who originally assigned the gym
    if assignment.manager_id != manager.id:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to reassign this gym. Only the assigning manager can reassign."
        )

    # Update assignment
    assignment.telecaller_id = data.to_telecaller_id
    assignment.assigned_at = datetime.utcnow()

    # Create history record
    history = GymAssignmentHistory(
        gym_id=data.gym_id,
        telecaller_id=data.to_telecaller_id,
        manager_id=manager.id,
        action="reassigned",
        remarks=f"Gym reassigned from {from_telecaller.name} to {to_telecaller.name}"
    )
    db.add(history)

    db.commit()

    return {"message": "Gym reassigned successfully"}

@router.get("/assigned-gyms/{telecaller_id}")
async def get_telecaller_assigned_gyms(
    telecaller_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Get gyms assigned to a specific telecaller"""
    # Verify telecaller belongs to manager
    telecaller = db.query(Telecaller).filter(
        Telecaller.id == telecaller_id,
        Telecaller.manager_id == manager.id
    ).first()

    if not telecaller:
        raise HTTPException(
            status_code=404,
            detail="Telecaller not found or not under your management"
        )

    # Get assigned gyms
    assignments = db.query(GymAssignment, GymDatabase).join(
        GymDatabase,
        GymAssignment.gym_id == GymDatabase.id
    ).filter(
        GymAssignment.telecaller_id == telecaller_id,
        GymAssignment.status == "active"
    ).offset(skip).limit(limit).all()

    gyms = []
    for assignment, gym in assignments:
        # Get the latest call log for this gym to determine current status
        latest_call_log = db.query(GymCallLogs).filter(
            GymCallLogs.gym_id == gym.id,
            GymCallLogs.telecaller_id == telecaller_id
        ).order_by(GymCallLogs.created_at.desc()).first()

        current_status = "pending"  # Default status if no calls made
        if latest_call_log:
            current_status = latest_call_log.call_status

        gym_info = GymInfo(
            id=gym.id,
            gym_name=gym.gym_name,
            contact_person=getattr(gym, 'contact_person', None),
            contact_phone=getattr(gym, 'contact_phone', None),
            address=getattr(gym, 'address', None),
            area=getattr(gym, 'area', None),
            city=getattr(gym, 'city', None),
            state=getattr(gym, 'state', None),
            pincode=getattr(gym, 'pincode', None),
            zone=getattr(gym, 'zone', None),
            approval_status=getattr(gym, 'approval_status', None),
            location=getattr(gym, 'location', None),
            verified=getattr(gym, 'verified', None)
        )
        # Add current status as an additional field
        gym_dict = gym_info.dict()
        gym_dict["current_call_status"] = current_status
        gyms.append(gym_dict)

    return {
        "telecaller": {
            "id": telecaller.id,
            "name": telecaller.name,
            "mobile_number": telecaller.mobile_number
        },
        "assigned_gyms": gyms,
        "page": (skip // limit) + 1,
        "limit": limit,
        "total_count": len(gyms),
        "total_pages": (len(gyms) + limit - 1) // limit
    }

@router.get("/unassigned-gyms")
async def get_unassigned_gyms(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 10,
    search: Optional[str] = None,
    city: Optional[str] = None,
    zone: Optional[str] = None,
    approval_status: Optional[str] = None
):
    """Get gyms that are not assigned to any telecaller with pagination and filters"""
    try:


        offset = (page - 1) * limit

        # Subquery to get all assigned gym IDs
        assigned_gym_ids_subquery = db.query(GymAssignment.gym_id).filter(
            GymAssignment.status == "active"
        ).subquery()

        # Build base query for unassigned gyms
        query = db.query(GymDatabase).filter(
            not_(GymDatabase.id.in_(assigned_gym_ids_subquery))
        )

        # Apply filters
        if search:
            search_filter = or_(
                GymDatabase.gym_name.ilike(f"%{search}%"),
                GymDatabase.area.ilike(f"%{search}%"),
                GymDatabase.contact_person.ilike(f"%{search}%"),
                GymDatabase.contact_phone.ilike(f"%{search}%")
            )
            query = query.filter(search_filter)

        if city:
            query = query.filter(GymDatabase.city.ilike(f"%{city}%"))

        if zone:
            query = query.filter(GymDatabase.zone == zone)

        if approval_status:
            query = query.filter(GymDatabase.approval_status == approval_status)

        # Get total count
        total = query.count()
        #(f"Found {total} unassigned gyms for manager {manager.id}")

        # Get gyms with pagination
        gyms = query.offset(offset).limit(limit).all()
        #(f"Retrieved {len(gyms)} unassigned gyms for page {page}")

        gym_list = []
        for gym in gyms:
            gym_info = GymInfo(
                id=gym.id,
                gym_name=gym.gym_name,
                contact_person=getattr(gym, 'contact_person', None),
                contact_phone=getattr(gym, 'contact_phone', None),
                address=getattr(gym, 'address', None),
                area=getattr(gym, 'area', None),
                city=getattr(gym, 'city', None),
                state=getattr(gym, 'state', None),
                pincode=getattr(gym, 'pincode', None),
                zone=getattr(gym, 'zone', None),
                approval_status=getattr(gym, 'approval_status', None),
                location=getattr(gym, 'location', None),
                verified=getattr(gym, 'verified', None)
            )
            gym_list.append(gym_info)

        total_pages = (total + limit - 1) // limit

        return {
            "unassigned_gyms": gym_list,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }

    except Exception as e:
        logger.error(f"Error fetching unassigned gyms for manager_id {manager.id}: "
                    f"{str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch unassigned gyms: {str(e)}"
        )



@router.get("/gyms/all")
async def get_all_gyms(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 50,  # Reduced default limit
    search: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None
):
    """Get all gyms from gym_database with pagination and filters"""
    try:
        #(f"Fetching all gyms for manager_id: {manager.id}, page: {page}, limit: {limit}, search: {search}, city: {city}, area: {area}")

        offset = (page - 1) * limit

        # Build base query
        query = db.query(GymDatabase)

        # Apply filters
        if search:
            search_filter = or_(
                GymDatabase.gym_name.ilike(f"%{search}%"),
                GymDatabase.area.ilike(f"%{search}%"),
                GymDatabase.contact_person.ilike(f"%{search}%"),
                GymDatabase.contact_phone.ilike(f"%{search}%")
            )
            query = query.filter(search_filter)

        if city:
            query = query.filter(GymDatabase.city.ilike(f"%{city}%"))

        if area:
            query = query.filter(GymDatabase.area == area)

        # Get total count
        total = query.count()

        # Get gyms with pagination
        gyms = query.offset(offset).limit(limit).all()

        gym_list = []
        for gym in gyms:
            gym_info = {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "contact_person": getattr(gym, "contact_person", None),
                "contact_phone": getattr(gym, "contact_phone", None),
                "contact_number": getattr(gym, "contact_phone", None),  # Alias for frontend
                "address": getattr(gym, "address", None),
                "area": getattr(gym, "area", None),
                "city": getattr(gym, "city", None),
                "state": getattr(gym, "state", None),
                "pincode": getattr(gym, "pincode", None),
                "zone": getattr(gym, "zone", None),
                "approval_status": getattr(gym, "approval_status", None),
                "location": getattr(gym, "location", None),
                "verified": getattr(gym, "verified", None)
            }
            gym_list.append(gym_info)

        total_pages = (total + limit - 1) // limit

        return {
            "gyms": gym_list,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }

    except Exception as e:
        logger.error(f"Error fetching all gyms: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch gyms: {str(e)}"
        )

@router.get("/locations/all")
async def get_all_locations(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Get all unique cities and their areas with statistics (total gyms and converted count)"""
    try:
        #(f"Fetching all locations for manager_id: {manager.id}")

        # Query all distinct cities and areas from gym_database
        # Using a single query to get all location data efficiently
        query = db.query(
            GymDatabase.city,
            GymDatabase.area
        ).filter(
            GymDatabase.city.isnot(None)
        ).distinct()

        results = query.all()

        # Build city -> areas map
        city_areas_map = {}
        all_cities = set()

        for city, area in results:
            if city:
                all_cities.add(city)
                if city not in city_areas_map:
                    city_areas_map[city] = set()
                if area:
                    city_areas_map[city].add(area)

        # Convert sets to sorted arrays
        cities_array = sorted(list(all_cities))
        city_areas_array = {}
        for city, areas in city_areas_map.items():
            city_areas_array[city] = sorted(list(areas))

        # Calculate statistics for each area (total gyms and converted count)
        # This should be across ALL managers, not just the current manager
        area_stats = {}

        for city, areas in city_areas_map.items():
            for area in areas:
                if not area or not area.strip():  # Skip empty areas
                    continue

                area_clean = area.strip()
                city_clean = city.strip()

                # Get total gyms in this area
                total_gyms = db.query(GymDatabase).filter(
                    GymDatabase.city == city_clean,
                    GymDatabase.area == area_clean
                ).count()

                # Get converted gyms in this area (across all telecallers/managers)
                # A gym is considered converted ONLY if its LATEST call log has "converted" status
                # We use a subquery to find gyms whose latest log is "converted"
                from sqlalchemy import func

                # Subquery: Get the latest call log for each gym
                latest_log_subquery = db.query(
                    GymCallLogs.gym_id,
                    func.max(GymCallLogs.created_at).label('latest_created_at')
                ).group_by(GymCallLogs.gym_id).subquery()

                # Find gyms whose latest log has "converted" status
                converted_gym_ids = db.query(GymCallLogs.gym_id).join(
                    latest_log_subquery,
                    (GymCallLogs.gym_id == latest_log_subquery.c.gym_id) &
                    (GymCallLogs.created_at == latest_log_subquery.c.latest_created_at)
                ).filter(
                    GymCallLogs.call_status == "converted"
                ).distinct().all()

                converted_gym_id_list = [gid[0] for gid in converted_gym_ids]

                # Now count gyms in this area that are in the converted list
                converted_count = db.query(GymDatabase).filter(
                    GymDatabase.city == city_clean,
                    GymDatabase.area == area_clean,
                    GymDatabase.id.in_(converted_gym_id_list) if converted_gym_id_list else False
                ).count()

                # Check if this area has any prime gyms (isprime = 1)
                prime_gyms_count = db.query(GymDatabase).filter(
                    GymDatabase.city == city_clean,
                    GymDatabase.area == area_clean,
                    GymDatabase.isprime == 1
                ).count()

                area_key = f"{city_clean}__{area_clean}"
                area_stats[area_key] = {
                    "total": total_gyms,
                    "converted": converted_count,
                    "is_prime": prime_gyms_count > 0  # Area is prime if it has at least one prime gym
                }

                #(f"Area stats for {city_clean}__{area_clean}: total={total_gyms}, converted={converted_count}, is_prime={prime_gyms_count > 0}")

        #(f"Found {len(cities_array)} unique cities and {len(area_stats)} area stats")

        return {
            "cities": cities_array,
            "city_areas": city_areas_array,
            "area_stats": area_stats
        }

    except Exception as e:
        logger.error(f"Error fetching locations: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch locations: {str(e)}"
        )

@router.get("/assignments/all")
async def get_all_assignments_system(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 50,  # Reduced default limit
    call_status: Optional[str] = None,
    telecaller_id: Optional[int] = Query(None, description="Filter by specific telecaller ID"),
    target_date_filter: Optional[str] = Query(None, description="Filter by target date: today, this_week, this_month, overdue, custom"),
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
    no_response_end_date: Optional[date] = Query(None, description="No response end date")
):
    """
    Get all gym assignments GLOBALLY across ALL managers.
    - This endpoint now returns ALL assignments for ALL managers to ensure consistent UI.
    - Each manager can see which gyms are assigned by whom.
    - Unassign permissions are still enforced based on ownership.
    """
    try:
        #(f"Fetching ALL GLOBAL assignments for manager_id: {manager.id}, telecaller_id: {telecaller_id}, call_status: {call_status}")

        offset = (page - 1) * limit
        from sqlalchemy import desc as sql_desc

        # Define statuses that should exclude gym from Pending tab (tracked statuses)
        # According to spec: "If the gym exists in gym_call_log under any status
        # (Follow-up, Converted, Rejected, No Response, Out of Service), it must not appear in Pending"
        # ALSO: delegated status should show in Follow-up tab, not Pending
        tracked_statuses = ['follow_up', 'follow_up_required', 'converted', 'rejected', 'no_response', 'out_of_service', 'delegated']

        # Define IST timezone
        ist_tz = pytz.timezone('Asia/Kolkata')

        assignment_list = []

        # If pending status requested, follow spec: query gym_assignments, exclude gyms in gym_call_log
        if call_status == 'pending' or call_status is None:
            # Build query to get assignments from gym_assignments table
            # CRITICAL: Do NOT filter by manager - return ALL assignments globally
            # This ensures all managers can see which gyms are assigned by whom
            query = db.query(GymAssignment, GymDatabase, Telecaller).join(
                GymDatabase, GymAssignment.gym_id == GymDatabase.id
            ).join(
                Telecaller, GymAssignment.telecaller_id == Telecaller.id
            ).filter(
                GymAssignment.status == "active",
                Telecaller.manager_id == manager.id
            )

            # Filter by telecaller_id if provided
            if telecaller_id:
                query = query.filter(GymAssignment.telecaller_id == telecaller_id)

            # Get total count and assignments
            total = query.count()
            all_assignments = query.offset(offset).limit(limit).all()

            for assignment, gym, telecaller in all_assignments:
                # Get the latest call log for this gym
                latest_call_log = db.query(GymCallLogs).filter(
                    GymCallLogs.gym_id == assignment.gym_id
                ).order_by(GymCallLogs.created_at.desc()).first()

                # EXCLUSION RULE: Skip if gym's latest status is a tracked status
                # According to spec: "If the gym exists in gym_call_log under any status
                # (Follow-up, Converted, Rejected, No Response, Out of Service), it must not appear in Pending"
                if latest_call_log and latest_call_log.call_status in tracked_statuses:
                    continue

                # This gym is truly pending (no call log or only pending/contacted/interested statuses)
                assignment_list.append({
                    'assignment': assignment,
                    'gym': gym,
                    'telecaller': telecaller,
                    'call_log': latest_call_log
                })

        else:
            # For other tabs (follow_up, converted, rejected, no_response, out_of_service)
            # Follow spec: Query gym_call_log table, get ACTUAL most recent entry per gym
            # THEN filter by whether that most recent status matches the requested tab
            # This ensures each gym only appears in ONE tab based on its latest status

            # Step 1: Build subquery to get the ACTUAL most recent call log per gym (regardless of status)
            latest_log_subq = db.query(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label('max_created')
            ).group_by(GymCallLogs.gym_id).subquery()

            # Step 2: Define which statuses belong to which tab
            status_map = {
                'follow_up': ['follow_up', 'follow_up_required', 'delegated'],
                'converted': ['converted'],
                'rejected': ['rejected'],
                'no_response': ['no_response'],
                'out_of_service': ['out_of_service']
            }

            # Get the statuses that should match this tab
            matching_statuses = status_map.get(call_status, [])

            # Step 3: Main query to get latest call logs with gym and telecaller info
            # We join with the subquery to get ONLY the most recent call log per gym
            query = db.query(
                GymCallLogs,
                GymDatabase,
                Telecaller
            ).join(
                GymDatabase, GymCallLogs.gym_id == GymDatabase.id
            ).join(
                latest_log_subq, and_(
                    GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                    GymCallLogs.created_at == latest_log_subq.c.max_created
                )
            ).join(
                Telecaller, GymCallLogs.telecaller_id == Telecaller.id
            )

            # Step 4: Filter by telecaller_id if provided
            if telecaller_id:
                query = query.filter(GymCallLogs.telecaller_id == telecaller_id)

            # Get all results (we'll filter by status and manager after)
            all_call_logs = query.all()

            # Step 5: Filter by matching status and manager
            for call_log, gym, telecaller in all_call_logs:
                # FIRST check if the most recent log's status matches this tab
                if call_log.call_status not in matching_statuses:
                    continue

                # THEN determine current_manager_id based on delegation
                current_manager_id = telecaller.manager_id
                if call_log.assigned_telecaller_id:
                    assigned_tc = db.query(Telecaller).filter(Telecaller.id == call_log.assigned_telecaller_id).first()
                    if assigned_tc:
                        current_manager_id = assigned_tc.manager_id

                # CRITICAL: Only show gyms that belong to the requesting manager
                if current_manager_id != manager.id:
                    continue

                assignment_list.append({
                    'assignment': call_log,  # For non-pending, call_log acts as assignment
                    'gym': gym,
                    'telecaller': telecaller,
                    'call_log': call_log
                })

            total = len(assignment_list)

            # Apply pagination after manager and status filtering
            start_idx = offset
            end_idx = offset + limit
            assignment_list = assignment_list[start_idx:end_idx]

        # Convert assignment_list to assignment_info format
        final_assignments = []
        for item in assignment_list:
            assignment = item['assignment']
            gym = item['gym']
            telecaller = item['telecaller']
            call_log = item['call_log']

            # Determine gym_id and telecaller_id based on whether it's pending or other tab
            if call_status == 'pending' or call_status is None:
                gym_id = assignment.gym_id
                telecaller_id = assignment.telecaller_id
                manager_id = assignment.manager_id
                assigned_at = assignment.assigned_at
                target_date = assignment.target_date
                status = assignment.status
                current_call_status = call_log.call_status if call_log else 'pending'
            else:
                # For non-pending tabs, data comes from call_log
                gym_id = call_log.gym_id
                telecaller_id = call_log.telecaller_id
                manager_id = telecaller.manager_id
                assigned_at = call_log.created_at
                target_date = None
                status = 'active'
                current_call_status = call_log.call_status

            # Get converted status if available
            converted_status = db.query(ConvertedStatus).filter(
                ConvertedStatus.gym_id == gym_id,
                ConvertedStatus.telecaller_id == telecaller_id
            ).first()

            # Get latest follow-up date for this gym
            latest_followup_log = db.query(GymCallLogs).filter(
                GymCallLogs.gym_id == gym_id,
                GymCallLogs.telecaller_id == telecaller_id,
                GymCallLogs.follow_up_date.isnot(None)
            ).order_by(GymCallLogs.follow_up_date.desc()).first()

            follow_up_date = None
            if latest_followup_log and latest_followup_log.follow_up_date:
                # Convert to IST and format
                follow_up_utc = pytz.UTC.localize(latest_followup_log.follow_up_date)
                follow_up_date_ist = follow_up_utc.astimezone(ist_tz)
                follow_up_date = follow_up_date_ist.isoformat()

            # Determine contact_phone: Check gym_call_logs for new_contact_number first
            # Get the most recent call log entry for this gym_id across all telecallers
            most_recent_call_log = db.query(GymCallLogs).filter(
                GymCallLogs.gym_id == gym_id
            ).order_by(desc(GymCallLogs.created_at)).first()

            # Use new_contact_number from most recent call log if available, otherwise fall back to gym.contact_phone
            contact_phone = None
            contact_phone_source = None  # 'call_logs' or 'database'
            if most_recent_call_log and most_recent_call_log.new_contact_number:
                contact_phone = most_recent_call_log.new_contact_number
                contact_phone_source = 'call_logs'
                #(f"[DEBUG MANAGER ALL] Gym {gym_id}: Using call_logs phone: {contact_phone}")
            else:
                contact_phone = getattr(gym, 'contact_phone', None)
                contact_phone_source = 'database'
                #(f"[DEBUG MANAGER ALL] Gym {gym_id}: Using database phone: {contact_phone}, call_log_found: {most_recent_call_log is not None}")

            assignment_info = {
                "gym_id": gym_id,
                "gym_name": gym.gym_name,
                "telecaller_id": telecaller_id,
                "telecaller_name": telecaller.name,
                "telecaller_mobile": telecaller.mobile_number,
                "manager_id": manager_id,
                "manager_name": manager.name,
                "assigned_at": assigned_at.isoformat() if assigned_at else None,
                "target_date": target_date.isoformat() if target_date else None,  # For frontend display
                "gym_target_date": target_date,  # Raw date object for filtering
                "follow_up_date": follow_up_date,  # Add follow-up date
                "status": status,
                "current_call_status": current_call_status,
                "city": getattr(gym, "city", None),
                "contact_phone": contact_phone,
                "contact_phone_source": contact_phone_source,
                "address": getattr(gym, "address", None),
                "area": getattr(gym, "area", None),
                "document_collected": converted_status.document_uploaded if converted_status else False,
                "membership_collected": converted_status.membership_plan_created if converted_status else False,
                "session_collected": converted_status.session_created if converted_status else False,
                "daily_pass_collected": converted_status.daily_pass_created if converted_status else False,
                "studio_images_collected": converted_status.gym_studio_images_uploaded if converted_status else False,
                "agreement_collected": converted_status.agreement_signed if converted_status else False
            }
            final_assignments.append(assignment_info)

        # Replace assignment_list with final_assignments for subsequent filtering
        assignment_list = final_assignments

        # Apply target date filter if specified
        if target_date_filter and call_status == "pending":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            filtered_assignments = []
            for assignment in assignment_list:
                if assignment.get("gym_target_date"):
                    # Use the gym_target_date directly since it's already a date object
                    target_date = assignment["gym_target_date"]

                    if target_date_filter == "today" and target_date == today:
                        filtered_assignments.append(assignment)
                    elif target_date_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= target_date <= week_end:
                            filtered_assignments.append(assignment)
                    elif target_date_filter == "this_month":
                        if target_date.month == today.month and target_date.year == today.year:
                            filtered_assignments.append(assignment)
                    elif target_date_filter == "overdue" and target_date < today:
                        filtered_assignments.append(assignment)
                    elif target_date_filter == "custom" and target_start_date and target_end_date:
                        if target_start_date <= target_date <= target_end_date:
                            filtered_assignments.append(assignment)

            # Apply the filter - even if no assignments match, return empty list
            assignment_list = filtered_assignments

        # Apply follow-up date filter if specified
        if follow_up_filter and call_status == "follow_up":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            # First, get all call logs with follow_up_date
            gym_ids_with_followup = db.query(GymCallLogs.gym_id).filter(
                GymCallLogs.follow_up_date.isnot(None)
            ).distinct().all()
            gym_ids_with_followup = [g[0] for g in gym_ids_with_followup]

            # Filter assignments to only those with follow-ups
            follow_up_assignments = []
            for assignment in assignment_list:
                if assignment['gym_id'] in gym_ids_with_followup:
                    # Get the latest call log with follow_up_date for this gym
                    latest_followup_log = db.query(GymCallLogs).filter(
                        GymCallLogs.gym_id == assignment['gym_id'],
                        GymCallLogs.telecaller_id == assignment.get('telecaller_id'),
                        GymCallLogs.follow_up_date.isnot(None)
                    ).order_by(GymCallLogs.follow_up_date.desc()).first()

                    if latest_followup_log and latest_followup_log.follow_up_date:
                        # Convert to IST date
                        followup_utc = pytz.UTC.localize(latest_followup_log.follow_up_date)
                        followup_date = followup_utc.astimezone(ist_tz).date()

                        if follow_up_filter == "today" and followup_date == today:
                            follow_up_assignments.append(assignment)
                        elif follow_up_filter == "this_week":
                            week_start = today - timedelta(days=today.weekday())
                            week_end = week_start + timedelta(days=6)
                            if week_start <= followup_date <= week_end:
                                follow_up_assignments.append(assignment)
                        elif follow_up_filter == "overdue" and followup_date < today:
                            follow_up_assignments.append(assignment)
                        elif follow_up_filter == "custom" and follow_up_start_date and follow_up_end_date:
                            if follow_up_start_date <= followup_date <= follow_up_end_date:
                                follow_up_assignments.append(assignment)

            # Apply the filter - even if no assignments match, return empty list
            assignment_list = follow_up_assignments

        # Apply converted date filter if specified
        if converted_filter and call_status == "converted":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            # First, get all converted call logs
            gym_ids_with_converted = db.query(GymCallLogs.gym_id).filter(
                GymCallLogs.call_status == "converted"
            ).distinct().all()
            gym_ids_with_converted = [g[0] for g in gym_ids_with_converted]

            # Filter assignments to only those with converted calls
            converted_assignments = []
            for assignment in assignment_list:
                if assignment['gym_id'] in gym_ids_with_converted:
                    # Get the latest converted call log for this gym
                    latest_converted_log = db.query(GymCallLogs).filter(
                        GymCallLogs.gym_id == assignment['gym_id'],
                        GymCallLogs.telecaller_id == assignment.get('telecaller_id'),
                        GymCallLogs.call_status == "converted"
                    ).order_by(GymCallLogs.created_at.desc()).first()

                    if latest_converted_log and latest_converted_log.created_at:
                        # Convert to IST date
                        created_utc = pytz.UTC.localize(latest_converted_log.created_at)
                        converted_date = created_utc.astimezone(ist_tz).date()

                        if converted_filter == "today" and converted_date == today:
                            converted_assignments.append(assignment)
                        elif converted_filter == "this_week":
                            week_start = today - timedelta(days=today.weekday())
                            week_end = week_start + timedelta(days=6)
                            if week_start <= converted_date <= week_end:
                                converted_assignments.append(assignment)
                        elif converted_filter == "this_month":
                            if converted_date.month == today.month and converted_date.year == today.year:
                                converted_assignments.append(assignment)
                        elif converted_filter == "custom" and converted_start_date and converted_end_date:
                            if converted_start_date <= converted_date <= converted_end_date:
                                converted_assignments.append(assignment)

            # Apply the filter - even if no assignments match, return empty list
            assignment_list = converted_assignments

        # Apply rejected date filter if specified
        if rejected_filter and call_status == "rejected":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            #(f"Applying rejected filter: {rejected_filter}, start_date: {rejected_start_date}, end_date: {rejected_end_date}")
            #(f"Initial assignment_list count: {len(assignment_list)}")

            # Filter assignments to only those with rejected calls
            rejected_assignments = []
            for assignment in assignment_list:
                # Get the latest rejected call log for this gym
                latest_rejected_log = db.query(GymCallLogs).filter(
                    GymCallLogs.gym_id == assignment['gym_id'],
                    GymCallLogs.telecaller_id == assignment.get('telecaller_id'),
                    GymCallLogs.call_status == "rejected"
                ).order_by(GymCallLogs.created_at.desc()).first()

                if latest_rejected_log and latest_rejected_log.created_at:
                    # Convert to IST date
                    created_utc = pytz.UTC.localize(latest_rejected_log.created_at)
                    rejected_date = created_utc.astimezone(ist_tz).date()

                    if rejected_filter == "today" and rejected_date == today:
                        rejected_assignments.append(assignment)
                    elif rejected_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= rejected_date <= week_end:
                            rejected_assignments.append(assignment)
                    elif rejected_filter == "this_month":
                        if rejected_date.month == today.month and rejected_date.year == today.year:
                            rejected_assignments.append(assignment)
                    elif rejected_filter == "custom":
                        if rejected_start_date and rejected_end_date:
                            if rejected_start_date <= rejected_date <= rejected_end_date:
                                rejected_assignments.append(assignment)
                                logger.debug(f"Assignment {assignment['gym_id']} passed custom date filter: {rejected_date} (range: {rejected_start_date} to {rejected_end_date})")
                            else:
                                logger.debug(f"Assignment {assignment['gym_id']} FAILED custom date filter: {rejected_date} (range: {rejected_start_date} to {rejected_end_date})")
                        else:
                            logger.warning(f"Custom rejected filter selected but dates are missing - start_date: {rejected_start_date}, end_date: {rejected_end_date}")

            #(f"Rejected assignments after filtering: {len(rejected_assignments)}")
            # Apply the filter - even if no assignments match, return empty list
            assignment_list = rejected_assignments

        # Apply no response date filter if specified
        if no_response_filter and call_status == "no_response":
            ist_tz = pytz.timezone('Asia/Kolkata')
            today = datetime.now(ist_tz).date()

            #(f"Applying no_response filter: {no_response_filter}, start_date: {no_response_start_date}, end_date: {no_response_end_date}")
            #(f"Initial assignment_list count: {len(assignment_list)}")

            # Filter assignments to only those with no response calls
            no_response_assignments = []
            for assignment in assignment_list:
                # Get the latest no response call log for this gym
                latest_no_response_log = db.query(GymCallLogs).filter(
                    GymCallLogs.gym_id == assignment['gym_id'],
                    GymCallLogs.telecaller_id == assignment.get('telecaller_id'),
                    GymCallLogs.call_status == "no_response"
                ).order_by(GymCallLogs.created_at.desc()).first()

                if latest_no_response_log and latest_no_response_log.created_at:
                    # Convert to IST date
                    created_utc = pytz.UTC.localize(latest_no_response_log.created_at)
                    no_response_date = created_utc.astimezone(ist_tz).date()

                    if no_response_filter == "today" and no_response_date == today:
                        no_response_assignments.append(assignment)
                    elif no_response_filter == "this_week":
                        week_start = today - timedelta(days=today.weekday())
                        week_end = week_start + timedelta(days=6)
                        if week_start <= no_response_date <= week_end:
                            no_response_assignments.append(assignment)
                    elif no_response_filter == "this_month":
                        if no_response_date.month == today.month and no_response_date.year == today.year:
                            no_response_assignments.append(assignment)
                    elif no_response_filter == "custom":
                        if no_response_start_date and no_response_end_date:
                            if no_response_start_date <= no_response_date <= no_response_end_date:
                                no_response_assignments.append(assignment)
                                logger.debug(f"Assignment {assignment['gym_id']} passed custom date filter: {no_response_date} (range: {no_response_start_date} to {no_response_end_date})")
                            else:
                                logger.debug(f"Assignment {assignment['gym_id']} FAILED custom date filter: {no_response_date} (range: {no_response_start_date} to {no_response_end_date})")
                        else:
                            logger.warning(f"Custom no_response filter selected but dates are missing - start_date: {no_response_start_date}, end_date: {no_response_end_date}")

            #(f"No response assignments after filtering: {len(no_response_assignments)}")
            # Apply the filter - even if no assignments match, return empty list
            assignment_list = no_response_assignments

        # Apply verification complete filter for converted assignments
        if verification_complete and call_status == "converted":
            verification_filtered_assignments = []
            for assignment in assignment_list:
                # Check if all verification items are complete
                all_complete = (
                    assignment.get("document_collected", False) and
                    assignment.get("membership_collected", False) and
                    assignment.get("session_collected", False) and
                    assignment.get("daily_pass_collected", False) and
                    assignment.get("studio_images_collected", False) and
                    assignment.get("agreement_collected", False)
                )

                # Filter based on the verification_complete parameter
                if verification_complete == "true" and all_complete:
                    verification_filtered_assignments.append(assignment)
                elif verification_complete == "false" and not all_complete:
                    verification_filtered_assignments.append(assignment)

            # Apply the filter - even if no assignments match, return empty list
            assignment_list = verification_filtered_assignments

        # Apply pagination to filtered results
        # NOTE: For non-pending tabs, pagination was already applied earlier, so skip it here
        if call_status == 'pending' or call_status is None:
            # For pending tab, apply pagination after all filters
            total = len(assignment_list)
            total_pages = (total + limit - 1) // limit
            start_idx = offset
            end_idx = offset + limit
            paginated_list = assignment_list[start_idx:end_idx]
        else:
            # For non-pending tabs, pagination was already applied
            # Just calculate total_pages based on the filtered results
            total = len(assignment_list)
            total_pages = (total + limit - 1) // limit
            paginated_list = assignment_list

        return {
            "assignments": paginated_list,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }

    except Exception as e:
        logger.error(f"Error fetching all assignments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch assignments: {str(e)}"
        )

@router.get("/assignments/check-status")
async def check_all_gym_assignments(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """
    Check assignment status for ALL gyms globally across ALL managers.

    Logic:
    1. First check gym_call_logs table (latest entry per gym) - if gym exists here with ANY status, it's ASSIGNED
    2. Only if NOT in gym_call_logs, then check gym_assignment table with status='active'

    This ensures all managers can see which gyms are assigned by whom, regardless of status.
    """
    try:
        #(f"Checking ALL gym assignments globally for manager_id: {manager.id}")

        # Step 1: Get all gyms from gym_call_logs (latest entry per gym)
        # This gives us gyms that have been called/processed
        latest_log_subq = db.query(
            GymCallLogs.gym_id,
            func.max(GymCallLogs.created_at).label('max_created')
        ).group_by(GymCallLogs.gym_id).subquery()

        # Get the latest call log for each gym with all details
        latest_call_logs = db.query(
            GymCallLogs,
            GymDatabase,
            Telecaller
        ).join(
            GymDatabase, GymCallLogs.gym_id == GymDatabase.id
        ).join(
            latest_log_subq, and_(
                GymCallLogs.gym_id == latest_log_subq.c.gym_id,
                GymCallLogs.created_at == latest_log_subq.c.max_created
            )
        ).join(
            Telecaller, GymCallLogs.telecaller_id == Telecaller.id
        ).all()

        # Step 2: Get all gyms from gym_assignment where status='active'
        # These are gyms that are assigned but haven't been called yet
        active_assignments = db.query(
            GymAssignment,
            GymDatabase,
            Telecaller
        ).join(
            GymDatabase, GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller, GymAssignment.telecaller_id == Telecaller.id
        ).filter(
            GymAssignment.status == "active"
        ).all()

        # Step 3: Build a map of all assigned gyms
        # Priority: gym_call_logs takes precedence over gym_assignment
        assigned_gyms = {}  # gym_id -> assignment_details

        # First, add all gyms from gym_call_logs
        for call_log, gym, telecaller in latest_call_logs:
            assigned_gyms[gym.id] = {
                'gym_id': gym.id,
                'telecaller_id': telecaller.id,
                'telecaller_name': telecaller.name,
                'telecaller_mobile': telecaller.mobile_number,
                'manager_id': telecaller.manager_id,
                'manager_name': None,  # Will be fetched if needed
                'assigned_at': call_log.created_at,
                'target_date': None,
                'status': 'active',
                'current_call_status': call_log.call_status,
                'source': 'gym_call_logs',  # Indicates this came from call_logs
                'call_log_id': call_log.id
            }

        # Then, add gyms from gym_assignment ONLY if not already in assigned_gyms
        # This ensures gym_call_logs takes precedence
        for assignment, gym, telecaller in active_assignments:
            if gym.id not in assigned_gyms:
                assigned_gyms[gym.id] = {
                    'gym_id': gym.id,
                    'telecaller_id': telecaller.id,
                    'telecaller_name': telecaller.name,
                    'telecaller_mobile': telecaller.mobile_number,
                    'manager_id': telecaller.manager_id,
                    'manager_name': None,
                    'assigned_at': assignment.assigned_at,
                    'target_date': assignment.target_date.isoformat() if assignment.target_date else None,
                    'status': assignment.status,
                    'current_call_status': 'pending',  # No call logs yet, so status is pending
                    'source': 'gym_assignment',  # Indicates this came from gym_assignment
                    'call_log_id': None
                }

        # Convert to list
        assignments_list = list(assigned_gyms.values())

        #(f"Found {len(assignments_list)} total assigned gyms globally")
        #(f"  - From gym_call_logs: {len([a for a in assignments_list if a['source'] == 'gym_call_logs'])}")
        #(f"  - From gym_assignment: {len([a for a in assignments_list if a['source'] == 'gym_assignment'])}")

        return {
            "assignments": assignments_list,
            "total": len(assignments_list)
        }

    except Exception as e:
        logger.error(f"Error checking gym assignments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check gym assignments: {str(e)}"
        )

@router.get("/gym/{gym_id}/call-history")
async def get_gym_call_history(
    gym_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """
    Get the complete call history for a gym.
    This is used by the follow-up history modal in the action column.
    No delegation or assignment rules apply - simply query gym_call_logs by gym_id.
    """
    # Define IST timezone
    ist_tz = pytz.timezone('Asia/Kolkata')

    try:
        # Get ALL call logs for this gym from gym_call_logs table
        # No manager filtering, no assignment checks - just query by gym_id
        call_logs = db.query(GymCallLogs).filter(
            GymCallLogs.gym_id == gym_id
        ).order_by(desc(GymCallLogs.created_at)).all()

        # Format the response
        history = []
        for log in call_logs:
            # Datetimes are now stored in IST, handle both naive and timezone-aware
            # If naive, assume it's IST
            if log.created_at:
                if log.created_at.tzinfo is None:
                    created_at_ist = ist_tz.localize(log.created_at)
                else:
                    created_at_ist = log.created_at.astimezone(ist_tz)
            else:
                created_at_ist = None

            if log.follow_up_date:
                if log.follow_up_date.tzinfo is None:
                    follow_up_date_ist = ist_tz.localize(log.follow_up_date)
                else:
                    follow_up_date_ist = log.follow_up_date.astimezone(ist_tz)
            else:
                follow_up_date_ist = None

            # Get telecaller info who created this log
            creator_tc = db.query(Telecaller).filter(Telecaller.id == log.telecaller_id).first()

            # Get assigned telecaller info if this was delegated
            assigned_tc_info = None
            if log.assigned_telecaller_id:
                assigned_tc = db.query(Telecaller).filter(Telecaller.id == log.assigned_telecaller_id).first()
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
        logger.error(f"Error fetching gym history: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )
