from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from app.models.database import get_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymAssignmentHistory, GymCallLogs,GymDatabase
)
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

class GymAssignmentInfo(BaseModel):
    gym_id: int
    gym_name: str
    telecaller_id: int
    telecaller_name: str
    telecaller_mobile: str
    assigned_at: datetime
    status: str
    manager_id: int  # Added to track which manager made the assignment
    current_call_status: Optional[str] = None  # Latest call status for this gym
    target_date: Optional[datetime] = None  # Target date for assignment
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_phone_source: Optional[str] = None  # 'call_logs' or 'database'
    address: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    zone: Optional[str] = None
    approval_status: Optional[str] = None

class AssignmentResponse(BaseModel):
    assignments: List[GymAssignmentInfo]
    page: int
    limit: int
    total: int
    total_pages: int

class BulkAssignRequest(BaseModel):
    gym_ids: List[int]
    telecaller_id: int

class BulkUnassignRequest(BaseModel):
    gym_ids: List[int]

@router.get("/assignments", response_model=AssignmentResponse)
async def get_all_assignments(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 10,
    search: Optional[str] = None,
    telecaller_id: Optional[int] = None,
    status: Optional[str] = None
):
    """Get all gym assignments under the manager"""
    try:
        #(f"Fetching assignments for manager_id: {manager.id}, "
                  

        offset = (page - 1) * limit

        # Build query
        query = db.query(GymAssignment, GymDatabase, Telecaller).join(
            GymDatabase, GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller, GymAssignment.telecaller_id == Telecaller.id
        ).filter(
            GymAssignment.manager_id == manager.id
        )

        # Apply filters
        if search:
            search_filter = or_(
                GymDatabase.gym_name.ilike(f"%{search}%"),
                GymDatabase.area.ilike(f"%{search}%"),
                GymDatabase.city.ilike(f"%{search}%"),
                GymDatabase.contact_person.ilike(f"%{search}%")
            )
            query = query.filter(search_filter)

        if telecaller_id:
            query = query.filter(GymAssignment.telecaller_id == telecaller_id)

        if status:
            query = query.filter(GymAssignment.status == status)

        # Get total count
        total = query.count()
        #(f"Found {total} total assignments for manager {manager.id}")

        # Get assignments with pagination
        assignments = query.offset(offset).limit(limit).all()
        #(f"Retrieved {len(assignments)} assignments for page {page}")

        assignment_list = []
        for assignment, gym, telecaller in assignments:
            # Determine contact_phone: Check gym_call_logs for new_contact_number first
            # Get the most recent call log entry for this gym_id across all telecallers
            from sqlalchemy import desc as sql_desc

            most_recent_call_log = db.query(GymCallLogs).filter(
                GymCallLogs.gym_id == assignment.gym_id
            ).order_by(sql_desc(GymCallLogs.created_at)).first()

            # Use new_contact_number from most recent call log if available, otherwise fall back to gym.contact_phone
            contact_phone = None
            contact_phone_source = None  # 'call_logs' or 'database'
            if most_recent_call_log and most_recent_call_log.new_contact_number:
                contact_phone = most_recent_call_log.new_contact_number
                contact_phone_source = 'call_logs'
                #(f"[DEBUG MANAGER] Gym {assignment.gym_id}: Using call_logs phone: {contact_phone}")
            else:
                contact_phone = getattr(gym, 'contact_phone', None)
                contact_phone_source = 'database'
                #(f"[DEBUG MANAGER] Gym {assignment.gym_id}: Using database phone: {contact_phone}, call_log_found: {most_recent_call_log is not None}")

            assignment_info = GymAssignmentInfo(
                gym_id=assignment.gym_id,
                gym_name=gym.gym_name,
                telecaller_id=assignment.telecaller_id,
                telecaller_name=telecaller.name,
                telecaller_mobile=telecaller.mobile_number,
                assigned_at=assignment.assigned_at,
                status=assignment.status,
                manager_id=assignment.manager_id,  # Include manager_id
                current_call_status=most_recent_call_log.call_status if most_recent_call_log else 'pending',  # Latest call status
                target_date=assignment.target_date,  # Include target_date
                contact_person=getattr(gym, 'contact_person', None),
                contact_phone=contact_phone,
                contact_phone_source=contact_phone_source,
                address=getattr(gym, 'address', None),
                area=getattr(gym, 'area', None),
                city=getattr(gym, 'city', None),
                state=getattr(gym, 'state', None),
                pincode=getattr(gym, 'pincode', None),
                zone=getattr(gym, 'zone', None),
                approval_status=getattr(gym, 'approval_status', None)
            )
            assignment_list.append(assignment_info)

        total_pages = (total + limit - 1) // limit

        return AssignmentResponse(
            assignments=assignment_list,
            page=page,
            limit=limit,
            total=total,
            total_pages=total_pages
        )

    except Exception as e:
        logger.error(f"Error fetching assignments for manager_id {manager.id}: "
                    f"{str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch assignments: {str(e)}"
        )

@router.post("/assignments/assign")
async def assign_gym_to_telecaller(
    gym_id: int,
    telecaller_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Assign a gym to a telecaller"""
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

    # Check if gym exists
    gym = db.query(GymDatabase).filter(
        GymDatabase.id == gym_id
    ).first()

    if not gym:
        raise HTTPException(status_code=404, detail="Gym not found")

    # Check if gym is already assigned
    existing_assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == gym_id,
        GymAssignment.status == "active"
    ).first()

    if existing_assignment:
        raise HTTPException(
            status_code=400,
            detail="Gym is already assigned to another telecaller"
        )

    # Create assignment
    assignment_record = GymAssignment(
        gym_id=gym_id,
        telecaller_id=telecaller_id,
        manager_id=manager.id,
        status="active"
    )
    db.add(assignment_record)

    # Create history record
    history = GymAssignmentHistory(
        gym_id=gym_id,
        telecaller_id=telecaller_id,
        manager_id=manager.id,
        action="assigned",
        remarks=f"Gym assigned to {telecaller.name}"
    )
    db.add(history)

    db.commit()

    return {"message": "Gym assigned successfully"}

@router.post("/assignments/unassign")
async def unassign_gym(
    gym_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Unassign a gym from telecaller"""
    # Get active assignment
    assignment = db.query(GymAssignment).filter(
        GymAssignment.gym_id == gym_id,
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

    # Get telecaller for history
    telecaller = db.query(Telecaller).filter(
        Telecaller.id == assignment.telecaller_id
    ).first()

    # Update assignment status
    assignment.status = "inactive"

    # Create history record
    history = GymAssignmentHistory(
        gym_id=gym_id,
        telecaller_id=assignment.telecaller_id,
        manager_id=manager.id,
        action="unassigned",
        remarks=f"Gym unassigned from {telecaller.name if telecaller else 'unknown'}"
    )
    db.add(history)

    db.commit()

    return {"message": "Gym unassigned successfully"}

@router.post("/assignments/bulk-assign")
async def bulk_assign_gyms(
    request: BulkAssignRequest,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Bulk assign multiple gyms to a telecaller"""
    try:
        #(f"Bulk assigning {len(request.gym_ids)} gyms to telecaller_id: {request.telecaller_id}")

        # Verify telecaller belongs to manager
        telecaller = db.query(Telecaller).filter(
            Telecaller.id == request.telecaller_id,
            Telecaller.manager_id == manager.id
        ).first()

        if not telecaller:
            raise HTTPException(
                status_code=404,
                detail="Telecaller not found or not under your management"
            )

        # Check if gyms exist and are not already assigned
        gyms = db.query(GymDatabase).filter(
            GymDatabase.id.in_(request.gym_ids)
        ).all()

        if len(gyms) != len(request.gym_ids):
            raise HTTPException(
                status_code=404,
                detail="One or more gyms not found"
            )

        # Check for existing assignments
        existing_assignments = db.query(GymAssignment).filter(
            GymAssignment.gym_id.in_(request.gym_ids),
            GymAssignment.status == "active"
        ).all()

        if existing_assignments:
            assigned_gym_ids = [a.gym_id for a in existing_assignments]
            raise HTTPException(
                status_code=400,
                detail=f"Gyms {assigned_gym_ids} are already assigned"
            )

        # Create assignments
        assignment_records = []
        history_records = []

        for gym_id in request.gym_ids:
            assignment = GymAssignment(
                gym_id=gym_id,
                telecaller_id=request.telecaller_id,
                manager_id=manager.id,
                status="active"
            )
            assignment_records.append(assignment)

            history = GymAssignmentHistory(
                gym_id=gym_id,
                telecaller_id=request.telecaller_id,
                manager_id=manager.id,
                action="assigned",
                remarks=f"Bulk assigned to {telecaller.name}"
            )
            history_records.append(history)

        db.add_all(assignment_records)
        db.add_all(history_records)
        db.commit()

        #(f"Successfully bulk assigned {len(request.gym_ids)} gyms")
        return {"message": f"Successfully assigned {len(request.gym_ids)} gyms"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in bulk assignment: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to bulk assign gyms: {str(e)}"
        )

@router.post("/assignments/bulk-unassign")
async def bulk_unassign_gyms(
    request: BulkUnassignRequest,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Bulk unassign multiple gyms"""
    try:
        #(f"Bulk unassigning {len(request.gym_ids)} gyms")

        # Get active assignments
        assignments = db.query(GymAssignment).filter(
            GymAssignment.gym_id.in_(request.gym_ids),
            GymAssignment.status == "active"
        ).all()

        if not assignments:
            raise HTTPException(
                status_code=404,
                detail="No active assignments found for the provided gyms"
            )

        # Verify all assignments belong to manager
        for assignment in assignments:
            if assignment.manager_id != manager.id:
                raise HTTPException(
                    status_code=403,
                    detail=f"You don't have permission to unassign gym_id {assignment.gym_id}"
                )

        # Get telecaller names for history
        telecaller_ids = list(set(a.telecaller_id for a in assignments))
        telecallers = {t.id: t.name for t in db.query(Telecaller).filter(
            Telecaller.id.in_(telecaller_ids)
        ).all()}

        # Update assignments and create history records
        history_records = []
        for assignment in assignments:
            assignment.status = "inactive"

            history = GymAssignmentHistory(
                gym_id=assignment.gym_id,
                telecaller_id=assignment.telecaller_id,
                manager_id=manager.id,
                action="unassigned",
                remarks=f"Bulk unassigned from {telecallers.get(assignment.telecaller_id, 'unknown')}"
            )
            history_records.append(history)

        db.add_all(history_records)
        db.commit()

        #(f"Successfully bulk unassigned {len(assignments)} gyms")
        return {"message": f"Successfully unassigned {len(assignments)} gyms"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in bulk unassignment: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to bulk unassign gyms: {str(e)}"
        )

@router.get("/assignments/telecaller/{telecaller_id}")
async def get_telecaller_assignments(
    telecaller_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 10
):
    """Get all assignments for a specific telecaller"""
    try:
        #(f"Fetching assignments for telecaller_id: {telecaller_id}")

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

        offset = (page - 1) * limit

        query = db.query(GymAssignment, GymDatabase).join(
            GymDatabase,
            GymAssignment.gym_id == GymDatabase.id
        ).filter(
            GymAssignment.telecaller_id == telecaller_id,
            GymAssignment.manager_id == manager.id
        )

        total = query.count()
        assignments = query.offset(offset).limit(limit).all()

        assignment_list = []
        for assignment, gym in assignments:
           
            from sqlalchemy import desc as sql_desc

            most_recent_call_log = db.query(GymCallLogs).filter(
                GymCallLogs.gym_id == assignment.gym_id
            ).order_by(sql_desc(GymCallLogs.created_at)).first()

            # Use new_contact_number from most recent call log if available, otherwise fall back to gym.contact_phone
            contact_phone = None
            contact_phone_source = None  # 'call_logs' or 'database'
            if most_recent_call_log and most_recent_call_log.new_contact_number:
                contact_phone = most_recent_call_log.new_contact_number
                contact_phone_source = 'call_logs'
                #(f"[DEBUG MANAGER TC] Gym {assignment.gym_id}: Using call_logs phone: {contact_phone}")
            else:
                contact_phone = getattr(gym, 'contact_phone', None)
                contact_phone_source = 'database'
                #(f"[DEBUG MANAGER TC] Gym {assignment.gym_id}: Using database phone: {contact_phone}, call_log_found: {most_recent_call_log is not None}")

            assignment_info = GymAssignmentInfo(
                gym_id=assignment.gym_id,
                gym_name=gym.gym_name,
                telecaller_id=assignment.telecaller_id,
                telecaller_name=telecaller.name,
                telecaller_mobile=telecaller.mobile_number,
                assigned_at=assignment.assigned_at,
                status=assignment.status,
                manager_id=assignment.manager_id,  # Include manager_id
                current_call_status=most_recent_call_log.call_status if most_recent_call_log else 'pending',  # Latest call status
                target_date=assignment.target_date,  # Include target_date
                contact_person=getattr(gym, 'contact_person', None),
                contact_phone=contact_phone,
                contact_phone_source=contact_phone_source,
                address=getattr(gym, 'address', None),
                area=getattr(gym, 'area', None),
                city=getattr(gym, 'city', None),
                state=getattr(gym, 'state', None),
                pincode=getattr(gym, 'pincode', None),
                zone=getattr(gym, 'zone', None),
                approval_status=getattr(gym, 'approval_status', None)
            )
            assignment_list.append(assignment_info)

        total_pages = (total + limit - 1) // limit

        return {
            "telecaller": {
                "id": telecaller.id,
                "name": telecaller.name,
                "mobile_number": telecaller.mobile_number
            },
            "assignments": assignment_list,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching telecaller assignments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch telecaller assignments: {str(e)}"
        )

@router.get("/telecaller/{telecaller_id}/total-calls")
async def get_telecaller_total_calls(
    telecaller_id: int,
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db)
):
    """Get total call count for a specific telecaller"""
    try:
        #(f"Fetching total calls for telecaller_id: {telecaller_id} by manager_id: {manager.id}")

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

        # Count total call logs - using the exact same query pattern as the telecaller call history
        total_calls = db.query(func.count(GymCallLogs.id)).filter(
            GymCallLogs.telecaller_id == telecaller_id
        ).scalar() or 0

        #(f"Total calls for telecaller {telecaller_id}: {total_calls}")

        return {
            "telecaller_id": telecaller_id,
            "telecaller_name": telecaller.name,
            "total_calls": total_calls
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching total calls for telecaller_id {telecaller_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch total calls: {str(e)}"
        )
    
@router.get("/assignments/all", response_model=AssignmentResponse)
async def get_all_global_assignments(
    manager: Manager = Depends(get_current_manager),
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 1000,
    status: Optional[str] = None
):
   
    try:
        #(f"Fetching GLOBAL assignments for manager_id: {manager.id}, page: {page}, limit: {limit}")

        offset = (page - 1) * limit

        # Build query - NO manager filter, get all active assignments across all managers
        query = db.query(GymAssignment, GymDatabase, Telecaller).join(
            GymDatabase, GymAssignment.gym_id == GymDatabase.id
        ).join(
            Telecaller, GymAssignment.telecaller_id == Telecaller.id
        ).filter(
            GymAssignment.status == "active"  # Only active assignments
        )

        if status:
            query = query.filter(GymAssignment.status == status)

        # Get total count
        total = query.count()
        #(f"Found {total} total GLOBAL assignments")

        # Get assignments with pagination
        assignments = query.offset(offset).limit(limit).all()
        #(f"Retrieved {len(assignments)} global assignments for page {page}")

        assignment_list = []
        for assignment, gym, telecaller in assignments:
            # Determine contact_phone: Check gym_call_logs for new_contact_number first
            from sqlalchemy import desc as sql_desc

            most_recent_call_log = db.query(GymCallLogs).filter(
                GymCallLogs.gym_id == assignment.gym_id
            ).order_by(sql_desc(GymCallLogs.created_at)).first()

            contact_phone = None
            contact_phone_source = None
            if most_recent_call_log and most_recent_call_log.new_contact_number:
                contact_phone = most_recent_call_log.new_contact_number
                contact_phone_source = 'call_logs'
            else:
                contact_phone = getattr(gym, 'contact_phone', None)
                contact_phone_source = 'database'

            assignment_info = GymAssignmentInfo(
                gym_id=assignment.gym_id,
                gym_name=gym.gym_name,
                telecaller_id=assignment.telecaller_id,
                telecaller_name=telecaller.name,
                telecaller_mobile=telecaller.mobile_number,
                assigned_at=assignment.assigned_at,
                status=assignment.status,
                manager_id=assignment.manager_id,  # Include manager_id
                current_call_status=most_recent_call_log.call_status if most_recent_call_log else 'pending',  # Latest call status
                target_date=assignment.target_date,  # Include target_date
                contact_person=getattr(gym, 'contact_person', None),
                contact_phone=contact_phone,
                contact_phone_source=contact_phone_source,
                address=getattr(gym, 'address', None),
                area=getattr(gym, 'area', None),
                city=getattr(gym, 'city', None),
                state=getattr(gym, 'state', None),
                pincode=getattr(gym, 'pincode', None),
                zone=getattr(gym, 'zone', None),
                approval_status=getattr(gym, 'approval_status', None)
            )
            assignment_list.append(assignment_info)

        total_pages = (total + limit - 1) // limit

        return AssignmentResponse(
            assignments=assignment_list,
            page=page,
            limit=limit,
            total=total,
            total_pages=total_pages
        )

    except Exception as e:
        logger.error(f"Error fetching global assignments: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch global assignments: {str(e)}"
        )