from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from app.models.marketingmodels import GymDatabase, GymAssignments, Executives, Managers, GymVisits
from app.models.database import get_db
from typing import Optional, List, Dict, Any
from datetime import datetime

router = APIRouter(prefix="/marketing/gym-database", tags=["Gym Database"])

class CreateGymRequest(BaseModel):
    gym_name: str
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    zone: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    address: Optional[str] = None
    operating_hours: Optional[List[Dict[str, Any]]] = None
    submission_notes: Optional[str] = None

class ApprovalRequest(BaseModel):
    gym_id: int
    action: str  
    admin_notes: Optional[str] = None
    rejection_reason: Optional[str] = None

class AssignGymRequest(BaseModel):
    gym_id: int
    gym_name: str
    gym_address: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    visit_date: str
    visit_type: str = 'sales_call'
    visit_purpose: str = 'Initial gym visit and assessment'
    assigned_date: str
    notes: Optional[str] = None

class CreateVisitFromAssignmentRequest(BaseModel):
    executive_id: int
    manager_id: int
    gym_id: int
    assignment_id: int
    visit_date: str
    visit_purpose: str
    visit_type: str = 'sales_call'

class EditAssignmentRequest(BaseModel):
    assigned_date: str

class UpdateGymRequest(BaseModel):
    gym_name: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    zone: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    address: Optional[str] = None
    operating_hours: Optional[List[Dict[str, Any]]] = None

class CreateGymRequestV2(BaseModel):
    """V2 API: All basic info mandatory, contact info optional, auto-approved"""
    gym_name: str  # Required
    area: str  # Required
    city: str  # Required
    state: str  # Required
    pincode: str  # Required
    zone: str  # Required (North, West, South, East)
    address: str  # Required
    contact_person: Optional[str] = None  # Optional, no validation
    contact_phone: Optional[str] = None  # Optional, no validation
    operating_hours: Optional[List[Dict[str, Any]]] = None
    submission_notes: Optional[str] = None

def generate_referral_id(gym_id: int) -> str:
    """Generate referral ID in format FIT{gym_id}{uuid4_short}"""
    import uuid
    return f"FIT{gym_id}{str(uuid.uuid4())[:4]}"

@router.post("/create")
async def create_gym_for_approval(
    request: CreateGymRequest,
    user_id: int = Query(...),
    user_type: str = Query(...),
    db: Session = Depends(get_db)
):
    try:
        manager = None
        executive=None
        if user_type == 'manager':
            manager = db.query(Managers).filter(Managers.id == user_id).first()
            if not manager:
                raise HTTPException(status_code=404, detail="Manager not found")
            
        else:
            executive = db.query(Executives).filter(Executives.id == user_id).first()
            if not executive:
                raise HTTPException(status_code=404, detail="Manager not found")

        existing_gym = db.query(GymDatabase).filter(
            GymDatabase.gym_name == request.gym_name,
            GymDatabase.area == request.area,
            GymDatabase.city == request.city
        ).first()
        
        if existing_gym:
            raise HTTPException(
                status_code=400, 
                detail="A gym with this name and location already exists"
            )
        
        if user_type == 'manager':

            new_gym = GymDatabase(
                gym_name=request.gym_name,
                area=request.area,
                city=request.city,
                state=request.state,
                pincode=request.pincode,
                contact_person=request.contact_person,
                contact_phone=request.contact_phone,
                address=request.address,
                operating_hours=request.operating_hours,
                submission_notes=request.submission_notes,
                approval_status='pending',
                submitter_type=user_type,
                submitted_by_manager=user_id,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        else:

            new_gym = GymDatabase(
                gym_name=request.gym_name,
                area=request.area,
                city=request.city,
                state=request.state,
                pincode=request.pincode,
                contact_person=request.contact_person,
                contact_phone=request.contact_phone,
                address=request.address,
                operating_hours=request.operating_hours,
                submission_notes=request.submission_notes,
                approval_status='pending',
                submitted_by_executive=user_id,
                submitter_type=user_type,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )

        db.add(new_gym)
        db.commit()
        db.refresh(new_gym)

        new_gym.referal_id = generate_referral_id(new_gym.id)
        db.commit()
        
        return {
            "status": 200,
            "message": "Gym submitted for approval successfully",
            "data": {
                "gym_id": new_gym.id,
                "gym_name": new_gym.gym_name,
                "approval_status": new_gym.approval_status,
                "submitted_by": manager.name if user_type=='manager' else executive.name
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating gym: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/create-v2")
async def create_gym_v2(
    request: CreateGymRequestV2,
    user_id: int = Query(...),
    user_type: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    V2 API for creating gyms with the following changes:
    - All basic info fields are mandatory (gym_name, area, city, state, pincode, zone, address)
    - Contact person and phone are optional with no validation
    - Gyms are automatically approved (approval_status = 'approved')
    - Zone field is required (North, West, South, East)
    """
    try:
        manager = None
        executive = None

        # Validate user exists
        if user_type == 'manager':
            manager = db.query(Managers).filter(Managers.id == user_id).first()
            if not manager:
                raise HTTPException(status_code=404, detail="Manager not found")
        else:
            executive = db.query(Executives).filter(Executives.id == user_id).first()
            if not executive:
                raise HTTPException(status_code=404, detail="Executive not found")

        # Check for duplicate gym
        existing_gym = db.query(GymDatabase).filter(
            GymDatabase.gym_name == request.gym_name,
            GymDatabase.area == request.area,
            GymDatabase.city == request.city
        ).first()

        if existing_gym:
            raise HTTPException(
                status_code=400,
                detail="A gym with this name and location already exists"
            )

        # Create new gym with approval_status = 'approved'
        # Strip whitespace from city to ensure consistent location matching
        clean_city = request.city.strip()

        if user_type == 'manager':
            new_gym = GymDatabase(
                gym_name=request.gym_name,
                area=request.area,
                city=request.city,
                state=request.state,
                pincode=request.pincode,
                zone=request.zone,
                location=clean_city,  # Location is same as city (trimmed)
                contact_person=request.contact_person,
                contact_phone=request.contact_phone,
                address=request.address,
                operating_hours=request.operating_hours,
                submission_notes=request.submission_notes,
                approval_status='approved',  # Auto-approved in v2
                submitter_type=user_type,
                submitted_by_manager=user_id,
                approved_by=user_id,  # Set as approved by the manager
                approval_date=datetime.now(),  # Set approval date
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        else:
            new_gym = GymDatabase(
                gym_name=request.gym_name,
                area=request.area,
                city=request.city,
                state=request.state,
                pincode=request.pincode,
                zone=request.zone,
                location=clean_city,  # Location is same as city (trimmed)
                contact_person=request.contact_person,
                contact_phone=request.contact_phone,
                address=request.address,
                operating_hours=request.operating_hours,
                submission_notes=request.submission_notes,
                approval_status='approved',  # Auto-approved in v2
                submitted_by_executive=user_id,
                submitter_type=user_type,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )

        db.add(new_gym)
        db.commit()
        db.refresh(new_gym)

        # No referral ID generation for v2 API

        return {
            "status": 200,
            "message": "Gym created and approved successfully",
            "data": {
                "gym_id": new_gym.id,
                "gym_name": new_gym.gym_name,
                "zone": new_gym.zone,
                "location": new_gym.location,
                "approval_status": new_gym.approval_status,
                "submitted_by": manager.name if user_type == 'manager' else executive.name
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating gym (v2): {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/list")
async def get_gym_database_list(
    manager_id: int = Query(...),
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),  # New zone filter parameter
    pincode: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    include_pending: Optional[bool] = Query(False),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    limit: int = Query(50, ge=1, le=100, description="Number of items per page"),
    db: Session = Depends(get_db)
):
    try:
        include_pending=False



        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        # Apply zone-based filtering for BDMs based on their assigned zones
        base_query = db.query(
            GymDatabase,
            GymAssignments.status.label('assignment_status'),
            GymAssignments.conversion_status.label('final_status'),
            GymAssignments.assigned_date,
            Executives.name.label('executive_name'),
            Managers.name.label('manager_name'),
            GymAssignments.id.label('assignment_id')
        ).outerjoin(
            GymAssignments, GymDatabase.id == GymAssignments.gym_id
        ).outerjoin(
            Executives, GymAssignments.executive_id == Executives.id
        ).outerjoin(
            Managers, GymAssignments.manager_id == Managers.id
        )

        # Apply location and zone-based filtering for BDMs (show only assigned zones from assigned locations)
        if manager.assigned and isinstance(manager.assigned, dict):
            zone_conditions = []
            for location, zones in manager.assigned.items():
                if isinstance(zones, list):
                    for zone_name in zones:

                        zone_conditions.append(
                            and_(
                                GymDatabase.location == location,
                                GymDatabase.zone == zone_name
                            )
                        )
                else:

                    zone_conditions.append(
                            and_(
                                GymDatabase.location == location,
                                GymDatabase.zone == zone_name
                            )
                        )

            if zone_conditions:
                from sqlalchemy import or_
                base_query = base_query.filter(or_(*zone_conditions))

        query = base_query

        if include_pending:
            query = query.filter(GymDatabase.approval_status.in_(['approved', 'pending']))
        else:
            query = query.filter(GymDatabase.approval_status == 'approved')

        if state:
            query = query.filter(GymDatabase.state == state)
        if city:
            query = query.filter(GymDatabase.city == city)
        if area:
            query = query.filter(GymDatabase.area == area)
        if zone:
            query = query.filter(GymDatabase.zone == zone)
        if pincode:
            query = query.filter(GymDatabase.pincode == pincode)
        
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    GymDatabase.gym_name.ilike(search_term),
                    GymDatabase.area.ilike(search_term),
                    GymDatabase.city.ilike(search_term),
                    GymDatabase.contact_person.ilike(search_term)
                )
            )

        # Get total count for pagination
        total_count = query.count()

        # Apply pagination
        offset = (page - 1) * limit
        results = query.order_by(GymDatabase.gym_name).offset(offset).limit(limit).all()

        gym_data = []
        for result in results:
            gym, assignment_status, final_status, assigned_date, executive_name, manager_name, assignment_id = result
            
            gym_info = {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
                "zone": gym.zone,  # Add zone information
                "referal_id":gym.referal_id,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "operating_hours": gym.operating_hours,
                "approval_status": gym.approval_status,
                "submitted_by": gym.submitted_by_manager if gym.submitted_by_manager else gym.submitted_by_executive,
                "is_assigned": assignment_status == 'assigned' if assignment_status else False,
                "assignment_status": assignment_status,
                "final_status": final_status,
                "assigned_date": assigned_date.isoformat() if assigned_date else None,
                "executive_name": executive_name,
                "manager_name": manager_name,
                "assignment_id": assignment_id,
                "self_assigned": gym.self_assigned if gym.self_assigned else False,
                "created_at": gym.created_at.isoformat() if gym.created_at else None
            }
            gym_data.append(gym_info)

        # Apply location-based filtering to filter options (show all zones from assigned locations)
        filter_query = db.query(GymDatabase).filter(GymDatabase.approval_status == 'approved')

        # Apply location-based filtering to filter options query if manager has assignments
        if manager.assigned and isinstance(manager.assigned, dict):
            assigned_locations = list(manager.assigned.keys())
            if assigned_locations:
                # Filter by assigned locations, but show all zones within those locations
                from sqlalchemy import or_
                location_conditions = []
                for location in assigned_locations:
                    location_conditions.append(GymDatabase.city == location)
                    # Handle Bangalore/Bengaluru mapping
                    if location == "Bangalore":
                        location_conditions.append(GymDatabase.city == "Bengaluru")
                    elif location == "Bengaluru":
                        location_conditions.append(GymDatabase.city == "Bangalore")

                filter_query = filter_query.filter(or_(*location_conditions))

        states = filter_query.with_entities(GymDatabase.state).distinct().filter(GymDatabase.state.isnot(None)).all()
        cities = filter_query.with_entities(GymDatabase.city).distinct().filter(GymDatabase.city.isnot(None)).all()
        areas = filter_query.with_entities(GymDatabase.area).distinct().filter(GymDatabase.area.isnot(None)).all()
        zones = filter_query.with_entities(GymDatabase.zone).distinct().filter(GymDatabase.zone.isnot(None)).all()
        pincodes = filter_query.with_entities(GymDatabase.pincode).distinct().filter(GymDatabase.pincode.isnot(None)).all()

        filter_options = {
            "states": [s[0] for s in states if s[0]],
            "cities": [c[0] for c in cities if c[0]],
            "areas": [a[0] for a in areas if a[0]],
            "zones": [z[0] for z in zones if z[0]],  # Add zone filter options
            "pincodes": [p[0] for p in pincodes if p[0]]
        }

        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit  # Ceiling division
        has_next = page < total_pages
        has_prev = page > 1

        # Calculate summary statistics
        summary_stats = {
            "total_gyms": total_count,
            "assigned_gyms": 0,
            "available_gyms": 0,
            "pending_gyms": 0
        }

        # Get summary counts for the filtered results
        summary_query = query  # Use the same query with all filters applied

        # Count assigned gyms
        assigned_count = summary_query.filter(
            GymAssignments.status == 'assigned'
        ).count()

        # Count available gyms (not assigned and approved)
        available_count = summary_query.filter(
            and_(
                GymAssignments.status.is_(None),
                GymDatabase.approval_status == 'approved'
            )
        ).count()

        # Count pending gyms (awaiting approval)
        pending_count = summary_query.filter(
            GymDatabase.approval_status == 'pending'
        ).count()

        summary_stats = {
            "total_gyms": total_count,
            "assigned_gyms": assigned_count,
            "available_gyms": available_count,
            "pending_gyms": pending_count
        }



        return {
            "status": 200,
            "message": "Gym database retrieved successfully",
            "data": {
                "gyms": gym_data,
                "pagination": {
                    "current_page": page,
                    "per_page": limit,
                    "total_count": total_count,
                    "total_pages": total_pages,
                    "has_next": has_next,
                    "has_prev": has_prev
                },
                "summary_stats": summary_stats,
                "filter_options": filter_options
            }
        }

    except Exception as e:
        print(f"Error getting gym database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/assigned/{executive_id}")
async def get_executive_assigned_gyms(
    executive_id: int,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")

        assignments = db.query(
            GymAssignments,
            GymDatabase,
            GymVisits.final_status.label('visit_status'),
            GymVisits.id.label('visit_id'),
            GymVisits.completed
        ).join(
            GymDatabase, GymAssignments.gym_id == GymDatabase.id
        ).outerjoin(
            GymVisits, and_(
                GymVisits.gym_id == GymDatabase.id,
                GymVisits.user_id == executive_id
            )
        ).filter(
            GymAssignments.executive_id == executive_id,
            GymAssignments.status == 'assigned'
        ).order_by(GymAssignments.assigned_date.desc()).all()

        assigned_gyms = []
        for assignment, gym, visit_status, visit_id, completed in assignments:
            gym_info = {
                "assignment_id": assignment.id,
                "gym_id": gym.id,
                "gym_name": gym.gym_name,
                "gym_address": gym.address,                
                "referal_id":gym.referal_id,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "assigned_date": assignment.assigned_date.isoformat() if assignment.assigned_date else None,
                "conversion_status": assignment.conversion_status,
                "visit_status": visit_status,
                "visit_id": visit_id,
                "completed": completed,
                "created_at": assignment.created_at.isoformat() if assignment.created_at else None
            }
            assigned_gyms.append(gym_info)

        print("gym info is",assigned_gyms)

        return {
            "status": 200,
            "message": "Assigned gyms retrieved successfully",
            "data": assigned_gyms
        }

    except Exception as e:
        print(f"Error getting assigned gyms: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")



@router.get("/assigned/{executive_id}/date/{assignment_date}")
def get_executive_assigned_gyms_by_date(
    executive_id: int,
    assignment_date: str, 
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")

        try:
            target_date = datetime.strptime(assignment_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        assignments = db.query(
            GymAssignments,
            GymDatabase,
            GymVisits.final_status.label('visit_status'),
            GymVisits.id.label('visit_id'),
            GymVisits.completed
        ).join(
            GymDatabase, GymAssignments.gym_id == GymDatabase.id
        ).outerjoin(
            GymVisits, and_(
                GymVisits.gym_id == GymDatabase.id,
                GymVisits.user_id == executive_id,
                func.date(GymVisits.assigned_date) == target_date
            )
        ).filter(
            GymAssignments.executive_id == executive_id,
            GymAssignments.status == 'assigned',
            func.date(GymAssignments.assigned_date) == target_date
        ).order_by(GymAssignments.created_at.desc()).all()

        assigned_gyms = []
        for assignment, gym, visit_status, visit_id, completed in assignments:
            gym_info = {
                "assignment_id": assignment.id,
                "gym_id": gym.id,
                "gym_name": gym.gym_name,
                "gym_address": gym.address,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "gym_area": gym.area,
                "gym_city": gym.city,
                "gym_state": gym.state,                
                "referal_id":gym.referal_id,
                "assigned_date": assignment.assigned_date.isoformat() if assignment.assigned_date else None,
                "conversion_status": assignment.conversion_status,
                "final_status": assignment.conversion_status,  
                "visit_status": visit_status,
                "visit_id": visit_id,
                "completed": completed,
                "created_at": assignment.created_at.isoformat() if assignment.created_at else None,
                "updated_at": assignment.updated_at.isoformat() if assignment.updated_at else None
            }
            assigned_gyms.append(gym_info)

        return {
            "status": 200,
            "message": f"Assigned gyms for {assignment_date} retrieved successfully",
            "data": assigned_gyms
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting assigned gyms by date: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/stats")
def get_gym_assignment_stats(
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executives = db.query(Executives).filter(Executives.manager_id == manager_id).all()
        executive_ids = [exec.id for exec in executives]

        total_gyms = db.query(GymDatabase).count()

        assigned_gyms = db.query(GymAssignments).filter(
            GymAssignments.manager_id == manager_id,
            GymAssignments.status == 'assigned'
        ).count()

        unassigned_gyms = total_gyms - assigned_gyms

        conversion_stats = db.query(
            GymAssignments.conversion_status,
            func.count(GymAssignments.id).label('count')
        ).filter(
            GymAssignments.manager_id == manager_id,
            GymAssignments.status == 'assigned'
        ).group_by(GymAssignments.conversion_status).all()

        conversion_breakdown = {}
        for status, count in conversion_stats:
            conversion_breakdown[status] = count

        executive_stats = db.query(
            Executives.id,
            Executives.name,
            func.count(GymAssignments.id).label('assigned_count')
        ).outerjoin(
            GymAssignments, and_(
                GymAssignments.executive_id == Executives.id,
                GymAssignments.status == 'assigned'
            )
        ).filter(
            Executives.manager_id == manager_id
        ).group_by(Executives.id, Executives.name).all()

        executive_breakdown = []
        for exec_id, exec_name, assigned_count in executive_stats:
            executive_breakdown.append({
                "executive_id": exec_id,
                "executive_name": exec_name,
                "assigned_gyms": assigned_count
            })

        stats = {
            "total_gyms": total_gyms,
            "assigned_gyms": assigned_gyms,
            "unassigned_gyms": unassigned_gyms,
            "assignment_percentage": round((assigned_gyms / total_gyms * 100) if total_gyms > 0 else 0, 1),
            "conversion_breakdown": conversion_breakdown,
            "executive_breakdown": executive_breakdown
        }

        return {
            "status": 200,
            "message": "Gym assignment statistics retrieved successfully",
            "data": stats
        }

    except Exception as e:
        print(f"Error getting gym assignment stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/details/{gym_id}")
def get_gym_details(
    gym_id: int,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        assignment = db.query(
            GymAssignments,
            Executives.name.label('executive_name'),
            Managers.name.label('manager_name')
        ).outerjoin(
            Executives, GymAssignments.executive_id == Executives.id
        ).outerjoin(
            Managers, GymAssignments.manager_id == Managers.id
        ).filter(GymAssignments.gym_id == gym_id).first()

        gym_details = {
            "id": gym.id,
            "gym_name": gym.gym_name,
            "area": gym.area,
            "city": gym.city,
            "state": gym.state,
            "pincode": gym.pincode,
            "referal_id":gym.referal_id,
            "contact_person": gym.contact_person,
            "contact_phone": gym.contact_phone,
            "address": gym.address,
            "operating_hours": gym.operating_hours,
            "created_at": gym.created_at.isoformat() if gym.created_at else None,
            "assignment": None
        }

        if assignment:
            assignment_obj, executive_name, manager_name = assignment
            gym_details["assignment"] = {
                "assignment_id": assignment_obj.id,
                "status": assignment_obj.status,
                "conversion_status": assignment_obj.conversion_status,
                "executive_name": executive_name,
                "manager_name": manager_name,
                "assigned_date": assignment_obj.assigned_date.isoformat() if assignment_obj.assigned_date else None,
                "assigned_on": assignment_obj.assigned_on.isoformat() if assignment_obj.assigned_on else None
            }

        return {
            "status": 200,
            "message": "Gym details retrieved successfully",
            "data": gym_details
        }

    except Exception as e:
        print(f"Error getting gym details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.put("/assignment/status/{assignment_id}")
def update_gym_assignment_status(
    assignment_id: int,
    request: dict,
    db: Session = Depends(get_db)
):
    try:
        assignment = db.query(GymAssignments).filter(GymAssignments.id == assignment_id).first()
        if not assignment:
            raise HTTPException(status_code=404, detail="Assignment not found")

        status = request.get('status')
        notes = request.get('notes', {})

        if status:
            assignment.conversion_status = status
            assignment.updated_at = datetime.now()

        db.commit()
        db.refresh(assignment)

        return {
            "status": 200,
            "message": "Assignment status updated successfully",
            "data": {
                "assignment_id": assignment.id,
                "new_status": assignment.conversion_status
            }
        }

    except Exception as e:
        print(f"Error updating assignment status: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/assignments/check/{executive_id}")
def check_assignments_for_date_range(
    executive_id: int,
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        assignments = db.query(
            func.date(GymAssignments.assigned_date).label('assignment_date'),
            func.count(GymAssignments.id).label('count')
        ).filter(
            GymAssignments.executive_id == executive_id,
            GymAssignments.status == 'assigned',
            func.date(GymAssignments.assigned_date) >= start_dt,
            func.date(GymAssignments.assigned_date) <= end_dt
        ).group_by(func.date(GymAssignments.assigned_date)).all()

        assignment_counts = {}
        for assignment_date, count in assignments:
            assignment_counts[assignment_date.strftime("%Y-%m-%d")] = count

        return {
            "status": 200,
            "message": "Assignment counts retrieved successfully",
            "data": assignment_counts
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error checking assignments for date range: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/pending-approvals")
def get_pending_gym_approvals(
    admin_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Get all gyms pending approval - Admin only"""
    try:
        admin = db.query(Managers).filter(Managers.id == admin_id).first()
        if not admin:
            raise HTTPException(status_code=404, detail="Admin not found")

        pending_gyms = db.query(
            GymDatabase,
            Managers.name.label('submitted_by_name')
        ).join(
            Managers, GymDatabase.submitted_by == Managers.id
        ).filter(
            GymDatabase.approval_status == 'pending'
        ).order_by(GymDatabase.created_at.desc()).all()

        gym_data = []
        for gym, submitted_by_name in pending_gyms:
            gym_info = {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
                "referal_id":gym.referal_id,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "operating_hours": gym.operating_hours,
                "submission_notes": gym.submission_notes,
                "submitted_by_name": submitted_by_name,
                "submitted_by_id": gym.submitted_by,
                "created_at": gym.created_at.isoformat() if gym.created_at else None
            }
            gym_data.append(gym_info)

        return {
            "status": 200,
            "message": "Pending gym approvals retrieved successfully",
            "data": {
                "gyms": gym_data,
                "total_count": len(gym_data)
            }
        }

    except Exception as e:
        print(f"Error getting pending approvals: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
    
@router.get("/my-submissions/{manager_id}")
def get_my_gym_submissions(
    manager_id: int,
    user_type:str,
    status: Optional[str] = Query(None),  
    db: Session = Depends(get_db)
):
    """Get gyms submitted by a specific manager"""
    try:
        if user_type == 'manager':
            manager = db.query(Managers).filter(Managers.id == manager_id).first()
            if not manager:
                raise HTTPException(status_code=404, detail="Manager not found")

            query = db.query(GymDatabase).filter(GymDatabase.submitted_by_manager == manager_id)

        else:
            executive = db.query(Executives).filter(Executives.id == manager_id).first()
            if not executive:
                raise HTTPException(status_code=404, detail='Exectuive not found')
            
            query = db.query(GymDatabase).filter(GymDatabase.submitted_by_executive == manager_id)
        
        if status and status != 'all':
            query = query.filter(GymDatabase.approval_status == status)

        submissions = query.order_by(GymDatabase.created_at.desc()).all()

        gym_data = []
        for gym in submissions:
            gym_info = {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "referal_id":gym.referal_id,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "approval_status": gym.approval_status,
                "submission_notes": gym.submission_notes,
                "admin_notes": gym.admin_notes,
                "rejection_reason": gym.rejection_reason,
                "verified": False,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "approval_date": gym.approval_date.isoformat() if gym.approval_date else None
            }
            gym_data.append(gym_info)

        return {
            "status": 200,
            "message": "Gym submissions retrieved successfully",
            "data": {
                "gyms": gym_data,
                "total_count": len(gym_data)
            }
        }

    except Exception as e:
        print(f"Error getting gym submissions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/assign/{executive_id}")
def assign_gym_to_executive(
    executive_id: int,
    request: AssignGymRequest,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Assign an approved gym to executive"""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")

        # Only allow assignment of approved gyms
        gym = db.query(GymDatabase).filter(
            GymDatabase.id == request.gym_id,
            GymDatabase.approval_status == 'approved'
        ).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Approved gym not found")

        visit_date = datetime.fromisoformat(request.visit_date.replace('Z', '+00:00')).date()
        existing_assignment = db.query(GymAssignments).filter(
            GymAssignments.gym_id == request.gym_id,
            GymAssignments.executive_id == executive_id,
            func.date(GymAssignments.assigned_date) == visit_date,
            GymAssignments.status == 'assigned'
        ).first()

        if existing_assignment:
            raise HTTPException(status_code=400, detail="Gym is already assigned to this executive for this date")

        
        visit_date_dt = datetime.fromisoformat(request.visit_date.replace('Z', '+00:00'))
        
        assignment = GymAssignments(
            gym_id=request.gym_id,
            executive_id=executive_id,
            referal_id=gym.referal_id,
            manager_id=manager_id,
            status='assigned',
            conversion_status='pending',
            assigned_date=visit_date_dt,
            assigned_on=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        db.add(assignment)

        visit_date_dt = datetime.fromisoformat(request.visit_date.replace('Z', '+00:00'))
        
        new_visit = GymVisits(
            user_id=executive_id,
            gym_id=request.gym_id,
            referal_id=gym.referal_id,
            start_date=datetime.now(),
            assigned_date=visit_date_dt,
            assigned_on=datetime.now(),
            gym_name=request.gym_name,
            gym_address=request.gym_address,
            contact_person=request.contact_person or '',
            contact_phone=request.contact_phone or '',
            visit_type=request.visit_type,
            status='assigned',
            notes=request.notes,
            visit_purpose=request.visit_purpose,
            current_step=0,
            completed=False,
            final_status='pending',
            presentation_given=False,
            demo_provided=False,
            interest_level=0,
            decision_maker_present=False,
            overall_rating=0,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        db.add(new_visit)
        db.commit()
        db.refresh(assignment)
        db.refresh(new_visit)

        return {
            "status": 200,
            "message": "Gym assigned to executive successfully",
            "data": {
                "assignment_id": assignment.id,
                "visit_id": new_visit.id,
                "gym_name": request.gym_name,
                "executive_name": executive.name,
                "visit_date": request.visit_date
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error assigning gym to executive: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.put("/assignment/edit/{assignment_id}")
def edit_gym_assignment_date(
    assignment_id: int,
    request: EditAssignmentRequest,
    executive_id: int = Query(...),
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Edit the assigned_date for a gym assignment"""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        assignment = db.query(GymAssignments).filter(
            GymAssignments.id == assignment_id,
            GymAssignments.executive_id == executive_id
        ).first()

        if not assignment:
            raise HTTPException(
                status_code=404,
                detail="Assignment not found for this executive"
            )

        try:
            new_date = datetime.fromisoformat(request.assigned_date.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

        old_date = assignment.assigned_date

        assignment.assigned_date = new_date
        assignment.updated_at = datetime.now()

        gym_visit = db.query(GymVisits).filter(
            GymVisits.gym_id == assignment.gym_id,
            GymVisits.user_id == executive_id,
            func.date(GymVisits.assigned_date) == old_date.date()
        ).first()

        if gym_visit:
            gym_visit.assigned_date = new_date
            gym_visit.updated_at = datetime.now()

        db.commit()
        db.refresh(assignment)

        return {
            "status": 200,
            "message": "Assignment date updated successfully",
            "data": {
                "assignment_id": assignment.id,
                "old_date": old_date.isoformat() if old_date else None,
                "new_date": new_date.isoformat(),
                "gym_id": assignment.gym_id
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error editing assignment date: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.delete("/delete")
async def delete_gym_assignment(
    assignment_id: int,
    executive_id: int ,
    manager_id: int,
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        assignment = db.query(GymAssignments).filter(
            GymAssignments.id == assignment_id,
            GymAssignments.executive_id == executive_id
        ).first()
        gym_id=assignment.gym_id

        gym_visit= db.query(GymVisits).filter(GymVisits.gym_id==gym_id).first()

        if not assignment:
            raise HTTPException(
                status_code=404,
                detail="Assignment not found for this executive"
            )

        db.delete(assignment)
        if gym_visit:
            db.delete(gym_visit)
        db.commit()

        return {
            "status": 200,
            "message": "Assignment and related records deleted successfully",

            }
        

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting assignment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.put("/update/{gym_id}")
async def update_gym_details(
    gym_id: int,
    request: UpdateGymRequest,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Update gym details - BDM only"""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        # Update only provided fields
        if request.gym_name is not None:
            gym.gym_name = request.gym_name
        if request.area is not None:
            gym.area = request.area
        if request.city is not None:
            gym.city = request.city
        if request.state is not None:
            gym.state = request.state
        if request.pincode is not None:
            gym.pincode = request.pincode
        if request.zone is not None:
            gym.zone = request.zone
        if request.contact_person is not None:
            gym.contact_person = request.contact_person
        if request.contact_phone is not None:
            gym.contact_phone = request.contact_phone
        if request.address is not None:
            gym.address = request.address
        if request.operating_hours is not None:
            gym.operating_hours = request.operating_hours

        gym.updated_at = datetime.now()

        print(f"Updating gym ID {gym_id}: {gym.gym_name}")

        db.commit()
        db.refresh(gym)

        print(f"Successfully updated gym ID {gym_id}")

        return {
            "status": 200,
            "message": "Gym details updated successfully",
            "data": {
                "gym_id": gym.id,
                "gym_name": gym.gym_name,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
                "zone": gym.zone
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating gym details: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/self-assign-bulk")
async def self_assign_gyms_to_manager(
    request: dict,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Self-assign multiple gyms to manager (BDM) without executive"""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        gym_ids = request.get('gym_ids', [])
        if not gym_ids:
            raise HTTPException(status_code=400, detail="No gym IDs provided")

        assigned_gyms = []
        failed_gyms = []

        for gym_id in gym_ids:
            try:
                # Check if gym exists and is approved
                gym = db.query(GymDatabase).filter(
                    GymDatabase.id == gym_id,
                    GymDatabase.approval_status == 'approved'
                ).first()

                if not gym:
                    failed_gyms.append({
                        "gym_id": gym_id,
                        "reason": "Gym not found or not approved"
                    })
                    continue

                # Check if gym is already assigned
                existing_assignment = db.query(GymAssignments).filter(
                    GymAssignments.gym_id == gym_id,
                    GymAssignments.status == 'assigned'
                ).first()

                if existing_assignment:
                    failed_gyms.append({
                        "gym_id": gym_id,
                        "gym_name": gym.gym_name,
                        "reason": "Gym is already assigned"
                    })
                    continue

                # Mark gym as self-assigned
                gym.self_assigned = True
                gym.updated_at = datetime.now()

                # Create self-assignment (manager only, no executive)
                assignment = GymAssignments(
                    gym_id=gym_id,
                    executive_id=None,  # No executive for self-assignment
                    manager_id=manager_id,
                    referal_id=gym.referal_id,
                    status='assigned',
                    conversion_status='pending',
                    assigned_date=datetime.now(),
                    assigned_on=datetime.now(),
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                db.add(assignment)
                db.flush()  # Flush to get assignment ID

                # Create gym visit with user_id=NULL and manager_id set for self-assignment
                # When user_id is NULL, queries should use manager_id to track manager's visits
                new_visit = GymVisits(
                    user_id=None,  # NULL indicates manager self-assignment
                    manager_id=manager_id,  # Manager who self-assigned this gym
                    gym_id=gym_id,
                    referal_id=gym.referal_id,
                    start_date=datetime.now(),
                    assigned_date=datetime.now(),
                    assigned_on=datetime.now(),
                    gym_name=gym.gym_name,
                    gym_address=gym.address or '',
                    contact_person=gym.contact_person or '',
                    contact_phone=gym.contact_phone or '',
                    visit_type='sales_call',
                    status='assigned',
                    notes=f'Self-assigned by {manager.name}',
                    visit_purpose='BDM self-assignment',
                    current_step=0,
                    completed=False,
                    final_status='pending',
                    presentation_given=False,
                    demo_provided=False,
                    interest_level=0,
                    decision_maker_present=False,
                    overall_rating=0,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                db.add(new_visit)

                assigned_gyms.append({
                    "gym_id": gym_id,
                    "gym_name": gym.gym_name,
                    "assignment_id": assignment.id
                })

            except Exception as e:
                print(f"Error assigning gym {gym_id}: {str(e)}")
                failed_gyms.append({
                    "gym_id": gym_id,
                    "reason": str(e)
                })

        db.commit()

        return {
            "status": 200,
            "message": f"Successfully assigned {len(assigned_gyms)} gym(s) to yourself",
            "data": {
                "assigned_count": len(assigned_gyms),
                "failed_count": len(failed_gyms),
                "assigned_gyms": assigned_gyms,
                "failed_gyms": failed_gyms
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in self-assignment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/self-unassign-bulk")
async def self_unassign_gyms_from_manager(
    request: dict,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Unassign multiple self-assigned gyms from manager (BDM)"""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        gym_ids = request.get('gym_ids', [])
        if not gym_ids:
            raise HTTPException(status_code=400, detail="No gym IDs provided")

        unassigned_gyms = []
        failed_gyms = []

        for gym_id in gym_ids:
            try:
                # Check if gym exists
                gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
                if not gym:
                    failed_gyms.append({
                        "gym_id": gym_id,
                        "reason": "Gym not found"
                    })
                    continue

                # Check if gym is self-assigned to this manager
                assignment = db.query(GymAssignments).filter(
                    GymAssignments.gym_id == gym_id,
                    GymAssignments.manager_id == manager_id,
                    GymAssignments.executive_id == None,  # Self-assigned has no executive
                    GymAssignments.status == 'assigned'
                ).first()

                if not assignment:
                    failed_gyms.append({
                        "gym_id": gym_id,
                        "gym_name": gym.gym_name,
                        "reason": "Gym is not self-assigned to you"
                    })
                    continue

                # Delete related gym visits
                deleted_visits = db.query(GymVisits).filter(
                    GymVisits.gym_id == gym_id,
                    GymVisits.user_id == manager_id
                ).delete()

                # Delete the assignment
                db.delete(assignment)

                # Mark gym as not self-assigned
                gym.self_assigned = False
                gym.updated_at = datetime.now()

                unassigned_gyms.append({
                    "gym_id": gym_id,
                    "gym_name": gym.gym_name,
                    "deleted_visits": deleted_visits
                })

            except Exception as e:
                print(f"Error unassigning gym {gym_id}: {str(e)}")
                failed_gyms.append({
                    "gym_id": gym_id,
                    "reason": str(e)
                })

        db.commit()

        return {
            "status": 200,
            "message": f"Successfully unassigned {len(unassigned_gyms)} gym(s)",
            "data": {
                "unassigned_count": len(unassigned_gyms),
                "failed_count": len(failed_gyms),
                "unassigned_gyms": unassigned_gyms,
                "failed_gyms": failed_gyms
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in self-unassignment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.delete("/gym/{gym_id}")
async def delete_gym(
    gym_id: int,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    """Delete gym from database - BDM only. Also deletes related assignments and visits."""
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        gym_name = gym.gym_name

        # Check and delete related gym visits first
        gym_visits = db.query(GymVisits).filter(GymVisits.gym_id == gym_id).all()
        visits_count = len(gym_visits)

        if visits_count > 0:
            print(f"Deleting {visits_count} gym visits for gym_id {gym_id}")
            for visit in gym_visits:
                db.delete(visit)
            db.flush()  # Flush to ensure visits are deleted before assignments

        # Check and delete related gym assignments
        gym_assignments = db.query(GymAssignments).filter(GymAssignments.gym_id == gym_id).all()
        assignments_count = len(gym_assignments)

        if assignments_count > 0:
            print(f"Deleting {assignments_count} gym assignments for gym_id {gym_id}")
            for assignment in gym_assignments:
                db.delete(assignment)
            db.flush()  # Flush to ensure assignments are deleted before gym

        # Finally delete the gym
        print(f"Deleting gym '{gym_name}' (ID: {gym_id})")
        db.delete(gym)
        db.commit()

        # Prepare detailed message
        deleted_items = []
        if visits_count > 0:
            deleted_items.append(f"{visits_count} visit(s)")
        if assignments_count > 0:
            deleted_items.append(f"{assignments_count} assignment(s)")

        detail_msg = f" along with {', '.join(deleted_items)}" if deleted_items else ""

        return {
            "status": 200,
            "message": f"Gym '{gym_name}' deleted successfully{detail_msg}",
            "data": {
                "gym_id": gym_id,
                "gym_name": gym_name,
                "deleted_visits": visits_count,
                "deleted_assignments": assignments_count
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting gym: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")