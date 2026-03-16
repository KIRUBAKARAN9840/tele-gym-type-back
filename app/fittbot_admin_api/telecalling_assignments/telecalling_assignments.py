from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, func, desc, case, or_, text
from typing import List, Optional
from datetime import datetime, date, timedelta
from pydantic import BaseModel, Field
import math

from app.models.adminmodels import TelecallingGymAssignment, TelecallingRetentionTracking, Employees, EmployeeAssignments
from app.models.fittbot_models import Client, ClientFittbotAccess, FittbotPlans, Gym, GymOwner
from app.models.database import get_db
from app.fittbot_admin_api.auth.authentication import get_current_employee_for_support
from fastapi import Request

router = APIRouter(prefix="/telecalling-assignments", tags=["Telecalling Assignments"])


class TelecallingGymAssignmentCreate(BaseModel):
    marketing_gym_id: int
    fittbot_gym_id: Optional[int] = None
    referal_id: str = Field(..., max_length=15)
    employee_id: int
    target_clients: Optional[int] = None
    priority: str = Field(default="medium", pattern="^(low|medium|high|urgent)$")
    notes: Optional[str] = None


class TelecallingGymAssignmentUpdate(BaseModel):
    fittbot_gym_id: Optional[int] = None
    assignment_status: Optional[str] = Field(None, pattern="^(active|completed|paused|cancelled)$")
    target_clients: Optional[int] = None
    priority: Optional[str] = Field(None, pattern="^(low|medium|high|urgent)$")
    notes: Optional[str] = None


class TelecallingRetentionTrackingCreate(BaseModel):
    assignment_id: int
    client_id: int
    fittbot_gym_id: int
    referal_id: str = Field(..., max_length=15)
    retention_period: str = Field(..., pattern="^(7_days|30_days|60_days|90_days|180_days|365_days)$")
    join_date: datetime
    retention_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    notes: Optional[str] = None


class TelecallingRetentionTrackingUpdate(BaseModel):
    retention_status: Optional[str] = Field(None, pattern="^(active|churned|at_risk|renewed)$")
    last_activity_date: Optional[datetime] = None
    churn_date: Optional[datetime] = None
    churn_reason: Optional[str] = None
    retention_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    follow_up_required: Optional[bool] = None
    follow_up_date: Optional[datetime] = None
    notes: Optional[str] = None


@router.post("/create-assignment", status_code=status.HTTP_201_CREATED)
async def create_telecalling_assignment(
    assignment_data: TelecallingGymAssignmentCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Create a new telecalling gym assignment"""
    
    # Check if employee exists
    employee = db.query(Employees).filter(Employees.id == assignment_data.employee_id).first()
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    # Check if assignment already exists for this referal_id
    existing = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.referal_id == assignment_data.referal_id,
        TelecallingGymAssignment.assignment_status.in_(["active", "paused"])
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Active assignment already exists for this referal_id")
    
    new_assignment = TelecallingGymAssignment(
        marketing_gym_id=assignment_data.marketing_gym_id,
        fittbot_gym_id=assignment_data.fittbot_gym_id,
        referal_id=assignment_data.referal_id,
        employee_id=assignment_data.employee_id,
        assigned_by=current_user.get("id"),
        target_clients=assignment_data.target_clients,
        priority=assignment_data.priority,
        notes=assignment_data.notes
    )
    
    db.add(new_assignment)
    db.commit()
    db.refresh(new_assignment)
    
    return {"message": "Assignment created successfully", "assignment_id": new_assignment.assignment_id}


@router.get("/assignments")
async def get_telecalling_assignments(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    status_filter: Optional[str] = Query(None, pattern="^(active|completed|paused|cancelled)$"),
    employee_id: Optional[int] = None,
    priority: Optional[str] = Query(None, pattern="^(low|medium|high|urgent)$"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get telecalling assignments with filters"""
    
    query = db.query(TelecallingGymAssignment).options(
        joinedload(TelecallingGymAssignment.assigned_employee),
        joinedload(TelecallingGymAssignment.assigner)
    )
    
    if status_filter:
        query = query.filter(TelecallingGymAssignment.assignment_status == status_filter)
    if employee_id:
        query = query.filter(TelecallingGymAssignment.employee_id == employee_id)
    if priority:
        query = query.filter(TelecallingGymAssignment.priority == priority)
    
    total_count = query.count()
    assignments = query.order_by(desc(TelecallingGymAssignment.created_at)).offset(skip).limit(limit).all()
    
    return {
        "assignments": assignments,
        "total_count": total_count,
        "skip": skip,
        "limit": limit
    }


@router.put("/assignments/{assignment_id}")
async def update_telecalling_assignment(
    assignment_id: int,
    update_data: TelecallingGymAssignmentUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Update a telecalling assignment"""
    
    assignment = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.assignment_id == assignment_id
    ).first()
    
    if not assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    update_dict = update_data.dict(exclude_unset=True)
    for key, value in update_dict.items():
        setattr(assignment, key, value)
    
    assignment.updated_at = datetime.now()
    db.commit()
    db.refresh(assignment)
    
    return {"message": "Assignment updated successfully"}


@router.post("/retention-tracking", status_code=status.HTTP_201_CREATED)
async def create_retention_tracking(
    tracking_data: TelecallingRetentionTrackingCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Create retention tracking for a client"""
    
    # Check if assignment exists
    assignment = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.assignment_id == tracking_data.assignment_id
    ).first()
    if not assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    # Check if tracking already exists for this client and period
    existing = db.query(TelecallingRetentionTracking).filter(
        TelecallingRetentionTracking.client_id == tracking_data.client_id,
        TelecallingRetentionTracking.retention_period == tracking_data.retention_period,
        TelecallingRetentionTracking.retention_status.in_(["active", "at_risk"])
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Active retention tracking already exists for this client and period")
    
    new_tracking = TelecallingRetentionTracking(
        assignment_id=tracking_data.assignment_id,
        client_id=tracking_data.client_id,
        fittbot_gym_id=tracking_data.fittbot_gym_id,
        referal_id=tracking_data.referal_id,
        retention_period=tracking_data.retention_period,
        join_date=tracking_data.join_date,
        retention_score=tracking_data.retention_score,
        notes=tracking_data.notes
    )
    
    db.add(new_tracking)
    db.commit()
    db.refresh(new_tracking)
    
    return {"message": "Retention tracking created successfully", "retention_id": new_tracking.retention_id}


@router.get("/retention-tracking")
async def get_retention_tracking(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    assignment_id: Optional[int] = None,
    retention_status: Optional[str] = Query(None, pattern="^(active|churned|at_risk|renewed)$"),
    retention_period: Optional[str] = Query(None, pattern="^(7_days|30_days|60_days|90_days|180_days|365_days)$"),
    referal_id: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get retention tracking records with filters"""
    
    query = db.query(TelecallingRetentionTracking).options(
        joinedload(TelecallingRetentionTracking.assignment)
    )
    
    if assignment_id:
        query = query.filter(TelecallingRetentionTracking.assignment_id == assignment_id)
    if retention_status:
        query = query.filter(TelecallingRetentionTracking.retention_status == retention_status)
    if retention_period:
        query = query.filter(TelecallingRetentionTracking.retention_period == retention_period)
    if referal_id:
        query = query.filter(TelecallingRetentionTracking.referal_id == referal_id)
    
    total_count = query.count()
    tracking_records = query.order_by(desc(TelecallingRetentionTracking.created_at)).offset(skip).limit(limit).all()
    
    return {
        "retention_tracking": tracking_records,
        "total_count": total_count,
        "skip": skip,
        "limit": limit
    }


@router.put("/retention-tracking/{retention_id}")
async def update_retention_tracking(
    retention_id: int,
    update_data: TelecallingRetentionTrackingUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Update retention tracking record"""
    
    tracking = db.query(TelecallingRetentionTracking).filter(
        TelecallingRetentionTracking.retention_id == retention_id
    ).first()
    
    if not tracking:
        raise HTTPException(status_code=404, detail="Retention tracking not found")
    
    update_dict = update_data.dict(exclude_unset=True)
    for key, value in update_dict.items():
        setattr(tracking, key, value)
    
    tracking.updated_at = datetime.now()
    db.commit()
    db.refresh(tracking)
    
    return {"message": "Retention tracking updated successfully"}


@router.get("/analytics/conversion-rates")
async def get_conversion_rates(
    request: Request,
    referal_id: Optional[str] = None,
    employee_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get conversion rates analytics"""
    
    query = db.query(TelecallingGymAssignment)
    
    if referal_id:
        query = query.filter(TelecallingGymAssignment.referal_id == referal_id)
    if employee_id:
        query = query.filter(TelecallingGymAssignment.employee_id == employee_id)
    if start_date:
        query = query.filter(TelecallingGymAssignment.created_at >= start_date)
    if end_date:
        query = query.filter(TelecallingGymAssignment.created_at <= end_date)
    
    # Get conversion analytics by joining with retention tracking to find converted clients
    conversion_query = query.join(
        TelecallingRetentionTracking,
        TelecallingGymAssignment.assignment_id == TelecallingRetentionTracking.assignment_id
    ).with_entities(
        TelecallingGymAssignment.referal_id,
        TelecallingGymAssignment.employee_id,
        func.count(TelecallingRetentionTracking.client_id).label("total_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "active", 1)])).label("active_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "churned", 1)])).label("churned_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "at_risk", 1)])).label("at_risk_clients"),
        func.avg(TelecallingRetentionTracking.retention_score).label("avg_retention_score")
    ).group_by(TelecallingGymAssignment.referal_id, TelecallingGymAssignment.employee_id).all()
    
    return {"conversion_analytics": conversion_query}


@router.get("/analytics/retention-rates")
async def get_retention_rates(
    request: Request,
    referal_id: Optional[str] = None,
    retention_period: Optional[str] = Query(None, pattern="^(7_days|30_days|60_days|90_days|180_days|365_days)$"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get retention rates by period"""
    
    query = db.query(TelecallingRetentionTracking)
    
    if referal_id:
        query = query.filter(TelecallingRetentionTracking.referal_id == referal_id)
    if retention_period:
        query = query.filter(TelecallingRetentionTracking.retention_period == retention_period)
    
    retention_stats = query.with_entities(
        TelecallingRetentionTracking.retention_period,
        TelecallingRetentionTracking.referal_id,
        func.count(TelecallingRetentionTracking.retention_id).label("total_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "active", 1)])).label("retained_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "churned", 1)])).label("churned_clients"),
        func.count(case([(TelecallingRetentionTracking.retention_status == "at_risk", 1)])).label("at_risk_clients"),
        func.avg(TelecallingRetentionTracking.retention_score).label("avg_retention_score")
    ).group_by(
        TelecallingRetentionTracking.retention_period, 
        TelecallingRetentionTracking.referal_id
    ).all()
    
    return {"retention_analytics": retention_stats}


@router.get("/gym-performance/{referal_id}")
async def get_gym_performance(
    referal_id: str,
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get detailed performance metrics for a specific gym by referal_id"""
    
    # Get assignment details
    assignment = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.referal_id == referal_id
    ).options(joinedload(TelecallingGymAssignment.assigned_employee)).first()
    
    if not assignment:
        raise HTTPException(status_code=404, detail="Assignment not found for this referal_id")
    
    # Get all retention tracking for this gym
    retention_records = db.query(TelecallingRetentionTracking).filter(
        TelecallingRetentionTracking.referal_id == referal_id
    ).all()
    
    # Calculate metrics
    total_clients = len(retention_records)
    active_clients = len([r for r in retention_records if r.retention_status == "active"])
    churned_clients = len([r for r in retention_records if r.retention_status == "churned"])
    at_risk_clients = len([r for r in retention_records if r.retention_status == "at_risk"])
    
    retention_rate = (active_clients / total_clients * 100) if total_clients > 0 else 0
    churn_rate = (churned_clients / total_clients * 100) if total_clients > 0 else 0
    
    avg_retention_score = sum([r.retention_score for r in retention_records if r.retention_score]) / len([r for r in retention_records if r.retention_score]) if any(r.retention_score for r in retention_records) else 0
    
    return {
        "referal_id": referal_id,
        "assignment_details": assignment,
        "metrics": {
            "total_clients": total_clients,
            "active_clients": active_clients,
            "churned_clients": churned_clients,
            "at_risk_clients": at_risk_clients,
            "retention_rate": round(retention_rate, 2),
            "churn_rate": round(churn_rate, 2),
            "avg_retention_score": round(avg_retention_score, 3)
        },
        "retention_records": retention_records
    }


@router.get("/gym-assignments-by-role")
async def get_gym_assignments_by_role(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    assignment_status: Optional[str] = Query(None, pattern="^(active|completed|paused|cancelled)$"),
    priority: Optional[str] = Query(None, pattern="^(low|medium|high|urgent)$"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get gym assignments based on user role - telecaller managers see team assignments, telecallers see only their own"""
    
    employee_id = current_user.get("id")
    if not employee_id:
        raise HTTPException(status_code=403, detail="Employee ID not found")
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    base_query = db.query(TelecallingGymAssignment).options(
        joinedload(TelecallingGymAssignment.assigned_employee),
        joinedload(TelecallingGymAssignment.assigner)
    )
    
    # Apply role-based filtering
    if current_user.get("manager_role", False):
        # Manager in telecaller department: Show assignments for all telecallers under their management
        
        # Get all telecaller employees assigned to this manager
        managed_telecaller_ids = db.query(EmployeeAssignments.employee_id).join(
            Employees, EmployeeAssignments.employee_id == Employees.id
        ).filter(
            EmployeeAssignments.manager_id == employee_id,
            EmployeeAssignments.status == "active",
            Employees.department == "telecaller",
            Employees.manager_role == False  # Only get non-manager telecallers
        ).subquery()
        
        # Show assignments for managed telecallers + manager's own assignments
        base_query = base_query.filter(
            or_(
                TelecallingGymAssignment.employee_id.in_(managed_telecaller_ids),
                TelecallingGymAssignment.employee_id == employee_id
            )
        )
    else:
        # Regular telecaller: Show only their own assignments
        base_query = base_query.filter(TelecallingGymAssignment.employee_id == employee_id)
    
    # Apply additional filters
    if assignment_status:
        base_query = base_query.filter(TelecallingGymAssignment.assignment_status == assignment_status)
    if priority:
        base_query = base_query.filter(TelecallingGymAssignment.priority == priority)
    
    total_count = base_query.count()
    
    # Get assignments with detailed information
    assignments = base_query.order_by(desc(TelecallingGymAssignment.created_at)).offset(skip).limit(limit).all()
    
    # Fetch gym details from fittbot_models for each assignment
    assignment_details = []
    for assignment in assignments:
        # Get gym details from fittbot_models using referal_id or fittbot_gym_id with owner details
        fittbot_gym = None
        gym_owner = None
        client_count = 0
        gym_status = "inactive"
        
        if assignment.fittbot_gym_id:
            # Use fittbot_gym_id if available
            gym_result = db.query(Gym, GymOwner).outerjoin(
                GymOwner, Gym.owner_id == GymOwner.owner_id
            ).filter(Gym.gym_id == assignment.fittbot_gym_id).first()
            
            if gym_result:
                fittbot_gym, gym_owner = gym_result
        elif assignment.referal_id:
            # Fallback to referal_id if fittbot_gym_id is not set
            gym_result = db.query(Gym, GymOwner).outerjoin(
                GymOwner, Gym.owner_id == GymOwner.owner_id
            ).filter(Gym.referal_id == assignment.referal_id).first()
            
            if gym_result:
                fittbot_gym, gym_owner = gym_result
        
        if fittbot_gym:
            # Get active client count for this gym (based on ClientFittbotAccess.access_status)
            client_count = db.query(Client).join(
                ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
            ).filter(
                Client.gym_id == fittbot_gym.gym_id,
                ClientFittbotAccess.access_status == "active"
            ).count()
            
            # Determine gym status based on active clients (same logic as gymstats.py)
            gym_status = "active" if client_count > 0 else "inactive"
        
        # Get retention stats for this assignment
        retention_stats = db.query(TelecallingRetentionTracking).filter(
            TelecallingRetentionTracking.assignment_id == assignment.assignment_id
        ).all()
        
        # Calculate basic metrics
        total_tracked_clients = len(retention_stats)
        active_clients = len([r for r in retention_stats if r.retention_status == "active"])
        churned_clients = len([r for r in retention_stats if r.retention_status == "churned"])
        at_risk_clients = len([r for r in retention_stats if r.retention_status == "at_risk"])
        
        assignment_detail = {
            "assignment": {
                "assignment_id": assignment.assignment_id,
                "marketing_gym_id": assignment.marketing_gym_id,
                "fittbot_gym_id": assignment.fittbot_gym_id,
                "referal_id": assignment.referal_id,
                "assignment_status": assignment.assignment_status,
                "assignment_date": assignment.assignment_date,
                "target_clients": assignment.target_clients,
                "priority": assignment.priority,
                "notes": assignment.notes,
                "created_at": assignment.created_at,
                "updated_at": assignment.updated_at,
                "assigned_employee": {
                    "id": assignment.assigned_employee.id,
                    "name": assignment.assigned_employee.name,
                    "employee_id": assignment.assigned_employee.employee_id,
                    "email": assignment.assigned_employee.email,
                    "department": assignment.assigned_employee.department,
                    "manager_role": assignment.assigned_employee.manager_role
                } if assignment.assigned_employee else None,
                "assigner": {
                    "id": assignment.assigner.id,
                    "name": assignment.assigner.name,
                    "employee_id": assignment.assigner.employee_id
                } if assignment.assigner else None
            },
            "gym": {
                "gym_id": fittbot_gym.gym_id,
                "name": fittbot_gym.name,
                "location": fittbot_gym.location,
                "referal_id": fittbot_gym.referal_id,
                "max_clients": fittbot_gym.max_clients,
                "logo": fittbot_gym.logo,
                "cover_pic": fittbot_gym.cover_pic,
                "owner_id": fittbot_gym.owner_id,
                "contact_person": gym_owner.name if gym_owner else None,
                "contact_phone": gym_owner.contact_number if gym_owner else None,
                "contact_email": gym_owner.email if gym_owner else None,
                "status": gym_status
            } if fittbot_gym else None,
            "gym_metrics": {
                "total_gym_clients": client_count,
                "tracked_clients": total_tracked_clients,
                "active_tracked_clients": active_clients,
                "churned_clients": churned_clients,
                "at_risk_clients": at_risk_clients,
                "retention_rate": round((active_clients / total_tracked_clients * 100) if total_tracked_clients > 0 else 0, 2),
                "churn_rate": round((churned_clients / total_tracked_clients * 100) if total_tracked_clients > 0 else 0, 2)
            }
        }
        assignment_details.append(assignment_detail)
    
    return {
        "assignments": assignment_details,
        "total_count": total_count,
        "skip": skip,
        "limit": limit,
        "user_info": {
            "id": current_user.get("id"),
            "name": current_user.get("name"),
            "employee_id": current_user.get("employee_id", ""),
            "department": current_user.get("department"),
            "manager_role": current_user.get("manager_role", False),
            "role_type": "telecaller_manager" if current_user.get("manager_role", False) else "telecaller"
        },
        "access_level": "team_assignments" if current_user.get("manager_role", False) else "own_assignments_only"
    }


def get_clients_query(db: Session):
    """Base query for clients with necessary joins - same as userDashboard.py"""
    return db.query(
        Client.client_id,
        Client.name,
        Client.contact,
        Client.email,
        Client.created_at,
        Gym.name.label('gym_name'),
        ClientFittbotAccess.access_status,
        FittbotPlans.plan_name
    ).outerjoin(
        Gym, Client.gym_id == Gym.gym_id
    ).outerjoin(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).outerjoin(
        FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
    )


@router.get("/assignment/{assignment_id}/clients")
async def get_assignment_gym_clients(
    assignment_id: int,
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, mobile"),
    status: Optional[str] = Query(None, description="Filter by access status"),
    plan: Optional[str] = Query(None, description="Filter by plan name"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Sort order for created_at"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get clients list for a specific gym assignment with same structure as userDashboard.py"""
    
    employee_id = current_user.get("id")
    if not employee_id:
        raise HTTPException(status_code=403, detail="Employee ID not found")
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    # Get the assignment and verify access
    assignment = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.assignment_id == assignment_id
    ).first()
    
    if not assignment:
        raise HTTPException(status_code=404, detail="Assignment not found")
    
    # Check if user has access to this assignment
    if not current_user.get("manager_role", False):
        # Regular telecaller: can only access their own assignments
        if assignment.employee_id != employee_id:
            raise HTTPException(status_code=403, detail="You can only access your own assignments")
    else:
        # Manager: check if assignment belongs to their team or themselves
        if assignment.employee_id != employee_id:
            # Check if the assigned employee is under this manager
            managed_employee = db.query(EmployeeAssignments).join(
                Employees, EmployeeAssignments.employee_id == Employees.id
            ).filter(
                EmployeeAssignments.manager_id == employee_id,
                EmployeeAssignments.employee_id == assignment.employee_id,
                EmployeeAssignments.status == "active",
                Employees.department == "telecaller"
            ).first()
            
            if not managed_employee:
                raise HTTPException(status_code=403, detail="You can only access assignments for your team members")
    
    # Get the gym_id for the assignment (use fittbot_gym_id if available, otherwise map from marketing gym)
    gym_id = assignment.fittbot_gym_id
    
    if not gym_id:
        # Try to find gym by referal_id if no direct fittbot_gym_id mapping
        gym = db.query(Gym).filter(Gym.referal_id == assignment.referal_id).first()
        if gym:
            gym_id = gym.gym_id
        else:
            # No gym found, return empty client list
            return {
                "success": True,
                "data": {
                    "users": [],
                    "total": 0,
                    "page": page,
                    "limit": limit,
                    "totalPages": 0,
                    "hasNext": False,
                    "hasPrev": False
                },
                "message": "No gym found for this assignment or gym not yet linked to fittbot system",
                "assignment_info": {
                    "assignment_id": assignment.assignment_id,
                    "referal_id": assignment.referal_id,
                    "marketing_gym_id": assignment.marketing_gym_id,
                    "fittbot_gym_id": assignment.fittbot_gym_id,
                    "gym_linked": False
                }
            }
    
    try:
        # Base query for clients in this gym - same structure as userDashboard.py
        query = get_clients_query(db).filter(Client.gym_id == gym_id)
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term)
                )
            )
        
        # Apply status filter
        if status and status != "all":
            query = query.filter(ClientFittbotAccess.access_status == status)
        
        # Apply plan filter
        if plan and plan != "all":
            query = query.filter(FittbotPlans.plan_name == plan)
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(Client.created_at.asc())
        else:
            query = query.order_by(desc(Client.created_at))
        
        # Get total count before pagination
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        paginated_query = query.offset(offset).limit(limit)
        
        # Execute query
        results = paginated_query.all()
        
        # Convert to response format - exact same as userDashboard.py
        users = []
        for result in results:
            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "access_status": result.access_status or "inactive",
                "plan_name": result.plan_name,
                "created_at": result.created_at.isoformat() if result.created_at else None
            }
            users.append(user_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        has_next = page < total_pages
        has_prev = page > 1
        
        # Get gym info for response
        gym_info = db.query(Gym).filter(Gym.gym_id == gym_id).first()
        
        return {
            "success": True,
            "data": {
                "users": users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Users fetched successfully",
            "assignment_info": {
                "assignment_id": assignment.assignment_id,
                "referal_id": assignment.referal_id,
                "marketing_gym_id": assignment.marketing_gym_id,
                "fittbot_gym_id": assignment.fittbot_gym_id,
                "gym_linked": True,
                "gym_name": gym_info.name if gym_info else None,
                "gym_location": gym_info.location if gym_info else None
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching clients: {str(e)}")


@router.get("/referal/{referal_id}/clients")
async def get_referal_gym_clients(
    referal_id: str,
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, mobile"),
    status: Optional[str] = Query(None, description="Filter by access status"),
    plan: Optional[str] = Query(None, description="Filter by plan name"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Sort order for created_at"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get clients list for a gym by referal_id with same structure as userDashboard.py"""
    
    employee_id = current_user.get("id")
    if not employee_id:
        raise HTTPException(status_code=403, detail="Employee ID not found")
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    # Check if user has access to this referal_id
    assignment = db.query(TelecallingGymAssignment).filter(
        TelecallingGymAssignment.referal_id == referal_id
    ).first()
    
    if not assignment:
        raise HTTPException(status_code=404, detail="No assignment found for this referal_id")
    
    # Verify access based on role
    if not current_user.get("manager_role", False):
        # Regular telecaller: can only access their own assignments
        if assignment.employee_id != employee_id:
            raise HTTPException(status_code=403, detail="You can only access gyms assigned to you")
    else:
        # Manager: check if assignment belongs to their team or themselves
        if assignment.employee_id != employee_id:
            # Check if the assigned employee is under this manager
            managed_employee = db.query(EmployeeAssignments).join(
                Employees, EmployeeAssignments.employee_id == Employees.id
            ).filter(
                EmployeeAssignments.manager_id == employee_id,
                EmployeeAssignments.employee_id == assignment.employee_id,
                EmployeeAssignments.status == "active",
                Employees.department == "telecaller"
            ).first()
            
            if not managed_employee:
                raise HTTPException(status_code=403, detail="You can only access gyms assigned to your team members")
    
    # Find gym by referal_id
    gym = db.query(Gym).filter(Gym.referal_id == referal_id).first()
    
    if not gym:
        return {
            "success": True,
            "data": {
                "users": [],
                "total": 0,
                "page": page,
                "limit": limit,
                "totalPages": 0,
                "hasNext": False,
                "hasPrev": False
            },
            "message": "Gym not found in fittbot system for this referal_id",
            "referal_info": {
                "referal_id": referal_id,
                "gym_linked": False
            }
        }
    
    try:
        # Base query for clients in this gym - same structure as userDashboard.py
        query = get_clients_query(db).filter(Client.gym_id == gym.gym_id)
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term)
                )
            )
        
        # Apply status filter
        if status and status != "all":
            query = query.filter(ClientFittbotAccess.access_status == status)
        
        # Apply plan filter
        if plan and plan != "all":
            query = query.filter(FittbotPlans.plan_name == plan)
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(Client.created_at.asc())
        else:
            query = query.order_by(desc(Client.created_at))
        
        # Get total count before pagination
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        paginated_query = query.offset(offset).limit(limit)
        
        # Execute query
        results = paginated_query.all()
        
        # Convert to response format - exact same as userDashboard.py
        users = []
        for result in results:
            user_data = {
                "client_id": result.client_id,
                "name": result.name,
                "contact": result.contact,
                "email": result.email,
                "gym_name": result.gym_name,
                "access_status": result.access_status or "inactive",
                "plan_name": result.plan_name,
                "created_at": result.created_at.isoformat() if result.created_at else None
            }
            users.append(user_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "success": True,
            "data": {
                "users": users,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Users fetched successfully",
            "referal_info": {
                "referal_id": referal_id,
                "gym_linked": True,
                "gym_id": gym.gym_id,
                "gym_name": gym.name,
                "gym_location": gym.location
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching clients: {str(e)}")


@router.get("/clients/plans")
async def get_available_plans_for_telecaller(
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get all available Fittbot plans for filter dropdown - same as userDashboard.py"""
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    try:
        plans = db.query(FittbotPlans).filter(
            FittbotPlans.id.in_(
                db.query(ClientFittbotAccess.fittbot_plan).distinct()
            )
        ).all()
        
        plans_data = [{"id": plan.id, "plan_name": plan.plan_name} for plan in plans]
        
        return {
            "success": True,
            "data": plans_data,
            "message": "Plans fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching plans: {str(e)}")


@router.get("/available-gyms")
async def get_available_gyms_for_assignment(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    search: Optional[str] = Query(None, description="Search by gym name"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get gyms from fittbot_models Gym table"""
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    try:
        # Query fittbot gyms with owner details using LEFT JOIN
        gym_query = db.query(Gym, GymOwner).outerjoin(
            GymOwner, Gym.owner_id == GymOwner.owner_id
        ).filter(
            Gym.referal_id.isnot(None),
            Gym.referal_id != ""
        )
        
        # Apply search filter
        if search:
            gym_query = gym_query.filter(
                or_(
                    Gym.name.like(f"%{search}%"),
                    GymOwner.name.like(f"%{search}%")
                )
            )
        
        # Get total count
        total_count = gym_query.count()
        
        # Apply pagination and ordering
        gym_results = gym_query.order_by(desc(Gym.created_at)).offset(skip).limit(limit).all()
        
        # Format gym data for response with contact person details and status
        gyms_data = []
        for gym, owner in gym_results:
            # Get active client count for this gym (same logic as gymstats.py)
            active_client_count = db.query(Client).join(
                ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
            ).filter(
                Client.gym_id == gym.gym_id,
                ClientFittbotAccess.access_status == "active"
            ).count()
            
            # Determine gym status based on active clients (same logic as gymstats.py)
            gym_status = "active" if active_client_count > 0 else "inactive"
            
            gym_data = {
                "gym_id": gym.gym_id,
                "name": gym.name,
                "location": gym.location,
                "referal_id": gym.referal_id,
                "max_clients": gym.max_clients,
                "logo": gym.logo,
                "cover_pic": gym.cover_pic,
                "owner_id": gym.owner_id,
                "subscription_start_date": gym.subscription_start_date.isoformat() if gym.subscription_start_date else None,
                "subscription_end_date": gym.subscription_end_date.isoformat() if gym.subscription_end_date else None,
                "created_at": gym.created_at.isoformat() if gym.created_at else None,
                "updated_at": gym.updated_at.isoformat() if gym.updated_at else None,
                "contact_person": owner.name if owner else None,
                "contact_phone": owner.contact_number if owner else None,
                "contact_email": owner.email if owner else None,
                "status": gym_status,
                "active_clients": active_client_count
            }
            
            gyms_data.append(gym_data)
        
        # Calculate pagination
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        
        return {
            "success": True,
            "data": {
                "gyms": gyms_data,
                "total": total_count,
                "skip": skip,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": skip + limit < total_count,
                "hasPrev": skip > 0
            },
            "message": "Available gyms fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching available gyms: {str(e)}")


# Dashboard Endpoints for Telecaller Dashboard

def get_fittbot_statistics(db: Session):
    """Get Fittbot statistics: total users and unsubscribed users"""
    
    # Current date calculations
    today = datetime.now().date()
    last_month_start = today - timedelta(days=30)
    two_months_ago_start = today - timedelta(days=60)
    two_months_ago_end = today - timedelta(days=30)
    
    # Current total users with active fittbot access
    total_users = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active"
    ).count()
    
    # Current unsubscribed users (clients with inactive fittbot access)
    unsubscribed_users = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "inactive"
    ).count()
    
    # Previous month active users (users created in the previous 30 days period)
    prev_month_active_users = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active",
        Client.created_at >= two_months_ago_start,
        Client.created_at < two_months_ago_end
    ).count()
    
    # Previous month inactive users
    prev_month_inactive_users = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "inactive",
        Client.created_at >= two_months_ago_start,
        Client.created_at < two_months_ago_end
    ).count()
    
    # Calculate percentage changes
    # Active users growth
    new_active_users_this_month = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active",
        Client.created_at >= last_month_start
    ).count()
    
    if prev_month_active_users > 0:
        total_users_change_percent = ((new_active_users_this_month - prev_month_active_users) / prev_month_active_users) * 100
        total_users_change = f"{'+' if total_users_change_percent >= 0 else ''}{total_users_change_percent:.1f}%"
    else:
        total_users_change = "+100%" if new_active_users_this_month > 0 else "0%"
    
    # Unsubscribed users change
    new_inactive_users_this_month = db.query(Client).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "inactive",
        Client.created_at >= last_month_start
    ).count()
    
    if prev_month_inactive_users > 0:
        unsubscribed_change_percent = ((new_inactive_users_this_month - prev_month_inactive_users) / prev_month_inactive_users) * 100
        unsubscribed_change = f"{'+' if unsubscribed_change_percent >= 0 else ''}{unsubscribed_change_percent:.1f}%"
    else:
        unsubscribed_change = "+100%" if new_inactive_users_this_month > 0 else "0%"
    
    return {
        "total_users": total_users,
        "unsubscribed_users": unsubscribed_users,
        "total_users_change": total_users_change,
        "unsubscribed_change": unsubscribed_change
    }


def get_fittbot_business_statistics(db: Session):
    """Get Fittbot Business statistics: total gyms, active gyms, and total gym users"""
    
    # Current date calculations
    today = datetime.now().date()
    last_month_start = today - timedelta(days=30)
    two_months_ago_start = today - timedelta(days=60)
    two_months_ago_end = today - timedelta(days=30)
    
    # Current total gyms count
    total_gyms = db.query(Gym).count()
    
    # Current active gyms (gyms with at least one active client)
    active_gyms_subquery = db.query(Client.gym_id).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active",
        Client.gym_id.isnot(None)
    ).distinct().subquery()
    
    active_gyms = db.query(active_gyms_subquery).count()
    
    # Current total users in gyms (clients associated with any gym)
    total_gym_users = db.query(Client).filter(
        Client.gym_id.isnot(None)
    ).count()
    
    # Calculate percentage changes based on creation dates
    
    # Gyms added this month vs previous month
    new_gyms_this_month = db.query(Gym).filter(
        Gym.created_at >= last_month_start
    ).count()
    
    new_gyms_prev_month = db.query(Gym).filter(
        Gym.created_at >= two_months_ago_start,
        Gym.created_at < two_months_ago_end
    ).count()
    
    if new_gyms_prev_month > 0:
        gyms_change_percent = ((new_gyms_this_month - new_gyms_prev_month) / new_gyms_prev_month) * 100
        total_gyms_change = f"{'+' if gyms_change_percent >= 0 else ''}{gyms_change_percent:.1f}%"
    else:
        total_gyms_change = "+100%" if new_gyms_this_month > 0 else "0%"
    
    # Active gyms change (gyms that became active this month vs previous month)
    # Count gyms that got their first active client in the last 30 days
    gyms_with_new_active_clients = db.query(Client.gym_id).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active",
        Client.gym_id.isnot(None),
        Client.created_at >= last_month_start
    ).distinct().count()
    
    gyms_with_prev_month_active_clients = db.query(Client.gym_id).join(
        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
    ).filter(
        ClientFittbotAccess.access_status == "active",
        Client.gym_id.isnot(None),
        Client.created_at >= two_months_ago_start,
        Client.created_at < two_months_ago_end
    ).distinct().count()
    
    if gyms_with_prev_month_active_clients > 0:
        active_gyms_change_percent = ((gyms_with_new_active_clients - gyms_with_prev_month_active_clients) / gyms_with_prev_month_active_clients) * 100
        active_gyms_change = f"{'+' if active_gyms_change_percent >= 0 else ''}{active_gyms_change_percent:.1f}%"
    else:
        active_gyms_change = "+100%" if gyms_with_new_active_clients > 0 else "0%"
    
    # Gym users change (new gym users this month vs previous month)
    new_gym_users_this_month = db.query(Client).filter(
        Client.gym_id.isnot(None),
        Client.created_at >= last_month_start
    ).count()
    
    new_gym_users_prev_month = db.query(Client).filter(
        Client.gym_id.isnot(None),
        Client.created_at >= two_months_ago_start,
        Client.created_at < two_months_ago_end
    ).count()
    
    if new_gym_users_prev_month > 0:
        gym_users_change_percent = ((new_gym_users_this_month - new_gym_users_prev_month) / new_gym_users_prev_month) * 100
        total_users_change = f"{'+' if gym_users_change_percent >= 0 else ''}{gym_users_change_percent:.1f}%"
    else:
        total_users_change = "+100%" if new_gym_users_this_month > 0 else "0%"
    
    return {
        "total_gyms": total_gyms,
        "active_gyms": active_gyms,
        "total_users": total_gym_users,
        "total_gyms_change": total_gyms_change,
        "active_gyms_change": active_gyms_change,
        "total_users_change": total_users_change
    }


@router.get("/dashboard")
async def get_telecaller_dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get complete dashboard data for telecaller dashboard"""
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    try:
        fittbot_stats = get_fittbot_statistics(db)
        business_stats = get_fittbot_business_statistics(db)
        
        return {
            "success": True,
            "data": {
                "fittbot": {
                    "totalUsers": fittbot_stats["total_users"],
                    "unsubscribedUsers": fittbot_stats["unsubscribed_users"],
                    "totalUsersChange": fittbot_stats["total_users_change"],
                    "unsubscribedChange": fittbot_stats["unsubscribed_change"]
                },
                "business": {
                    "totalGyms": business_stats["total_gyms"],
                    "activeGyms": business_stats["active_gyms"],
                    "totalUsers": business_stats["total_users"],
                    "totalGymsChange": business_stats["total_gyms_change"],
                    "activeGymsChange": business_stats["active_gyms_change"],
                    "totalUsersChange": business_stats["total_users_change"]
                }
            },
            "message": "Dashboard data fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dashboard data: {str(e)}")


@router.get("/dashboard/fittbot")
async def get_fittbot_stats(
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get Fittbot statistics for telecaller dashboard"""
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    try:
        stats = get_fittbot_statistics(db)
        
        return {
            "success": True,
            "data": stats,
            "message": "Fittbot statistics fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Fittbot statistics: {str(e)}")


@router.get("/dashboard/business")
async def get_business_stats(
    request: Request,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_employee_for_support)
):
    """Get Fittbot Business statistics for telecaller dashboard"""
    
    # Check if employee is in telecaller department
    if current_user.get("department", "").lower() != "telecaller":
        raise HTTPException(status_code=403, detail="This endpoint is only for telecaller department employees")
    
    try:
        stats = get_fittbot_business_statistics(db)
        
        return {
            "success": True,
            "data": stats,
            "message": "Fittbot Business statistics fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching Business statistics: {str(e)}")