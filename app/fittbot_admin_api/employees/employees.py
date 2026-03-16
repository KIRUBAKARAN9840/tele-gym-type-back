from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_, desc, alias
from app.models.adminmodels import Employees, EmployeeAssignments, EmployeeRoles
from app.models.marketingmodels import Managers, Executives
from app.models.database import get_db
from typing import Optional, List
from datetime import datetime, date
import math
import uuid
import hashlib

router = APIRouter(prefix="/api/admin/employees", tags=["Admin Employees"])

# Pydantic models
class EmployeeCreate(BaseModel):
    name: str
    email: EmailStr
    contact: str
    password: str
    dob: date
    age: Optional[int] = None
    gender: Optional[str] = None
    department: Optional[str] = None
    designation: Optional[str] = None
    joined_date: Optional[date] = None
    employee_id: str
    profile: Optional[str] = None
    manager_role:bool

class EmployeeUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    contact: Optional[str] = None
    password: Optional[str] = None
    dob: Optional[date] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    department: Optional[str] = None
    designation: Optional[str] = None
    joined_date: Optional[date] = None
    status: Optional[str] = None
    access: Optional[bool] = None
    profile: Optional[str] = None
    manager_role:bool

class EmployeeResponse(BaseModel):
    id: int
    name: str
    email: str
    contact: str
    dob: date
    age: Optional[int]
    gender: Optional[str]
    department: Optional[str]
    designation: Optional[str]
    joined_date: Optional[date]
    status: str
    employee_id: str
    access: bool
    profile: Optional[str]
    manager_role:bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class ExecutiveAssignmentRequest(BaseModel):
    executive_id: int
    manager_id: int

class MultipleExecutiveAssignmentRequest(BaseModel):
    executive_ids: List[int]
    manager_id: int

class EmployeeAssignmentRequest(BaseModel):
    manager_id: int
    employee_id: int

class MultipleEmployeeAssignmentRequest(BaseModel):
    manager_id: int
    employee_ids: List[int]

class RoleCreate(BaseModel):
    name: str
    department: str

class RoleUpdate(BaseModel):
    name: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = None

class RoleResponse(BaseModel):
    role_id: int
    name: str
    department: str
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class PaginatedEmployeesResponse(BaseModel):
    employees: List[EmployeeResponse]
    total: int
    page: int
    limit: int
    totalPages: int
    hasNext: bool
    hasPrev: bool

def hash_password(password: str) -> str:
    """Simple password hashing - in production, use proper password hashing like bcrypt"""
    return hashlib.sha256(password.encode()).hexdigest()

@router.post("/", response_model=dict)
async def create_employee(
    employee: EmployeeCreate,
    db: Session = Depends(get_db)
):
    """Create a new employee"""
    try:
        # Check if email already exists
        existing_email = db.query(Employees).filter(Employees.email == employee.email).first()
        if existing_email:
            raise HTTPException(status_code=400, detail="Email already exists")
        
        # Check if contact already exists
        existing_contact = db.query(Employees).filter(Employees.contact == employee.contact).first()
        if existing_contact:
            raise HTTPException(status_code=400, detail="Contact number already exists")
        
        # Check if employee_id already exists
        existing_emp_id = db.query(Employees).filter(Employees.employee_id == employee.employee_id).first()
        if existing_emp_id:
            raise HTTPException(status_code=400, detail="Employee ID already exists")
        
        # Validate that the designation exists in the EmployeeRoles table for the given department
        role_exists = db.query(EmployeeRoles).filter(
            and_(
                EmployeeRoles.name == employee.designation,
                EmployeeRoles.department == employee.department,
                EmployeeRoles.status == "active"
            )
        ).first()
        
        if not role_exists:
            raise HTTPException(
                status_code=400, 
                detail=f"Role '{employee.designation}' does not exist in {employee.department} department. Please create the role first."
            )

        # Create new employee
        new_employee = Employees(
            name=employee.name,
            email=employee.email,
            contact=employee.contact,
            password=hash_password(employee.password),
            dob=employee.dob,
            age=employee.age,
            gender=employee.gender,
            department=employee.department,
            role=employee.department,
            designation=employee.designation,
            joined_date=employee.joined_date or date.today(),
            employee_id=employee.employee_id,
            manager_role = employee.manager_role,
            uuid=str(uuid.uuid4()),
            profile=employee.profile or 'https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png',
            status='active',
            access=True,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        db.add(new_employee)
        db.commit()
        db.refresh(new_employee)
        
        # Create manager or executive record if designation matches
        if employee.designation and employee.designation.upper() == 'BDM':
            new_manager = Managers(
                employee_id=new_employee.id,
                name=new_employee.name,
                email=new_employee.email,
                contact=new_employee.contact,
                password=new_employee.password,
                dob=new_employee.dob,
                age=new_employee.age,
                gender=new_employee.gender,
                role=employee.designation,
                joined_date=new_employee.joined_date,
                emp_id=new_employee.employee_id,
                uuid=str(uuid.uuid4()),
                profile=new_employee.profile,
                status='active',
                access=True,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_manager)
            db.commit()
            
        elif employee.designation and employee.designation.upper() == 'BDE':
            new_executive = Executives(
                employee_id=new_employee.id,
                name=new_employee.name,
                email=new_employee.email,
                contact=new_employee.contact,
                password=new_employee.password,
                dob=new_employee.dob,
                age=new_employee.age,
                gender=new_employee.gender,
                role=employee.designation,
                joined_date=new_employee.joined_date,
                emp_id=new_employee.employee_id,
                uuid=str(uuid.uuid4()),
                profile=new_employee.profile,
                status='active',
                access=True,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_executive)
            db.commit()
        
        return {
            "status": 201,
            "message": "Employee created successfully",
            "data": {
                "id": new_employee.id,
                "name": new_employee.name,
                "email": new_employee.email,
                "employee_id": new_employee.employee_id
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating employee: {str(e)}")

@router.get("/", response_model=dict)
async def get_all_employees(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by name, email, contact, or employee ID"),
    department: Optional[str] = Query(None, description="Filter by department"),
    designation: Optional[str] = Query(None, description="Filter by designation"),
    status: Optional[str] = Query(None, description="Filter by status"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),
    db: Session = Depends(get_db)
):
    """Get all employees with pagination and filters"""
    try:
        # Base query
        query = db.query(Employees)
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(Employees.name).like(search_term),
                    func.lower(Employees.email).like(search_term),
                    Employees.contact.like(search_term),
                    func.lower(Employees.employee_id).like(search_term)
                )
            )
        
        # Apply filters
        if department:
            query = query.filter(Employees.department == department)
        
        if designation:
            query = query.filter(Employees.designation == designation)
            
        if status:
            query = query.filter(Employees.status == status)
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(Employees.created_at.asc())
        else:
            query = query.order_by(Employees.created_at.desc())
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        employees = query.offset(offset).limit(limit).all()
        
        # Format response
        employees_data = []
        for emp in employees:
            emp_data = {
                "id": emp.id,
                "name": emp.name,
                "email": emp.email,
                "contact": emp.contact,
                "dob": emp.dob.isoformat() if emp.dob else None,
                "age": emp.age,
                "gender": emp.gender,
                "department": emp.department,
                "designation": emp.designation,
                "joined_date": emp.joined_date.isoformat() if emp.joined_date else None,
                "status": emp.status,
                "employee_id": emp.employee_id,
                "access": emp.access,
                "profile": emp.profile,
                "manager_role":emp.manager_role,
                "created_at": emp.created_at.isoformat() if emp.created_at else None,
                "updated_at": emp.updated_at.isoformat() if emp.updated_at else None
            }
            
            # Add manager details if employee has an assignment
            assignment = db.query(EmployeeAssignments).filter(
                and_(
                    EmployeeAssignments.employee_id == emp.id,
                    EmployeeAssignments.status == "active"
                )
            ).first()
            
            if assignment:
                manager = db.query(Employees).filter(Employees.id == assignment.manager_id).first()
                if manager:
                    emp_data["assigned_manager"] = {
                        "manager_id": manager.id,
                        "manager_name": manager.name,
                        "manager_email": manager.email,
                        "manager_emp_id": manager.employee_id,
                        "manager_department": manager.department,
                        "assignment_date": assignment.assignment_date.isoformat() if assignment.assignment_date else None
                    }
                else:
                    emp_data["assigned_manager"] = None
            else:
                emp_data["assigned_manager"] = None
            
            # Also add legacy BDE manager details if applicable (for backward compatibility)
            if emp.designation and emp.designation.upper() == 'BDE':
                executive = db.query(Executives).filter(Executives.employee_id == emp.id).first()
                if executive and executive.manager_id:
                    legacy_manager = db.query(Managers).filter(Managers.id == executive.manager_id).first()
                    if legacy_manager:
                        emp_data["legacy_assigned_manager"] = {
                            "manager_id": legacy_manager.id,
                            "manager_name": legacy_manager.name,
                            "manager_email": legacy_manager.email,
                            "manager_emp_id": legacy_manager.emp_id
                        }
                    else:
                        emp_data["legacy_assigned_manager"] = None
                else:
                    emp_data["legacy_assigned_manager"] = None
            
            employees_data.append(emp_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "status": 200,
            "message": "Employees fetched successfully",
            "data": {
                "employees": employees_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employees: {str(e)}")


@router.post("/assign-executive-to-manager", response_model=dict)
async def assign_executive_to_manager(
    assignment: ExecutiveAssignmentRequest,
    db: Session = Depends(get_db)
):
    """Assign an executive to a manager"""
    try:
        # Check if executive exists
        executive = db.query(Executives).filter(Executives.id == assignment.executive_id).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        # Check if manager exists
        manager = db.query(Managers).filter(Managers.employee_id == assignment.manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        # Update executive's manager_id
        executive.manager_id = manager.id
        executive.updated_at = datetime.now()
        
        db.commit()
        db.refresh(executive)
        
        return {
            "status": 200,
            "message": "Executive assigned to manager successfully",
            "data": {
                "executive_id": executive.id,
                "executive_name": executive.name,
                "manager_id": manager.id,
                "manager_name": manager.name,
                "assigned_at": datetime.now().isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error assigning executive to manager: {str(e)}")

@router.post("/assign-multiple-executives-to-manager", response_model=dict)
async def assign_multiple_executives_to_manager(
    assignment: MultipleExecutiveAssignmentRequest,
    db: Session = Depends(get_db)
):
    """Assign multiple executives to a manager"""
    try:
        # Check if manager exists using employee_id (following the current pattern)
        manager = db.query(Managers).filter(Managers.employee_id == assignment.manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        assigned_executives = []
        failed_assignments = []
        
        for executive_id in assignment.executive_ids:
            # Check if executive exists
            executive = db.query(Executives).filter(Executives.id == executive_id).first()
            if not executive:
                failed_assignments.append({
                    "executive_id": executive_id,
                    "reason": "Executive not found"
                })
                continue
            
            # Update executive's manager_id
            executive.manager_id = manager.id
            executive.updated_at = datetime.now()
            
            assigned_executives.append({
                "executive_id": executive.id,
                "executive_name": executive.name
            })
        
        db.commit()
        
        return {
            "status": 200,
            "message": f"Successfully assigned {len(assigned_executives)} executives to manager",
            "data": {
                "manager": {
                    "manager_id": manager.id,
                    "manager_name": manager.name,
                    "manager_emp_id": manager.emp_id
                },
                "assigned_executives": assigned_executives,
                "failed_assignments": failed_assignments,
                "total_assigned": len(assigned_executives),
                "total_failed": len(failed_assignments),
                "assigned_at": datetime.now().isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error assigning executives to manager: {str(e)}")

@router.get("/assignments/executive-manager-pairs", response_model=dict)
async def get_executive_manager_assignments(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db)
):
    """Get all executive-manager assignments"""
    try:
        # Query executives with their assigned managers
        query = db.query(
            Executives.id.label('executive_id'),
            Executives.name.label('executive_name'),
            Executives.email.label('executive_email'),
            Executives.emp_id.label('executive_emp_id'),
            Managers.employee_id.label('manager_id'),
            Managers.name.label('manager_name'),
            Managers.email.label('manager_email'),
            Managers.emp_id.label('manager_emp_id')
        ).outerjoin(
            Managers, Executives.manager_id == Managers.id
        ).order_by(desc(Executives.updated_at))
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        results = query.offset(offset).limit(limit).all()
        
        # Format response
        assignments_data = []
        for result in results:
            assignment_data = {
                "executive": {
                    "id": result.executive_id,
                    "name": result.executive_name,
                    "email": result.executive_email,
                    "emp_id": result.executive_emp_id
                },
                "manager": {
                    "id": result.manager_id,
                    "name": result.manager_name,
                    "email": result.manager_email,
                    "emp_id": result.manager_emp_id
                } if result.manager_id else None
            }
            assignments_data.append(assignment_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "status": 200,
            "message": "Executive-Manager assignments fetched successfully",
            "data": {
                "assignments": assignments_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching assignments: {str(e)}")

@router.put("/assignments/executives/{executive_id}/unassign", response_model=dict)
async def unassign_executive_from_manager(
    executive_id: int,
    db: Session = Depends(get_db)
):
    """Unassign an executive from their manager"""
    try:
        executive = db.query(Executives).filter(Executives.id == executive_id).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        if not executive.manager_id:
            raise HTTPException(status_code=400, detail="Executive is not assigned to any manager")
        
        # Store manager info for response
        old_manager = db.query(Managers).filter(Managers.id == executive.manager_id).first()
        
        # Unassign executive
        executive.manager_id = None
        executive.updated_at = datetime.now()
        
        db.commit()
        db.refresh(executive)
        
        return {
            "status": 200,
            "message": "Executive unassigned from manager successfully",
            "data": {
                "executive_id": executive.id,
                "executive_name": executive.name,
                "previously_assigned_to": {
                    "manager_id": old_manager.id if old_manager else None,
                    "manager_name": old_manager.name if old_manager else None
                },
                "unassigned_at": datetime.now().isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error unassigning executive: {str(e)}")

@router.post("/assign-employee-to-manager", response_model=dict)
async def assign_employee_to_manager(
    assignment: EmployeeAssignmentRequest,
    db: Session = Depends(get_db)
):
    """Assign an employee to a manager (same department validation)"""
    try:
        # Check if employee exists
        employee = db.query(Employees).filter(Employees.id == assignment.employee_id).first()
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Check if manager exists and has manager_role=True
        manager = db.query(Employees).filter(
            and_(Employees.id == assignment.manager_id, Employees.manager_role == True)
        ).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found or doesn't have manager role")
        
        # Validate same department
        if employee.department != manager.department:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot assign employee from {employee.department} department to manager from {manager.department} department"
            )
        
        # Check if employee is already assigned to any manager (one employee can only have one manager)
        existing_assignment = db.query(EmployeeAssignments).filter(
            and_(
                EmployeeAssignments.employee_id == assignment.employee_id,
                EmployeeAssignments.status == "active"
            )
        ).first()
        
        if existing_assignment:
            existing_manager = db.query(Employees).filter(Employees.id == existing_assignment.manager_id).first()
            raise HTTPException(
                status_code=400, 
                detail=f"Employee is already assigned to manager: {existing_manager.name if existing_manager else 'Unknown'}"
            )
        
        # Create new assignment
        new_assignment = EmployeeAssignments(
            manager_id=assignment.manager_id,
            employee_id=assignment.employee_id,
            assignment_date=date.today(),
            status="active"
        )
        
        db.add(new_assignment)
        db.commit()
        db.refresh(new_assignment)
        
        return {
            "status": 201,
            "message": "Employee assigned to manager successfully",
            "data": {
                "assignment_id": new_assignment.assignment_id,
                "manager": {
                    "id": manager.id,
                    "name": manager.name,
                    "employee_id": manager.employee_id,
                    "department": manager.department
                },
                "employee": {
                    "id": employee.id,
                    "name": employee.name,
                    "employee_id": employee.employee_id,
                    "department": employee.department
                },
                "assignment_date": new_assignment.assignment_date.isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error assigning employee to manager: {str(e)}")

@router.post("/assign-multiple-employees-to-manager", response_model=dict)
async def assign_multiple_employees_to_manager(
    assignment: MultipleEmployeeAssignmentRequest,
    db: Session = Depends(get_db)
):
    """Assign multiple employees to a manager (same department validation)"""
    try:
        # Check if manager exists and has manager_role=True
        manager = db.query(Employees).filter(
            and_(Employees.id == assignment.manager_id, Employees.manager_role == True)
        ).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found or doesn't have manager role")
        
        successful_assignments = []
        failed_assignments = []
        
        for employee_id in assignment.employee_ids:
            try:
                # Check if employee exists
                employee = db.query(Employees).filter(Employees.id == employee_id).first()
                if not employee:
                    failed_assignments.append({
                        "employee_id": employee_id,
                        "reason": "Employee not found"
                    })
                    continue
                
                # Validate same department
                if employee.department != manager.department:
                    failed_assignments.append({
                        "employee_id": employee_id,
                        "employee_name": employee.name,
                        "reason": f"Different department: employee in {employee.department}, manager in {manager.department}"
                    })
                    continue
                
                # Check if employee is already assigned to any manager (one employee can only have one manager)
                existing_assignment = db.query(EmployeeAssignments).filter(
                    and_(
                        EmployeeAssignments.employee_id == employee_id,
                        EmployeeAssignments.status == "active"
                    )
                ).first()
                
                if existing_assignment:
                    existing_manager = db.query(Employees).filter(Employees.id == existing_assignment.manager_id).first()
                    failed_assignments.append({
                        "employee_id": employee_id,
                        "employee_name": employee.name,
                        "reason": f"Already assigned to manager: {existing_manager.name if existing_manager else 'Unknown'}"
                    })
                    continue
                
                # Create new assignment
                new_assignment = EmployeeAssignments(
                    manager_id=assignment.manager_id,
                    employee_id=employee_id,
                    assignment_date=date.today(),
                    status="active"
                )
                
                db.add(new_assignment)
                
                successful_assignments.append({
                    "employee_id": employee.id,
                    "employee_name": employee.name,
                    "employee_emp_id": employee.employee_id,
                    "department": employee.department
                })
                
            except Exception as e:
                failed_assignments.append({
                    "employee_id": employee_id,
                    "reason": f"Error: {str(e)}"
                })
        
        db.commit()
        
        return {
            "status": 200,
            "message": f"Successfully assigned {len(successful_assignments)} employees to manager",
            "data": {
                "manager": {
                    "id": manager.id,
                    "name": manager.name,
                    "employee_id": manager.employee_id,
                    "department": manager.department
                },
                "successful_assignments": successful_assignments,
                "failed_assignments": failed_assignments,
                "total_successful": len(successful_assignments),
                "total_failed": len(failed_assignments),
                "assignment_date": date.today().isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error assigning employees to manager: {str(e)}")

@router.get("/assignments/employee-manager-pairs", response_model=dict)
async def get_employee_manager_assignments(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    department: Optional[str] = Query(None, description="Filter by department"),
    manager_id: Optional[int] = Query(None, description="Filter by manager ID"),
    status: Optional[str] = Query("active", description="Filter by assignment status"),
    db: Session = Depends(get_db)
):
    """Get all employee-manager assignments"""
    try:
        # Use the simpler approach with separate queries
        assignment_query = db.query(EmployeeAssignments)
        
        if status:
            assignment_query = assignment_query.filter(EmployeeAssignments.status == status)
        if manager_id:
            assignment_query = assignment_query.filter(EmployeeAssignments.manager_id == manager_id)
        
        assignment_query = assignment_query.order_by(desc(EmployeeAssignments.created_at))
        
        # Get total count
        total_count = assignment_query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        assignments = assignment_query.offset(offset).limit(limit).all()
        
        # Format response
        assignments_data = []
        
        for assignment in assignments:
            employee = db.query(Employees).filter(Employees.id == assignment.employee_id).first()
            manager = db.query(Employees).filter(Employees.id == assignment.manager_id).first()
            
            # Apply department filter if specified
            if department and (not employee or employee.department != department):
                continue
                
            assignment_data = {
                "assignment_id": assignment.assignment_id,
                "assignment_date": assignment.assignment_date.isoformat() if assignment.assignment_date else None,
                "assignment_status": assignment.status,
                "employee": {
                    "id": employee.id if employee else None,
                    "name": employee.name if employee else None,
                    "email": employee.email if employee else None,
                    "employee_id": employee.employee_id if employee else None,
                    "department": employee.department if employee else None,
                    "designation": employee.designation if employee else None
                } if employee else None,
                "manager": {
                    "id": manager.id if manager else None,
                    "name": manager.name if manager else None,
                    "email": manager.email if manager else None,
                    "employee_id": manager.employee_id if manager else None,
                    "department": manager.department if manager else None
                } if manager else None
            }
            assignments_data.append(assignment_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "status": 200,
            "message": "Employee-Manager assignments fetched successfully",
            "data": {
                "assignments": assignments_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employee assignments: {str(e)}")

@router.put("/assignments/{assignment_id}/unassign", response_model=dict)
async def unassign_employee_from_manager(
    assignment_id: int,
    db: Session = Depends(get_db)
):
    """Unassign an employee from their manager"""
    try:
        assignment = db.query(EmployeeAssignments).filter(EmployeeAssignments.assignment_id == assignment_id).first()
        
        if not assignment:
            raise HTTPException(status_code=404, detail="Assignment not found")
        
        # if assignment.status == "inactive":
        #     raise HTTPException(status_code=400, detail="Assignment is already inactive")
        
        # # Get employee and manager details for response
        # employee = db.query(Employees).filter(Employees.id == assignment.employee_id).first()
        # manager = db.query(Employees).filter(Employees.id == assignment.manager_id).first()
        
        # # Update assignment status to inactive
        # assignment.status = "inactive"
        # assignment.updated_at = datetime.now()
        
        # db.commit()
        # db.refresh(assignment)

        db.delete(assignment)
        db.commit()
        
        return {
            "status": 200,
            "message": "Employee unassigned from manager successfully",
            "data": {
                "assignment_id": assignment.assignment_id,
                # "employee": {
                #     "id": employee.id if employee else None,
                #     "name": employee.name if employee else None,
                #     "employee_id": employee.employee_id if employee else None
                # },
                # "manager": {
                #     "id": manager.id if manager else None,
                #     "name": manager.name if manager else None,
                #     "employee_id": manager.employee_id if manager else None
                # },
                # "unassigned_at": datetime.now().isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error unassigning employee: {str(e)}")

@router.get("/managers", response_model=dict)
async def get_managers(
    department: Optional[str] = Query(None, description="Filter managers by department"),
    db: Session = Depends(get_db)
):
    """Get all employees with manager_role=True"""
    try:
        query = db.query(Employees).filter(Employees.manager_role == True)
        
        if department:
            query = query.filter(Employees.department == department)
        
        managers = query.order_by(Employees.name).all()
        
        managers_data = []
        for manager in managers:
            assigned_count = db.query(EmployeeAssignments).filter(
                and_(
                    EmployeeAssignments.manager_id == manager.id,
                    EmployeeAssignments.status == "active"
                )
            ).count()
            
            manager_data = {
                "id": manager.id,
                "name": manager.name,
                "email": manager.email,
                "employee_id": manager.employee_id,
                "department": manager.department,
                "designation": manager.designation,
                "assigned_employees_count": assigned_count
            }
            managers_data.append(manager_data)
        
        return {
            "status": 200,
            "message": "Managers fetched successfully",
            "data": {
                "managers": managers_data,
                "total": len(managers_data)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching managers: {str(e)}")

@router.get("/departments", response_model=dict)
async def get_departments(db: Session = Depends(get_db)):
    """Get all unique departments"""
    try:
        # departments = db.query(Employees.department).filter(Employees.department.isnot(None)).distinct().all()
        # department_list = [dept[0] for dept in departments if dept[0]]

        department_list = ["marketing","support","software","graphic", "tele_calling","nutritionists"]
        
        return {
            "status": 200,
            "message": "Departments fetched successfully",
            "data": {
                "departments": sorted(department_list)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching departments: {str(e)}")

# Role Management Endpoints

@router.post("/roles", response_model=dict)
async def create_role(
    role: RoleCreate,
    db: Session = Depends(get_db)
):
    """Create a new employee role"""
    try:
        # Check if role with same name and department already exists
        existing_role = db.query(EmployeeRoles).filter(
            and_(
                EmployeeRoles.name == role.name,
                EmployeeRoles.department == role.department,
                EmployeeRoles.status == "active"
            )
        ).first()
        
        if existing_role:
            raise HTTPException(
                status_code=400, 
                detail=f"Role '{role.name}' already exists in {role.department} department"
            )
        
        # Create new role
        new_role = EmployeeRoles(
            name=role.name,
            department=role.department,
            status="active",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        db.add(new_role)
        db.commit()
        db.refresh(new_role)
        
        return {
            "status": 201,
            "message": "Role created successfully",
            "data": {
                "role_id": new_role.role_id,
                "name": new_role.name,
                "department": new_role.department,
                "status": new_role.status
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating role: {str(e)}")

@router.get("/roles", response_model=dict)
async def get_roles(
    department: Optional[str] = Query(None, description="Filter by department"),
    status: Optional[str] = Query("active", description="Filter by status"),
    db: Session = Depends(get_db)
):
    """Get all employee roles"""
    try:
        query = db.query(EmployeeRoles)
        
        if department:
            query = query.filter(EmployeeRoles.department == department)
        
        if status:
            query = query.filter(EmployeeRoles.status == status)
        
        roles = query.order_by(EmployeeRoles.department, EmployeeRoles.name).all()
        
        roles_data = []
        for role in roles:
            role_data = {
                "role_id": role.role_id,
                "name": role.name,
                "department": role.department,
                "status": role.status,
                "created_at": role.created_at.isoformat() if role.created_at else None,
                "updated_at": role.updated_at.isoformat() if role.updated_at else None
            }
            roles_data.append(role_data)
        
        return {
            "status": 200,
            "message": "Roles fetched successfully",
            "data": {
                "roles": roles_data,
                "total": len(roles_data)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching roles: {str(e)}")

@router.get("/roles/{role_id}", response_model=dict)
async def get_role_by_id(
    role_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific role by ID"""
    try:
        role = db.query(EmployeeRoles).filter(EmployeeRoles.role_id == role_id).first()
        
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        
        role_data = {
            "role_id": role.role_id,
            "name": role.name,
            "department": role.department,
            "status": role.status,
            "created_at": role.created_at.isoformat() if role.created_at else None,
            "updated_at": role.updated_at.isoformat() if role.updated_at else None
        }
        
        return {
            "status": 200,
            "message": "Role details fetched successfully",
            "data": role_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching role: {str(e)}")

@router.put("/roles/{role_id}", response_model=dict)
async def update_role(
    role_id: int,
    role_update: RoleUpdate,
    db: Session = Depends(get_db)
):
    """Update an employee role"""
    try:
        role = db.query(EmployeeRoles).filter(EmployeeRoles.role_id == role_id).first()
        
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        
        # Check for unique constraint if name or department is being updated
        if (role_update.name and role_update.name != role.name) or \
           (role_update.department and role_update.department != role.department):
            
            new_name = role_update.name or role.name
            new_department = role_update.department or role.department
            
            existing_role = db.query(EmployeeRoles).filter(
                and_(
                    EmployeeRoles.name == new_name,
                    EmployeeRoles.department == new_department,
                    EmployeeRoles.role_id != role_id,
                    EmployeeRoles.status == "active"
                )
            ).first()
            
            if existing_role:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Role '{new_name}' already exists in {new_department} department"
                )
        
        # Update fields
        update_data = role_update.dict(exclude_unset=True)
        update_data['updated_at'] = datetime.now()
        
        for field, value in update_data.items():
            setattr(role, field, value)
        
        db.commit()
        db.refresh(role)
        
        return {
            "status": 200,
            "message": "Role updated successfully",
            "data": {
                "role_id": role.role_id,
                "name": role.name,
                "department": role.department,
                "status": role.status
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating role: {str(e)}")

@router.delete("/roles/{role_id}", response_model=dict)
async def delete_role(
    role_id: int,
    db: Session = Depends(get_db)
):
    """Delete/deactivate an employee role"""
    try:
        role = db.query(EmployeeRoles).filter(EmployeeRoles.role_id == role_id).first()
        
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        
        # Check if role is being used by any active employees
        employees_using_role = db.query(Employees).filter(
            and_(
                Employees.designation == role.name,
                Employees.status == "active"
            )
        ).count()
        
        if employees_using_role > 0:
            # Soft delete - mark as inactive instead of deleting
            role.status = "inactive"
            role.updated_at = datetime.now()
            db.commit()
            
            return {
                "status": 200,
                "message": f"Role deactivated successfully. {employees_using_role} employees are still using this role.",
                "data": {
                    "role_id": role.role_id,
                    "name": role.name,
                    "status": "inactive",
                    "employees_count": employees_using_role
                }
            }
        else:
            # Hard delete if no employees are using this role
            db.delete(role)
            db.commit()
            
            return {
                "status": 200,
                "message": "Role deleted successfully",
                "data": {
                    "role_id": role_id,
                    "name": role.name
                }
            }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting role: {str(e)}")

@router.get("/roles/by-department/{department}", response_model=dict)
async def get_roles_by_department(
    department: str,
    status: Optional[str] = Query("active", description="Filter by status"),
    db: Session = Depends(get_db)
):
    """Get all roles for a specific department"""
    try:
        query = db.query(EmployeeRoles).filter(EmployeeRoles.department == department)
        
        if status:
            query = query.filter(EmployeeRoles.status == status)
        
        roles = query.order_by(EmployeeRoles.name).all()
        
        roles_data = []
        for role in roles:
            # Get count of employees using this role
            employee_count = db.query(Employees).filter(
                and_(
                    Employees.designation == role.name,
                    Employees.department == department,
                    Employees.status == "active"
                )
            ).count()
            
            role_data = {
                "role_id": role.role_id,
                "name": role.name,
                "department": role.department,
                "status": role.status,
                "employee_count": employee_count,
                "created_at": role.created_at.isoformat() if role.created_at else None
            }
            roles_data.append(role_data)
        
        return {
            "status": 200,
            "message": f"Roles for {department} department fetched successfully",
            "data": {
                "department": department,
                "roles": roles_data,
                "total": len(roles_data)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching roles for department: {str(e)}")

@router.post("/roles/bulk-create", response_model=dict)
async def bulk_create_roles(
    roles: List[RoleCreate],
    db: Session = Depends(get_db)
):
    """Bulk create multiple roles"""
    try:
        created_roles = []
        failed_roles = []
        
        for role_data in roles:
            try:
                # Check if role already exists
                existing_role = db.query(EmployeeRoles).filter(
                    and_(
                        EmployeeRoles.name == role_data.name,
                        EmployeeRoles.department == role_data.department,
                        EmployeeRoles.status == "active"
                    )
                ).first()
                
                if existing_role:
                    failed_roles.append({
                        "name": role_data.name,
                        "department": role_data.department,
                        "reason": "Role already exists"
                    })
                    continue
                
                # Create new role
                new_role = EmployeeRoles(
                    name=role_data.name,
                    department=role_data.department,
                    status="active",
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                db.add(new_role)
                created_roles.append({
                    "name": new_role.name,
                    "department": new_role.department
                })
                
            except Exception as e:
                failed_roles.append({
                    "name": role_data.name,
                    "department": role_data.department,
                    "reason": str(e)
                })
        
        db.commit()
        
        return {
            "status": 200,
            "message": f"Bulk role creation completed. {len(created_roles)} created, {len(failed_roles)} failed.",
            "data": {
                "created_roles": created_roles,
                "failed_roles": failed_roles,
                "total_created": len(created_roles),
                "total_failed": len(failed_roles)
            }
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error in bulk role creation: {str(e)}")
    



@router.get("/{employee_id}", response_model=dict)
async def get_employee_by_id(
    employee_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific employee by ID"""
    try:
        employee = db.query(Employees).filter(Employees.id == employee_id).first()
        
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        emp_data = {
            "id": employee.id,
            "name": employee.name,
            "email": employee.email,
            "contact": employee.contact,
            "dob": employee.dob.isoformat() if employee.dob else None,
            "age": employee.age,
            "gender": employee.gender,
            "department": employee.department,
            "designation": employee.designation,
            "joined_date": employee.joined_date.isoformat() if employee.joined_date else None,
            "status": employee.status,
            "employee_id": employee.employee_id,
            "access": employee.access,
            "profile": employee.profile,
            "uuid": employee.uuid,
            "manager_role":employee.manager_role,
            "created_at": employee.created_at.isoformat() if employee.created_at else None,
            "updated_at": employee.updated_at.isoformat() if employee.updated_at else None
        }
        
        # Add manager details if employee has an assignment
        assignment = db.query(EmployeeAssignments).filter(
            and_(
                EmployeeAssignments.employee_id == employee.id,
                EmployeeAssignments.status == "active"
            )
        ).first()
        
        if assignment:
            manager = db.query(Employees).filter(Employees.id == assignment.manager_id).first()
            if manager:
                emp_data["assigned_manager"] = {
                    "manager_id": manager.id,
                    "manager_name": manager.name,
                    "manager_email": manager.email,
                    "manager_emp_id": manager.employee_id,
                    "manager_department": manager.department,
                    "assignment_date": assignment.assignment_date.isoformat() if assignment.assignment_date else None
                }
            else:
                emp_data["assigned_manager"] = None
        else:
            emp_data["assigned_manager"] = None
        
        # Also add legacy BDE manager details if applicable (for backward compatibility)
        if employee.designation and employee.designation.upper() == 'BDE':
            executive = db.query(Executives).filter(Executives.employee_id == employee.id).first()
            if executive and executive.manager_id:
                legacy_manager = db.query(Managers).filter(Managers.id == executive.manager_id).first()
                if legacy_manager:
                    emp_data["legacy_assigned_manager"] = {
                        "manager_id": legacy_manager.id,
                        "manager_name": legacy_manager.name,
                        "manager_email": legacy_manager.email,
                        "manager_emp_id": legacy_manager.emp_id
                    }
                else:
                    emp_data["legacy_assigned_manager"] = None
            else:
                emp_data["legacy_assigned_manager"] = None
        
        return {
            "status": 200,
            "message": "Employee details fetched successfully",
            "data": emp_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employee: {str(e)}")

@router.put("/{employee_id}", response_model=dict)
async def update_employee(
    employee_id: int,
    employee_update: EmployeeUpdate,
    db: Session = Depends(get_db)
):
    """Update an employee"""
    try:
        employee = db.query(Employees).filter(Employees.id == employee_id).first()
        
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Check for unique constraints if email or contact is being updated
        if employee_update.email and employee_update.email != employee.email:
            existing_email = db.query(Employees).filter(
                and_(Employees.email == employee_update.email, Employees.id != employee_id)
            ).first()
            if existing_email:
                raise HTTPException(status_code=400, detail="Email already exists")
        
        if employee_update.contact and employee_update.contact != employee.contact:
            existing_contact = db.query(Employees).filter(
                and_(Employees.contact == employee_update.contact, Employees.id != employee_id)
            ).first()
            if existing_contact:
                raise HTTPException(status_code=400, detail="Contact number already exists")
        
        # Update fields
        update_data = employee_update.dict(exclude_unset=True)
        
        # Hash password if provided
        if 'password' in update_data:
            update_data['password'] = hash_password(update_data['password'])
        
        # Update timestamp
        update_data['updated_at'] = datetime.now()
        
        # Store old designation for comparison
        old_designation = employee.designation
        
        for field, value in update_data.items():
            setattr(employee, field, value)
        
        db.commit()
        db.refresh(employee)
        
        # Handle designation changes for manager/executive tables
        new_designation = employee.designation
        
        # If designation changed, handle manager/executive table updates
        if old_designation != new_designation:
            # Remove from old designation table if applicable
            if old_designation and old_designation.upper() == 'BDM':
                old_manager = db.query(Managers).filter(Managers.employee_id == employee_id).first()
                if old_manager:
                    db.delete(old_manager)
                    db.commit()
            elif old_designation and old_designation.upper() == 'BDE':
                old_executive = db.query(Executives).filter(Executives.employee_id == employee_id).first()
                if old_executive:
                    db.delete(old_executive)
                    db.commit()
            
            # Add to new designation table if applicable
            if new_designation and new_designation.upper() == 'BDM':
                new_manager = Managers(
                    employee_id=employee.id,
                    name=employee.name,
                    email=employee.email,
                    contact=employee.contact,
                    password=employee.password,
                    dob=employee.dob,
                    age=employee.age,
                    gender=employee.gender,
                    role=new_designation,
                    joined_date=employee.joined_date,
                    emp_id=employee.employee_id,
                    uuid=str(uuid.uuid4()),
                    profile=employee.profile,
                    status='active',
                    access=True,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                db.add(new_manager)
                db.commit()
            elif new_designation and new_designation.upper() == 'BDE':
                new_executive = Executives(
                    employee_id=employee.id,
                    name=employee.name,
                    email=employee.email,
                    contact=employee.contact,
                    password=employee.password,
                    dob=employee.dob,
                    age=employee.age,
                    gender=employee.gender,
                    role=new_designation,
                    joined_date=employee.joined_date,
                    emp_id=employee.employee_id,
                    uuid=str(uuid.uuid4()),
                    profile=employee.profile,
                    status='active',
                    access=True,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                db.add(new_executive)
                db.commit()
        
        # If designation didn't change but other fields did, update manager/executive table
        elif new_designation and new_designation.upper() == 'BDM':
            manager = db.query(Managers).filter(Managers.employee_id == employee_id).first()
            if manager:
                manager.name = employee.name
                manager.email = employee.email
                manager.contact = employee.contact
                if 'password' in update_data:
                    manager.password = employee.password
                manager.dob = employee.dob
                manager.age = employee.age
                manager.gender = employee.gender
                manager.joined_date = employee.joined_date
                manager.profile = employee.profile
                if 'status' in update_data:
                    manager.status = employee.status
                if 'access' in update_data:
                    manager.access = employee.access
                manager.updated_at = datetime.now()
                db.commit()
        
        elif new_designation and new_designation.upper() == 'BDE':
            executive = db.query(Executives).filter(Executives.employee_id == employee_id).first()
            if executive:
                executive.name = employee.name
                executive.email = employee.email
                executive.contact = employee.contact
                if 'password' in update_data:
                    executive.password = employee.password
                executive.dob = employee.dob
                executive.age = employee.age
                executive.gender = employee.gender
                executive.joined_date = employee.joined_date
                executive.profile = employee.profile
                if 'status' in update_data:
                    executive.status = employee.status
                if 'access' in update_data:
                    executive.access = employee.access
                executive.updated_at = datetime.now()
                db.commit()
        
        return {
            "status": 200,
            "message": "Employee updated successfully",
            "data": {
                "id": employee.id,
                "name": employee.name,
                "email": employee.email,
                "employee_id": employee.employee_id
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating employee: {str(e)}")

@router.delete("/{employee_id}", response_model=dict)
async def delete_employee(
    employee_id: int,
    db: Session = Depends(get_db)
):
    """Delete an employee"""
    try:
        employee = db.query(Employees).filter(Employees.id == employee_id).first()
        
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Check if employee is referenced in managers or executives tables
        manager_ref = db.query(Managers).filter(Managers.employee_id == employee_id).first()
        executive_ref = db.query(Executives).filter(Executives.employee_id == employee_id).first()
        
        if manager_ref or executive_ref:
            raise HTTPException(
                status_code=400, 
                detail="Cannot delete employee. Employee is referenced in managers or executives table."
            )
        
        db.delete(employee)
        db.commit()
        
        return {
            "status": 200,
            "message": "Employee deleted successfully",
            "data": {
                "id": employee_id,
                "name": employee.name
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting employee: {str(e)}")
