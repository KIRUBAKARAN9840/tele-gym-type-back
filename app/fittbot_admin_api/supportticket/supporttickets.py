# Enhanced Support API with Employee Management - Complete Integration
from fastapi import APIRouter, Depends, HTTPException, Query, status, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, and_, desc, asc, case, text, union_all, literal
from typing import Optional, List, Union
from app.models.fittbot_models import ClientToken, OwnerToken, Client, Gym
from app.models.adminmodels import Employees, TicketAssignment, EmployeeAssignments
from app.models.database import get_db
from datetime import datetime, date
import math

router = APIRouter(prefix="/api/admin/support", tags=["AdminSupport"])

# Import your auth functions
from ..auth.authentication import (
    get_current_employee_for_support,
    require_manager_role_for_support,
    require_support_access,
    check_ticket_access_permission
)

# Pydantic models
class EmployeeInfo(BaseModel):
    id: int
    name: str
    email: str
    department: str
    designation: str
    avatar: Optional[str] = None

class TicketAssignmentInfo(BaseModel):
    assignment_id: Optional[int] = None
    employee: Optional[EmployeeInfo] = None
    assigned_by: Optional[EmployeeInfo] = None
    assigned_date: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None

class TicketResponse(BaseModel):
    id: int
    ticket_id: str
    source: str
    name: str
    email: str
    subject: Optional[str] = None
    issue_type: Optional[str] = None
    issue: Optional[str] = None
    status: str
    comments: Optional[str] = None
    created_at: str
    assignment: Optional[TicketAssignmentInfo] = None
    
    class Config:
        from_attributes = True

class PaginatedTicketsResponse(BaseModel):
    tickets: List[TicketResponse]
    total: int
    page: int
    limit: int
    totalPages: int
    hasNext: bool
    hasPrev: bool

class AssignTicketRequest(BaseModel):
    employee_id: int
    notes: Optional[str] = None

class StatusUpdateRequest(BaseModel):
    status: str
    source: str

class CommentRequest(BaseModel):
    comment: str
    source: str

# Helper functions
def map_status_from_db(followed_up: bool, resolved: bool) -> str:
    """Map database status fields to UI status"""
    if resolved:
        return "resolved"
    elif followed_up:
        return "working"
    else:
        return "yet to start"

def map_status_to_db(status: str) -> tuple:
    """Map UI status to database fields"""
    if status == "resolved":
        return (True, True) 
    elif status == "working":
        return (True, False)  
    else:  
        return (False, False)

def get_employee_avatar(name: str) -> str:
    """Generate avatar initials from name"""
    if not name:
        return "NA"
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}{parts[1][0]}".upper()
    return name[:2].upper()

def get_unified_tickets_query_with_assignments(db: Session):
    """Create a unified query for both ClientToken and OwnerToken tables with assignments"""
    try:
        # Query for Fittbot tickets (ClientToken) with assignments
        client_tickets = db.query(
            ClientToken.id.label('id'),
            ClientToken.token.label('ticket_id'),
            literal("Fittbot").label('source'),
            Client.name.label('name'),
            ClientToken.email.label('email'),
            ClientToken.subject.label("subject"),
            ClientToken.issue.label('issue'),
            ClientToken.followed_up.label('followed_up'),
            ClientToken.resolved.label('resolved'),
            ClientToken.comments.label('comments'),
            ClientToken.created_at.label('created_at'),
            TicketAssignment.assignment_id.label('assignment_id'),
            TicketAssignment.employee_id.label('assigned_employee_id'),
            Employees.name.label('assigned_employee_name'),
            Employees.email.label('assigned_employee_email'),
            Employees.department.label('assigned_employee_department'),
            Employees.designation.label('assigned_employee_designation'),
            TicketAssignment.assigned_by.label('assigned_by_id'),
            TicketAssignment.assigned_date.label('assigned_date'),
            TicketAssignment.status.label('assignment_status'),
            TicketAssignment.notes.label('assignment_notes')
        ).outerjoin(
            Client, ClientToken.client_id == Client.client_id
        ).outerjoin(
            TicketAssignment, 
            and_(
                TicketAssignment.ticket_id == ClientToken.id,
                TicketAssignment.ticket_source == "Fittbot",
                TicketAssignment.status.in_(["active", "completed"]),
            )
        ).outerjoin(
            Employees, TicketAssignment.employee_id == Employees.id
        )
        
        # Query for Fittbot Business tickets (OwnerToken) with assignments
        owner_tickets = db.query(
            OwnerToken.id.label('id'),
            OwnerToken.token.label('ticket_id'),
            literal("Fittbot Business").label('source'),
            Gym.name.label('name'),
            OwnerToken.email.label('email'),
            OwnerToken.subject.label('subject'),
            OwnerToken.issue.label('issue'),
            OwnerToken.followed_up.label('followed_up'),
            OwnerToken.resolved.label('resolved'),
            OwnerToken.comments.label('comments'),
            OwnerToken.created_at.label('created_at'),
            TicketAssignment.assignment_id.label('assignment_id'),
            TicketAssignment.employee_id.label('assigned_employee_id'),
            Employees.name.label('assigned_employee_name'),
            Employees.email.label('assigned_employee_email'),
            Employees.department.label('assigned_employee_department'),
            Employees.designation.label('assigned_employee_designation'),
            TicketAssignment.assigned_by.label('assigned_by_id'),
            TicketAssignment.assigned_date.label('assigned_date'),
            TicketAssignment.status.label('assignment_status'),
            TicketAssignment.notes.label('assignment_notes')
        ).outerjoin(
            Gym, OwnerToken.gym_id == Gym.gym_id
        ).outerjoin(
            TicketAssignment, 
            and_(
                TicketAssignment.ticket_id == OwnerToken.id,
                TicketAssignment.ticket_source == "Fittbot Business",
                TicketAssignment.status.in_(["active", "completed"])
            )
        ).outerjoin(
            Employees, TicketAssignment.employee_id == Employees.id
        )
        
        # Union both queries
        unified_query = union_all(client_tickets, owner_tickets)
        return unified_query
    except Exception as e:
        
        raise HTTPException(status_code=500, detail=f'An error occurred: {str(e)}')

@router.get("/tickets")
async def get_support_tickets(
    request:Request,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by ticket ID, name, or email"),
    source: Optional[str] = Query(None, description="Filter by source"),
    status: Optional[str] = Query(None, description="Filter by status"),
    issue_type: Optional[str] = Query(None, description="Filter by issue type"),
    assigned_to: Optional[int] = Query(None, description="Filter by assigned employee"),
    assignment_status: Optional[str] = Query(None, description="Filter by assignment status"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
    db: Session = Depends(get_db)
):
    try:
        # Get current user info (adapt this to your auth system)
        current_user = await get_current_employee_for_support(request,db)
        is_manager = current_user.get("manager_role", True)  # Assuming manager access for now
        employee_id = current_user.get("id")
        
        # Get unified query as subquery
        unified_subquery = get_unified_tickets_query_with_assignments(db).subquery()
        
        # Create main query from subquery
        query = db.query(unified_subquery)
        
        # Apply permission-based filtering for non-managers
        if not is_manager:
            query = query.filter(unified_subquery.c.assigned_employee_id == employee_id)
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(unified_subquery.c.ticket_id).like(search_term),
                    func.lower(unified_subquery.c.name).like(search_term),
                    func.lower(unified_subquery.c.email).like(search_term)
                )
            )
        
        # Apply source filter
        if source and source != "all":
            query = query.filter(unified_subquery.c.source == source)
            
        # Apply issue type filter (based on subject)
        if issue_type and issue_type != "all":
            query = query.filter(func.lower(unified_subquery.c.subject).like(f"%{issue_type.lower()}%"))
        
        # Apply status filter
        if status and status != "all":
            if status == "resolved":
                query = query.filter(unified_subquery.c.resolved == True)
            elif status == "working":
                query = query.filter(
                    and_(
                        unified_subquery.c.followed_up == True,
                        unified_subquery.c.resolved == False
                    )
                )
            elif status == "yet to start":
                query = query.filter(
                    and_(
                        unified_subquery.c.followed_up == False,
                        unified_subquery.c.resolved == False
                    )
                )
        
        # Apply assignment filters
        if assigned_to:
            query = query.filter(unified_subquery.c.assigned_employee_id == assigned_to)
        
        if assignment_status:
            if assignment_status == "unassigned":
                query = query.filter(unified_subquery.c.assignment_id.is_(None))
            else:
                query = query.filter(unified_subquery.c.assignment_status == assignment_status)
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(asc(unified_subquery.c.created_at))
        else:
            query = query.order_by(desc(unified_subquery.c.created_at))
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        results = query.offset(offset).limit(limit).all()
        
        # Convert to response format
        tickets = []
        for result in results:
            # Build assignment info
            assignment_info = None
            if result.assignment_id:
                assignment_info = TicketAssignmentInfo(
                    assignment_id=result.assignment_id,
                    employee=EmployeeInfo(
                        id=result.assigned_employee_id,
                        name=result.assigned_employee_name,
                        email=result.assigned_employee_email,
                        department=result.assigned_employee_department or "",
                        designation=result.assigned_employee_designation or "",
                        avatar=get_employee_avatar(result.assigned_employee_name)
                    ) if result.assigned_employee_id else None,
                    assigned_date=result.assigned_date.isoformat() if result.assigned_date else None,
                    status=result.assignment_status,
                    notes=result.assignment_notes
                )
            
            ticket_data = TicketResponse(
                id=result.id,
                ticket_id=result.ticket_id or f"ticket-{result.id}",
                source=result.source,
                name=result.name or "N/A",
                email=result.email or "N/A",
                subject=result.subject,
                issue_type=result.subject,
                issue=result.issue,
                status=map_status_from_db(result.followed_up, result.resolved),
                comments=result.comments,
                created_at=result.created_at.isoformat() if result.created_at else None,
                assignment=assignment_info
            )
            tickets.append(ticket_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "success": True,
            "data": {
                "tickets": tickets,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Support tickets fetched successfully"
        }
        
    except Exception as e:
        
        raise HTTPException(status_code=500, detail=f"Error fetching support tickets: {str(e)}")



@router.get("/summary")
async def get_support_summary(
    request: Request,
    db: Session = Depends(get_db),
    current_employee: dict = Depends(require_support_access)
):
    """Get support tickets summary/statistics"""
    try:
        today = date.today()
        employee_id = current_employee.get("id")
        is_manager = current_employee.get("manager_role", False)
        
        if is_manager:
            # Manager sees all tickets
            # Today's tickets counts
            today_client_tickets = db.query(ClientToken).filter(
                func.date(ClientToken.created_at) == today
            ).count()
            
            today_owner_tickets = db.query(OwnerToken).filter(
                func.date(OwnerToken.created_at) == today
            ).count()
            
            # Total tickets counts
            total_client_tickets = db.query(ClientToken).count()
            total_owner_tickets = db.query(OwnerToken).count()
            
            # Unresolved tickets counts (not resolved)
            unresolved_client_tickets = db.query(ClientToken).filter(
                ClientToken.resolved == False
            ).count()
            
            unresolved_owner_tickets = db.query(OwnerToken).filter(
                OwnerToken.resolved == False
            ).count()
            
            # Unassigned tickets (exclude tickets with any assignment - active or completed)
            assigned_client_ids = db.query(TicketAssignment.ticket_id).filter(
                and_(
                    TicketAssignment.ticket_source == "Fittbot",
                    TicketAssignment.status.in_(["active", "completed"])
                )
            ).subquery()
            
            assigned_owner_ids = db.query(TicketAssignment.ticket_id).filter(
                and_(
                    TicketAssignment.ticket_source == "Fittbot Business",
                    TicketAssignment.status.in_(["active", "completed"])
                )
            ).subquery()
            
            unassigned_client = db.query(ClientToken).filter(
                ~ClientToken.id.in_(assigned_client_ids)
            ).count()
            
            unassigned_owner = db.query(OwnerToken).filter(
                ~OwnerToken.id.in_(assigned_owner_ids)
            ).count()
            
        else:
            # Non-manager sees only assigned tickets (both active and completed)
            assigned_tickets = db.query(TicketAssignment).filter(
                and_(
                    TicketAssignment.employee_id == employee_id,
                    TicketAssignment.status.in_(["active", "completed"])
                )
            ).all()
            
            client_ticket_ids = [t.ticket_id for t in assigned_tickets if t.ticket_source == "Fittbot"]
            owner_ticket_ids = [t.ticket_id for t in assigned_tickets if t.ticket_source == "Fittbot Business"]
            
            today_client_tickets = db.query(ClientToken).filter(
                and_(
                    ClientToken.id.in_(client_ticket_ids),
                    func.date(ClientToken.created_at) == today
                )
            ).count() if client_ticket_ids else 0
            
            today_owner_tickets = db.query(OwnerToken).filter(
                and_(
                    OwnerToken.id.in_(owner_ticket_ids),
                    func.date(OwnerToken.created_at) == today
                )
            ).count() if owner_ticket_ids else 0
            
            total_client_tickets = len(client_ticket_ids)
            total_owner_tickets = len(owner_ticket_ids)
            
            unresolved_client_tickets = db.query(ClientToken).filter(
                and_(
                    ClientToken.id.in_(client_ticket_ids),
                    ClientToken.resolved == False
                )
            ).count() if client_ticket_ids else 0
            
            unresolved_owner_tickets = db.query(OwnerToken).filter(
                and_(
                    OwnerToken.id.in_(owner_ticket_ids),
                    OwnerToken.resolved == False
                )
            ).count() if owner_ticket_ids else 0
            
            unassigned_client = 0
            unassigned_owner = 0
        
        return {
            "success": True,
            "data": {
                "todayTickets": {
                    "fittbot": today_client_tickets,
                    "fittbotBusiness": today_owner_tickets,
                    "total": today_client_tickets + today_owner_tickets
                },
                "totalTickets": {
                    "fittbot": total_client_tickets,
                    "fittbotBusiness": total_owner_tickets,
                    "total": total_client_tickets + total_owner_tickets
                },
                "unresolvedTickets": {
                    "fittbot": unresolved_client_tickets,
                    "fittbotBusiness": unresolved_owner_tickets,
                    "total": unresolved_client_tickets + unresolved_owner_tickets
                },
                "unassignedTickets": {
                    "fittbot": unassigned_client,
                    "fittbotBusiness": unassigned_owner,
                    "total": unassigned_client + unassigned_owner
                }
            },
            "message": "Support summary fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching support summary: {str(e)}")

@router.put("/tickets/{ticket_id}")
async def update_ticket_status(
    ticket_id: int,
    request: StatusUpdateRequest,
    http_request: Request,
    db: Session = Depends(get_db),
    current_employee: dict = Depends(require_support_access)
):
    """Update ticket status"""
    try:
        is_manager = current_employee.get("manager_role", False)
        employee_id = current_employee.get("id")
        
        # Check permissions for non-managers
        if not is_manager:
            assignment = db.query(TicketAssignment).filter(
                and_(
                    TicketAssignment.ticket_id == ticket_id,
                    TicketAssignment.ticket_source == request.source,
                    TicketAssignment.employee_id == employee_id,
                    TicketAssignment.status == "active"
                )
            ).first()
            
            if not assignment:
                raise HTTPException(status_code=403, detail="You can only update tickets assigned to you")
        
        followed_up, resolved = map_status_to_db(request.status)
        
        if request.source == "Fittbot":
            ticket = db.query(ClientToken).filter(ClientToken.id == ticket_id).first()
        elif request.source == "Fittbot Business":
            ticket = db.query(OwnerToken).filter(OwnerToken.id == ticket_id).first()
        else:
            raise HTTPException(status_code=400, detail="Invalid source")
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        
        ticket.followed_up = followed_up
        ticket.resolved = resolved
        ticket.updated_at = datetime.now()
        
        # If ticket is resolved, mark assignment as completed
        if resolved:
            assignment = db.query(TicketAssignment).filter(
                and_(
                    TicketAssignment.ticket_id == ticket_id,
                    TicketAssignment.ticket_source == request.source,
                    TicketAssignment.status == "active"
                )
            ).first()
            
            if assignment:
                assignment.status = "completed"
                assignment.updated_at = datetime.now()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Ticket status updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating ticket status: {str(e)}")

@router.get("/tickets/{ticket_id}")
async def get_ticket_by_id(
    ticket_id: int,
    source: str = Query(..., description="Ticket source (Fittbot or Fittbot Business)"),
    http_request: Request = None,
    db: Session = Depends(get_db),
    current_employee: dict = Depends(require_support_access)
):
    """Get specific ticket details by ID"""
    try:
        employee_id = current_employee.get("id")
        is_manager = current_employee.get("manager_role", False)
        
        if source == "Fittbot":
            result = db.query(
                ClientToken.id,
                ClientToken.token.label('ticket_id'),
                text("'Fittbot'").label('source'),
                Client.name.label('name'),
                ClientToken.email,
                ClientToken.subject,
                ClientToken.issue,
                ClientToken.followed_up,
                ClientToken.resolved,
                ClientToken.comments,
                ClientToken.created_at,
                TicketAssignment.assignment_id,
                TicketAssignment.employee_id.label('assigned_employee_id'),
                Employees.name.label('assigned_employee_name'),
                Employees.email.label('assigned_employee_email'),
                Employees.department.label('assigned_employee_department'),
                Employees.designation.label('assigned_employee_designation'),
                TicketAssignment.assigned_date,
                TicketAssignment.status.label('assignment_status'),
                TicketAssignment.notes.label('assignment_notes')
            ).outerjoin(
                Client, ClientToken.client_id == Client.client_id
            ).outerjoin(
                TicketAssignment, 
                and_(
                    TicketAssignment.ticket_id == ClientToken.id,
                    TicketAssignment.ticket_source == "Fittbot",
                    TicketAssignment.status == "active"
                )
            ).outerjoin(
                Employees, TicketAssignment.employee_id == Employees.id
            ).filter(ClientToken.id == ticket_id).first()
            
        elif source == "Fittbot Business":
            result = db.query(
                OwnerToken.id,
                OwnerToken.token.label('ticket_id'),
                text("'Fittbot Business'").label('source'),
                Gym.name.label('name'),
                OwnerToken.email,
                OwnerToken.subject,
                OwnerToken.issue,
                OwnerToken.followed_up,
                OwnerToken.resolved,
                OwnerToken.comments,
                OwnerToken.created_at,
                TicketAssignment.assignment_id,
                TicketAssignment.employee_id.label('assigned_employee_id'),
                Employees.name.label('assigned_employee_name'),
                Employees.email.label('assigned_employee_email'),
                Employees.department.label('assigned_employee_department'),
                Employees.designation.label('assigned_employee_designation'),
                TicketAssignment.assigned_date,
                TicketAssignment.status.label('assignment_status'),
                TicketAssignment.notes.label('assignment_notes')
            ).outerjoin(
                Gym, OwnerToken.gym_id == Gym.gym_id
            ).outerjoin(
                TicketAssignment, 
                and_(
                    TicketAssignment.ticket_id == OwnerToken.id,
                    TicketAssignment.ticket_source == "Fittbot Business",
                    TicketAssignment.status == "active"
                )
            ).outerjoin(
                Employees, TicketAssignment.employee_id == Employees.id
            ).filter(OwnerToken.id == ticket_id).first()
        else:
            raise HTTPException(status_code=400, detail="Invalid source")
        
        if not result:
            raise HTTPException(status_code=404, detail="Ticket not found")
        
        # Check permissions for non-managers
        if not is_manager and result.assigned_employee_id != employee_id:
            raise HTTPException(status_code=403, detail="You can only view tickets assigned to you")
        
        # Build assignment info
        assignment_info = None
        if result.assignment_id:
            assignment_info = TicketAssignmentInfo(
                assignment_id=result.assignment_id,
                employee=EmployeeInfo(
                    id=result.assigned_employee_id,
                    name=result.assigned_employee_name,
                    email=result.assigned_employee_email,
                    department=result.assigned_employee_department or "",
                    designation=result.assigned_employee_designation or "",
                    avatar=get_employee_avatar(result.assigned_employee_name)
                ) if result.assigned_employee_id else None,
                assigned_date=result.assigned_date.isoformat() if result.assigned_date else None,
                status=result.assignment_status,
                notes=result.assignment_notes
            )
        
        ticket_data = TicketResponse(
            id=result.id,
            ticket_id=result.ticket_id or f"ticket-{result.id}",
            source=result.source,
            name=result.name or "N/A",
            email=result.email or "N/A",
            subject=result.subject,
            issue_type=result.subject,
            issue=result.issue,
            status=map_status_from_db(result.followed_up, result.resolved),
            comments=result.comments,
            created_at=result.created_at.isoformat() if result.created_at else None,
            assignment=assignment_info
        )
        
        return {
            "success": True,
            "data": ticket_data,
            "message": "Ticket details fetched successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching ticket details: {str(e)}")

@router.get("/issue-types")
async def get_issue_types(db: Session = Depends(get_db)):
    """Get available issue types from tickets"""
    try:
        # Get unique subjects from both tables
        client_subjects = db.query(ClientToken.subject).filter(
            ClientToken.subject.isnot(None)
        ).distinct().all()
        
        owner_subjects = db.query(OwnerToken.subject).filter(
            OwnerToken.subject.isnot(None)
        ).distinct().all()
        
        # Combine and clean up
        all_subjects = set()
        for subject in client_subjects:
            if subject[0]:
                all_subjects.add(subject[0])
        
        for subject in owner_subjects:
            if subject[0]:
                all_subjects.add(subject[0])
        
        # Convert to sorted list
        issue_types = sorted(list(all_subjects))
        
        return {
            "success": True,
            "data": issue_types,
            "message": "Issue types fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching issue types: {str(e)}")

@router.post("/tickets/{ticket_id}/comments")
async def add_ticket_comment(
    requestOne:Request,
    ticket_id: int,
    request: CommentRequest,
    db: Session = Depends(get_db)
):
    """Add comment to ticket"""
    try:
        current_user = await get_current_employee_for_support(requestOne,db)
        is_manager = current_user.get("manager_role", True)
        employee_id = current_user.get("id")
        user_name = current_user.get("name", "Support Agent")
        
        # Check permissions for non-managers
        if not is_manager:
            assignment = db.query(TicketAssignment).filter(
                and_(
                    TicketAssignment.ticket_id == ticket_id,
                    TicketAssignment.ticket_source == request.source,
                    TicketAssignment.employee_id == employee_id,
                    TicketAssignment.status == "active"
                )
            ).first()
            
            if not assignment:
                raise HTTPException(status_code=403, detail="You can only comment on tickets assigned to you")
        
        if request.source == "Fittbot":
            ticket = db.query(ClientToken).filter(ClientToken.id == ticket_id).first()
        elif request.source == "Fittbot Business":
            ticket = db.query(OwnerToken).filter(OwnerToken.id == ticket_id).first()
        else:
            raise HTTPException(status_code=400, detail="Invalid source")
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        
        # Append to existing comments or create new
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        new_comment = f"\n\n--- {user_name} ({timestamp}) ---\n{request.comment}"
        
        if ticket.comments:
            ticket.comments += new_comment
        else:
            ticket.comments = f"--- {user_name} ({timestamp}) ---\n{request.comment}"
        
        ticket.updated_at = datetime.now()
        db.commit()
        
        return {
            "success": True,
            "message": "Comment added successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error adding comment: {str(e)}")

# New endpoints for employee assignment management

@router.get("/employees")
async def get_support_employees(
    request: Request,
    db: Session = Depends(get_db),
    current_employee: dict = Depends(require_manager_role_for_support)
):
    """Get list of employees who can be assigned tickets"""
    try:
        current_employee_id = current_employee.get("id")
        
        
        # Get employees assigned to the current manager through EmployeeAssignments
        employees = db.query(Employees).join(
            EmployeeAssignments, 
            and_(
                EmployeeAssignments.employee_id == Employees.id,
                EmployeeAssignments.manager_id == current_employee_id,
                EmployeeAssignments.status == "active"
            )
        ).filter(
            and_(
                Employees.status == "active",
                Employees.access == True,
                or_(
                    Employees.department.ilike("%support%"),
                    Employees.role.ilike("%support%")
                )
            )
        ).all()
        
        employees_list = []
        for emp in employees:
            employees_list.append({
                "id": emp.id,
                "name": emp.name,
                "email": emp.email,
                "department": emp.department or "",
                "designation": emp.designation or "",
                "avatar": get_employee_avatar(emp.name),
                "manager_role": emp.manager_role
            })
        
        return {
            "success": True,
            "data": employees_list,
            "message": "Support employees fetched successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employees: {str(e)}")

@router.post("/tickets/{ticket_id}/assign")
async def assign_ticket(
    ticket_id: int,
    source: str = Query(..., description="Ticket source (Fittbot or Fittbot Business)"),
    request: AssignTicketRequest = ...,
    http_request: Request = None,
    db: Session = Depends(get_db),
    current_employee: dict = Depends(require_manager_role_for_support)
):
    """Assign ticket to an employee"""
    try:
        # Verify the ticket exists
        if source == "Fittbot":
            ticket = db.query(ClientToken).filter(ClientToken.id == ticket_id).first()
        elif source == "Fittbot Business":
            ticket = db.query(OwnerToken).filter(OwnerToken.id == ticket_id).first()
        else:
            raise HTTPException(status_code=400, detail="Invalid source")
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        
        # Verify the employee exists
        employee = db.query(Employees).filter(Employees.id == request.employee_id).first()
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Check if there's an existing active assignment
        existing_assignment = db.query(TicketAssignment).filter(
            and_(
                TicketAssignment.ticket_id == ticket_id,
                TicketAssignment.ticket_source == source,
                TicketAssignment.status == "active"
            )
        ).first()
        
        if existing_assignment:
            # Mark the existing assignment as reassigned
            existing_assignment.status = "reassigned"
            existing_assignment.updated_at = datetime.now()
        
        # Create new assignment
        new_assignment = TicketAssignment(
            ticket_id=ticket_id,
            ticket_source=source,
            employee_id=request.employee_id,
            assigned_by=current_employee.get("id"),
            notes=request.notes,
            status="active"
        )
        
        db.add(new_assignment)
        
        # Update ticket status to working if it was "yet to start"
        if not ticket.followed_up:
            ticket.followed_up = True
            ticket.updated_at = datetime.now()
        
        db.commit()
        
        return {
            "success": True,
            "message": f"Ticket assigned to {employee.name} successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error assigning ticket: {str(e)}")

@router.delete("/tickets/{ticket_id}/assign")
async def unassign_ticket(
    request:Request,
    ticket_id: int,
    source: str = Query(..., description="Ticket source (Fittbot or Fittbot Business)"),
    db: Session = Depends(get_db)
):
    """Unassign ticket from employee"""
    try:
        current_user = await get_current_employee_for_support(request,db)
        is_manager = current_user.get("manager_role", True)
        
        if not is_manager:
            raise HTTPException(status_code=403, detail="Only managers can unassign tickets")
        
        # Find existing active assignment
        assignment = db.query(TicketAssignment).filter(
            and_(
                TicketAssignment.ticket_id == ticket_id,
                TicketAssignment.ticket_source == source,
                TicketAssignment.status == "active"
            )
        ).first()
        
        if not assignment:
            raise HTTPException(status_code=404, detail="No active assignment found for this ticket")
        
        # Mark assignment as inactive
        assignment.status = "inactive"
        assignment.updated_at = datetime.now()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Ticket unassigned successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error unassigning ticket: {str(e)}")
