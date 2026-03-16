from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_, and_, desc, case
from app.models.marketingmodels import GymDetailRequests, GymVisits, Managers, Executives, GymDatabase
from app.models.fittbot_models import Gym, Client, ClientFittbotAccess, FittbotPlans
from app.models.database import get_db
from typing import Optional, List
from datetime import datetime
import math

router = APIRouter(prefix="/api/admin/gym-detail-requests", tags=["Admin Gym Detail Requests"])

class ApproveRejectRequestModel(BaseModel):
    request_id: int
    action: str  # "approve" or "reject"
    admin_notes: Optional[str] = None
    reviewed_by: int
    rejection_reason: Optional[str] = None

class BulkUpdateRequestModel(BaseModel):
    request_ids: List[int]
    action: str  # "approve" or "reject"
    admin_notes: Optional[str] = None
    reviewed_by: int
    rejection_reason: Optional[str] = None

@router.get("/")
async def get_all_requests(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    status: Optional[str] = Query(None, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search by gym name or contact"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),
    db: Session = Depends(get_db)
):
    """Get all gym detail requests for admin panel"""
    try:
        # Base query with joins
        query = db.query(
            GymDetailRequests,
            Executives.name.label('executive_name'),
            Managers.name.label('manager_name'),
            GymVisits.final_status.label('visit_status')
        ).join(
            Executives, GymDetailRequests.executive_id == Executives.id
        ).join(
            Managers, GymDetailRequests.manager_id == Managers.id
        ).join(
            GymVisits, GymDetailRequests.visit_id == GymVisits.id
        )
        
        # Apply status filter
        if status and status != "all":
            query = query.filter(GymDetailRequests.status == status)
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(GymDetailRequests.gym_name).like(search_term),
                    func.lower(GymDetailRequests.contact_person).like(search_term),
                    func.lower(Executives.name).like(search_term),
                    func.lower(Managers.name).like(search_term)
                )
            )
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(GymDetailRequests.requested_at.asc())
        else:
            query = query.order_by(GymDetailRequests.requested_at.desc())
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        results = query.offset(offset).limit(limit).all()
        
        # Format response
        requests_data = []
        for request, executive_name, manager_name, visit_status in results:
            request_dict = request.to_dict()
            request_dict['executive_name'] = executive_name
            request_dict['manager_name'] = manager_name
            request_dict['visit_status'] = visit_status
            
            # Get retention rate for the gym using referal_id
            retention_rate = 0.0
            if request.referal_id:
                # Find gym using referal_id
                gym = db.query(Gym).filter(Gym.referal_id == request.referal_id).first()
                if gym:
                    # Calculate retention rate using the same logic as gymstats.py
                    retention_query = db.query(
                        func.count(Client.client_id).label('total_clients'),
                        func.sum(case(
                            (ClientFittbotAccess.access_status == 'active', 1),
                            else_=0
                        )).label('active_clients')
                    ).filter(
                        Client.gym_id == gym.gym_id
                    ).outerjoin(
                        ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
                    ).first()
                    
                    if retention_query and retention_query.total_clients > 0:
                        active_clients = retention_query.active_clients or 0
                        retention_rate = round((active_clients * 100.0) / retention_query.total_clients, 2)
            
            request_dict['retention_rate'] = retention_rate
            requests_data.append(request_dict)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "status": 200,
            "message": "Requests fetched successfully",
            "data": {
                "requests": requests_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/approve-reject")
async def approve_reject_request(
    request_data: ApproveRejectRequestModel,
    db: Session = Depends(get_db)
):
    """Approve or reject a gym detail request"""
    try:
        request = db.query(GymDetailRequests).filter(
            GymDetailRequests.id == request_data.request_id
        ).first()
        
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        if request.status != "pending":
            raise HTTPException(status_code=400, detail="Request has already been reviewed")
        
        # Update request
        request.status = request_data.action
        request.admin_notes = request_data.admin_notes
        request.reviewed_by = request_data.reviewed_by
        request.reviewed_at = datetime.now()
        request.updated_at = datetime.now()
        
        if request_data.action == "rejected" and request_data.rejection_reason:
            request.admin_notes = f"{request_data.rejection_reason}. {request_data.admin_notes or ''}"
        
        db.commit()
        db.refresh(request)
        
        return {
            "status": 200,
            "message": f"Request {request_data.action}d successfully",
            "data": request.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/bulk-update")
async def bulk_approve_reject_requests(
    request_data: BulkUpdateRequestModel,
    db: Session = Depends(get_db)
):
    """Bulk approve or reject multiple requests"""
    try:
        requests = db.query(GymDetailRequests).filter(
            GymDetailRequests.id.in_(request_data.request_ids),
            GymDetailRequests.status == "pending"
        ).all()
        
        if not requests:
            raise HTTPException(status_code=404, detail="No pending requests found")
        
        updated_count = 0
        for request in requests:
            request.status = request_data.action
            request.admin_notes = request_data.admin_notes
            request.reviewed_by = request_data.reviewed_by
            request.reviewed_at = datetime.now()
            request.updated_at = datetime.now()
            
            if request_data.action == "rejected" and request_data.rejection_reason:
                request.admin_notes = f"{request_data.rejection_reason}. {request_data.admin_notes or ''}"
            
            updated_count += 1
        
        db.commit()
        
        return {
            "status": 200,
            "message": f"{updated_count} requests {request_data.action}d successfully",
            "data": {
                "updated_count": updated_count,
                "total_requested": len(request_data.request_ids)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/stats")
async def get_admin_request_stats(db: Session = Depends(get_db)):
    """Get statistics for admin dashboard"""
    try:
        total_requests = db.query(GymDetailRequests).count()
        pending_requests = db.query(GymDetailRequests).filter(
            GymDetailRequests.status == "pending"
        ).count()
        approved_requests = db.query(GymDetailRequests).filter(
            GymDetailRequests.status == "approved"
        ).count()
        rejected_requests = db.query(GymDetailRequests).filter(
            GymDetailRequests.status == "rejected"
        ).count()
        
        # Recent requests (last 7 days)
        from datetime import datetime, timedelta
        week_ago = datetime.now() - timedelta(days=7)
        recent_requests = db.query(GymDetailRequests).filter(
            GymDetailRequests.requested_at >= week_ago
        ).count()
        
        return {
            "status": 200,
            "message": "Stats fetched successfully",
            "data": {
                "total_requests": total_requests,
                "pending_requests": pending_requests,
                "approved_requests": approved_requests,
                "rejected_requests": rejected_requests,
                "recent_requests": recent_requests,
                "approval_rate": round((approved_requests / total_requests * 100), 2) if total_requests > 0 else 0
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# Gym Details and Clients API
@router.get("/gym-details/{request_id}")
async def get_gym_details_by_request(
    request_id: int,
    db: Session = Depends(get_db)
):
    """Get gym details and clients for an approved request"""
    try:
        # First check if request exists and is approved
        request = db.query(GymDetailRequests).filter(
            GymDetailRequests.id == request_id,
            GymDetailRequests.status == "approved"
        ).first()
        
        if not request:
            raise HTTPException(status_code=404, detail="Approved request not found")
        
        # Get gym details from marketing database using referal_id
        gym_data = db.query(GymDatabase).filter(
            GymDatabase.referal_id == request.referal_id
        ).first()
        
        if not gym_data:
            raise HTTPException(status_code=404, detail="Gym not found in database")
        
        # Get gym from main database using referal_id
        main_gym = db.query(Gym).filter(
            Gym.referal_id == request.referal_id
        ).first()
        
        if not main_gym:
            return {
                "status": 200,
                "message": "Gym details found but no clients yet",
                "data": {
                    "gym_details": {
                        "gym_name": gym_data.gym_name,
                        "address": gym_data.address,
                        "contact_person": gym_data.contact_person,
                        "contact_phone": gym_data.contact_phone,
                        "operating_hours": gym_data.operating_hours,
                        "referal_id": gym_data.referal_id
                    },
                    "clients": [],
                    "client_stats": {
                        "total_count": 0,
                        "active_count": 0,
                        "premium_count": 0
                    }
                }
            }
        
        # Get clients for this gym
        clients_query = db.query(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.joined_date,
            Client.status,
            Client.gender,
            Gym.name.label('gym_name'),
            ClientFittbotAccess.access_status,
            FittbotPlans.plan_name
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        ).outerjoin(
            FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
        ).filter(
            Client.gym_id == main_gym.gym_id
        ).order_by(Client.joined_date.desc())
        
        clients_results = clients_query.all()
        
        # Format client data
        clients = []
        for result in clients_results:
            client_data = {
                "id": result.client_id,
                "name": result.name,
                "mobile": result.contact,
                "email": result.email,
                "membership_type": result.plan_name or "Basic",
                "join_date": result.joined_date.isoformat() if result.joined_date else None,
                "status": result.status or "Active",
                "last_visit": "2024-07-20",  # This would need to come from attendance data
                "gender": result.gender,
                "access_status": result.access_status or "inactive"
            }
            clients.append(client_data)
        
        # Calculate stats
        total_count = len(clients)
        active_count = len([c for c in clients if c["status"] == "active"])
        premium_count = len([c for c in clients if c["membership_type"] == "Premium"])
        
        return {
            "status": 200,
            "message": "Gym details and clients retrieved successfully",
            "data": {
                "gym_details": {
                    "gym_name": gym_data.gym_name,
                    "address": gym_data.address,
                    "contact_person": gym_data.contact_person,
                    "contact_phone": gym_data.contact_phone,
                    "operating_hours": gym_data.operating_hours,
                    "referal_id": gym_data.referal_id
                },
                "clients": clients,
                "client_stats": {
                    "total_count": total_count,
                    "active_count": active_count,
                    "premium_count": premium_count
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/gym-clients/{referal_id}")
async def get_gym_clients_by_referal_id(
    referal_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Get gym clients using referal_id"""
    try:
        # Find gym using referal_id
        gym = db.query(Gym).filter(Gym.referal_id == referal_id).first()
        
        if not gym:
            return {
                "status": 200,
                "message": "No clients found - gym not registered yet",
                "data": {
                    "clients": [],
                    "total_count": 0,
                    "active_count": 0,
                    "premium_count": 0
                }
            }
        
        # Build clients query
        clients_query = db.query(
            Client.client_id,
            Client.name,
            Client.contact,
            Client.email,
            Client.joined_date,
            Client.status,
            Client.gender,
            Gym.name.label('gym_name'),
            ClientFittbotAccess.access_status,
            FittbotPlans.plan_name
        ).outerjoin(
            Gym, Client.gym_id == Gym.gym_id
        ).outerjoin(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        ).outerjoin(
            FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
        ).filter(
            Client.gym_id == gym.gym_id
        )
        
        # Apply status filter
        if status and status != "all":
            clients_query = clients_query.filter(Client.status == status)
        
        # Get total count
        total_count = clients_query.count()
        
        # Apply pagination and ordering
        offset = (page - 1) * limit
        clients_results = clients_query.order_by(Client.joined_date.desc()).offset(offset).limit(limit).all()
        
        # Format client data
        clients = []
        for result in clients_results:
            client_data = {
                "id": result.client_id,
                "name": result.name,
                "mobile": result.contact,
                "email": result.email,
                "membership_type": result.plan_name or "Basic",
                "join_date": result.joined_date.isoformat() if result.joined_date else None,
                "status": result.status or "Active",
                "last_visit": "2024-07-20",  # This would come from attendance data
                "gender": result.gender,
                "access_status": result.access_status or "inactive",
                "referal_id": referal_id
            }
            clients.append(client_data)
        
        # Calculate stats (from all clients, not just current page)
        all_clients = db.query(Client).filter(Client.gym_id == gym.gym_id).all()
        active_count = len([c for c in all_clients if c.status == "active"])
        
        # Get premium count by checking FittbotAccess
        premium_count = db.query(Client).join(
            ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
        ).join(
            FittbotPlans, ClientFittbotAccess.fittbot_plan == FittbotPlans.id
        ).filter(
            Client.gym_id == gym.gym_id,
            FittbotPlans.plan_name.like("%Premium%")
        ).count()
        
        return {
            "status": 200,
            "message": "Gym clients retrieved successfully",
            "data": {
                "clients": clients,
                "total_count": total_count,
                "active_count": active_count,
                "premium_count": premium_count,
                "gym_name": gym.name,
                "referal_id": referal_id
            }
        }
        
    except Exception as e:
        print(f"Error fetching gym clients: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")