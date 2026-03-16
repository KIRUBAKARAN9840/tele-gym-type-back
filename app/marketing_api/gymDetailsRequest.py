from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.marketingmodels import GymDetailRequests, GymVisits, Managers, Executives
from app.models.database import get_db
from typing import Optional, List
from datetime import datetime

router = APIRouter(prefix="/marketing/gym-detail-requests", tags=["Gym Detail Requests"])

class CreateGymDetailRequestModel(BaseModel):
    visit_id: int
    manager_id: int
    executive_id: int
    referal_id:str
    gym_name: str
    gym_address: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    request_reason: Optional[str] = None

class UpdateRequestStatusModel(BaseModel):
    request_id: int
    status: str  
    admin_notes: Optional[str] = None
    reviewed_by: int

@router.post("/create")
def create_gym_detail_request(
    request_data: CreateGymDetailRequestModel,
    db: Session = Depends(get_db)
):
    try:
        visit = db.query(GymVisits).filter(
            GymVisits.id == request_data.visit_id,
            GymVisits.user_id == request_data.executive_id
        ).first()
        
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found or doesn't belong to this executive")
        
        executive = db.query(Executives).filter(
            Executives.id == request_data.executive_id,
            Executives.manager_id == request_data.manager_id
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")
        
        existing_request = db.query(GymDetailRequests).filter(
            GymDetailRequests.visit_id == request_data.visit_id
        ).first()
        
        if existing_request:
            raise HTTPException(status_code=400, detail="Request already exists for this visit")
        
        new_request = GymDetailRequests(
            visit_id=request_data.visit_id,
            manager_id=request_data.manager_id,
            referal_id=request_data.referal_id,
            executive_id=request_data.executive_id,
            gym_name=request_data.gym_name,
            gym_address=request_data.gym_address,
            contact_person=request_data.contact_person,
            contact_phone=request_data.contact_phone,
            request_reason=request_data.request_reason,
            status='pending',
            requested_at=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        db.add(new_request)
        db.commit()
        db.refresh(new_request)
        
        return {
            "status": 200,
            "message": "Gym detail request created successfully",
            "data": new_request.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating gym detail request: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/manager/{manager_id}")
def get_manager_gym_detail_requests(
    manager_id: int,
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        query = db.query(
            GymDetailRequests,
            Executives.name.label('executive_name'),
            GymVisits.final_status.label('visit_status')
        ).join(
            Executives, GymDetailRequests.executive_id == Executives.id
        ).join(
            GymVisits, GymDetailRequests.visit_id == GymVisits.id
        ).filter(
            GymDetailRequests.manager_id == manager_id
        )
        
        if status:
            query = query.filter(GymDetailRequests.status == status)
        
        results = query.order_by(GymDetailRequests.created_at.desc()).all()
        
        requests_data = []
        for request, executive_name, visit_status in results:
            request_dict = request.to_dict()
            request_dict['executive_name'] = executive_name
            request_dict['visit_status'] = visit_status
            requests_data.append(request_dict)
        
        return {
            "status": 200,
            "message": "Gym detail requests retrieved successfully",
            "data": requests_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting gym detail requests: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/visit/{visit_id}/status")
def get_visit_request_status(
    visit_id: int,
    manager_id: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        request = db.query(GymDetailRequests).filter(
            GymDetailRequests.visit_id == visit_id,
            GymDetailRequests.manager_id == manager_id
        ).first()
        
        if not request:
            return {
                "status": 200,
                "message": "No request found for this visit",
                "data": {
                    "has_request": False,
                    "request_status": None,
                    "can_request": True
                }
            }
        
        return {
            "status": 200,
            "message": "Request status retrieved successfully",
            "data": {
                "has_request": True,
                "request_status": request.status,
                "can_request": False,
                "request_id": request.id,
                "requested_at": request.requested_at.isoformat() if request.requested_at else None
            }
        }
        
    except Exception as e:
        print(f"Error getting visit request status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.put("/admin/update-status")
def update_request_status(
    request_data: UpdateRequestStatusModel,
    db: Session = Depends(get_db)
):
    try:
        request = db.query(GymDetailRequests).filter(
            GymDetailRequests.id == request_data.request_id
        ).first()
        
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        request.status = request_data.status
        request.admin_notes = request_data.admin_notes
        request.reviewed_by = request_data.reviewed_by
        request.reviewed_at = datetime.now()
        request.updated_at = datetime.now()
        
        db.commit()
        db.refresh(request)
        
        return {
            "status": 200,
            "message": f"Request {request_data.status} successfully",
            "data": request.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating request status: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
