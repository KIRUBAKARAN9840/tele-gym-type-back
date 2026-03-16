from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.marketingmodels import Executives, GymVisits, Managers, GymAssignments, GymDatabase, Feedback, FollowupAttempts, PostConversionActivities, ActivityTimeline
from app.models.database import get_db
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import or_, and_, func
import os
import boto3
import time

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
VISIT_PHOTOS_PREFIX = "visit_photos/"
PHOTO_MAX_SIZE = 10 * 1024 * 1024  # 1 MB
_s3 = boto3.client("s3", region_name=AWS_REGION)

router = APIRouter(prefix="/marketing/gym-visits", tags=["Gym Visits"])



class LocationData(BaseModel):
    latitude: float
    longitude: float
    address: Optional[str] = None
    timestamp: Optional[str] = None

class OperatingHours(BaseModel):
    id: str
    day: str
    startTime: str
    endTime: str

class FacilityPhoto(BaseModel):
    uri: str
    category: str
    timestamp: str
    location: Optional[Dict] = None

class Step1Data(BaseModel):
    gym_name: str
    gym_address: str
    contact_person: str
    contact_phone: str
    inquired_person1: Optional[str] = None
    inquired_phone1: Optional[str] = None
    inquired_person2: Optional[str] = None
    inquired_phone2: Optional[str] = None
    visit_purpose: str
    visit_purpose_other: Optional[str] = None

class PreVisitData(BaseModel):
    gym_name: str
    gym_address: str
    contact_person: str
    contact_phone: str
    inquired_person1: Optional[str] = None
    inquired_phone1: Optional[str] = None
    inquired_person2: Optional[str] = None
    inquired_phone2: Optional[str] = None
    visit_purpose: str
    visit_purpose_other: Optional[str] = None

class CheckInData(BaseModel):
    check_in_time: str
    check_in_location: Optional[Dict] = None
    exterior_photo: Optional[str] = None
    attendance_selfie: Optional[str] = None

class AssessmentData(BaseModel):
    # gym_size: str
    total_member_count: int
    active_member_count: int
    expected_member_count: Optional[int] = None
    conversion_probability: Optional[str] = None
    operating_hours: List[Dict]
    # current_tech: Optional[str] = None
    monthly_leads: int
    monthly_conversion: int

class MeetingData(BaseModel):
    people_met: str
    meeting_duration: str
    presentation_given: bool = False
    demo_provided: bool = False
    interest_level: int = 0
    questions_asked: Optional[str] = None
    objections: Optional[str] = None

class BusinessData(BaseModel):
    decision_maker_present: bool = False
    decision_timeline: Optional[str] = None
    competitors: Optional[str] = None
    pain_points: str
    current_solutions: Optional[str] = None
    key_benefits: Optional[str] = None

class FollowUpData(BaseModel):
    next_steps: str
    materials_to_send: Optional[str] = None
    visit_outcome: str
    visit_summary: Optional[str] = None
    action_items: Optional[str] = None
    overall_rating: int = 0

class FinalStatusData(BaseModel):
    final_status: str
    next_follow_up_date: Optional[str] = None
    rejection_reason: Optional[str] = None
    next_meeting_date: Optional[str] = None
    follow_up_notes: Optional[str] = None
    conversion_notes: Optional[str] = None
    checklist: Dict

class CreateVisitRequest(BaseModel):
    user_id: int

class CreateVisitWithStep1Request(BaseModel):
    user_id: int
    step1_data: Dict[str, Any]

class UpdateVisitRequest(BaseModel):
    visit_id: int
    step: int
    data: Dict[str, Any]


def get_feedback_status(visit_id: int, db: Session) -> bool:
    feedback = db.query(Feedback).filter(
        Feedback.visit_id == visit_id,
        Feedback.category != 'followup_agenda'  
    ).first()
    return feedback is not None

def get_follow_up_agenda(visit_id: int, db: Session) -> str:
    visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
    if not visit:
        return None
    
    if visit.action_items:
        return visit.action_items
    
    if visit.follow_up_notes:
        if "AGENDA:" in visit.follow_up_notes:
            return visit.follow_up_notes.split("AGENDA:")[1].strip()
    
    return None

def set_follow_up_agenda(visit_id: int, agenda: str, db: Session):
    visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
    if visit:
        visit.action_items = agenda
        visit.updated_at = datetime.now()
        db.commit()


def apply_date_filters(query, start_date: Optional[str] = None, end_date: Optional[str] = None):
    if start_date:
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        query = query.filter(GymVisits.assigned_date >= start_dt)
    
    if end_date:
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        query = query.filter(GymVisits.assigned_date <= end_dt)
    
    return query

def apply_bde_filter(query, bde_id: Optional[int] = None):
    if bde_id:
        query = query.filter(GymVisits.user_id == bde_id)
    
    return query

def generate_visit_photo_upload_url(
    user_id: int,
    visit_id: int,
    photo_type: str,
    extension: str,
    content_type: str = "image/jpeg",
):
    if not content_type.startswith("image/"):
        raise HTTPException(
            status_code=400, detail="Invalid content type; must start with image/"
        )

    if not extension:
        raise HTTPException(
            status_code=400, detail="File extension is required"
        )

    version = int(time.time() * 1000)
    sanitized_photo_type = photo_type.replace(" ", "_")
    key = f"{VISIT_PHOTOS_PREFIX}user-{user_id}/visit-{visit_id}/{sanitized_photo_type}_{version}.{extension}"

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, PHOTO_MAX_SIZE],
    ]

    presigned = _s3.generate_presigned_post(
        Bucket=BUCKET_NAME,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=600,
    )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"

    return {
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": version,
    }


@router.post("/create-with-step1")
def create_visit_with_step1(request: CreateVisitWithStep1Request, db: Session = Depends(get_db)):
    try:
        user = db.query(Executives).filter(Executives.id == request.user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        step1_data = request.step1_data
        required_fields = ['gym_name', 'gym_address', 'contact_person', 'contact_phone', 'visit_purpose']
        
        for field in required_fields:
            if not step1_data.get(field):
                raise HTTPException(status_code=400, detail=f"Required field {field} is missing")

        current_time = datetime.now()

        gym_database=GymDatabase(
            gym_name = step1_data.get('gym_name'),
            address = step1_data.get('gym_address'),
            contact_person = step1_data.get('contact_person'),
            contact_phone = step1_data.get('contact_phone'),
        )
        db.add(gym_database)
        db.commit()
        
        new_visit = GymVisits(
            user_id=request.user_id,
            gym_id = gym_database.id,
            start_date=current_time,
            current_step=1,  
            completed=False,
            created_at=current_time,
            updated_at=current_time,
            
            gym_name=step1_data.get('gym_name'),
            gym_address=step1_data.get('gym_address'),
            contact_person=step1_data.get('contact_person'),
            contact_phone=step1_data.get('contact_phone'),
            visit_purpose=step1_data.get('visit_purpose'),
            visit_purpose_other=step1_data.get('visit_purpose_other'),
            inquired_person1=step1_data.get('inquired_person1'),
            inquired_phone1=step1_data.get('inquired_phone1'),
            inquired_person2=step1_data.get('inquired_person2'),
            inquired_phone2=step1_data.get('inquired_phone2'),
            
            presentation_given=False,
            demo_provided=False,
            interest_level=0,
            decision_maker_present=False,
            overall_rating=0,
            final_status="pending"
        )
        
        db.add(new_visit)
        db.commit()
        db.refresh(new_visit)
        
        return {
            "status": 200,
            "message": "Visit created successfully with step 1 data",
            "data": {
                "visit_id": new_visit.id,
                "current_step": new_visit.current_step,
                "gym_name": new_visit.gym_name
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating visit with step 1: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/create")
def create_new_visit(request: CreateVisitRequest, db: Session = Depends(get_db)):
    try:
        user = db.query(Executives).filter(Executives.id == request.user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        current_time = datetime.now()
        new_visit = GymVisits(
            user_id=request.user_id,
            start_date=current_time,
            current_step=0,
            completed=False,
            created_at=current_time,
            updated_at=current_time,
            gym_name="",  
            gym_address="",  
            contact_person="",  
            contact_phone="",  
            visit_purpose="",
            presentation_given=False,
            demo_provided=False,
            interest_level=0,
            decision_maker_present=False,
            overall_rating=0,
            final_status="pending"
        )
        
        db.add(new_visit)
        db.commit()
        db.refresh(new_visit)
        
        return {
            "status": 200,
            "message": "New visit created successfully",
            "data": {
                "visit_id": new_visit.id,
                "current_step": new_visit.current_step
            }
        }
    
    except Exception as e:
        print(f"Error creating visit: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.put("/update")
def update_visit(request: UpdateVisitRequest, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == request.visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        for field, value in request.data.items():
            if hasattr(visit, field):
                if (field.endswith('_date') or field.endswith('_time')) and value:
                    if isinstance(value, str):
                        try:
                            if value.endswith('Z'):
                                value = value.replace('Z', '+00:00')
                            value = datetime.fromisoformat(value)
                        except ValueError:
                            print(f"Warning: Could not parse datetime field {field}: {value}")
                            pass
                
                setattr(visit, field, value)

        visit.current_step = max(visit.current_step, request.step + 1)  
        visit.updated_at = datetime.now()
        
        if request.step == 7 and request.data.get('final_status'):
            visit.completed = True
            visit.check_out_time = datetime.now()

        db.commit()
        db.refresh(visit)
        
        return {
            "status": 200,
            "message": "Visit updated successfully",
            "data": {
                "visit_id": visit.id,
                "current_step": visit.current_step,
                "completed": visit.completed
            }
        }
    
    except Exception as e:
        print(f"Error updating visit: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/photo-upload-url")
def get_photo_upload_url(
    user_id: int,
    visit_id: int,
    photo_type: str,
    extension: str,
    is_self_assigned:bool,
    db: Session = Depends(get_db)
):
    try:

        if not is_self_assigned:
            visit = db.query(GymVisits).filter(
                GymVisits.id == visit_id,
                GymVisits.user_id == user_id
            ).first()
        else:
            visit = db.query(GymVisits).filter(
                GymVisits.id == visit_id,
                GymVisits.manager_id == user_id
            ).first()


        if not visit:
            print(f"Visit not found: visit_id={visit_id}, user_id={user_id}")
            raise HTTPException(status_code=404, detail="Visit not found")

        url_data = generate_visit_photo_upload_url(
            user_id, visit_id, photo_type, extension
        )

        print("url data is",url_data)

        return {
            "status": 200,
            "data": url_data
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting photo upload URL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
class confimPostRequest(BaseModel):
    visit_id:int
    photo_type:str
    cdn_url:str

@router.post("/confirm-photo")
def confirm_photo_upload(
    request:confimPostRequest,
    db: Session = Depends(get_db)
):
    visit_id=request.visit_id
    photo_type = request.photo_type
    cdn_url = request.cdn_url

    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        if photo_type == "exterior":
            visit.exterior_photo = cdn_url
        elif photo_type == "selfie":
            visit.attendance_selfie = cdn_url
        elif photo_type.startswith("facility_"):
            facility_photos = visit.facility_photos or []
            category = photo_type.replace("facility_", "")
            
            photo_data = {
                "uri": cdn_url,
                "category": category,
                "timestamp": str(datetime.now()),
                "type": photo_type
            }
            facility_photos.append(photo_data)
            visit.facility_photos = facility_photos

        visit.updated_at = datetime.now()
        db.commit()
        db.refresh(visit)
        
        return {
            "status": 200,
            "message": "Photo confirmed and saved"
        }
    
    except Exception as e:
        print(f"Error confirming photo upload: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

def get_visit_with_feedback_info(visit, db):
    feedback_exists = get_feedback_status(visit.id, db)
    follow_up_agenda = get_follow_up_agenda(visit.id, db)

    # Check if this is a self-assigned visit
    is_self_assigned = False
    if visit.gym_id:
        gym = db.query(GymDatabase).filter(GymDatabase.id == visit.gym_id).first()
        if gym and gym.self_assigned:
            is_self_assigned = True

    visit_data = {
        "id": visit.id,
        "gym_id":visit.gym_id,
        "executive_id":visit.user_id,
        "gym_name": visit.gym_name,
        "gym_address": visit.gym_address,
        "referal_id":visit.referal_id,
        "contact_person": visit.contact_person,
        "contact_phone": visit.contact_phone,
        "exterior_photo": visit.exterior_photo,
        "final_status": visit.final_status,
        "overall_rating": visit.overall_rating,
        "next_follow_up_date": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
        "next_meeting_date": visit.next_meeting_date.isoformat() if visit.next_meeting_date else None,
        "rejection_reason": visit.rejection_reason,
        "follow_up_notes": visit.follow_up_notes,
        "conversion_notes": visit.conversion_notes,
        "visit_summary": visit.visit_summary,
        "pain_points": visit.pain_points,
        "interest_level": visit.interest_level,
        "decision_timeline": visit.decision_timeline,
        "check_out_time": visit.check_out_time.isoformat() if visit.check_out_time else None,
        "updated_at": visit.updated_at.isoformat() if visit.updated_at else None,
        "created_at": visit.created_at.isoformat() if visit.created_at else None,
        "total_member_count": visit.total_member_count,
        "gym_size": visit.gym_size,
        "people_met": visit.people_met,
        "meeting_duration": visit.meeting_duration,
        "key_benefits": visit.key_benefits,
        "current_tech": visit.current_tech,
        "competitors": visit.competitors,
        "completed": visit.completed,
        "feedback_submitted": feedback_exists,
        "follow_up_agenda": follow_up_agenda,
        "monthly_leads": visit.monthly_leads,
        "monthly_conversion": visit.monthly_conversion,
        "checklist": visit.checklist,
        "is_self_assigned": is_self_assigned
    }

    return visit_data

@router.get("/get/{visit_id}")
def get_visit(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        visit_data = {
            "id": visit.id,
            "user_id": visit.user_id,
            "start_date": visit.start_date.isoformat() if visit.start_date else None,
            "gym_name": visit.gym_name,
            "gym_address": visit.gym_address,            
            "referal_id":visit.referal_id,
            "contact_person": visit.contact_person,
            "contact_phone": visit.contact_phone,
            "inquired_person1": visit.inquired_person1,
            "inquired_phone1": visit.inquired_phone1,
            "inquired_person2": visit.inquired_person2,
            "inquired_phone2": visit.inquired_phone2,
            "visit_purpose": visit.visit_purpose,
            "visit_purpose_other": visit.visit_purpose_other,
            "check_in_time": visit.check_in_time.isoformat() if visit.check_in_time else None,
            "check_in_location": visit.check_in_location,
            "exterior_photo": visit.exterior_photo,
            "attendance_selfie": visit.attendance_selfie,
            "facility_photos": visit.facility_photos or [],
            "gym_size": visit.gym_size,
            "total_member_count": visit.total_member_count,
            "active_member_count": visit.active_member_count,
            "expected_member_count": visit.expected_member_count,
            "conversion_probability": visit.conversion_probability,
            "operating_hours": visit.operating_hours or [],
            "current_tech": visit.current_tech,
            "people_met": visit.people_met,
            "meeting_duration": visit.meeting_duration,
            "presentation_given": visit.presentation_given,
            "demo_provided": visit.demo_provided,
            "interest_level": visit.interest_level,
            "questions_asked": visit.questions_asked,
            "objections": visit.objections,
            "decision_maker_present": visit.decision_maker_present,
            "decision_timeline": visit.decision_timeline,
            "competitors": visit.competitors,
            "pain_points": visit.pain_points,
            "current_solutions": visit.current_solutions,
            "key_benefits": visit.key_benefits,
            "next_steps": visit.next_steps,
            "materials_to_send": visit.materials_to_send,
            "visit_outcome": visit.visit_outcome,
            "visit_summary": visit.visit_summary,
            "action_items": visit.action_items,
            "overall_rating": visit.overall_rating,
            "final_status": visit.final_status,
            "next_follow_up_date": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
            "rejection_reason": visit.rejection_reason,
            "next_meeting_date": visit.next_meeting_date.isoformat() if visit.next_meeting_date else None,
            "follow_up_notes": visit.follow_up_notes,
            "conversion_notes": visit.conversion_notes,
            "check_out_time": visit.check_out_time.isoformat() if visit.check_out_time else None,
            "completed": visit.completed,
            "current_step": visit.current_step,
            "created_at": visit.created_at.isoformat() if visit.created_at else None,
            "updated_at": visit.updated_at.isoformat() if visit.updated_at else None
        }

        return {
            "status": 200,
            "message": "Visit retrieved successfully",
            "data": visit_data
        }
    
    except Exception as e:
        print(f"Error getting visit: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/dashboard/{user_id}/{self_assigned}")
async def get_user_visits(user_id: int,self_assigned:bool, db: Session = Depends(get_db)):
    try:

        if not self_assigned:
            user = db.query(Executives).filter(Executives.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

        else:
            user = db.query(Managers).filter(Managers.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            
        if not self_assigned:

            visits = db.query(GymVisits).filter(GymVisits.user_id == user_id).order_by(GymVisits.created_at.desc()).all()
        
        else:
            visits = db.query(GymVisits).filter(GymVisits.manager_id == user_id).order_by(GymVisits.created_at.desc()).all()


        
        visit_data = []
        for visit in visits:
            gym_name = visit.gym_name if visit.gym_name and visit.gym_name.strip() else "New Visit"
            
            visit_info = {
                "id": visit.id,
                "gym_name": gym_name,
                "gym_address": visit.gym_address,                
                "referal_id":visit.referal_id,
                "assigned_date":visit.assigned_date,
                "start_date": visit.start_date.isoformat() if visit.start_date else None,
                "final_status": visit.final_status,
                "completed": visit.completed,
                "current_step": visit.current_step,
                "total_steps": 8,
                "contact_person": visit.contact_person,
                "contact_phone": visit.contact_phone,
                "visit_purpose": visit.visit_purpose,
                "next_follow_up_date": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
                "overall_rating": visit.overall_rating,
                "created_at": visit.created_at.isoformat() if visit.created_at else None,
                "updated_at": visit.updated_at.isoformat() if visit.updated_at else None
            }
            visit_data.append(visit_info)

        total_visits = len(visits)
        completed_visits = len([v for v in visits if v.completed])
        pending_visits = len([v for v in visits if not v.completed])
        converted_visits = len([v for v in visits if v.final_status == 'converted'])
        
        summary = {
            "total_visits": total_visits,
            "completed_visits": completed_visits,
            "pending_visits": pending_visits,
            "converted_visits": converted_visits,
            "conversion_rate": round((converted_visits / completed_visits * 100) if completed_visits > 0 else 0, 1)
        }

        return {
            "status": 200,
            "message": "Visits retrieved successfully",
            "data": {
                "visits": visit_data,
                "summary": summary
            }
        }
    
    except HTTPException:
        raise
    
    except Exception as e:
        print(f"Error getting user visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.delete("/delete/{visit_id}")
def delete_visit(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")
        
        db.delete(visit)
        db.commit()

        return {
            "status": 200,
            "message": "Visit deleted successfully"
        }
    
    except Exception as e:
        print(f"Error deleting visit: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/analytics/{user_id}")
def get_visit_analytics(
    user_id: int,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        query = db.query(GymVisits).filter(GymVisits.user_id == user_id)
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date)
            query = query.filter(GymVisits.created_at >= start_dt)
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date)
            query = query.filter(GymVisits.created_at <= end_dt)
        
        visits = query.all()
        
        total_visits = len(visits)
        completed_visits = [v for v in visits if v.completed]
        
        analytics = {
            "total_visits": total_visits,
            "completed_visits": len(completed_visits),
            "average_rating": round(sum(v.overall_rating for v in completed_visits if v.overall_rating) / len(completed_visits), 1) if completed_visits else 0,
            "status_breakdown": {
                "pending": len([v for v in visits if v.final_status == 'pending']),
                "followup": len([v for v in visits if v.final_status == 'followup']),
                "converted": len([v for v in visits if v.final_status == 'converted']),
                "rejected": len([v for v in visits if v.final_status == 'rejected']),
                "scheduled": len([v for v in visits if v.final_status == 'scheduled'])
            },
            "monthly_visits": {},  
            "top_pain_points": [],  
            "average_interest_level": round(sum(v.interest_level for v in completed_visits if v.interest_level) / len(completed_visits), 1) if completed_visits else 0
        }
        
        return {
            "status": 200,
            "message": "Analytics retrieved successfully",
            "data": analytics
        }
    
    except Exception as e:
        print(f"Error getting analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

class statusUpdateRequest(BaseModel):
    visit_id:int
    status:str
    notes:dict
    additionalData:Optional[dict]

@router.put("/tracker/update-status")
def update_visit_status(request: statusUpdateRequest, db: Session = Depends(get_db)):
    try:
        visit_id = request.visit_id
        new_status = request.status
        notes = request.notes

        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        visit.final_status = new_status
        visit.updated_at = datetime.now()

        if new_status == 'followup':
            visit.follow_up_notes = notes.get('notes')
            if request.additionalData and request.additionalData.get('next_follow_up_date'):
                visit.next_follow_up_date = datetime.fromisoformat(request.additionalData.get('next_follow_up_date'))
        elif new_status == 'converted':
            visit.conversion_notes = notes.get('notes')
        elif new_status == 'rejected':
            visit.rejection_reason = notes.get('rejection_reason')

        # Sync conversion_status in gym_assignments table
        if visit.gym_id:
            gym_assignment = db.query(GymAssignments).filter(
                and_(
                    GymAssignments.gym_id == visit.gym_id,
                    GymAssignments.executive_id == visit.user_id,
                    GymAssignments.status == 'assigned'
                )
            ).first()

            if gym_assignment:
                gym_assignment.conversion_status = new_status
                gym_assignment.updated_at = datetime.now()

        db.commit()
        db.refresh(visit)

        return {
            "status": 200,
            "message": "Visit status updated successfully",
            "data": {
                "visit_id": visit.id,
                "new_status": visit.final_status
            }
        }

    except Exception as e:
        print(f"Error updating visit status: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/details/{visit_id}")
def get_visit_details(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        user = db.query(Executives).filter(Executives.id == visit.user_id).first()

        # Get followup attempts for this visit
        followup_attempts = db.query(FollowupAttempts).filter(
            FollowupAttempts.visit_id == visit_id
        ).order_by(FollowupAttempts.attempt_number.desc()).all()

        # Format followup data
        followup_data = []
        for attempt in followup_attempts:
            followup_data.append({
                "id": attempt.id,
                "followup_date": attempt.followup_date.isoformat() if attempt.followup_date else None,
                "next_followup_date": attempt.next_followup_date.isoformat() if attempt.next_followup_date else None,
                "followup_type": attempt.followup_type,
                "notes": attempt.notes,
                "decision_maker_involved": attempt.decision_maker_involved,
                "attempt_number": attempt.attempt_number
            })

        visit_details = {
            "id": visit.id,
            "user_id": visit.user_id,
            "user_name": user.name if user else "Unknown User",
            "start_date": visit.start_date.isoformat() if visit.start_date else None,
            "check_in_time": visit.check_in_time.isoformat() if visit.check_in_time else None,
            "check_out_time": visit.check_out_time.isoformat() if visit.check_out_time else None,
            "gym_name": visit.gym_name,
            "gym_address": visit.gym_address,
            "referal_id":visit.referal_id,
            "contact_person": visit.contact_person,
            "contact_phone": visit.contact_phone,
            "inquired_person1": visit.inquired_person1,
            "inquired_phone1": visit.inquired_phone1,
            "inquired_person2": visit.inquired_person2,
            "inquired_phone2": visit.inquired_phone2,
            "visit_purpose": visit.visit_purpose,
            "visit_purpose_other": visit.visit_purpose_other,

            "check_in_location": visit.check_in_location,
            "exterior_photo": visit.exterior_photo,
            "attendance_selfie": visit.attendance_selfie,
            "facility_photos": visit.facility_photos or [],

            "gym_size": visit.gym_size,
            "total_member_count": visit.total_member_count,
            "active_member_count": visit.active_member_count,
            "expected_member_count": visit.expected_member_count,
            "conversion_probability": visit.conversion_probability,
            "operating_hours": visit.operating_hours or [],
            "current_tech": visit.current_tech,

            "people_met": visit.people_met,
            "meeting_duration": visit.meeting_duration,
            "presentation_given": visit.presentation_given,
            "demo_provided": visit.demo_provided,
            "interest_level": visit.interest_level,
            "questions_asked": visit.questions_asked,
            "objections": visit.objections,

            "decision_maker_present": visit.decision_maker_present,
            "decision_timeline": visit.decision_timeline,
            "competitors": visit.competitors,
            "pain_points": visit.pain_points,
            "current_solutions": visit.current_solutions,
            "key_benefits": visit.key_benefits,

            "next_steps": visit.next_steps,
            "materials_to_send": visit.materials_to_send,
            "visit_outcome": visit.visit_outcome,
            "visit_summary": visit.visit_summary,
            "action_items": visit.action_items,
            "overall_rating": visit.overall_rating,

            "final_status": visit.final_status,
            "next_follow_up_date": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
            "rejection_reason": visit.rejection_reason,
            "next_meeting_date": visit.next_meeting_date.isoformat() if visit.next_meeting_date else None,
            "follow_up_notes": visit.follow_up_notes,
            "conversion_notes": visit.conversion_notes,

            "completed": visit.completed,
            "current_step": visit.current_step,
            "created_at": visit.created_at.isoformat() if visit.created_at else None,
            "updated_at": visit.updated_at.isoformat() if visit.updated_at else None,
            "monthly_leads": visit.monthly_leads,
            "monthly_conversion": visit.monthly_conversion,
            "checklist": visit.checklist,
            "decision_maker_present": visit.decision_maker_present,
            "followup": followup_data
        }

        return {
            "status": 200,
            "message": "Visit details retrieved successfully",
            "data": visit_details
        }

    except Exception as e:
        print(f"Error getting visit details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

class UpdateVisitStatusRequest(BaseModel):
    visit_id: int
    new_status: str
    notes: Optional[str] = None
    next_follow_up_date: Optional[str] = None
    next_meeting_date: Optional[str] = None
    conversion_notes: Optional[str] = None
    rejection_reason: Optional[str] = None
    follow_up_agenda: Optional[str] = None
    contract_value: Optional[str] = None
    implementation_timeline: Optional[str] = None
    checklist: Optional[Dict[str, Any]] = None

@router.put("/tracker/update-visit-status")
def update_visit_status_tracker(request: UpdateVisitStatusRequest, db: Session = Depends(get_db)):
    try:


        visit = db.query(GymVisits).filter(GymVisits.id == request.visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        print(f"Visit found - ID: {visit.id}, Current checklist: {visit.checklist}")

        visit.final_status = request.new_status
        visit.updated_at = datetime.now()


        if request.new_status == 'followup':
            print("Status is FOLLOWUP")
            if request.notes:
                visit.follow_up_notes = request.notes
            if request.next_follow_up_date:
                visit.next_follow_up_date = datetime.fromisoformat(request.next_follow_up_date.replace('Z', '+00:00'))
            if request.follow_up_agenda:
                visit.action_items = request.follow_up_agenda

        elif request.new_status == 'converted':
            print("Status is CONVERTED")
            if request.checklist:
                print(f"Checklist provided: {request.checklist}")
                visit.checklist = request.checklist
                print(f"Checklist set to visit object: {visit.checklist}")
            else:
                print("WARNING: No checklist provided in request!")

            if request.conversion_notes:
                visit.conversion_notes = request.conversion_notes
            if request.next_meeting_date:
                visit.next_meeting_date = datetime.fromisoformat(request.next_meeting_date.replace('Z', '+00:00'))
            if request.contract_value or request.implementation_timeline:
                conversion_details = {}
                if request.contract_value:
                    conversion_details['contract_value'] = request.contract_value
                if request.implementation_timeline:
                    conversion_details['implementation_timeline'] = request.implementation_timeline

                existing_notes = visit.conversion_notes or ""
                additional_info = f"\nContract Value: {request.contract_value or 'Not specified'}\nImplementation Timeline: {request.implementation_timeline or 'Not specified'}"
                visit.conversion_notes = existing_notes + additional_info

        elif request.new_status == 'rejected':
            print("Status is REJECTED")
            if request.rejection_reason:
                visit.rejection_reason = request.rejection_reason

        # Sync conversion_status in gym_assignments table
        if visit.gym_id:
            gym_assignment = db.query(GymAssignments).filter(
                and_(
                    GymAssignments.gym_id == visit.gym_id,
                    GymAssignments.executive_id == visit.user_id,
                    GymAssignments.status == 'assigned'
                )
            ).first()

            if gym_assignment:
                gym_assignment.conversion_status = request.new_status
                gym_assignment.updated_at = datetime.now()

        print(f"Before commit - visit.checklist: {visit.checklist}")
        db.commit()
        print("Database commit successful")

        db.refresh(visit)
        print(f"After refresh - visit.checklist: {visit.checklist}")

        print("=" * 80)
        print("UPDATE VISIT STATUS - DEBUG END")
        print("=" * 80)

        return {
            "status": 200,
            "message": "Visit status updated successfully",
            "data": {
                "visit_id": visit.id,
                "new_status": visit.final_status,
                "updated_at": visit.updated_at.isoformat(),
                "checklist": visit.checklist
            }
        }

    except Exception as e:
        print(f"Error updating visit status: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/tracker/status-options")
def get_status_update_options():
    try:
        status_options = {
            "followup": {
                "value": "followup",
                "label": "Follow-up Required",
                "icon": "time-outline",
                "colors": ["#FF9800", "#FFB74D"],
                "description": "Schedule another follow-up meeting",
                "fields": ["notes", "next_follow_up_date", "follow_up_agenda"]
            },
            "converted": {
                "value": "converted",
                "label": "Converted",
                "icon": "checkmark-circle-outline",
                "colors": ["#4CAF50", "#66BB6A"],
                "description": "Successfully converted to customer",
                "fields": ["conversion_notes", "next_meeting_date", "contract_value", "implementation_timeline"]
            },
            "rejected": {
                "value": "rejected",
                "label": "Rejected",
                "icon": "close-circle-outline",
                "colors": ["#F44336", "#EF5350"],
                "description": "Not interested in our services",
                "fields": ["rejection_reason"]
            }
        }

        return {
            "status": 200,
            "message": "Status options retrieved successfully",
            "data": status_options
        }

    except Exception as e:
        print(f"Error getting status options: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/tracker/pending")
def get_pending_visits(
    user_id: int = Query(...),
    role: str = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db)
):
    try:
       
        if role == 'BDE':
            query = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == False
            )
        elif role == 'BDM':
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]

            print(f"Manager's team executive IDs: {executive_ids}")

            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned visits")
                query = db.query(GymVisits).filter(
                    GymVisits.user_id.is_(None),  # user_id is NULL for self-assigned
                    GymVisits.manager_id == user_id,  # Manager's self-assigned visits
                    GymVisits.completed == False
                )
            else:
                # Include both executive visits AND self-assigned visits (where user_id is NULL and manager_id matches)
                print("Including ALL visits (team + self-assigned)")
                query = db.query(GymVisits).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),  # Team executive visits
                        and_(
                            GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                            GymVisits.manager_id == user_id  # Manager's self-assigned visits
                        )
                    ),
                    GymVisits.completed == False
                )
        else:
            raise HTTPException(status_code=400, detail="Invalid role")

        query = apply_date_filters(query, start_date, end_date)

        if role == 'BDM' and not self_assigned:  # Don't apply BDE filter when filtering for self-assigned
            query = apply_bde_filter(query, bde_id)

        pending_visits = query.order_by(GymVisits.assigned_date.asc()).all()
        print(f"Found {len(pending_visits)} pending visits")

        # === BATCH LOAD: Get all gyms at once (N+1 FIX) ===
        gym_ids = list({v.gym_id for v in pending_visits if v.gym_id})
        gym_self_assigned_map = {}
        if gym_ids:
            gyms = db.query(GymDatabase).filter(GymDatabase.id.in_(gym_ids)).all()
            gym_self_assigned_map = {g.id: g.self_assigned for g in gyms}

        visit_data = []
        for visit in pending_visits:
            # Check if this is a self-assigned visit from pre-loaded map (was N+1 query)
            is_self_assigned = bool(gym_self_assigned_map.get(visit.gym_id)) if visit.gym_id else False

            visit_info = {
                "id": visit.id,
                "user_id": visit.user_id,
                "gym_name": visit.gym_name or "New Visit",
                "gym_address": visit.gym_address,
                "referal_id":visit.referal_id,
                "contact_person": visit.contact_person,
                "contact_phone": visit.contact_phone,
                "visit_purpose": visit.visit_purpose,
                "current_step": visit.current_step,
                "total_steps": 8,
                "progress_percentage": round((visit.current_step / 8) * 100),
                "exterior_photo": visit.exterior_photo,
                "created_at": visit.assigned_date.isoformat() if visit.assigned_date else None,
                "updated_at": visit.updated_at.isoformat() if visit.updated_at else None,
                "start_date": visit.start_date.isoformat() if visit.start_date else None,
                "is_self_assigned": is_self_assigned
            }
            visit_data.append(visit_info)


        visit_data.sort(key=lambda x: (not x.get('is_self_assigned', False), x.get('created_at', '')))


        return {
            "status": 200,
            "message": "Pending visits retrieved successfully",
            "data": visit_data
        }

    except Exception as e:
        print(f"Error getting pending visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/tracker/{user_id}/{role}")
def get_tracker_data_with_all_filters(
    user_id: int,
    role: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    pincode: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
):
    return get_enhanced_tracker_data(
        user_id, role, start_date, end_date, bde_id,
        state, city, area, pincode, search, self_assigned, db
    )

@router.get("/tracker/{user_id}/{role}/followups")
def get_followup_visits_with_filters(
    user_id: int,
    role: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        print(f"\n=== FOLLOWUP VISITS API ===")
        print(f"user_id: {user_id}, role: {role}, self_assigned filter: {self_assigned}")

        if role == "BDE":
            query = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == True,
                GymVisits.final_status == 'followup'
            )
        elif role == "BDM":
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]

            print(f"Manager's team executive IDs: {executive_ids}")

            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned visits")
                query = db.query(GymVisits).filter(
                    GymVisits.user_id.is_(None),  # user_id is NULL for self-assigned
                    GymVisits.manager_id == user_id,  # Manager's self-assigned visits
                    GymVisits.final_status == 'followup',
                    GymVisits.completed == True
                )
            else:
                # Include both executive visits AND self-assigned visits (where user_id is NULL and manager_id matches)
                print("Including ALL visits (team + self-assigned)")
                query = db.query(GymVisits).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),  # Team executive visits
                        and_(
                            GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                            GymVisits.manager_id == user_id  # Manager's self-assigned visits
                        )
                    ),
                    GymVisits.final_status == 'followup',
                    GymVisits.completed == True
                )
        else:
            raise HTTPException(status_code=400, detail="Invalid role")

        query = apply_date_filters(query, start_date, end_date)

        if role == 'BDM' and not self_assigned:  # Don't apply BDE filter when filtering for self-assigned
            query = apply_bde_filter(query, bde_id)

        visits = query.order_by(GymVisits.next_follow_up_date.asc()).all()
        print(f"Found {len(visits)} followup visits")

        # === BATCH LOAD: Get all gyms at once (N+1 FIX) ===
        gym_ids = list({v.gym_id for v in visits if v.gym_id})
        gym_self_assigned_map = {}
        if gym_ids:
            gyms = db.query(GymDatabase).filter(GymDatabase.id.in_(gym_ids)).all()
            gym_self_assigned_map = {g.id: g.self_assigned for g in gyms}

        visit_data = []
        for visit in visits:
            # Check if this is a self-assigned visit from pre-loaded map (was N+1 query)
            is_self_assigned = bool(gym_self_assigned_map.get(visit.gym_id)) if visit.gym_id else False

            visit_info = {
                "id": visit.id,
                "gym_name": visit.gym_name,
                "gym_address": visit.gym_address,
                "contact_person": visit.contact_person,
                "referal_id":visit.referal_id,
                "contact_phone": visit.contact_phone,
                "exterior_photo": visit.exterior_photo,
                "last_contact": visit.check_out_time.isoformat() if visit.check_out_time else None,
                "next_follow_up": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
                "status": "pending" if visit.next_follow_up_date and visit.next_follow_up_date > datetime.now() else "overdue",
                'total_followup_attempts': visit.total_followup_attempts if visit.total_followup_attempts else 0,
                "notes": visit.follow_up_notes,
                "visit_summary": visit.visit_summary,
                "interest_level": visit.interest_level,
                "decision_timeline": visit.decision_timeline,
                "pain_points": visit.pain_points,
                "overall_rating": visit.overall_rating,
                "follow_up_agenda": visit.action_items,
                "completed": visit.completed,
                "created_at": visit.created_at.isoformat() if visit.created_at else None,
                "is_self_assigned": is_self_assigned
            }
            visit_data.append(visit_info)

        return {
            "status": 200,
            "message": "Followup visits retrieved successfully",
            "data": visit_data
        }

    except Exception as e:
        print(f"Error getting followup visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/tracker/{user_id}/{role}/converted")
def get_converted_visits_with_filters(
    user_id: int,
    role: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        print(f"\n=== CONVERTED VISITS API ===")
        print(f"user_id: {user_id}, role: {role}, self_assigned filter: {self_assigned}")

        if role == "BDE":
            query = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == True,
                GymVisits.final_status == 'converted'
            )
        elif role == "BDM":
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]

            print(f"Manager's team executive IDs: {executive_ids}")

            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned visits")
                query = db.query(GymVisits).filter(
                    GymVisits.user_id.is_(None),  # user_id is NULL for self-assigned
                    GymVisits.manager_id == user_id,  # Manager's self-assigned visits
                    GymVisits.final_status == 'converted',
                    GymVisits.completed == True
                )
            else:
                # Include both executive visits AND self-assigned visits (where user_id is NULL and manager_id matches)
                print("Including ALL visits (team + self-assigned)")
                query = db.query(GymVisits).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),  # Team executive visits
                        and_(
                            GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                            GymVisits.manager_id == user_id  # Manager's self-assigned visits
                        )
                    ),
                    GymVisits.final_status == 'converted',
                    GymVisits.completed == True
                )
        else:
            raise HTTPException(status_code=400, detail="Invalid role")

        query = apply_date_filters(query, start_date, end_date)

        if role == 'BDM' and not self_assigned:  # Don't apply BDE filter when filtering for self-assigned
            query = apply_bde_filter(query, bde_id)

        visits = query.order_by(GymVisits.updated_at.desc()).all()
        print(f"Found {len(visits)} converted visits")

        visit_data = []
        for visit in visits:
            visit_info = get_visit_with_feedback_info(visit, db)  
            visit_info["converted_date"] = visit.updated_at.isoformat() if visit.updated_at else None
            visit_data.append(visit_info)

        return {
            "status": 200,
            "message": "Converted visits retrieved successfully",
            "data": visit_data
        }

    except Exception as e:
        print(f"Error getting converted visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/tracker/{user_id}/{role}/rejected")
def get_rejected_visits_with_filters(
    user_id: int,
    role: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        print(f"\n=== REJECTED VISITS API ===")
        print(f"user_id: {user_id}, role: {role}, self_assigned filter: {self_assigned}")

        if role == "BDE":
            query = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == True,
                GymVisits.final_status == 'rejected'
            )
        elif role == "BDM":
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]

            print(f"Manager's team executive IDs: {executive_ids}")

            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned visits")
                query = db.query(GymVisits).join(
                    GymDatabase, GymVisits.gym_id == GymDatabase.id
                ).filter(
                    GymVisits.user_id == user_id,  # Only manager's own visits
                    GymDatabase.self_assigned == True,  # Only self-assigned gyms
                    GymVisits.final_status == 'rejected',
                    GymVisits.completed == True
                )
            else:
                # Include both executive visits AND self-assigned visits
                print("Including ALL visits (team + self-assigned)")
                query = db.query(GymVisits).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),
                        GymVisits.user_id == user_id  # Self-assigned visits
                    ),
                    GymVisits.final_status == 'rejected',
                    GymVisits.completed == True
                )
        else:
            raise HTTPException(status_code=400, detail="Invalid role")

        query = apply_date_filters(query, start_date, end_date)

        if role == 'BDM' and not self_assigned:  # Don't apply BDE filter when filtering for self-assigned
            query = apply_bde_filter(query, bde_id)

        visits = query.order_by(GymVisits.updated_at.desc()).all()
        print(f"Found {len(visits)} rejected visits")

        visit_data = []
        for visit in visits:
            visit_info = get_visit_with_feedback_info(visit, db)  
            visit_info["rejected_date"] = visit.updated_at.isoformat() if visit.updated_at else None
            visit_info["reason"] = visit.rejection_reason or "Not specified"
            visit_data.append(visit_info)

        return {
            "status": 200,
            "message": "Rejected visits retrieved successfully",
            "data": visit_data
        }

    except Exception as e:
        print(f"Error getting rejected visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/bulk-assign")
def bulk_assign_gyms(
    request: dict,
    db: Session = Depends(get_db)
):
    try:
        executive_id = request.get('executive_id')
        manager_id = request.get('manager_id')
        gym_assignments = request.get('gym_assignments', [])
        visit_date = request.get('visit_date')

        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found or not under this manager")

        created_visits = []
        
        for gym_data in gym_assignments:
            gym_id = gym_data.get('gym_id')
            
            gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
            if not gym:
                continue  
                
            existing_assignment = db.query(GymAssignments).filter(
                GymAssignments.gym_id == gym_id,
                GymAssignments.status == 'assigned'
            ).first()
            
            if existing_assignment:
                continue  
            
            assignment = GymAssignments(
                gym_id=gym_id,
                executive_id=executive_id,
                referal_id=gym.referal_id,
                manager_id=manager_id,
                status='assigned',
                conversion_status='pending',
                assigned_date=datetime.fromisoformat(visit_date.replace('Z', '+00:00')),
                assigned_on=datetime.now(),
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(assignment)

            new_visit = GymVisits(
                user_id=executive_id,
                gym_id=gym_id,
                start_date=datetime.now(),
                assigned_date=datetime.fromisoformat(visit_date.replace('Z', '+00:00')),
                assigned_on=datetime.now(),
                gym_name=gym.gym_name,
                gym_address=gym.address,
                referal_id=gym.referal_id,
                contact_person=gym.contact_person or '',
                contact_phone=gym.contact_phone or '',
                visit_type='sales_call',
                status='assigned',
                visit_purpose='Initial gym visit and assessment',
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
            created_visits.append({
                'gym_id': gym_id,
                'gym_name': gym.gym_name
            })

        db.commit()

        return {
            "status": 200,
            "message": f"Successfully assigned {len(created_visits)} gyms to {executive.name}",
            "data": {
                "assigned_gyms": created_visits,
                "total_assigned": len(created_visits)
            }
        }

    except Exception as e:
        print(f"Error in bulk assignment: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/executive-analytics/{executive_id}")
def get_executive_analytics(
    executive_id: int,
    manager_id: int = Query(...),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")

        query = db.query(GymVisits).filter(GymVisits.user_id == executive_id)
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            query = query.filter(GymVisits.created_at >= start_dt)
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            query = query.filter(GymVisits.created_at <= end_dt)

        visits = query.all()
        completed_visits = [v for v in visits if v.completed]
        
        total_visits = len(visits)
        total_completed = len(completed_visits)
        
        status_breakdown = {}
        for status in ['pending', 'followup', 'converted', 'rejected', 'scheduled']:
            status_breakdown[status] = len([v for v in visits if v.final_status == status])
        
        conversion_rate = round((status_breakdown['converted'] / total_completed * 100) if total_completed > 0 else 0, 1)
        
        monthly_data = {}
        for visit in visits:
            month_key = visit.created_at.strftime('%Y-%m')
            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    'total': 0,
                    'completed': 0,
                    'converted': 0
                }
            monthly_data[month_key]['total'] += 1
            if visit.completed:
                monthly_data[month_key]['completed'] += 1
            if visit.final_status == 'converted':
                monthly_data[month_key]['converted'] += 1

        avg_rating = round(sum(v.overall_rating for v in completed_visits if v.overall_rating) / len(completed_visits), 1) if completed_visits else 0
        avg_interest = round(sum(v.interest_level for v in completed_visits if v.interest_level) / len(completed_visits), 1) if completed_visits else 0

        pain_points = []
        for visit in completed_visits:
            if visit.pain_points:
                pain_points.append(visit.pain_points)
        
        analytics = {
            "executive_info": {
                "id": executive.id,
                "name": executive.name,
                "email": executive.email,
                "employee_id": executive.emp_id
            },
            "performance_metrics": {
                "total_visits": total_visits,
                "completed_visits": total_completed,
                "conversion_rate": conversion_rate,
                "average_rating": avg_rating,
                "average_interest_level": avg_interest
            },
            "status_breakdown": status_breakdown,
            "monthly_performance": monthly_data,
            "insights": {
                "most_common_pain_points": pain_points[:5],  # Top 5
                "best_performing_month": max(monthly_data.keys(), key=lambda k: monthly_data[k]['converted']) if monthly_data else None,
                "completion_rate": round((total_completed / total_visits * 100) if total_visits > 0 else 0, 1)
            }
        }

        return {
            "status": 200,
            "message": "Executive analytics retrieved successfully",
            "data": analytics
        }

    except Exception as e:
        print(f"Error getting executive analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/assignment-calendar/{executive_id}")
def get_assignment_calendar(
    executive_id: int,
    manager_id: int = Query(...),
    month: int = Query(...),
    year: int = Query(...),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")

        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        visits = db.query(GymVisits).filter(
            GymVisits.user_id == executive_id,
            GymVisits.assigned_date >= start_date,
            GymVisits.assigned_date < end_date
        ).all()

        calendar_data = {}
        for visit in visits:
            if visit.assigned_date:
                date_key = visit.assigned_date.strftime('%Y-%m-%d')
                if date_key not in calendar_data:
                    calendar_data[date_key] = []
                
                calendar_data[date_key].append({
                    "visit_id": visit.id,
                    "gym_name": visit.gym_name,
                    "gym_address": visit.gym_address,
                    "referal_id":visit.referal_id,
                    "contact_person": visit.contact_person,
                    "contact_phone": visit.contact_phone,
                    "visit_type": visit.visit_type,
                    "status": visit.status,
                    "final_status": visit.final_status,
                    "completed": visit.completed,
                    "overall_rating": visit.overall_rating
                })

        return {
            "status": 200,
            "message": "Assignment calendar retrieved successfully",
            "data": {
                "month": month,
                "year": year,
                "executive_name": executive.name,
                "calendar_data": calendar_data,
                "total_assignments": len(visits)
            }
        }

    except Exception as e:
        print(f"Error getting assignment calendar: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/unassigned-gyms")
def get_unassigned_gyms(
    manager_id: int = Query(...),
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        subquery = db.query(GymAssignments.gym_id).filter(
            GymAssignments.status == 'assigned',
            GymAssignments.conversion_status.in_(['pending', 'followup', 'scheduled'])
        ).subquery()

        query = db.query(GymDatabase).filter(
            ~GymDatabase.id.in_(subquery)
        )

        if state:
            query = query.filter(GymDatabase.state == state)
        if city:
            query = query.filter(GymDatabase.city == city)
        if area:
            query = query.filter(GymDatabase.area == area)

        total_count = query.count()
        unassigned_gyms = query.offset(offset).limit(limit).all()

        gym_data = []
        for gym in unassigned_gyms:
            gym_data.append({
                "id": gym.id,
                "gym_name": gym.gym_name,
                "area": gym.area,
                "referal_id":gym.referal_id,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "operating_hours": gym.operating_hours
            })

        return {
            "status": 200,
            "message": "Unassigned gyms retrieved successfully",
            "data": {
                "gyms": gym_data,
                "total_count": total_count,
                "returned_count": len(gym_data),
                "has_more": (offset + limit) < total_count
            }
        }

    except Exception as e:
        print(f"Error getting unassigned gyms: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/manager-dashboard-summary/{manager_id}")
def get_manager_dashboard_summary(
    manager_id: int,
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executives = db.query(Executives).filter(
            Executives.manager_id == manager_id,
            Executives.status == 'active'
        ).all()
        
        executive_ids = [exec.id for exec in executives]

        total_assignments = db.query(GymAssignments).filter(
            GymAssignments.manager_id == manager_id,
            GymAssignments.status == 'assigned'
        ).count()

        total_visits = db.query(GymVisits).filter(
            GymVisits.user_id.in_(executive_ids)
        ).count()

        completed_visits = db.query(GymVisits).filter(
            GymVisits.user_id.in_(executive_ids),
            GymVisits.completed == True
        ).count()

        conversion_stats = db.query(
            GymVisits.final_status,
            db.func.count(GymVisits.id).label('count')
        ).filter(
            GymVisits.user_id.in_(executive_ids),
            GymVisits.completed == True
        ).group_by(GymVisits.final_status).all()

        status_breakdown = {}
        for status, count in conversion_stats:
            status_breakdown[status] = count

        # === BATCH LOAD: Get all executive stats at once (N+1 FIX) ===
        # Get total visits per executive
        exec_visits_stats = db.query(
            GymVisits.user_id,
            db.func.count(GymVisits.id).label('total'),
            db.func.sum(db.func.cast(GymVisits.completed == True, db.Integer)).label('completed'),
            db.func.sum(db.func.cast(GymVisits.final_status == 'converted', db.Integer)).label('converted')
        ).filter(
            GymVisits.user_id.in_(executive_ids)
        ).group_by(GymVisits.user_id).all()

        # Build stats map
        exec_stats_map = {}
        for stat in exec_visits_stats:
            exec_stats_map[stat.user_id] = {
                'total': stat.total or 0,
                'completed': stat.completed or 0,
                'converted': stat.converted or 0
            }

        executive_summary = []
        for executive in executives:
            # Get stats from pre-loaded map (was 3 N+1 queries)
            stats = exec_stats_map.get(executive.id, {'total': 0, 'completed': 0, 'converted': 0})
            exec_visits = stats['total']
            exec_completed = stats['completed']
            exec_converted = stats['converted']

            executive_summary.append({
                "executive_id": executive.id,
                "executive_name": executive.name,
                "total_visits": exec_visits,
                "completed_visits": exec_completed,
                "converted_visits": exec_converted,
                "conversion_rate": round((exec_converted / exec_completed * 100) if exec_completed > 0 else 0, 1)
            })

        summary = {
            "manager_info": {
                "id": manager.id,
                "name": manager.name,
                "email": manager.email
            },
            "team_metrics": {
                "total_executives": len(executives),
                "total_assignments": total_assignments,
                "total_visits": total_visits,
                "completed_visits": completed_visits,
                "completion_rate": round((completed_visits / total_visits * 100) if total_visits > 0 else 0, 1)
            },
            "conversion_metrics": status_breakdown,
            "executive_performance": executive_summary
        }

        return {
            "status": 200,
            "message": "Manager dashboard summary retrieved successfully",
            "data": summary
        }

    except Exception as e:
        print(f"Error getting manager dashboard summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@router.patch("/{visit_id}/followup")
def update_followup_date(
    visit_id: int,
    request: dict,
    db: Session = Depends(get_db)
):
    try:
        manager_id = request.get('manager_id')
        next_follow_up_date = request.get('next_follow_up_date')
        notes = request.get('notes')
        agenda = request.get('follow_up_agenda')
        
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        executive = db.query(Executives).filter(
                Executives.id == visit.user_id,
                Executives.manager_id == manager_id
        ).first()
        
        if not executive:
            raise HTTPException(status_code=403, detail="Unauthorized to update this visit")

        if next_follow_up_date:
            visit.next_follow_up_date = datetime.fromisoformat(next_follow_up_date.replace('Z', '+00:00'))
        
        if notes:
            visit.follow_up_notes = notes
            
        if agenda:
            visit.action_items = agenda

        visit.updated_at = datetime.now()
        
        db.commit()
        db.refresh(visit)

        return {
            "status": 200,
            "message": "Follow-up date updated successfully",
            "data": {
                "visit_id": visit.id,
                "next_follow_up_date": visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
                "follow_up_notes": visit.follow_up_notes,
                "follow_up_agenda": visit.action_items  
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating follow-up date: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

def apply_search_filter(query, search_term: Optional[str] = None):
    if search_term:
        search_pattern = f"%{search_term.lower()}%"
        query = query.filter(
            or_(
                GymVisits.gym_name.ilike(search_pattern),
                GymVisits.gym_address.ilike(search_pattern),
                GymVisits.contact_person.ilike(search_pattern),
                GymVisits.contact_phone.ilike(search_pattern),
                GymVisits.pain_points.ilike(search_pattern),
                GymVisits.current_tech.ilike(search_pattern),
                GymVisits.competitors.ilike(search_pattern)
            )
        )
    return query

def apply_location_filters(query, state: Optional[str] = None, city: Optional[str] = None, 
                          area: Optional[str] = None, pincode: Optional[str] = None):
    if state:
        query = query.filter(GymDatabase.state == state)
    if city:
        query = query.filter(GymDatabase.city == city)
    if area:
        query = query.filter(GymDatabase.area == area)
    if pincode:
        query = query.filter(GymDatabase.pincode == pincode)
    return query

@router.get("/tracker/{user_id}/{role}/enhanced")
def get_enhanced_tracker_data(
    user_id: int,
    role: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bde_id: Optional[int] = Query(None),
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    pincode: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    self_assigned: Optional[bool] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        user = db.query(Executives).filter(Executives.id == user_id).first()
        manager = db.query(Managers).filter(Managers.id == user_id).first()
        if not user and not manager:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_profile = {}
        if role == "BDE":
            user_profile = {
                'id': user.id,
                'name': user.name,
                'email': user.email,
                'contact': user.contact,
                'profile': user.profile,
                'role': 'BDE',  
                'user_type': 'executive',
                'joined_date': user.joined_date.isoformat() if user.joined_date else None,
                'status': user.status,
                'employee_id': user.emp_id
            }
        elif role == "BDM":
            user_profile = {
                'id': manager.id,
                'name': manager.name,
                'email': manager.email,
                'contact': manager.contact,
                'profile': manager.profile,
                'role': 'BDM', 
                'user_type': 'manager',
                'joined_date': manager.joined_date.isoformat() if manager.joined_date else None,
                'status': manager.status,
                'employee_id': manager.emp_id
            }
        
        if role == 'BDM':
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]

            print(f"\n=== TRACKER SUMMARY API - COMPLETED ===")
            print(f"user_id: {user_id}, role: {role}, self_assigned filter: {self_assigned}")
            print(f"Manager's team executive IDs: {executive_ids}")

            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned completed visits")
                query = db.query(GymVisits).outerjoin(
                    GymDatabase, GymVisits.gym_id == GymDatabase.id
                ).filter(
                    GymVisits.user_id == user_id,  # Only manager's own visits
                    GymDatabase.self_assigned == True,  # Only self-assigned gyms
                    GymVisits.completed == True
                )
            else:
                # Include both executive visits AND self-assigned visits
                print("Including ALL completed visits (team + self-assigned)")
                query = db.query(GymVisits).outerjoin(
                    GymDatabase, GymVisits.gym_id == GymDatabase.id
                ).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),
                        GymVisits.user_id == user_id  # Self-assigned visits
                    ),
                    GymVisits.completed == True
                )
        else:
            query = db.query(GymVisits).outerjoin(
                GymDatabase, GymVisits.gym_id == GymDatabase.id
            ).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == True
            )

        query = apply_date_filters(query, start_date, end_date)
        query = apply_location_filters(query, state, city, area, pincode)
        query = apply_search_filter(query, search)

        # Don't apply BDE filter when filtering for self-assigned gyms
        if role == 'BDM' and bde_id and not self_assigned:
            query = apply_bde_filter(query, bde_id)

        completed_visits = query.order_by(GymVisits.updated_at.desc()).all()

        followup_visits = []
        converted_visits = []
        rejected_visits = []
        scheduled_visits = []
        pending_visits = []

        for visit in completed_visits:
            visit_data = get_visit_with_feedback_info(visit, db)

            if visit.final_status == 'followup':
                followup_visits.append(visit_data)
            elif visit.final_status == 'converted':
                converted_visits.append(visit_data)
            elif visit.final_status == 'rejected':
                rejected_visits.append(visit_data)
            elif visit.final_status == 'scheduled':
                scheduled_visits.append(visit_data)
            else:
                pending_visits.append(visit_data)

        print(f"\n=== TRACKER SUMMARY API - PENDING ===")
        print(f"user_id: {user_id}, role: {role}, self_assigned filter: {self_assigned}")

        if role == 'BDM':
            # If self_assigned filter is true, only show self-assigned gyms
            if self_assigned:
                print("Filtering for ONLY self-assigned pending visits")
                pending_query = db.query(GymVisits).outerjoin(
                    GymDatabase, GymVisits.gym_id == GymDatabase.id
                ).filter(
                    GymVisits.user_id == user_id,  # Only manager's own visits
                    GymDatabase.self_assigned == True,  # Only self-assigned gyms
                    GymVisits.completed == False
                )
            else:
                # Include both executive visits AND self-assigned visits
                print("Including ALL pending visits (team + self-assigned)")
                pending_query = db.query(GymVisits).outerjoin(
                    GymDatabase, GymVisits.gym_id == GymDatabase.id
                ).filter(
                    or_(
                        GymVisits.user_id.in_(executive_ids),
                        GymVisits.user_id == user_id  # Self-assigned visits
                    ),
                    GymVisits.completed == False
                )
        else:
            pending_query = db.query(GymVisits).outerjoin(
                GymDatabase, GymVisits.gym_id == GymDatabase.id
            ).filter(
                GymVisits.user_id == user_id,
                GymVisits.completed == False
            )

        pending_query = apply_date_filters(pending_query, start_date, end_date)
        pending_query = apply_location_filters(pending_query, state, city, area, pincode)
        pending_query = apply_search_filter(pending_query, search)

        # Don't apply BDE filter when filtering for self-assigned gyms
        if role == 'BDM' and bde_id and not self_assigned:
            pending_query = apply_bde_filter(pending_query, bde_id)

        pending_visits_data = pending_query.order_by(GymVisits.created_at.desc()).all()
        
        for visit in pending_visits_data:
            visit_info = {
                "id": visit.id,
                "gym_name": visit.gym_name or "New Visit",
                "gym_address": visit.gym_address,
                "referal_id": visit.referal_id,
                "contact_person": visit.contact_person,
                "contact_phone": visit.contact_phone,
                "exterior_photo": visit.exterior_photo,
                "final_status": "pending",
                "completed": visit.completed,
                "current_step": visit.current_step,
                "created_at": visit.created_at.isoformat() if visit.created_at else None,
                "updated_at": visit.updated_at.isoformat() if visit.updated_at else None
            }
            pending_visits.append(visit_info)

        total_completed = len(completed_visits)
        conversion_rate = round((len(converted_visits) / total_completed * 100) if total_completed > 0 else 0, 1)

        print(f"\n=== TRACKER SUMMARY COUNTS ===")
        print(f"Pending: {len(pending_visits)}")
        print(f"Converted: {len(converted_visits)}")
        print(f"Followup: {len(followup_visits)}")
        print(f"Rejected: {len(rejected_visits)}")
        print(f"Total Completed: {total_completed}")

        summary = {
            "total_completed": total_completed,
            "total_pending": len(pending_visits),
            "followup_count": len(followup_visits),
            "converted_count": len(converted_visits),
            "rejected_count": len(rejected_visits),
            "scheduled_count": len(scheduled_visits),
            "conversion_rate": conversion_rate,
            "average_rating": round(sum(v.overall_rating for v in completed_visits if v.overall_rating) / total_completed, 1) if total_completed > 0 else 0
        }

        all_visits = followup_visits + converted_visits + rejected_visits + scheduled_visits + pending_visits

        return {
            "status": 200,
            "message": "Enhanced tracker data retrieved successfully",
            "data": {
                "all_visits": all_visits,
                "followup_visits": followup_visits,
                "converted_visits": converted_visits,
                "rejected_visits": rejected_visits,
                "scheduled_visits": scheduled_visits,
                "pending_visits": pending_visits,
                "summary": summary,
                "user_profile": user_profile,
            }
        }

    except Exception as e:
        print(f"Error getting enhanced tracker data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/location-filters/{user_id}/{role}")
def get_location_filter_options(
    user_id: int,
    role: str,
    db: Session = Depends(get_db)
):
    try:
        if role == 'BDM':
            executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
            executive_ids = [exec.id for exec in executives]
            
            location_query = db.query(GymDatabase).join(
                GymVisits, GymVisits.gym_id == GymDatabase.id
            ).filter(
                GymVisits.user_id.in_(executive_ids)
            ).distinct()
        else:
            location_query = db.query(GymDatabase).join(
                GymVisits, GymVisits.gym_id == GymDatabase.id
            ).filter(
                GymVisits.user_id == user_id
            ).distinct()

        states = db.query(GymDatabase.state).join(
            GymVisits, GymVisits.gym_id == GymDatabase.id
        )
        if role == 'BDM':
            states = states.filter(GymVisits.user_id.in_(executive_ids))
        else:
            states = states.filter(GymVisits.user_id == user_id)
        
        states = states.distinct().filter(GymDatabase.state.isnot(None)).all()
        
        cities = db.query(GymDatabase.city).join(
            GymVisits, GymVisits.gym_id == GymDatabase.id
        )
        if role == 'BDM':
            cities = cities.filter(GymVisits.user_id.in_(executive_ids))
        else:
            cities = cities.filter(GymVisits.user_id == user_id)
        
        cities = cities.distinct().filter(GymDatabase.city.isnot(None)).all()
        
        areas = db.query(GymDatabase.area).join(
            GymVisits, GymVisits.gym_id == GymDatabase.id
        )
        if role == 'BDM':
            areas = areas.filter(GymVisits.user_id.in_(executive_ids))
        else:
            areas = areas.filter(GymVisits.user_id == user_id)
        
        areas = areas.distinct().filter(GymDatabase.area.isnot(None)).all()
        
        pincodes = db.query(GymDatabase.pincode).join(
            GymVisits, GymVisits.gym_id == GymDatabase.id
        )
        if role == 'BDM':
            pincodes = pincodes.filter(GymVisits.user_id.in_(executive_ids))
        else:
            pincodes = pincodes.filter(GymVisits.user_id == user_id)
        
        pincodes = pincodes.distinct().filter(GymDatabase.pincode.isnot(None)).all()

        filter_options = {
            "states": sorted([s[0] for s in states if s[0]]),
            "cities": sorted([c[0] for c in cities if c[0]]),
            "areas": sorted([a[0] for a in areas if a[0]]),
            "pincodes": sorted([p[0] for p in pincodes if p[0]])
        }

        return {
            "status": 200,
            "message": "Location filter options retrieved successfully",
            "data": filter_options
        }

    except Exception as e:
        print(f"Error getting location filter options: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


class AddFollowupAttemptRequest(BaseModel):
    visit_id: int
    followup_type: str
    notes: str
    next_followup_date: Optional[str] = None
    decision_maker_involved: bool = False
    created_by: int

@router.post("/followup-attempts/add")
def add_followup_attempt(request: AddFollowupAttemptRequest, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == request.visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        last_attempt = db.query(FollowupAttempts).filter(
            FollowupAttempts.visit_id == request.visit_id
        ).order_by(FollowupAttempts.attempt_number.desc()).first()
        
        attempt_number = (last_attempt.attempt_number + 1) if last_attempt else 1

        followup_attempt = FollowupAttempts(
            visit_id=request.visit_id,
            attempt_number=attempt_number,
            followup_type=request.followup_type,
            followup_date=datetime.today(),
            notes=request.notes,
            next_followup_date=datetime.fromisoformat(request.next_followup_date.replace('Z', '+00:00')) if request.next_followup_date else None,
            decision_maker_involved=request.decision_maker_involved,
            created_by=request.created_by,
            created_at=datetime.now()
        )
        
        db.add(followup_attempt)

        visit.total_followup_attempts = attempt_number
        visit.updated_at = datetime.now()
        
        if request.next_followup_date:
            visit.next_follow_up_date = followup_attempt.next_followup_date

        timeline_entry = ActivityTimeline(
            visit_id=request.visit_id,
            activity_type='followup_completed',
            title=f"Followup Attempt #{attempt_number}",
            meta_data={
                "attempt_number": attempt_number,
                "followup_type": request.followup_type,

            },
            performed_by=request.created_by,
            timestamp=datetime.now()
        )
        db.add(timeline_entry)

        db.commit()
        db.refresh(followup_attempt)

        return {
            "status": 200,
            "message": "Followup attempt added successfully"
        }

    except Exception as e:
        print(f"Error adding followup attempt: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/followup-attempts/{visit_id}")
def get_followup_attempts(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        attempts = db.query(FollowupAttempts).filter(
            FollowupAttempts.visit_id == visit_id
        ).order_by(FollowupAttempts.attempt_number.asc()).all()

        # === BATCH LOAD: Get all executives at once (N+1 FIX) ===
        exec_ids = list({a.created_by for a in attempts if a.created_by})
        exec_map = {}
        if exec_ids:
            executives = db.query(Executives).filter(Executives.id.in_(exec_ids)).all()
            exec_map = {e.id: e for e in executives}

        attempt_data = []
        for attempt in attempts:
            # Get user from pre-loaded map (was N+1 query)
            user = exec_map.get(attempt.created_by)

            attempt_info = {
                "id": attempt.id,
                "attempt_number": attempt.attempt_number,
                "followup_date": attempt.followup_date.isoformat() if attempt.followup_date else None,
                "followup_type": attempt.followup_type,
                "contact_person": attempt.contact_person,
                "notes": attempt.notes,
                "outcome": attempt.outcome,
                "next_action": attempt.next_action,
                "next_followup_date": attempt.next_followup_date.isoformat() if attempt.next_followup_date else None,
                "duration_minutes": attempt.duration_minutes,
                "interest_level": attempt.interest_level,
                "decision_maker_involved": attempt.decision_maker_involved,
                "budget_discussed": attempt.budget_discussed,
                "objections_raised": attempt.objections_raised,
                "materials_requested": attempt.materials_requested,
                "created_by": attempt.created_by,
                "created_by_name": user.name if user else "Unknown",
                "created_at": attempt.created_at.isoformat() if attempt.created_at else None
            }
            attempt_data.append(attempt_info)

        return {
            "status": 200,
            "message": "Followup attempts retrieved successfully",
            "data": {
                "visit_id": visit_id,
                "gym_name": visit.gym_name,
                "total_attempts": len(attempts),
                "attempts": attempt_data
            }
        }

    except Exception as e:
        print(f"Error getting followup attempts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# Post-Conversion Activities Endpoints

class AddPostConversionActivityRequest(BaseModel):
    visit_id: int
    activity_type: str
    title: str
    description: Optional[str] = None
    priority: str = 'medium'
    scheduled_date: Optional[str] = None
    due_date: Optional[str] = None
    assigned_to: Optional[int] = None
    assigned_by: int
    estimated_value: Optional[float] = None
    notes: Optional[str] = None

@router.post("/post-conversion-activities/add")
def add_post_conversion_activity(request: AddPostConversionActivityRequest, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == request.visit_id).first()
        executive = db.query(Executives).filter(Executives.id == request.assigned_to).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        if visit.final_status != 'converted':
            raise HTTPException(status_code=400, detail="Visit must be converted to add post-conversion activities")

        activity = PostConversionActivities(
            visit_id=request.visit_id,
            activity_type=request.activity_type,
            title=request.title,
            description=request.description,
            priority=request.priority,
            scheduled_date=datetime.fromisoformat(request.scheduled_date.replace('Z', '+00:00')) if request.scheduled_date else None,
            due_date=datetime.fromisoformat(request.due_date.replace('Z', '+00:00')) if request.due_date else None,
            assigned_to=request.assigned_to,
            assigned_by=request.assigned_by,
            estimated_value=request.estimated_value,
            notes=request.notes,
            activity_status='pending',
            created_at=datetime.now()
        )
        
        db.add(activity)

        # Add to timeline
        timeline_entry = ActivityTimeline(
            visit_id=request.visit_id,
            activity_type='post_activity_added',
            title=f"Post-Conversion Activity Added: {request.title}",
            description=f"Type: {request.activity_type}, Priority: {request.priority}",
            meta_data={
                "activity_type": request.activity_type,
                "priority": request.priority,
                "assigned_to": request.assigned_to,
                "assigned_to_name": executive.name
            },
            performed_by_manager=request.assigned_by,
            timestamp=datetime.now()
        )
        db.add(timeline_entry)

        db.commit()
        db.refresh(activity)

        return {
            "status": 200,
            "message": "Post-conversion activity added successfully",
            "data": {
                "activity_id": activity.id,
                "activity_type": activity.activity_type,
                "title": activity.title
            }
        }

    except Exception as e:
        print(f"Error adding post-conversion activity: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/post-conversion-activities/{visit_id}")
def get_post_conversion_activities(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        activities = db.query(PostConversionActivities).filter(
            PostConversionActivities.visit_id == visit_id
        ).order_by(PostConversionActivities.created_at.asc()).all()

        # === BATCH LOAD: Get all executives and managers at once (N+1 FIX) ===
        exec_ids = list({a.assigned_to for a in activities if a.assigned_to})
        manager_ids = list({a.assigned_by for a in activities if a.assigned_by})

        exec_map = {}
        if exec_ids:
            executives = db.query(Executives).filter(Executives.id.in_(exec_ids)).all()
            exec_map = {e.id: e for e in executives}

        manager_map = {}
        if manager_ids:
            managers = db.query(Managers).filter(Managers.id.in_(manager_ids)).all()
            manager_map = {m.id: m for m in managers}

        activity_data = []
        for activity in activities:
            # Get users from pre-loaded maps (was 2 N+1 queries)
            assigned_to_user = exec_map.get(activity.assigned_to) if activity.assigned_to else None
            assigned_by_user = manager_map.get(activity.assigned_by) if activity.assigned_by else None

            activity_info = {
                "id": activity.id,
                "activity_type": activity.activity_type,
                "title": activity.title,
                "description": activity.description,
                "activity_status": activity.activity_status,
                "priority": activity.priority,
                "scheduled_date": activity.scheduled_date.isoformat() if activity.scheduled_date else None,
                "completed_date": activity.completed_date.isoformat() if activity.completed_date else None,
                "due_date": activity.due_date.isoformat() if activity.due_date else None,
                "assigned_to": activity.assigned_to,
                "assigned_to_name": assigned_to_user.name if assigned_to_user else None,
                "assigned_by": activity.assigned_by,
                "assigned_by_name": assigned_by_user.name if assigned_by_user else None,
                "notes": activity.notes,
                "outcome": activity.outcome,
                "estimated_value": activity.estimated_value,
                "actual_value": activity.actual_value,
                "client_feedback": activity.client_feedback,
                "created_at": activity.created_at.isoformat() if activity.created_at else None,
                "updated_at": activity.updated_at.isoformat() if activity.updated_at else None
            }
            activity_data.append(activity_info)

        return {
            "status": 200,
            "message": "Post-conversion activities retrieved successfully",
            "data": {
                "visit_id": visit_id,
                "gym_name": visit.gym_name,
                "total_activities": len(activities),
                "activities": activity_data
            }
        }

    except Exception as e:
        print(f"Error getting post-conversion activities: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


class UpdateActivityStatusRequest(BaseModel):
    activity_id: int
    activity_status: str
    outcome: Optional[str] = None
    actual_value: Optional[float] = None
    client_feedback: Optional[str] = None
    internal_notes: Optional[str] = None
    completed_date: Optional[str] = None

class UpdateChecklistRequest(BaseModel):
    checklist: dict

@router.put("/post-conversion-activities/update-status")
def update_activity_status(request: UpdateActivityStatusRequest, db: Session = Depends(get_db)):
    try:
        activity = db.query(PostConversionActivities).filter(
            PostConversionActivities.id == request.activity_id
        ).first()
        
        if not activity:
            raise HTTPException(status_code=404, detail="Activity not found")

        activity.activity_status = request.activity_status
        activity.outcome = request.outcome
        activity.actual_value = request.actual_value
        activity.client_feedback = request.client_feedback
        activity.internal_notes = request.internal_notes
        activity.updated_at = datetime.now()
        
        if request.activity_status == 'completed' and request.completed_date:
            activity.completed_date = datetime.fromisoformat(request.completed_date.replace('Z', '+00:00'))
        elif request.activity_status == 'completed':
            activity.completed_date = datetime.now()

        # Add to timeline
        timeline_entry = ActivityTimeline(
            visit_id=activity.visit_id,
            activity_type='post_activity_completed',
            title=f"Activity {request.activity_status.title()}: {activity.title}",
            description=request.outcome or f"Status changed to {request.activity_status}",
            meta_data={
                "activity_id": activity.id,
                "activity_type": activity.activity_type,
                "new_status": request.activity_status,
                "actual_value": request.actual_value
            },
            performed_by=activity.assigned_to,
            timestamp=datetime.now()
        )
        db.add(timeline_entry)

        db.commit()
        db.refresh(activity)

        return {
            "status": 200,
            "message": "Activity status updated successfully",
            "data": {
                "activity_id": activity.id,
                "new_status": activity.activity_status,
                "completed_date": activity.completed_date.isoformat() if activity.completed_date else None
            }
        }

    except Exception as e:
        print(f"Error updating activity status: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/activity-timeline/{visit_id}")
def get_activity_timeline(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        timeline_items = db.query(ActivityTimeline).filter(
            ActivityTimeline.visit_id == visit_id
        ).order_by(ActivityTimeline.timestamp.asc()).all()

        # === BATCH LOAD: Get all executives and managers at once (N+1 FIX) ===
        exec_ids = list({t.performed_by for t in timeline_items if t.performed_by})
        manager_ids = list({t.performed_by_manager for t in timeline_items if t.performed_by_manager})

        exec_map = {}
        if exec_ids:
            executives = db.query(Executives).filter(Executives.id.in_(exec_ids)).all()
            exec_map = {e.id: e for e in executives}

        manager_map = {}
        if manager_ids:
            managers = db.query(Managers).filter(Managers.id.in_(manager_ids)).all()
            manager_map = {m.id: m for m in managers}

        timeline_data = []
        for item in timeline_items:
            # Get user from pre-loaded maps (was N+1 query)
            user = None
            if item.performed_by:
                user = exec_map.get(item.performed_by)
            elif item.performed_by_manager:
                user = manager_map.get(item.performed_by_manager)

            timeline_info = {
                "id": item.id,
                "activity_type": item.activity_type,
                "title": item.title,
                "description": item.description,
                "metadata": item.meta_data,
                "performed_by": item.performed_by or item.performed_by_manager,
                "performed_by_name": user.name if user else "System",
                "performed_by_type": "executive" if item.performed_by else "manager",
                "timestamp": item.timestamp.isoformat() if item.timestamp else None
            }
            timeline_data.append(timeline_info)

        return {
            "status": 200,
            "message": "Activity timeline retrieved successfully",
            "data": {
                "visit_id": visit_id,
                "gym_name": visit.gym_name,
                "timeline": timeline_data
            }
        }

    except Exception as e:
        print(f"Error getting activity timeline: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/conversion-dashboard/{visit_id}")
def get_conversion_dashboard(visit_id: int, db: Session = Depends(get_db)):
    try:
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        followup_attempts = db.query(FollowupAttempts).filter(
            FollowupAttempts.visit_id == visit_id
        ).order_by(FollowupAttempts.attempt_number.desc()).all()

        post_activities = db.query(PostConversionActivities).filter(
            PostConversionActivities.visit_id == visit_id
        ).order_by(PostConversionActivities.created_at.desc()).all()

        total_followups = len(followup_attempts)
        successful_followups = len([f for f in followup_attempts if f.outcome == 'successful'])
        
        pending_activities = len([a for a in post_activities if a.activity_status == 'pending'])
        completed_activities = len([a for a in post_activities if a.activity_status == 'completed'])
        
        total_estimated_value = sum([a.estimated_value for a in post_activities if a.estimated_value])
        total_actual_value = sum([a.actual_value for a in post_activities if a.actual_value])

        dashboard_data = {
            "visit_info": {
                "id": visit.id,
                "gym_name": visit.gym_name,
                "final_status": visit.final_status,
                "conversion_stage": visit.conversion_stage,
                "total_followup_attempts": visit.total_followup_attempts,
                "last_followup_date": visit.last_followup_date.isoformat() if visit.last_followup_date else None,
                "last_followup_outcome": visit.last_followup_outcome
            },
            "followup_stats": {
                "total_attempts": total_followups,
                "successful_attempts": successful_followups,
                "success_rate": round((successful_followups / total_followups * 100) if total_followups > 0 else 0, 1)
            },
            "post_conversion_stats": {
                "total_activities": len(post_activities),
                "pending_activities": pending_activities,
                "completed_activities": completed_activities,
                "completion_rate": round((completed_activities / len(post_activities) * 100) if post_activities else 0, 1),
                "estimated_value": total_estimated_value,
                "actual_value": total_actual_value
            },
            "recent_followups": [
                {
                    "attempt_number": f.attempt_number,
                    "followup_date": f.followup_date.isoformat() if f.followup_date else None,
                    "followup_type": f.followup_type,
                    "outcome": f.outcome,
                    "interest_level": f.interest_level
                } for f in followup_attempts[:3]
            ],
            "upcoming_activities": [
                {
                    "title": a.title,
                    "activity_type": a.activity_type,
                    "scheduled_date": a.scheduled_date.isoformat() if a.scheduled_date else None,
                    "priority": a.priority,
                    "activity_status": a.activity_status
                } for a in post_activities if a.activity_status in ['pending', 'in_progress']
            ][:5]
        }

        return {
            "status": 200,
            "message": "Conversion dashboard retrieved successfully",
            "data": dashboard_data
        }

    except Exception as e:
        print(f"Error getting conversion dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.patch("/{visit_id}/checklist")
def update_conversion_checklist(
    visit_id: int,
    checklist_data: dict = Body(...),
    db: Session = Depends(get_db)
):
    """
    Update the conversion checklist for a gym visit
    Merges new checklist data with existing values to preserve old true values
    """
    try:
        print(f"Received request for visit {visit_id}")
        print(f"Checklist data received: {checklist_data}")

        # Find the visit
        visit = db.query(GymVisits).filter(GymVisits.id == visit_id).first()

        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found")

        # Get existing checklist or initialize empty dict
        existing_checklist = visit.checklist or {}
        print(f"Existing checklist: {existing_checklist}")

        # Merge new data with existing data (new values override old ones)
        merged_checklist = {**existing_checklist, **checklist_data}
        print(f"Merged checklist: {merged_checklist}")

        # Update the checklist with merged data
        visit.checklist = merged_checklist
        visit.updated_at = datetime.now()

        # Commit the changes
        db.commit()
        db.refresh(visit)

        return {
            "status": 200,
            "message": "Conversion checklist updated successfully",
            "data": {
                "visit_id": visit.id,
                "checklist": visit.checklist
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating conversion checklist: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
