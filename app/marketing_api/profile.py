from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.marketingmodels import Executives, Managers, GymVisits
from app.models.database import get_db
from typing import Optional, Dict
from datetime import datetime
import boto3
import time

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
PROFILE_PHOTOS_PREFIX = "profile_photos/"
PHOTO_MAX_SIZE = 10 * 1024 * 1024 

_s3 = boto3.client("s3", region_name=AWS_REGION)

router = APIRouter(prefix="/marketing/profile", tags=["Employee Profile"])

class UpdateProfileRequest(BaseModel):
    user_id: int
    user_type: str 

class ConfirmUploadRequest(BaseModel):
    user_id: int
    user_type: str
    cdn_url: str

def generate_profile_photo_upload_url(
    user_id: int,
    user_type: str,
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

    timestamp = int(time.time() * 1000)
    key = f"{PROFILE_PHOTOS_PREFIX}{user_type}-{user_id}/profile_{timestamp}.{extension}"

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
    cdn_url = f"{presigned['url']}{key}?v={timestamp}"

    return {
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": timestamp,
    }

def get_executive_stats(executive_id: int, db: Session) -> Dict:
    total_visits = db.query(GymVisits).filter(GymVisits.user_id == executive_id).count()
    
    completed_visits = db.query(GymVisits).filter(
        GymVisits.user_id == executive_id,
        GymVisits.completed == True
    ).count()
    
    conversions = db.query(GymVisits).filter(
        GymVisits.user_id == executive_id,
        GymVisits.final_status == 'converted'
    ).count()
    
    conversion_rate = round((conversions / completed_visits * 100) if completed_visits > 0 else 0, 1)
    
    avg_rating_result = db.query(func.avg(GymVisits.overall_rating)).filter(
        GymVisits.user_id == executive_id,
        GymVisits.overall_rating > 0
    ).scalar()
    avg_rating = round(float(avg_rating_result), 1) if avg_rating_result else 0
    
    return {
        "total_visits": total_visits,
        "completed_visits": completed_visits,
        "conversions": conversions,
        "conversion_rate": conversion_rate,
        "avg_rating": avg_rating
    }

def get_manager_team_stats(manager_id: int, db: Session) -> Dict:
    executives = db.query(Executives).filter(Executives.manager_id == manager_id).all()
    executive_ids = [exec.id for exec in executives]
    
    if not executive_ids:
        return {
            "total_team_members": 0,
            "total_visits": 0,
            "conversions": 0,
            "conversion_rate": 0
        }
    
    total_visits = db.query(GymVisits).filter(GymVisits.user_id.in_(executive_ids)).count()
    
    completed_visits = db.query(GymVisits).filter(
        GymVisits.user_id.in_(executive_ids),
        GymVisits.completed == True
    ).count()
    
    conversions = db.query(GymVisits).filter(
        GymVisits.user_id.in_(executive_ids),
        GymVisits.final_status == 'converted'
    ).count()
    
    conversion_rate = round((conversions / completed_visits * 100) if completed_visits > 0 else 0, 1)
    
    return {
        "total_team_members": len(executives),
        "total_visits": total_visits,
        "conversions": conversions,
        "conversion_rate": conversion_rate
    }

@router.get("/get_profile")
async def get_employee_profile(
    user_id: int,
    user_type: str = Query(..., description="'executive' for BDE or 'manager' for BDM"),
    db: Session = Depends(get_db)
):
    try:
        if user_type not in ["executive", "manager"]:
            print(f"Invalid user_type received: {user_type}")
            raise HTTPException(status_code=400, detail=f"Invalid user_type '{user_type}'. Must be 'executive' or 'manager'")

        if user_type == "executive":
            user = db.query(Executives).filter(Executives.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Executive not found")
            
            user_data = {
                "id": user.id,
                "name": user.name,
                "email": user.email,
                "contact": user.contact,
                "profile": user.profile,
                "dob": user.dob.strftime("%Y-%m-%d") if user.dob else None,
                "age": user.age,
                "gender": user.gender,
                "role": user.role,
                "joined_date": user.joined_date.strftime("%Y-%m-%d") if user.joined_date else None,
                "status": user.status,
                "employee_id": user.emp_id
            }
            
            response_data = {"profile": user_data}
            
            if user.manager_id:
                manager = db.query(Managers).filter(Managers.id == user.manager_id).first()
                if manager:
                    print(f"Found manager: {manager.name}")
                    manager_stats = get_manager_team_stats(manager.id, db)
                    response_data["manager"] = {
                        "id": manager.id,
                        "name": manager.name,
                        "email": manager.email,
                        "contact": manager.contact,
                        "profile": manager.profile,
                        "employee_id": manager.emp_id,
                        "stats": manager_stats
                    }
        
        elif user_type == "manager":
            user = db.query(Managers).filter(Managers.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Manager not found")
            
            user_data = {
                "id": user.id,
                "name": user.name,
                "email": user.email,
                "contact": user.contact,
                "profile": user.profile,
                "dob": user.dob.strftime("%Y-%m-%d") if user.dob else None,
                "age": user.age,
                "gender": user.gender,
                "role": user.role,
                "joined_date": user.joined_date.strftime("%Y-%m-%d") if user.joined_date else None,
                "status": user.status,
                "employee_id": user.emp_id
            }
            
            response_data = {"profile": user_data}
            
            executives = db.query(Executives).filter(
                Executives.manager_id == user_id,
                Executives.status == 'active'
            ).all()
            
            
            team_members = []
            for executive in executives:
                exec_stats = get_executive_stats(executive.id, db)
                team_members.append({
                    "id": executive.id,
                    "name": executive.name,
                    "email": executive.email,
                    "contact": executive.contact,
                    "profile": executive.profile,
                    "employee_id": executive.emp_id,
                    "status": executive.status,
                    "joined_date": executive.joined_date.strftime("%Y-%m-%d") if executive.joined_date else None,
                    "stats": exec_stats
                })
            
            response_data["team_members"] = team_members


        return {
            'status': 200,
            'message': 'Profile data retrieved successfully',
            'data': response_data
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting employee profile: {str(e)}")
        raise HTTPException(status_code=500, detail=f'An error occurred: {str(e)}')

@router.get("/upload-url")
def get_profile_photo_upload_url(
    user_id: int,
    user_type: str,
    extension: str,
    db: Session = Depends(get_db)
):
    try:
        if user_type not in ["executive", "manager"]:
            raise HTTPException(status_code=400, detail=f"Invalid user_type '{user_type}'. Must be 'executive' or 'manager'")

        if user_type == "executive":
            user = db.query(Executives).filter(Executives.id == user_id).first()
        elif user_type == "manager":
            user = db.query(Managers).filter(Managers.id == user_id).first()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        url_data = generate_profile_photo_upload_url(
            user_id, user_type, extension
        )
        
        return {
            "status": 200,
            "data": url_data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting profile photo upload URL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.post("/confirm-upload")
def confirm_profile_photo_upload(
    request: ConfirmUploadRequest,
    db: Session = Depends(get_db)
):
    try:
        user_id = request.user_id
        user_type = request.user_type
        cdn_url = request.cdn_url

        if user_type not in ["executive", "manager"]:
            raise HTTPException(status_code=400, detail=f"Invalid user_type '{user_type}'. Must be 'executive' or 'manager'")

        if user_type == "executive":
            user = db.query(Executives).filter(Executives.id == user_id).first()
        elif user_type == "manager":
            user = db.query(Managers).filter(Managers.id == user_id).first()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user.profile = cdn_url
        user.updated_at = datetime.now()
        db.commit()
        db.refresh(user)
        
        return {
            "status": 200,
            "message": "Profile photo updated successfully"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error confirming profile photo upload: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.put("/update_profile")
async def update_profile(request: UpdateProfileRequest, db: Session = Depends(get_db)):
    try:
        user_id = request.user_id
        user_type = request.user_type
        
        if user_type == "executive":
            user = db.query(Executives).filter(Executives.id == user_id).first()
        elif user_type == "manager":
            user = db.query(Managers).filter(Managers.id == user_id).first()
        else:
            raise HTTPException(status_code=400, detail="Invalid user_type")
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "status": 200,
            "message": "Profile updated successfully"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating profile: {str(e)}")
        raise HTTPException(status_code=500, detail=f'An error occurred: {str(e)}')

@router.get("/manager-details/{manager_id}")
async def get_manager_details(
    manager_id: int,
    executive_id: int = Query(..., description="Executive ID for authorization"),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            Executives.id == executive_id,
            Executives.manager_id == manager_id
        ).first()
        
        if not executive:
            raise HTTPException(status_code=403, detail="Unauthorized access to manager details")
        
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        manager_stats = get_manager_team_stats(manager_id, db)
        
        manager_data = {
            "id": manager.id,
            "name": manager.name,
            "email": manager.email,
            "contact": manager.contact,
            "profile": manager.profile,
            "role": manager.role,
            "employee_id": manager.emp_id,
            "joined_date": manager.joined_date.strftime("%Y-%m-%d") if manager.joined_date else None,
            "status": manager.status,
            "stats": manager_stats
        }
        
        return {
            "status": 200,
            "message": "Manager details retrieved successfully",
            "data": manager_data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting manager details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/team-members/{manager_id}")
async def get_team_members(
    manager_id: int,
    db: Session = Depends(get_db)
):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        executives = db.query(Executives).filter(
            Executives.manager_id == manager_id
        ).order_by(Executives.name.asc()).all()
        
        team_members = []
        for executive in executives:
            exec_stats = get_executive_stats(executive.id, db)
            team_members.append({
                "id": executive.id,
                "name": executive.name,
                "email": executive.email,
                "contact": executive.contact,
                "profile": executive.profile,
                "role": executive.role,
                "employee_id": executive.employee_id,
                "joined_date": executive.joined_date.strftime("%Y-%m-%d") if executive.joined_date else None,
                "status": executive.status,
                "stats": exec_stats
            })
        
        return {
            "status": 200,
            "message": "Team members retrieved successfully",
            "data": {
                "manager": {
                    "id": manager.id,
                    "name": manager.name,
                    "email": manager.email
                },
                "team_members": team_members,
                "total_members": len(team_members)
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting team members: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/executive-stats/{executive_id}")
async def get_executive_performance_stats(
    executive_id: int,
    manager_id: int = Query(..., description="Manager ID for authorization"),
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
            raise HTTPException(status_code=403, detail="Unauthorized access to executive stats")
        
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
        completion_rate = round((total_completed / total_visits * 100) if total_visits > 0 else 0, 1)
        
        avg_rating = round(sum(v.overall_rating for v in completed_visits if v.overall_rating) / len(completed_visits), 1) if completed_visits else 0
        avg_interest = round(sum(v.interest_level for v in completed_visits if v.interest_level) / len(completed_visits), 1) if completed_visits else 0
        
        monthly_data = {}
        for visit in visits:
            month_key = visit.created_at.strftime('%Y-%m')
            if month_key not in monthly_data:
                monthly_data[month_key] = {'total': 0, 'completed': 0, 'converted': 0}
            monthly_data[month_key]['total'] += 1
            if visit.completed:
                monthly_data[month_key]['completed'] += 1
            if visit.final_status == 'converted':
                monthly_data[month_key]['converted'] += 1
        
        stats = {
            "executive_info": {
                "id": executive.id,
                "name": executive.name,
                "email": executive.email,
                "employee_id": executive.employee_id
            },
            "performance_metrics": {
                "total_visits": total_visits,
                "completed_visits": total_completed,
                "completion_rate": completion_rate,
                "conversion_rate": conversion_rate,
                "average_rating": avg_rating,
                "average_interest_level": avg_interest
            },
            "status_breakdown": status_breakdown,
            "monthly_performance": monthly_data,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
        return {
            "status": 200,
            "message": "Executive performance stats retrieved successfully",
            "data": stats
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting executive stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")