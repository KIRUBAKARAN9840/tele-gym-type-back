import os, time
from datetime import datetime, date
from typing import Optional, List
import boto3
from fastapi import (
    FastAPI, APIRouter, Depends, HTTPException, Request, status
)
from jose import jwt, JWTError
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from app.models.fittbot_models import Trainer, Client, GymOwner, TrainerProfile
from sqlalchemy import and_, func
import json
 
AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
AVATAR_PREFIX = "Profile_pics/"      
AVATAR_MAX_SIZE = 10 * 1024 * 1024
 
router = APIRouter(prefix="/trainers", tags=["Trainers"])
 
_s3 = boto3.client("s3", region_name=AWS_REGION)
 
def generate_trainer_upload_url(
    gym_id: int,
    extension: str,
    scope: str,
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
    key = f"{AVATAR_PREFIX}trainer_{scope}_{timestamp}.{extension}"
    version = timestamp
 
    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, AVATAR_MAX_SIZE],
    ]
 
    try:
        
       
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
    except Exception as e:
        print(f"Error generating trainer upload URL: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate upload URL: {str(e)}"
        )
 
@router.get("/upload-url")
async def create_trainer_upload_url(
    gym_id: int,
    extension: str,
    scope: str,
    content_type:Optional[str]="image/jpeg"
):
    try:
        if not gym_id:
            raise HTTPException(status_code=400, detail="gym_id is required")


        if not extension:
            raise HTTPException(status_code=400, detail="extension is required")

        if not scope:
            raise HTTPException(status_code=400, detail="scope is required")

        print(f"Creating trainer upload URL - Gym ID: {gym_id},Extension: {extension}, Scope: {scope}")

        url_data = generate_trainer_upload_url(gym_id, extension, scope, content_type=content_type)
       
        return {"status": 200, "data": url_data}
       
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error in create_trainer_upload_url: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while creating upload URL"
        )
 
class ConfirmTrainerUploadBody(BaseModel):
    cdn_url: str
    gym_id: int
    scope: str
 
@router.post("/confirm")
async def confirm_trainer_upload(
    body: ConfirmTrainerUploadBody,
    db: Session = Depends(get_db)
):
    try:
       
 
        if body.scope == "profile_image":
            print(f"Trainer profile image upload confirmed for gym {body.gym_id}")
           
        return {
            "status": 200,
            "message": "Trainer image upload confirmed"
        }
 
    except Exception as e:
       
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
 
 
class AddTrainerRequest(BaseModel):
    gym_id: int
    full_name: str
    gender: str
    contact: str
    email: Optional[str] = None
    specializations: Optional[List[str]] = []  # Changed to list
    experience: Optional[float] = 0
    certifications: Optional[str] = None
    work_timings: Optional[List[dict]] = []  # Changed from availability to work_timings
    profile_image: Optional[str] = None
    can_view_client_data: Optional[bool] = False
    personal_trainer: Optional[bool] = False  
 
@router.post("/gym/add_trainer")
async def add_trainer(
    request: AddTrainerRequest,  
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    try:
        gym_id = request.gym_id
        full_name = request.full_name
        gender = request.gender
        contact = request.contact
        email = request.email
        specializations = request.specializations  # Changed from specialization
        experience = request.experience
        certifications = request.certifications
        work_timings = request.work_timings  # Changed from availability
        profile_image = request.profile_image
        can_view_client_data = request.can_view_client_data
        personal_trainer = request.personal_trainer
 
        existing_trainer = db.query(Trainer).filter(Trainer.contact == contact
        ).first()
 
        if existing_trainer:
            existing_profile = db.query(TrainerProfile).filter(
                and_(
                    TrainerProfile.trainer_id == existing_trainer.trainer_id,
                    TrainerProfile.gym_id == gym_id
                )
            ).first()
 
            if existing_profile:
                raise HTTPException(
                    status_code=400,
                    detail="Trainer is already registered with this gym."
                )
 
            existing_trainer.full_name = full_name
            existing_trainer.gender = gender
            existing_trainer.contact = contact
            existing_trainer.email = email
            existing_trainer.specializations = specializations
            existing_trainer.experience = experience
            existing_trainer.certifications = certifications
            existing_trainer.work_timings = work_timings
            existing_trainer.profile_image = profile_image
            existing_trainer.updated_at = datetime.now()
 
            new_profile = TrainerProfile(
                trainer_id=existing_trainer.trainer_id,
                gym_id=gym_id,
                full_name=full_name,
                email=email,
                specializations=specializations,
                experience=experience,
                certifications=certifications,
                work_timings=work_timings,
                profile_image=profile_image,
                can_view_client_data=can_view_client_data,
                personal_trainer=personal_trainer,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_profile)
            trainer_id = existing_trainer.trainer_id
 
        else:
           
            new_trainer = Trainer(
                full_name=full_name,
                gender=gender,
                contact=contact,
                email=email,
                specializations=specializations,
                experience=experience,
                certifications=certifications,
                work_timings=work_timings,
                profile_image=profile_image,
                password='',  
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            db.add(new_trainer)
            db.flush()  
           
            new_profile = TrainerProfile(
                trainer_id=new_trainer.trainer_id,
                gym_id=gym_id,
                full_name=full_name,
                email=email,
                specializations=specializations,
                experience=experience,
                certifications=certifications,
                work_timings=work_timings,
                profile_image=profile_image,
                can_view_client_data=can_view_client_data,
                personal_trainer=personal_trainer,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_profile)
            trainer_id = new_trainer.trainer_id
 
        db.commit()
 
        redis_key_trainers = f"gym:{gym_id}:trainers"
        if await redis.exists(redis_key_trainers):
            await redis.delete(redis_key_trainers)
 
        members_key = f"gym:{gym_id}:members"
        if await redis.exists(members_key):
            await redis.delete(members_key)


        today = date.today()
        trainer_attendance_key = f"gym:{gym_id}:trainer_attendance_summary:{today.strftime('%Y-%m-%d')}"
        if await redis.exists(trainer_attendance_key):
            await redis.delete(trainer_attendance_key)
        
 
        return {"status": 200, "message": "Trainer added successfully."}
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()  
        print(f"Error adding trainer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error adding trainer: {str(e)}")
 
 
def serialize_trainer_profile(trainer, profile):
    """Serialize trainer and profile data"""
    trainer_data = {}
   
    for column in trainer.__table__.columns:
        value = getattr(trainer, column.name)
        if isinstance(value, datetime):
            trainer_data[column.name] = value.isoformat()  
        else:
            trainer_data[column.name] = value
   
    if profile:
        profile_fields = {
            'profile_id': profile.profile_id,
            'gym_id': profile.gym_id,
            'can_view_client_data': profile.can_view_client_data,
            'personal_trainer': profile.personal_trainer,
            'full_name': profile.full_name,
            'email': profile.email,
            'specializations': profile.specializations,  # New field name
            'experience': profile.experience,
            'certifications': profile.certifications,
            'work_timings': profile.work_timings,  # New field name
            'profile_image': profile.profile_image,
            # Backward compatibility fields
            'specialization': profile.specializations[0] if profile.specializations else None,  # First specialization for old frontend
            'availability': f"{len(profile.work_timings)} time slots" if profile.work_timings else "Not specified",  # Summary for old frontend
        }
        trainer_data.update(profile_fields)
   
    return trainer_data
 
 
class EditTrainerRequest(BaseModel):
    trainer_id: int
    gym_id: int  
    full_name: Optional[str] = None
    gender: Optional[str] = None
    contact: Optional[str] = None
    email: Optional[str] = None
    specializations: Optional[List[str]] = None  # Changed to list
    experience: Optional[float] = None
    certifications: Optional[str] = None
    work_timings: Optional[List[dict]] = None  # Changed from availability to work_timings
    profile_image: Optional[str] = None
    can_view_client_data: Optional[bool] = None
    personal_trainer: Optional[bool] = None  
 
@router.put("/gym/edit_trainer")
async def edit_trainer(
    request: EditTrainerRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    try:
        trainer = db.query(Trainer).filter(Trainer.trainer_id == request.trainer_id).first()
        if not trainer:
            raise HTTPException(status_code=404, detail="Trainer not found")
 
        profile = db.query(TrainerProfile).filter(
            and_(
                TrainerProfile.trainer_id == request.trainer_id,
                TrainerProfile.gym_id == request.gym_id
            )
        ).first()
 
        if not profile:
            raise HTTPException(
                status_code=403,
                detail="Trainer profile not found for this gym"
            )
 
        trainer_fields = ['full_name', 'gender', 'contact', 'email', 'specializations',
                         'experience', 'certifications', 'work_timings', 'profile_image']
       
        for field in trainer_fields:
            value = getattr(request, field, None)
            if value is not None:
                setattr(trainer, field, value)
       
        trainer.updated_at = datetime.now()
 
        profile_fields = ['full_name', 'email', 'specializations', 'experience',
                         'certifications', 'work_timings', 'profile_image',
                         'can_view_client_data', 'personal_trainer']
       
        for field in profile_fields:
            value = getattr(request, field, None)
            if value is not None:
                setattr(profile, field, value)
       
        profile.updated_at = datetime.now()
 
        db.commit()
 
        members_key = f"gym:{request.gym_id}:members"
        if await redis.exists(members_key):
            await redis.delete(members_key)
 
        redis_key_trainers = f"gym:{request.gym_id}:trainers"
        if await redis.exists(redis_key_trainers):
            await redis.delete(redis_key_trainers)
 
        pattern = f"*:{request.gym_id}:assigned_plans"
        keys = await redis.keys(pattern)  
        if keys:  
            await redis.delete(*keys)
 
        return {"status": 200, "message": "Trainer details updated successfully."}
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error updating trainer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating trainer: {str(e)}")
 
 
@router.delete("/gym/delete_trainer")
async def delete_trainer(
    trainer_id: int,
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    try:
        trainer = db.query(Trainer).filter(Trainer.trainer_id == trainer_id).first()
        if not trainer:
            raise HTTPException(status_code=404, detail="Trainer not found")
 
        profile = db.query(TrainerProfile).filter(
            and_(
                TrainerProfile.trainer_id == trainer_id,
                TrainerProfile.gym_id == gym_id
            )
        ).first()
 
        if not profile:
            raise HTTPException(status_code=404, detail="Trainer profile not found for this gym")
 
        gym_count = db.query(func.count(TrainerProfile.gym_id)).filter(
            TrainerProfile.trainer_id == trainer_id
        ).scalar()
 
        if gym_count > 1:
            db.delete(profile)
        else:
            db.delete(profile)
            db.delete(trainer)
 
        db.commit()
 
        redis_key_trainers = f"gym:{gym_id}:trainers"
        if await redis.exists(redis_key_trainers):
            await redis.delete(redis_key_trainers)
 
        client_redis_key = f"gym:{gym_id}:all_clients"
        if await redis.exists(client_redis_key):
            await redis.delete(client_redis_key)
 
        members_key = f"gym:{gym_id}:members"
        if await redis.exists(members_key):
            await redis.delete(members_key)
 
        pattern = f"*:{gym_id}:assigned_plans"
        keys = await redis.keys(pattern)  
        if keys:  
            await redis.delete(*keys)
 
        return {"status": 200, "message": "Trainer deleted successfully."}
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error deleting trainer: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting trainer: {str(e)}")
 
 
@router.get("/gym/get_trainers")
async def get_trainers(
    gym_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    try:
        response = {}

        trainers_query = db.query(Trainer, TrainerProfile).join(
            TrainerProfile, Trainer.trainer_id == TrainerProfile.trainer_id
        ).filter(TrainerProfile.gym_id == gym_id).all()

        if not trainers_query:
            return {"data": {"trainers": []}, "status": 200, "message": "Trainers listed successfully"}

        trainers_data = []
        for trainer, profile in trainers_query:
            trainer_dict = serialize_trainer_profile(trainer, profile)
            trainers_data.append(trainer_dict)

 
        response["trainers"] = trainers_data
        print("response is", response)
        return {"data": response, "status": 200, "message": "Trainers listed successfully"}
   
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
 
 
@router.get("/gym/trainer_permissions")
async def get_trainer_permissions(
    trainer_id: int,
    gym_id: int,
    db: Session = Depends(get_db)
):
    try:
        profile = db.query(TrainerProfile).filter(
            and_(
                TrainerProfile.trainer_id == trainer_id,
                TrainerProfile.gym_id == gym_id
            )
        ).first()
 
        return {
            "status": 200,
            "data": {
                "can_view_client_data": profile.can_view_client_data if profile else False,
                "personal_trainer": profile.personal_trainer if profile else False,
            }
        }
    except Exception as e:
        print(f"Error getting trainer permissions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting permissions: {str(e)}")
 
 
@router.get("/gym/trainer/{trainer_id}")
async def get_trainer_details(
    trainer_id: int,
    gym_id: int,
    db: Session = Depends(get_db)
):
    try:
        result = db.query(Trainer, TrainerProfile).join(
            TrainerProfile, Trainer.trainer_id == TrainerProfile.trainer_id
        ).filter(
            and_(
                Trainer.trainer_id == trainer_id,
                TrainerProfile.gym_id == gym_id
            )
        ).first()
 
        if not result:
            raise HTTPException(status_code=404, detail="Trainer not found for this gym")
 
        trainer, profile = result
        trainer_data = serialize_trainer_profile(trainer, profile)
 
        return {
            "status": 200,
            "data": trainer_data,
            "message": "Trainer details retrieved successfully"
        }
 
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting trainer details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting trainer details: {str(e)}")
 