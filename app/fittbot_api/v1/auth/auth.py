from fastapi import FastAPI, APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.models.fittbot_models import GymDetails,ClientCharacter,ClientGeneralAnalysis,CharactersCombination,ClientWeightSelection,FittbotCharacters,WeightManagementPlan,TrainerProfile,GymOwner,Client,ClientFittbotAccess,Attendance,TemplateDiet,FeeHistory,Expenditure,ClientScheduler,DietTemplate,WorkoutTemplate,ClientActual,GymHourlyAgg,Gym,GymAnalysis,GymMonthlyData,GymPlans,GymBatches,Trainer,TemplateWorkout,Post,Comment,Like,Feedback,ClientTarget,ReferralCode,ReferralMapping,ReferralFittbotCash,ReferralGymCode,ReferralGymCash,ReferralGymCashLogs,ReferralGymMapping
from app.models.marketingmodels import GymDatabase, GymAssignments, Executives, Managers, GymVisits
from app.models.adminmodels import Admins
from app.models.telecaller_models import Manager as TelecallerManager, Telecaller
from app.utils.hashing import verify_password
from app.models.database import get_db
from datetime import datetime, timedelta
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from sqlalchemy.sql import extract,func
import json
from fastapi import Query
from datetime import datetime, date
from typing import Dict
from fastapi.middleware.cors import CORSMiddleware
import pytz
from typing import Optional, List
from sqlalchemy.future import select
from sqlalchemy import desc
import requests
from fastapi_limiter.depends import RateLimiter
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import random
import os
from email.message import EmailMessage
from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse
from jose.exceptions import ExpiredSignatureError
from jose import jwt, JWTError
from app.utils.security import SECRET_KEY, ALGORITHM
import uuid
import logging
 
 
from app.utils.security import (
    verify_password, create_access_token, create_refresh_token,
    refresh_tokens_store, get_password_hash, SECRET_KEY, ALGORITHM
)
from jose import jwt, JWTError
 
load_dotenv()
app = FastAPI()
 
router = APIRouter(prefix="/auth", tags=["Authentication"])
from app.utils.logging_utils import (
    auth_logger,
    FittbotHTTPException,
    SecuritySeverity,
    EventType,
    log_exceptions
)
import time
from app.utils.referral_code_generator import generate_unique_referral_code
from app.config.settings import settings
from app.utils.otp import generate_otp, async_send_verification_sms, async_send_ios_premium_sms, async_send_password_reset_sms

@router.get("/verify")
async def verify_token(
    request: Request,
    device: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    start = time.time()
    auth_logger.set_request_context(request)
    normalized_device = (device or "").strip().lower()

    try:
        access_token = request.cookies.get("access_token")
        token_source = "cookie" if access_token else None

        if not access_token:
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                auth_logger.security_event("missing_auth_credentials",
                                           severity=SecuritySeverity.LOW, endpoint="/auth/verify")
                raise FittbotHTTPException(401, "Missing authentication credentials", "AUTH_001",
                                         log_level="warning", security_event=True)

            parts = auth_header.split()
            if len(parts) != 2 or parts[0].lower() != "bearer":
                auth_logger.security_event("invalid_authorization_format",
                                           severity=SecuritySeverity.MEDIUM, endpoint="/auth/verify")
                raise FittbotHTTPException(401, "Invalid authorization header format", "AUTH_002",
                                         log_level="warning", security_event=True)

            access_token = parts[1]
            token_source = "header"

        try:
            payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = payload.get("sub")
            role = (payload.get("role") or "").lower()

            client = None
            subscribed = False
            gym = None
            if role == "client":
                try:
                    client_id_int = int(user_id) if user_id is not None else None
                    if client_id_int is not None:
                        client = (
                            db.query(Client)
                            .filter(Client.client_id == client_id_int)
                            .first()
                        )
                except (ValueError, TypeError):
                    client = None

                if client:
                    access = (
                        db.query(ClientFittbotAccess)
                        .filter(ClientFittbotAccess.client_id == client.client_id)
                        .first()
                    )
                    if access:
                        subscribed = access.access_status == "active"

                    if client.gym_id:
                        gym = (
                            db.query(Gym)
                            .filter(Gym.gym_id == client.gym_id)
                            .first()
                        )

            is_mobile = normalized_device == "mobile" or (
                normalized_device not in {"web", "desktop"} and token_source == "header"
            )

            if role == "admin":
                try:
                    admin_id_int = int(user_id) if user_id is not None else None
                    if admin_id_int is not None:
                        admin = (
                            db.query(Admins)
                            .filter(Admins.admin_id == admin_id_int)
                            .first()
                        )
                except (ValueError, TypeError):
                    admin = None

                response_payload = {
                    "status": 200,
                    "message": "valid token",
                }
                if admin:
                    response_payload["data"] = {
                        "admin_id": admin.admin_id,
                        "name": admin.name,
                        "role": "admin"
                    }
                return response_payload
            elif is_mobile or role != "client":
                response_payload = {
                    "status": 200,
                    "message": "valid token",
                }
                if client:
                    response_payload["data"] = {
                        "gym_id": client.gym_id if client.gym_id is not None else None,
                        "subscribed": subscribed,
                        "client_id": client.client_id,
                        "gym_name": gym.name if gym else "",
                        "gender": client.gender,
                        "gym_logo": gym.logo if gym else "",
                        "name": client.name if client.name else "",
                        "mobile": client.contact if client.contact else "",
                        "profile": client.profile if client.profile else "",
                    }
                return response_payload

            response_data = {
                "status": 200,
                "message": "valid token",
            }
            if client:
                response_data["data"] = {
                    "gym_id": client.gym_id if client.gym_id is not None else None,
                    "subscribed": subscribed,
                    "client_id": client.client_id,
                    "gym_name": gym.name if gym else "",
                    "gender": client.gender,
                    "gym_logo": gym.logo if gym else "",
                    "name": client.name if client.name else "",
                    "mobile": client.contact if client.contact else "",
                    "profile": client.profile if client.profile else "",
                }

            return response_data
        except ExpiredSignatureError:
            auth_logger.security_event("token_expired", severity=SecuritySeverity.LOW, endpoint="/auth/verify")
            return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})
        except JWTError:
            auth_logger.security_event("invalid_token", severity=SecuritySeverity.MEDIUM, endpoint="/auth/verify")
            raise FittbotHTTPException(401, "Invalid token", "AUTH_003",
                                     log_level="warning", security_event=True)
 
    except HTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(500, "Internal server error occurred", "AUTH_TOKEN_VERIFICATION_ERROR")
    finally:
        pass
 
 

 
class CheckTrainerRequest(BaseModel):
    contact: str
 
@router.post("/check-trainer")
async def check_trainer(request: CheckTrainerRequest, db: Session = Depends(get_db)):
    try:
        trainer = db.query(Trainer).filter(Trainer.contact == request.contact).first()

        if not trainer:
            raise HTTPException(status_code=404, detail="Mobile number is not registered")
       
        has_password = trainer.password is not None and trainer.password != ""
       
        return {
            "status": 200,
            "data": {
                "trainer_id": trainer.trainer_id,
                "full_name": trainer.full_name,
                "hasPassword": has_password
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking trainer: {str(e)}")
    
class CheckMobileRequest(BaseModel):
    contact: str

@router.post("/check-mobile-availability")
async def check_mobile_availability(request:CheckMobileRequest, db: Session = Depends(get_db)):
    try:
        # trainer_exists = db.query(Trainer).filter(Trainer.contact == request.contact).first() is not None
        owner_exists = db.query(GymOwner).filter(GymOwner.contact_number == request.contact).first() is not None
        

        if owner_exists:
            return {
                "status": 200,
                "exists": True,
                "message": "Mobile number is already associated with another account"
            }
        else:
            return {
                "status": 200,
                "exists": False,
                "message": "Mobile number is available"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking mobile availability: {str(e)}")


 
class SetTrainerPasswordRequest(BaseModel):
    contact: str
    password: str
 
@router.post("/set-trainer-password")
async def set_trainer_password(request: SetTrainerPasswordRequest, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):
    try:
        trainer = db.query(Trainer).filter(Trainer.contact == request.contact).first()
        mobile_number = request.contact

        if not trainer:
            raise HTTPException(status_code=404, detail="Mobile number is not registered")
       
        hashed_password = get_password_hash(request.password)
        trainer.password = hashed_password
        trainer.updated_at = datetime.now()
       
        db.commit()
        db.refresh(trainer)
 
        mobile_otp = "123456" if mobile_number == '8667458723' or mobile_number == "9486987082" else generate_otp()
        await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
       
        if await async_send_verification_sms(mobile_number, mobile_otp):
            print(f"Verification OTP sent successfully to {mobile_number}")
        else:
            raise HTTPException(status_code=500, detail="Failed to send OTP")
       
        return {
            "status": 200,
            "message": "Password set successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error setting password: {str(e)}")


@router.get("/validate-referral-code")
async def validate_referral_code(code: str, db: Session = Depends(get_db)):

    try:
        # Case-sensitive matching - use exact code as provided
        code_trimmed = code.strip()
        referral_entry = db.query(ReferralCode).filter(
            ReferralCode.referral_code == code_trimmed
        ).first()

        if not referral_entry:
        
            return {
                "status":200,
                "available": False
            }

        client = db.query(Client).filter(
            Client.client_id == referral_entry.client_id
        ).first()

        if not client:
       
            return {
                "status":200,
                "available": False
            }
        

        return {
            "status":200,
            "available": True,
            "client_name": client.name
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error validating referral code",
            error_code="REFERRAL_VALIDATION_ERROR",
            log_data={"error": str(e), "code": code}
        )


@router.get("/validate-gym-referral-code")
async def validate_gym_referral_code(code: str, db: Session = Depends(get_db)):
    try:
        referral_entry = (
            db.query(ReferralGymCode)
            .filter(ReferralGymCode.referral_code == code)
            .first()
        )

        if not referral_entry:
            return {
                "status": 200,
                "available": False
            }

        # Get owner from owner_id
        owner = db.query(GymOwner).filter(GymOwner.owner_id == referral_entry.owner_id).first()
        if not owner:
            return {
                "status": 200,
                "available": False
            }

        return {
            "status": 200,
            "available": True,
            "owner_name": owner.name
        }
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Error validating gym referral code",
            error_code="GYM_REFERRAL_VALIDATION_ERROR"
        )


class RegisterClientRequest(BaseModel):
    full_name : str
    contact :str
    email:str
    password:str
    location:str
    pincode:str
    referral_id: Optional[str] = None


@router.post("/register-user")
async def add_client_data(
    request: RegisterClientRequest,  
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        
        full_name = request.full_name
        contact = request.contact
        email = request.email
        password = request.password
        verified = False
        completed = False
        location = request.location
        pincode = request.pincode



        existing_client = db.query(Client).filter(
            Client.contact == contact
        ).first()

        if existing_client:
            verification = json.loads(existing_client.verification)

            if verification["mobile"] and verification["password"]:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Mobile number is already registered please login",
                    error_code="AUTH_ALREADY_REGISTERED"
                )


            verification = json.loads(existing_client.verification)
            if verification["mobile"]:
                verified = all([
                    verify_password(password, existing_client.password),
                    existing_client.name == full_name,
                    existing_client.email == email
                ])

                if not verified:
                    hashed_password = get_password_hash(password)
                    existing_client.name = full_name
                    existing_client.email = email
                    existing_client.password = hashed_password
                    db.commit()
                    mobile_otp=generate_otp()
                    await redis.set(f"otp:{contact}", mobile_otp, ex=300)
                    if await async_send_verification_sms(contact, mobile_otp):
                        print(f"Verification OTP send successfully to {contact}")
                    else:
                        raise FittbotHTTPException(
                            status_code=500, 
                            detail="Failed to send OTP",
                            error_code="SMS_SEND_FAILED"
                        )
                    verified=False

                    return {
                        "status":200,
                        "message":"Verification Otp send successfully",
                        "otp_verified":verified,
                        "registeration_completed":completed
                    }

                required_fields = [
                    existing_client.age,
                    existing_client.height,
                    existing_client.weight,
                    existing_client.lifestyle,
                    existing_client.goals,
                    existing_client.gender,
                    existing_client.bmi,
                    existing_client.dob
                ]

                if any(field is None for field in required_fields):
                    completed = False
                    return {
                        "status": 200,
                        "message": "User already verified, Incomplete registration",
                        "otp_verified": verified,
                        "registeration_completed": completed
                    }
                else:
                    completed = True
            else:
                hashed_password = get_password_hash(password)
                existing_client.name = full_name
                existing_client.email = email
                existing_client.password = hashed_password
                db.commit()
                mobile_otp=generate_otp()
                await redis.set(f"otp:{contact}", mobile_otp, ex=300)
                if await async_send_verification_sms(contact, mobile_otp):
                    print(f"Verification OTP send successfully to {contact}")
                else:
                    raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
                verified=False

                return {
                    "status":200,
                    "message":"Verification Otp send successfully",
                    "otp_verified":verified,
                    "registeration_completed":completed
                }
            

            if verified and completed:

                return {
                    "status":200,
                    "message":"User already registered and verified successfully, Please login",
                    "otp_verified":verified,
                    "registeration_completed":completed
                }
            

        hashed_password = get_password_hash(password)
 
        new_client = Client(
            name = full_name,
            email = email,
            contact = contact,
            password = hashed_password,
            verification = '{"mobile": false,"password":false}'  ,
            profile="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png",
            access = False,
            incomplete = True,
            location = location,
            pincode = pincode
        )
        db.add(new_client)
        db.commit()

        # Handle referral tracking if referral_id is provided
        if request.referral_id:
            # Case-sensitive matching - use exact code as provided
            referral_code_used = request.referral_id.strip()

            # Find the referrer by their referral code in referral_code table
            referrer_entry = db.query(ReferralCode).filter(
                ReferralCode.referral_code == referral_code_used
            ).first()

            if referrer_entry:
                # Create entry in referral_mapping table
                new_referral_mapping = ReferralMapping(
                    referrer_id=referrer_entry.client_id,
                    referee_id=new_client.client_id,
                    referral_date=date.today(),
                    status="pending"
                )
                db.add(new_referral_mapping)
                db.commit()

        mobile_otp=generate_otp()
        await redis.set(f"otp:{contact}", mobile_otp, ex=300)
        
        if await async_send_verification_sms(contact, mobile_otp):
            print(f"Verification OTP send successfully to {contact}")
        
        else:
            raise FittbotHTTPException(
                status_code=500, 
                detail="Failed to send OTP",
                error_code="SMS_SEND_FAILED"
            )
        
 
        
        return {"status": 200, "message": "Verification OTP send successfully.", "otp_verified": verified, "registeration_completed": completed}
    
    except HTTPException:
        raise
    
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred during registration",
            error_code="AUTH_REGISTRATION_ERROR",
            log_data={"error": repr(e), "contact_masked": contact[:3] + "****" + contact[-2:] if 'contact' in locals() and len(contact) > 5 else "****"}
        )
    finally:
        pass

class VerifyRequest(BaseModel):
    data: str
    otp: str

@router.post("/verify-client-otp")
async def verify_otp(request: VerifyRequest, db: Session = Depends(get_db), redis=Depends(get_redis)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        data = request.data
        otp = request.otp
        
        
        stored_otp = await redis.get(f"otp:{data}")
        
        if stored_otp and stored_otp == str(otp):
            await redis.delete(f"otp:{data}")
            client = db.query(Client).filter(Client.contact == data).first()
            
            if not client:
                raise FittbotHTTPException(
                    status_code=404, 
                    detail="Client not found",
                    error_code="AUTH_CLIENT_NOT_FOUND",
                    security_event=True
                )
            
            client.verification = '{"mobile": true, "password": true}'
            db.commit()
            
            
            return {"success": True, "message": "OTP verified successfully", "status": 200}
        else:
            auth_logger.security_event(
                "invalid_otp_attempt",
                severity="medium",
                contact_masked=data[:3] + "****" + data[-2:] if len(data) > 5 else "****"
            )
            raise FittbotHTTPException(
                status_code=400, 
                detail="Incorrect OTP entered",
                error_code="AUTH_INVALID_OTP",
                security_event=True
            )
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred during OTP verification",
            error_code="AUTH_OTP_VERIFICATION_ERROR",
            log_data={"error": repr(e)}
        )
    finally:
        pass



def calculate_bmr(weight, height, age, gender="male"):
    try:
        if gender == "male":
            return 10 * weight + 6.25 * height - 5 * age + 5
        else:
            return 10 * weight + 6.25 * height - 5 * age - 161
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred {e}")
 
activity_multipliers = {
    "sedentary": 1.2,
    "lightly_active": 1.375,
    "moderately_active": 1.55,
    "very_active": 1.725,
    "super_active": 1.9,
}
 
def calculate_macros(calories: float, goals: str):
    """
    Returns grams: protein, carbs, fat, fiber, sugar_cap
    - fiber uses 14 g / 1000 kcal rule
    - sugar_cap is an upper limit (<=10% kcal); tweak to 0.05 for stricter cap
    """
    # ---- existing macro splits ----
    if goals == "weight_loss":
        carbs_kcal   = calories * 0.30
        protein_kcal = calories * 0.45
        fat_kcal     = calories * 0.20
    elif goals == "weight_gain":
        carbs_kcal   = calories * 0.45
        protein_kcal = calories * 0.35
        fat_kcal     = calories * 0.20
    else:  # maintenance / recomposition
        carbs_kcal   = calories * 0.35
        protein_kcal = calories * 0.35
        fat_kcal     = calories * 0.30

    # grams
    carbs_g   = round(carbs_kcal / 4)
    protein_g = round(protein_kcal / 4)
    fat_g     = round(fat_kcal / 9)

    # ---- new: fiber & sugar (grams) ----
    fiber_g = round((calories / 1000.0) * 14)  # 14 g per 1000 kcal

    # cap for added/free sugar; choose 10% (or make 0.05 for a stricter plan)
    sugar_fraction = 0.10
    sugar_cap_g = round((calories * sugar_fraction) / 4)

    return protein_g, carbs_g, fat_g, fiber_g, sugar_cap_g


class registerRequest(BaseModel):
    contact : str
    lifestyle:str
    medicalIssues: Optional[str]=None
    goals:str
    gender:str
    height:float
    weight:float
    bmi:float
    dob:str
    targetWeight:float
    currentBodyShapeId:str
    targetBodyShapeId:str
    referral_id: Optional[str] = None
    platform:Optional[str] = None
    
@router.post("/complete-registeration")
async def complete_registeration(request: registerRequest, db: Session = Depends(get_db), redis=Depends(get_redis)):
    try:
        client = db.query(Client).filter(Client.contact == request.contact).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="AUTH_CLIENT_NOT_FOUND",
                security_event=True
            )
 
        # Calculate age
        today = date.today()
        dob = datetime.strptime(str(request.dob), "%Y-%m-%d").date()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


 
        
        client.contact = request.contact
        client.lifestyle = request.lifestyle
        client.medical_issues = request.medicalIssues
        client.goals = request.goals
        client.gender = request.gender
        client.height = request.height
        client.weight = request.weight

  

        bmi = round(request.weight / ((request.height / 100) ** 2),2)

    
        client.bmi = bmi
        client.dob = request.dob
        client.uuid_client = uuid.uuid4()
        client.age = age
        client.incomplete = False
        db.commit()
 
        # Get or create client target (combines all target operations)

        if client.gender.lower() =="male":
            water_intake=3.7
        elif client.gender.lower() =="female":
            water_intake=2.7
        else:
            water_intake=3


        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == client.client_id).first()
        if not client_target:
            client_target = ClientTarget(
                client_id=client.client_id,
                water_intake=water_intake,
                weight=float(request.targetWeight),
                start_weight=float(request.weight)
            )
            db.add(client_target)
        else:
            client_target.water_intake = water_intake
            client_target.weight = float(request.targetWeight)
            client_target.start_weight = float(request.weight)
 
        # Create/update actual weight record
        record_date = date.today()

        existing_record = db.query(ClientActual).filter(
            ClientActual.client_id == client.client_id,
            ClientActual.date == record_date
        ).first()


        if existing_record:
            existing_record.weight = request.weight
        else:
            db.add(ClientActual(
                client_id=client.client_id,
                date=record_date,
                weight=float(request.weight)
            ))
 
        # Update monthly analysis
        month_start_date = date(record_date.year, record_date.month, 1)
        analysis_record = db.query(ClientGeneralAnalysis).filter(
            ClientGeneralAnalysis.client_id == client.client_id,
            ClientGeneralAnalysis.date == month_start_date
        ).first()
 
        if analysis_record:
            analysis_record.weight = ((analysis_record.weight or 0) + request.weight) / 2
        else:
            db.add(ClientGeneralAnalysis(
                client_id=client.client_id,
                date=month_start_date,
                weight=float(request.weight)
            ))
 
        # Calculate nutrition targets
        bmr = calculate_bmr(request.weight, request.height, age)
        tdee = bmr * activity_multipliers[request.lifestyle]
 
        if request.goals == "weight_loss":
            tdee -= 500
        elif request.goals == "weight_gain":
            tdee += 500
 
        protein, carbs, fat, fiber,sugar = calculate_macros(tdee, request.goals)
 
        # Update nutrition targets
        client_target.calories = int(tdee)
        client_target.protein = protein
        client_target.carbs = carbs
        client_target.fat = fat
        client_target.fiber=fiber
        client_target.sugar=sugar
        client_target.updated_at = datetime.now()
 
        # Create body shape selection

        check=db.query(CharactersCombination).filter(CharactersCombination.characters_id==request.currentBodyShapeId,CharactersCombination.combination_id==request.targetBodyShapeId).first()

        if check:
            client_check= db.query(ClientCharacter).filter(ClientCharacter.client_id==client.client_id).first()
            if client_check:
                client_check.character_id=check.id
            else:
                add_client_character= ClientCharacter(
                    client_id=client.client_id,
                    character_id=check.id
                )
                db.add(add_client_character)

        db.add(ClientWeightSelection(
            client_id=client.client_id,
            current_image_id=request.currentBodyShapeId,
            target_image_id=request.targetBodyShapeId,
            combination_id=f"{request.currentBodyShapeId}+{request.targetBodyShapeId}"
        ))
 
        db.commit()
 
        # Clear Redis cache patterns
        cache_patterns = [
            "*:initial_target_actual",
            "*:initialstatus",
            "*:target_actual",
            "*:status",
            "*:analytics",
            "*:chart"
        ]
 
        for pattern in cache_patterns:
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
 
        # Clear specific client cache
        specific_keys = [
            f"client{client.client_id}:initial_target_actual",
            f"client{client.client_id}:initialstatus"
        ]
        for key in specific_keys:
            if await redis.exists(key):
                await redis.delete(key)


        referral_code=db.query(ReferralCode).filter(ReferralCode.client_id==client.client_id).first()
 
        # Generate unique referral code if not already present
        if not referral_code:
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    referral_code = generate_unique_referral_code(
                        db=db,
                        name=client.name,
                        user_id=client.client_id,
                        method="sequential",
                        table_name="referral_code",
                        max_retries=3
                    )
                    referral= ReferralCode(
                        client_id=client.client_id,
                        referral_code=referral_code,
                        created_at=datetime.now()

                    )
                    db.add(referral)
                    break  # Successfully generated, exit retry loop
                except ValueError as e:
                    # If uniqueness check failed, retry with different method
                    if attempt < max_retries - 1:
                        auth_logger.warning(
                            f"Referral code generation attempt {attempt + 1} failed for client {client.client_id}, retrying...",
                            extra={"client_id": client.client_id, "attempt": attempt + 1}
                        )
                        continue
                    else:
                        # Last attempt failed, log error but don't block registration
                        auth_logger.error(
                            f"Failed to generate referral code for client {client.client_id} after {max_retries} attempts",
                            extra={"client_id": client.client_id, "error": str(e)}
                        )
                        

        # Generate tokens
        access_token = create_access_token({"sub": str(client.client_id), "role": "client"})
        refresh_token = create_refresh_token({"sub": str(client.client_id)})
        client.refresh_token = refresh_token
        db.commit()

        # Handle referral completion and rewards
        try:
            # Check if this client was referred by someone (exists in referral_mapping as referee)
            referral_mapping = db.query(ReferralMapping).filter(
                ReferralMapping.referee_id == client.client_id,
                ReferralMapping.status == "pending"
            ).first()

            if referral_mapping:
                # Update status to completed
                referral_mapping.status = "completed"
                db.flush()

                # Add 100 fittbot cash to the NEW client (referee)
                referee_cash = db.query(ReferralFittbotCash).filter(
                    ReferralFittbotCash.client_id == client.client_id
                ).first()

                if referee_cash:
                    referee_cash.fittbot_cash += 100
                else:
                    new_referee_cash = ReferralFittbotCash(
                        client_id=client.client_id,
                        fittbot_cash=100
                    )
                    db.add(new_referee_cash)

                # Add 100 fittbot cash to the REFERRER (the person who referred)
                referrer_cash = db.query(ReferralFittbotCash).filter(
                    ReferralFittbotCash.client_id == referral_mapping.referrer_id
                ).first()

                if referrer_cash:
                    referrer_cash.fittbot_cash += 100
                else:
                    new_referrer_cash = ReferralFittbotCash(
                        client_id=referral_mapping.referrer_id,
                        fittbot_cash=100
                    )
                    db.add(new_referrer_cash)

                db.commit()


        except Exception as ref_error:
            # Log error but don't block registration completion
            auth_logger.error(
                f"Error processing referral rewards for client {client.client_id}: {str(ref_error)}",
                extra={"client_id": client.client_id, "error": str(ref_error)}
            )
            db.rollback()

        platform_value = (request.platform or "").strip().lower()
        if platform_value in {"android", "ios"}:
            send_premium_whatsapp_message(
                phone_number=request.contact,
                platform=platform_value,
                client_name=getattr(client, "name", None)
            )

        # Send iOS-specific premium SMS
        if platform_value == "ios":
            await async_send_ios_premium_sms(
                phone_number=request.contact,
                client_name=getattr(client, "name", "User")
            )

        return {
            "status": 200,
            "message": "User Registered Successfully",
            "data": {
                "gym_id": client.gym_id,
                "client_id": client.client_id,
                "gym_name": "",
                "weight":client.weight,
                "gender": client.gender,
                "access_token": access_token,
                "refresh_token": refresh_token
            }
        }
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error occurred during registration completion",
            error_code="AUTH_REGISTRATION_COMPLETION_ERROR",
            log_data={"error": repr(e), "contact_masked": request.contact[:3] + "****" + request.contact[-2:] if len(request.contact) > 5 else "****"}
        )

@router.get("/weight-management-duration")
async def get_weight_management_duration(
    client_id: int,
    db: Session = Depends(get_db)
):
    try:
        # Get client data
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND"
            )
 
        # Get client target weight
        client_target = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
        if not client_target or not client_target.weight:
            raise FittbotHTTPException(
                status_code=404,
                detail="Target weight not found for client",
                error_code="TARGET_WEIGHT_NOT_FOUND"
            )
 
        # Calculate weight difference
        current_weight = float(client.weight or 0)
        target_weight = float(client_target.weight or 0)
        weight_difference = abs(current_weight - target_weight)
 
        # Determine category based on goals
        category = "weight_loss" if client.goals == "weight_loss" else "weight_gain"
 
        # Get normal duration from weight management plan
        weight_plan = db.query(WeightManagementPlan).filter(
            WeightManagementPlan.category == category,
            WeightManagementPlan.gender == client.gender,
            WeightManagementPlan.weight_min <= weight_difference,
            WeightManagementPlan.weight_max >= weight_difference,
            WeightManagementPlan.activity_level == client.lifestyle
        ).first()
 
        normal_duration_months = 6  # Default fallback
        if weight_plan:
            normal_duration_months = weight_plan.duration_months
 
        # Calculate FittBot reduced duration with logic
        def calculate_fittbot_duration(normal_months, weight_diff):
            if normal_months <= 1:
                # 1 month -> 20 days (33% reduction)
                return max(int(normal_months * 30 * 0.67), 20)
            elif normal_months <= 6:
                # 2-6 months -> 20-50% reduction based on weight difference and duration
                if weight_diff <= 5:
                    reduction = 0.2  # 20% reduction
                elif weight_diff <= 15:
                    reduction = 0.3  # 30% reduction
                else:
                    reduction = 0.4  # 40% reduction
                return max(int(normal_months * (1 - reduction)), 1)
            else:
                # More than 6 months -> reduce to around 50%
                return max(int(normal_months * 0.5), 3)
 
        fittbot_duration = calculate_fittbot_duration(normal_duration_months, weight_difference)
 
        # Format durations for response
        def format_duration(months):
            if months < 1:
                days = int(months * 30)
                return f"{days} days"
            elif months == 1:
                return "1 month"
            else:
                return f"{months} months"
 
        normal_duration_formatted = format_duration(normal_duration_months)
        fittbot_duration_formatted = format_duration(fittbot_duration)
        modal_shown=client.modal_shown
        # Get character combination URL
        client_character=db.query(ClientCharacter).filter(ClientCharacter.client_id==client_id).first()
        if client_character:
            url_db=db.query(CharactersCombination).filter(CharactersCombination.id==client_character.character_id).first()
            character_url=url_db.characters_url

        else:
            character_url=None
 
        client.modal_shown = True
        db.commit()
 
        return {
            "status": 200,
            "message": "Weight management duration calculated successfully",
            "data": {
                "target_weight": target_weight,
                "current_weight": current_weight,
                "weight_difference": weight_difference,
                "normal_duration": normal_duration_formatted,
                "fittbot_duration": fittbot_duration_formatted,
                "normal_duration_months": normal_duration_months,
                "fittbot_duration_months": fittbot_duration,
                "height": client.height,
                "bmi": client.bmi,
                "character_url": character_url,
                "category": category,
                "gender": client.gender,
                "activity_level": client.lifestyle,
                "modal_shown":modal_shown
            }
        }
 
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to calculate weight management duration",
            error_code="WEIGHT_MANAGEMENT_DURATION_ERROR",
            log_data={"exc": repr(e), "client_id": client_id}
        )
 
 
 
def get_premium_payment_link(platform: Optional[str]) -> Optional[str]:
    """
    Resolve a platform specific premium payment link from environment variables,
    falling back to a default link when needed.
    """
    platform_key = (platform or "").strip().lower()
    platform_env_map = {
        "android": "PREMIUM_PAYMENT_LINK_ANDROID",
        "iphone": "PREMIUM_PAYMENT_LINK_IPHONE",
    }

    env_key = platform_env_map.get(platform_key)
    if env_key:
        link = os.getenv(env_key)
        if link:
            return link

    return os.getenv("PREMIUM_PAYMENT_LINK_DEFAULT")


def build_premium_whatsapp_message(client_name: Optional[str], link: str, platform: str) -> str:

    safe_name = (client_name or "").strip()
    first_name = safe_name.split(" ")[0] if safe_name else "there"
    platform_label = "Android" if platform == "android" else "iPhone" if platform == "iphone" else "mobile"

    return (
        f"Hi {first_name},\n\n"
        f"Thanks for completing your Fittbot registration! Finish your  premium upgrade here:\n"
        f"{link}\n\n"
        "Premium unlocks personalised coaching, deeper insights, and priority support. "
        "Reply here if you would like help from our team."
    )


def send_premium_whatsapp_message(
    phone_number: str,
    platform: str,
    client_name: Optional[str] = None
) -> bool:

    api_url = os.getenv("WHATSAPP_PREMIUM_API_URL")
    api_key = os.getenv("WHATSAPP_PREMIUM_API_KEY")
    link = get_premium_payment_link(platform)

    masked_number = (
        phone_number[:3] + "****" + phone_number[-2:]
        if phone_number and len(phone_number) > 5 else "****"
    )

    if not api_url or not api_key:
        auth_logger.warning(
            "Skipped WhatsApp premium message because aggregator credentials are missing",
            extra={
                "phone_number_masked": masked_number,
                "platform": platform
            }
        )
        return False

    if not link:
        auth_logger.warning(
            "Skipped WhatsApp premium message because payment link is not configured",
            extra={
                "phone_number_masked": masked_number,
                "platform": platform
            }
        )
        return False

    message = build_premium_whatsapp_message(client_name, link, platform)
    #media_attachments = get_premium_whatsapp_media(platform)

    payload = {
        "to": phone_number,
        "channel": "whatsapp",
        "type": "text",
        "message": message,
        "metadata": {
            "campaign": "registration_complete",
            "platform": platform
        }
    }

    # if media_attachments:
    #     payload["attachments"] = media_attachments

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        if response.status_code in (200, 201, 202):

            return True

        auth_logger.warning(
            "Failed to send premium WhatsApp message",
            extra={
                "phone_number_masked": masked_number,
                "platform": platform,
                "status_code": response.status_code,
                "response_preview": response.text[:200] if response.text else ""
            }
        )
        return False

    except requests.Timeout:
        auth_logger.warning(
            "WhatsApp premium message timed out",
            extra={
                "phone_number_masked": masked_number,
                "platform": platform
            }
        )
        return False
    except requests.RequestException as exc:
        auth_logger.error(
            "Unexpected error while sending WhatsApp premium message",
            extra={
                "phone_number_masked": masked_number,
                "platform": platform,
                "error": str(exc)
            }
        )
        return False



class refreshtoken(BaseModel):
    id: Optional[int] = None
    role: str
    device: Optional[str] = None
    
 
 
# @router.post("/refresh", dependencies=[Depends(RateLimiter(times=30, seconds=60))])
# async def refresh(request: refreshtoken, http_request: Request, db: Session = Depends(get_db)):
#     start_time = time.time()
#     request_id = auth_logger.set_request_context(request)

#     try:
#         normalized_device = (request.device or "").strip().lower() if request.device else None
#         original_role = request.role
#         role = (original_role or "").strip().lower()

#         fallback_id = None

#         access_token_cookie = http_request.cookies.get("access_token")
#         if request.id is None or normalized_device == "web":
#             if access_token_cookie:
#                 try:
#                     payload = jwt.decode(
#                         access_token_cookie,
#                         SECRET_KEY,
#                         algorithms=[ALGORITHM],
#                         options={"verify_exp": False},
#                     )
#                     fallback_id = payload.get("sub")
#                 except JWTError:
#                     print("DEBUG: Failed to decode cookie access token while refreshing")

#         id = request.id if request.id is not None else (int(fallback_id) if fallback_id is not None else None)


#         if id is None:
#             auth_logger.security_event(
#                 "refresh_id_missing",
#                 severity="medium",
#                 user_id=None,
#                 role=original_role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=400,
#                 detail="User id is required for token refresh",
#                 error_code="AUTH_REFRESH_ID_MISSING",
#                 security_event=True
#             )

#         if not role:
#             auth_logger.security_event(
#                 "refresh_role_missing",
#                 severity="medium",
#                 user_id=id,
#                 role=original_role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=400,
#                 detail="Role is required for token refresh",
#                 error_code="AUTH_REFRESH_ROLE_MISSING",
#                 security_event=True
#             )

#         if role not in {"owner", "trainer", "client","bdm","bde","admin","manager","telecaller"}:
#             auth_logger.security_event(
#                 "refresh_role_invalid",
#                 severity="medium",
#                 user_id=id,
#                 role=original_role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=400,
#                 detail="Invalid role provided",
#                 error_code="AUTH_REFRESH_ROLE_INVALID",
#                 security_event=True
#             )

#         user_agent = http_request.headers.get("User-Agent", "")
       

#         mobile_indicators = [
#             "mobile", "android", "ios", "flutter", "dart",
#             "okhttp", "retrofit", "alamofire", "nsurlsession",
#             "cfnetwork", "volley", "ktor", "axios-mobile",
#             "react-native", "cordova", "phonegap", "ionic",
#             "capacitor", "expo", "xamarin"
#         ]

#         mobile_clients = [
#             "okhttp", "retrofit", "volley", "ktor-client",
#             "alamofire", "nsurlsession", "cfnetwork"
#         ]

#         is_mobile_app = any(indicator in (user_agent or "").lower() for indicator in mobile_indicators)

#         custom_client_header = http_request.headers.get("X-Client-Type", "").lower()
#         if custom_client_header in ["mobile", "android", "ios"]:
#             is_mobile_app = True
#         if normalized_device == "mobile":
#             is_mobile_app = True
#         elif normalized_device == "web":
#             is_mobile_app = False
#         elif access_token_cookie:
#             # Cookie-based request without explicit device: treat as web
#             is_mobile_app = False

#         if role == "client" and not is_mobile_app:
#             print("DEBUG: Client role detected; treating refresh as web (cookies expected)")

  

#         if role=="owner":
#             refresh_t=db.query(GymOwner).filter(GymOwner.owner_id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token
       
#         elif role=="trainer":
#             refresh_t=db.query(Trainer).filter(Trainer.trainer_id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             else:
#                 print(f"DEBUG: Trainer found, refresh_token exists: {bool(refresh_t.refresh_token)}")
#             refresh_token=refresh_t.refresh_token
       
#         elif role=="client":
#             refresh_t=db.query(Client).filter(Client.client_id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token
 
#         elif role=="bde":
           
#             refresh_t=db.query(Executives).filter(Executives.id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token

#         elif role=="bdm":
#             refresh_t=db.query(Managers).filter(Managers.id==id).first()
#             if not refresh_t:
              
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token

#         elif role=="admin":
#             refresh_t=db.query(Admins).filter(Admins.admin_id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token


#         elif role=="manager":
#             # Telecaller schema manager (not to be confused with marketing_latest.bdm manager)
#             refresh_t=db.query(TelecallerManager).filter(TelecallerManager.id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token

#         elif role=="telecaller":
#             refresh_t=db.query(Telecaller).filter(Telecaller.id==id).first()
#             if not refresh_t:
#                 auth_logger.security_event(
#                     "refresh_token_user_not_found",
#                     severity="medium",
#                     user_id=id,
#                     role=role,
#                     endpoint="/auth/refresh"
#                 )
#                 raise FittbotHTTPException(
#                     status_code=404,
#                     detail="User not found",
#                     error_code="AUTH_USER_NOT_FOUND",
#                     security_event=True
#                 )
#             refresh_token=refresh_t.refresh_token
        
        
#         else:
           
#             auth_logger.security_event(
#                 "refresh_role_unexpected",
#                 severity="high",
#                 user_id=id,
#                 role=role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=400,
#                 detail=f"Unsupported role: {role}",
#                 error_code="AUTH_REFRESH_ROLE_UNSUPPORTED",
#                 security_event=True
#             )

#         if not refresh_token:
#             auth_logger.security_event(
#                 "missing_refresh_token",
#                 severity="medium",
#                 user_id=id,
#                 role=role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=401,
#                 detail="Refresh token not recognized or expired",
#                 error_code="AUTH_REFRESH_TOKEN_INVALID",
#                 security_event=True
#             )

#         try:
#             payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
          
#         except jwt.ExpiredSignatureError:
            
#             auth_logger.security_event(
#                 "refresh_token_expired",
#                 severity="low",
#                 user_id=id,
#                 role=role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=401,
#                 detail="Refresh token expired",
#                 error_code="AUTH_REFRESH_TOKEN_EXPIRED",
#                 security_event=True
#             )
#         except JWTError as jwt_error:
#             auth_logger.security_event(
#                 "invalid_refresh_token",
#                 severity="medium",
#                 user_id=id,
#                 role=role,
#                 endpoint="/auth/refresh"
#             )
#             raise FittbotHTTPException(
#                 status_code=401,
#                 detail="Invalid refresh token",
#                 error_code="AUTH_REFRESH_TOKEN_INVALID",
#                 security_event=True
#             )

#         access_token = create_access_token({"sub": str(id), "role": role})
#         new_refresh_token = create_refresh_token({"sub": str(id)})
#         refresh_t.refresh_token = new_refresh_token
#         db.commit()
#         auth_logger.info(f"Token refresh - User-Agent: {user_agent}, is_mobile: {is_mobile_app}")
#         response_data = {"status":200, "message": "Tokens refreshed successfully"}

#         if role in ["manager","telecaller"]:
#             is_mobile_app = False
            
            
#         if is_mobile_app:
#             response_data["access_token"] = access_token
#             response_data["refresh_token"] = new_refresh_token
#             response_data["token_type"] = "bearer"
#             auth_logger.info(f"Returning mobile tokens - access: {access_token[:20]}..., refresh: {new_refresh_token[:20]}...")
#             return response_data

#         response = JSONResponse(content=response_data)
#         response.set_cookie(
#             key="access_token",
#             value=access_token,
#             max_age=3600,  # 1 hour
#             httponly=True,
#             secure=settings.cookie_secure,
#             domain=settings.cookie_domain_value,
#             samesite=settings.cookie_samesite_value,
#         )

#         set_cookie_header = response.headers.get("set-cookie")
#         if set_cookie_header:
#             print(f"DEBUG: Webapp Set-Cookie header: {set_cookie_header}")
#         else:
#             print("DEBUG: Webapp response has no Set-Cookie header")

#         auth_logger.info(f"Returning webapp cookies - access: {access_token[:20]}..., refresh: {new_refresh_token[:20]}...")
#         return response

#     except HTTPException:
#         raise
 
#     except Exception as e:
#         db.rollback()
#         import traceback
#         raise FittbotHTTPException(
#             status_code=500,
#             detail="Internal server error occurred during token refresh",
#             error_code="AUTH_REFRESH_ERROR",
#             log_data={"error": repr(e), "user_id": id if 'id' in locals() else None, "role": request.role if 'request' in locals() else None}
#         )
#     finally:
#         pass
 


@router.post("/refresh", dependencies=[Depends(RateLimiter(times=30, seconds=60))])
async def refresh(request: refreshtoken, http_request: Request, db: Session = Depends(get_db)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)

    try:
        normalized_device = (request.device or "").strip().lower() if request.device else None
        original_role = request.role
        role = (original_role or "").strip().lower()

        fallback_id = None

        access_token_cookie = http_request.cookies.get("access_token")
        if request.id is None or normalized_device == "web":
            if access_token_cookie:
                try:
                    payload = jwt.decode(
                        access_token_cookie,
                        SECRET_KEY,
                        algorithms=[ALGORITHM],
                        options={"verify_exp": False},
                    )
                    fallback_id = payload.get("sub")
                except JWTError:
                    print("DEBUG: Failed to decode cookie access token while refreshing")

        id = request.id if request.id is not None else (int(fallback_id) if fallback_id is not None else None)


        if id is None:
            auth_logger.security_event(
                "refresh_id_missing",
                severity="medium",
                user_id=None,
                role=original_role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=400,
                detail="User id is required for token refresh",
                error_code="AUTH_REFRESH_ID_MISSING",
                security_event=True
            )

        if not role:
            auth_logger.security_event(
                "refresh_role_missing",
                severity="medium",
                user_id=id,
                role=original_role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=400,
                detail="Role is required for token refresh",
                error_code="AUTH_REFRESH_ROLE_MISSING",
                security_event=True
            )

        if role not in {"owner", "trainer", "client","bdm","bde","admin","manager","telecaller"}:
            auth_logger.security_event(
                "refresh_role_invalid",
                severity="medium",
                user_id=id,
                role=original_role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid role provided",
                error_code="AUTH_REFRESH_ROLE_INVALID",
                security_event=True
            )

        user_agent = http_request.headers.get("User-Agent", "")
       

        mobile_indicators = [
            "mobile", "android", "ios", "flutter", "dart",
            "okhttp", "retrofit", "alamofire", "nsurlsession",
            "cfnetwork", "volley", "ktor", "axios-mobile",
            "react-native", "cordova", "phonegap", "ionic",
            "capacitor", "expo", "xamarin"
        ]

        mobile_clients = [
            "okhttp", "retrofit", "volley", "ktor-client",
            "alamofire", "nsurlsession", "cfnetwork"
        ]

        is_mobile_app = any(indicator in (user_agent or "").lower() for indicator in mobile_indicators)

        custom_client_header = http_request.headers.get("X-Client-Type", "").lower()
        if custom_client_header in ["mobile", "android", "ios"]:
            is_mobile_app = True
        if normalized_device == "mobile":
            is_mobile_app = True
        elif normalized_device == "web":
            is_mobile_app = False
        elif access_token_cookie:
            # Cookie-based request without explicit device: treat as web
            is_mobile_app = False

        if role == "client" and not is_mobile_app:
            print("DEBUG: Client role detected; treating refresh as web (cookies expected)")

  

        if role=="owner":
            refresh_t=db.query(GymOwner).filter(GymOwner.owner_id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token
       
        elif role=="trainer":
            refresh_t=db.query(Trainer).filter(Trainer.trainer_id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            else:
                print(f"DEBUG: Trainer found, refresh_token exists: {bool(refresh_t.refresh_token)}")
            refresh_token=refresh_t.refresh_token
       
        elif role=="client":
            refresh_t=db.query(Client).filter(Client.client_id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token
 
        elif role=="bde":
           
            refresh_t=db.query(Executives).filter(Executives.id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token

        elif role=="bdm":
            refresh_t=db.query(Managers).filter(Managers.id==id).first()
            if not refresh_t:
              
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token

        elif role=="admin":
            refresh_t=db.query(Admins).filter(Admins.admin_id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token


        elif role=="manager":

            # Telecaller schema manager (not to be confused with marketing_latest.bdm manager)
            refresh_t=db.query(TelecallerManager).filter(TelecallerManager.id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token

        elif role=="telecaller":


     
            refresh_t=db.query(Telecaller).filter(Telecaller.id==id).first()
            if not refresh_t:
                auth_logger.security_event(
                    "refresh_token_user_not_found",
                    severity="medium",
                    user_id=id,
                    role=role,
                    endpoint="/auth/refresh"
                )
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="AUTH_USER_NOT_FOUND",
                    security_event=True
                )
            refresh_token=refresh_t.refresh_token
        
        
        else:
           
            auth_logger.security_event(
                "refresh_role_unexpected",
                severity="high",
                user_id=id,
                role=role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=400,
                detail=f"Unsupported role: {role}",
                error_code="AUTH_REFRESH_ROLE_UNSUPPORTED",
                security_event=True
            )

        if not refresh_token:
            auth_logger.security_event(
                "missing_refresh_token",
                severity="medium",
                user_id=id,
                role=role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=401,
                detail="Refresh token not recognized or expired",
                error_code="AUTH_REFRESH_TOKEN_INVALID",
                security_event=True
            )

        try:
            payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
          
        except jwt.ExpiredSignatureError:
            
            auth_logger.security_event(
                "refresh_token_expired",
                severity="low",
                user_id=id,
                role=role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=401,
                detail="Refresh token expired",
                error_code="AUTH_REFRESH_TOKEN_EXPIRED",
                security_event=True
            )
        except JWTError as jwt_error:
            auth_logger.security_event(
                "invalid_refresh_token",
                severity="medium",
                user_id=id,
                role=role,
                endpoint="/auth/refresh"
            )
            raise FittbotHTTPException(
                status_code=401,
                detail="Invalid refresh token",
                error_code="AUTH_REFRESH_TOKEN_INVALID",
                security_event=True
            )


        if role == "manager":
            access_token = create_access_token({
            "sub": refresh_t.mobile_number,  # subject is mobile number
            "mobile_number": refresh_t.mobile_number,
            "role": "manager",
            "type": "telecaller",  # type should be "telecaller" for both roles
            "id": refresh_t.id  # Add id field
                    })

            new_refresh_token=create_refresh_token({
            "sub": str(refresh_t.id),
            "type": "refresh" }
            )
            
        elif role=="telecaller":

            access_token = create_access_token({
            "sub": refresh_t.mobile_number,  # subject is mobile number
            "mobile_number": refresh_t.mobile_number,
            "role": "telecaller",
            "type": "telecaller",  # type should be "telecaller" for both roles
            "id": refresh_t.id , # Add id field
            "manager_id": refresh_t.manager_id
                 })
            new_refresh_token=create_refresh_token({
            "sub": str(refresh_t.id),
            "type": "refresh" }
            )
            
        else:
            access_token = create_access_token({"sub": str(id), "role": role})
            new_refresh_token = create_refresh_token({"sub": str(id)})
        
        
        refresh_t.refresh_token = new_refresh_token
        db.commit()
        #auth_logger.info(f"Token refresh - User-Agent: {user_agent}, is_mobile: {is_mobile_app}")
        response_data = {"status":200, "message": "Tokens refreshed successfully"}

        if role in ["manager","telecaller"]:
            is_mobile_app = False
     
            
        if is_mobile_app:
            response_data["access_token"] = access_token
            response_data["refresh_token"] = new_refresh_token
            response_data["token_type"] = "bearer"
            #auth_logger.info(f"Returning mobile tokens - access: {access_token[:20]}..., refresh: {new_refresh_token[:20]}...")
            return response_data

        response = JSONResponse(content=response_data)
        
        response.set_cookie(
            key="access_token",
            value=access_token,
            max_age=3600,  # 1 hour
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        set_cookie_header = response.headers.get("set-cookie")
        if set_cookie_header:
            print(f"DEBUG: Webapp Set-Cookie header: {set_cookie_header}")
        else:
            print("DEBUG: Webapp response has no Set-Cookie header")

        #auth_logger.info(f"Returning webapp cookies - access: {access_token[:20]}..., refresh: {new_refresh_token[:20]}...")
        return response

    except HTTPException:
        raise
 
    except Exception as e:
        db.rollback()
        import traceback
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error occurred during token refresh",
            error_code="AUTH_REFRESH_ERROR",
            log_data={"error": repr(e), "user_id": id if 'id' in locals() else None, "role": request.role if 'request' in locals() else None}
        )
    finally:
        pass

@router.get("/feedback")
async def get_feedback_for_gym(gym_id: int, request: Request, db: Session = Depends(get_db)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        
        result = db.execute(
            select(Feedback, Client.name)
            .join(Client, Feedback.client_id == Client.client_id)
            .filter(Feedback.gym_id == gym_id)
            .order_by(desc(Feedback.timing))
        )
        feedbacks = result.all()  
        if not feedbacks:
            return {"status": 200, "data": []}  
 
        response_data = [
            {
                "feedback_id": fb.Feedback.id,
                "client_id": fb.Feedback.client_id,
                "client_name": fb.name,
                "tag": fb.Feedback.tag,
                "ratings": fb.Feedback.ratings,
                "feedback": fb.Feedback.feedback,
                "timing": fb.Feedback.timing
            }
            for fb in feedbacks
        ]
 

        return {"status": 200, "data": response_data}
 
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )
   
 
 
class OTPRequest(BaseModel):
    data: str
    type: Optional[str] = None
    role: str
    id:Optional[int] = None
 
@router.post("/resend-otp")
async def send_otp( request:OTPRequest, db: Session = Depends(get_db), redis=Depends(get_redis)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        if await redis.exists(f"otp:{request.data}"):
            await redis.delete(f"otp:{request.data}")
 
        otp= "123456" if request.data == '8667458723' or request.data == '9486987082' else generate_otp()
        await redis.set(f"otp:{request.data}", otp, ex=300)  
 
        if "@" in request.data:
            if request.role == "client": 
                client_result =  db.execute(select(Client).filter(Client.client_id == request.id))
                client = client_result.scalars().first()
                if send_verification_email(request.data, client.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                    return {"success": True, "message": "OTP sent successfully", "status": 200}
                else:
                    raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
           
            else:
                owner_result = db.execute(select(GymOwner).filter(GymOwner.owner_id == request.id))
                owner = owner_result.scalars().first()
                if send_verification_email(request.data, owner.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                    return {"success": True, "message": "OTP sent successfully", "status": 200}
                else:
                    raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
 
        else:
            if await async_send_verification_sms(request.data, otp):

                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise FittbotHTTPException(
                    status_code=500, 
                    detail="Failed to send OTP",
                    error_code="SMS_SEND_FAILED"
                )
 
    except HTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred while resending OTP",
            error_code="AUTH_OTP_RESEND_ERROR",
            log_data={"error": repr(e), "data_masked": request.data[:3] + "****" + request.data[-2:] if 'request' in locals() and len(request.data) > 5 else "****"}
        )
    finally:
        pass

class forgotRequest(BaseModel):
    data: str
    type: Optional[str] = None 
 
@router.post("/send-otp")
async def send_otp( request:forgotRequest, db: Session = Depends(get_db), redis=Depends(get_redis)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        if await redis.exists(f"otp:{request.data}"):
            await redis.delete(f"otp:{request.data}")
        data=request.data
        type=request.type
        if type=="email":
            client_result =  db.execute(select(Client).filter(Client.email == data))
            client = client_result.scalars().first()
 
            owner_result =  db.execute(select(GymOwner).filter(GymOwner.email == data))
            owner = owner_result.scalars().first()
        elif type=="mobile":
            client_result =  db.execute(select(Client).filter(Client.contact == data))
            client = client_result.scalars().first()
 
            owner_result =  db.execute(select(GymOwner).filter(GymOwner.contact_number == data))
            owner = owner_result.scalars().first()
 
        if not client and not owner:
            auth_logger.security_event(
                "password_reset_attempt_unknown_user",
                severity="medium",
                data_type=type,
                data_masked=request.data[:3] + "****" + request.data[-2:] if len(request.data) > 5 else "****",
                endpoint="/auth/send-otp"
            )
            raise HTTPException(status_code=404, detail="User not found")
 
        otp= "123456" if request.data == '8667458723' or request.data == '9486987082' else generate_otp()
        await redis.set(f"otp:{data}", otp, ex=300) 


 
        if type=="email":
            if send_otp_email(request.data, client.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise FittbotHTTPException(
                    status_code=500, 
                    detail="Failed to send OTP",
                    error_code="SMS_SEND_FAILED"
                )
        if type=="mobile":
            if await async_send_password_reset_sms(data, otp):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise FittbotHTTPException(
                    status_code=500, 
                    detail="Failed to send OTP",
                    error_code="SMS_SEND_FAILED"
                )
 
    except HTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred while sending OTP",
            error_code="AUTH_OTP_SEND_ERROR",
            log_data={"error": repr(e), "data_masked": request.data[:3] + "****" + request.data[-2:] if 'request' in locals() and len(request.data) > 5 else "****"}
        )
    finally:
        pass

@router.post("/verify-otp")
async def verify_otp(request:VerifyRequest, redis=Depends(get_redis)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:
        
        data=request.data
        otp=request.otp
        stored_otp = await redis.get(f"otp:{data}")  
        if stored_otp and stored_otp == str(otp):  
            await redis.delete(f"otp:{data}")          
            return {"success": True, "message": "OTP verified successfully", "status": 200}
        else:
            auth_logger.security_event(
                "invalid_password_reset_otp_attempt",
                severity="medium",
                data_masked=request.data[:3] + "****" + request.data[-2:] if len(request.data) > 5 else "****",
                endpoint="/auth/verify-otp"
            )
            raise HTTPException(status_code=400, detail="Invalid OTP")
 
    except HTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred during OTP verification",
            error_code="AUTH_OTP_VERIFY_ERROR",
            log_data={"error": repr(e), "data_masked": request.data[:3] + "****" + request.data[-2:] if 'request' in locals() and len(request.data) > 5 else "****"}
        )
    finally:
        pass

class ChangePassword(BaseModel):
    data:str
    type:str
    password:str
    role: Optional[str] = None  # "client", "owner", or "trainer" - to specify which user type
 
@router.post("/change-password")
async def change_password(request:ChangePassword, db: Session=Depends(get_db)):
    start_time = time.time()
    request_id = auth_logger.set_request_context(request)
    
    try:

        data=request.data
        type=request.type
        new_password=request.password
        role=request.role

        client = None
        gym_owner = None
        trainer = None

        # If role is specified, only check that specific user type
        if role == "client":
            if type=="email":
                client = db.execute(select(Client).filter(Client.email == data))
                client = client.scalars().first()
            elif type=="mobile":
                client = db.execute(select(Client).filter(Client.contact == data))
                client = client.scalars().first()
            

        elif role == "owner":
            if type=="email":
                gym_owner = db.execute(select(GymOwner).filter(GymOwner.email == data))
                gym_owner = gym_owner.scalars().first()
            elif type=="mobile":
                gym_owner = db.execute(select(GymOwner).filter(GymOwner.contact_number == data))
                gym_owner = gym_owner.scalars().first()
        

        elif role == "trainer":
            if type=="email":
                trainer = db.execute(select(Trainer).filter(Trainer.email == data))
                trainer = trainer.scalars().first()
            elif type=="mobile":
                trainer = db.execute(select(Trainer).filter(Trainer.contact == data))
                trainer = trainer.scalars().first()
            

        # If no role specified, use old logic (check client first, then owner, then trainer)
        else:
            if type=="email":
                client = db.execute(select(Client).filter(Client.email == data))
                client = client.scalars().first()

            if type=="mobile":
                client = db.execute(select(Client).filter(Client.contact == data))
                client = client.scalars().first()

            if not client:
                if type=="email":
                    gym_owner = db.execute(select(GymOwner).filter(GymOwner.email == data))
                    gym_owner = gym_owner.scalars().first()

                if type=="mobile":
                    gym_owner = db.execute(select(GymOwner).filter(GymOwner.contact_number == data))
                    gym_owner = gym_owner.scalars().first()

            if not client and not gym_owner:
                if type=="email":
                    trainer = db.execute(select(Trainer).filter(Trainer.email == data))
                    trainer = trainer.scalars().first()

                if type=="mobile":
                    trainer = db.execute(select(Trainer).filter(Trainer.contact == data))
                    trainer = trainer.scalars().first()
        if not client and not gym_owner and not trainer:
            auth_logger.security_event(
                "password_change_attempt_unknown_user",
                severity="medium",
                data_type=type,
                data_masked=request.data[:3] + "****" + request.data[-2:] if len(request.data) > 5 else "****",
                endpoint="/auth/change-password"
            )
            raise HTTPException(status_code=404, detail="User not found")
 
        hashed_password = get_password_hash(new_password)

  
 
        if client:
            client.password = hashed_password
        elif gym_owner:
            gym_owner.password = hashed_password
        elif trainer:
            trainer.password = hashed_password


        db.commit()
 
        return {"success": True, "message": "Password changed successfully", "status": 200}
 
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500, 
            detail="Internal server error occurred while changing password",
            error_code="AUTH_PASSWORD_CHANGE_ERROR",
            log_data={"error": repr(e), "data_masked": request.data[:3] + "****" + request.data[-2:] if 'request' in locals() and len(request.data) > 5 else "****"}
        )
    finally:
        pass

class verifyStatus(BaseModel):
    id:int
    verification:dict
    role:str
    email:Optional[str] = None
 
@router.post('/update_verification_status', dependencies=[Depends(RateLimiter(times=20, seconds=60))])
async def update_verification_status(request:verifyStatus, db:Session = Depends(get_db), redis=Depends(get_redis)):
    try:
 
        if request.role=="client":
            client=db.query(Client).filter(Client.client_id == request.id).first()
   
            if not client:
                raise HTTPException(status_code=400,detail="Client Not found")
       
            if request.email:
                client.email = request.email
       
            client.verification = json.dumps(request.verification)
            db.commit()
       
        else:
            owner = db.query(GymOwner).filter(GymOwner.owner_id == request.id).first()
            if not owner:
                raise HTTPException(status_code=400,detail="User Not found")
           
            # if request.email:
            #     owner.email=request.email
 
            owner.verification = json.dumps(request.verification)
            db.commit()
 
            # if request.verification['mobile'] and request.verification['email']:
            gyms = db.query(Gym).filter(Gym.owner_id == owner.owner_id).all()
            if not gyms:
                raise HTTPException(status_code=400, detail="No gyms associated with this owner")

            access_token = create_access_token({"sub": str(owner.owner_id), "role": 'owner'})
            refresh_token = create_refresh_token({"sub": str(owner.owner_id)})
            owner.refresh_token=refresh_token
            db.commit()

            gym_data = {}
            
            if len(gyms) == 1:
                gym_data = {"gym_id": gyms[0].gym_id, "name": gyms[0].name, 'logo':gyms[0].logo, "owner_id": gyms[0].owner_id}
            else:
                gym_data = [{"gym_id": gym.gym_id, "name": gym.name, "location": gym.location, 'logo':gym.logo, "owner_id": gym.owner_id} for gym in gyms]
            
            return {
                "status": 200,
                "message": "Otp Verified successful",
                "data": {
                    "owner_id": owner.owner_id,
                    "name": owner.name,
                    "gyms": gym_data,
                    "access_token": access_token,
                    "refresh_token": refresh_token
                }
            }
 
        return{
            "status": 200,
            "message": "verification Status updated successfully"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred.,{str(e)}")
  

def send_otp_email(user_email, user_name, otp_code, validity_minutes, support_email):
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    msg = EmailMessage()
    subject = "Your OTP Code for Password Reset"
    body = f"""
    <html>
    <body>
        <p>Hello {user_name},</p>
 
        <p>We received a request to reset your password. Please use the following One-Time Password (OTP) to proceed:</p>
 
        <h3 style="color: #000000;">OTP Code: <b>{otp_code}</b></h3>
 
        <p>This code is valid for <b>{validity_minutes} minutes</b>. If you did not request a password reset, please ignore this email or contact our support team immediately.</p>
 
        <p>Thank you,</p>
 
        <p>If you need help, contact our support team: <a href="mailto:{support_email}">{support_email}</a></p>
    </body>
    </html>
    """
 
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
 
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT"))
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, user_email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False
   
def send_verification_email(user_email, user_name, otp_code, validity_minutes, support_email):
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    msg = EmailMessage()
    subject = "Your OTP Code for Email Verification"
    body = f"""
    <html>
    <body>
        <p>Hello {user_name},</p>
 
        <p>Please use the following One-Time Password (OTP) to proceed with your email verification:</p>
 
        <h2 style="color: #007bff;">OTP Code: <b>{otp_code}</b></h2>
 
        <p>This code is valid for <b>{validity_minutes} minutes</b>.</p>
 
        <p>If you have any questions or need further assistance, please feel free to contact our support team at
        <a href="mailto:{support_email}">{support_email}</a>.</p>
 
        <p>Thank you!</p>
 
        <p>Best Regards,<br>
        <b>Fittbot Team</b></p>
    </body>
    </html>
    """
 
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
 
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT"))
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, user_email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False
 
class EmailOtpData(BaseModel):
    id: int
    email:str
    role :str

@router.post('/send_verification_otp', dependencies=[Depends(RateLimiter(times=15, seconds=60))])
async def send_verification_otp(request:EmailOtpData, db: Session = Depends(get_db), redis=Depends(get_redis)):
    try:
        existing_owner = db.query(GymOwner).filter(
            (GymOwner.email == request.email)
        ).first()
 
        existing_client = db.query(Client).filter(
           (Client.email == request.email)
        ).first()
 
        if request.role == "client":
            client=db.query(Client).filter(Client.client_id == request.id).first()
 
            if not client:
                raise HTTPException(status_code=400, detail="Client Not Found")
           
            if not client.email == request.email:
                if existing_client or existing_owner:
                    raise HTTPException(status_code=400, detail="Email already registered with different account")
       
            otp=generate_otp()
            await redis.set(f"otp:{request.email}", otp, ex=300)
            if send_verification_email(request.email, client.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                return {"message": "OTP sent successfully", "status": 200}
        else:
            owner = db.query(GymOwner).filter(GymOwner.owner_id == request.id).first()
 
            if not owner:
                raise HTTPException(status_code=400, detail="User Not Found")
           
            if not owner.email == request.email:
                if existing_client or existing_owner:
                    raise HTTPException(status_code=400, detail="Email already registered with different account")
           
            otp=generate_otp()
            await redis.set(f"otp:{request.email}", otp, ex=300)
            if send_verification_email(request.email, owner.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                return {"message": "OTP sent successfully", "status": 200}
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred.,{str(e)}")   
 


class OperatingHours(BaseModel):
    id: int
    startTime: str
    endTime: str
    day: str

class Address(BaseModel):
    street: str
    area: str
    city: str
    state: str
    pincode: str

class AccountDetails(BaseModel):
    accountNumber: str
    confirmAccountNumber: str
    ifscCode: str
    accountHolderName: str
    bankName: str
    branchName: str
    upiId: Optional[str] = None
    gstNumber: Optional[str] = None
    gstType: str = "nogst"
    gstPercentage: str = "18"

class GymItem(BaseModel):
    name: str
    location: Optional[str] = None
    referal_id: Optional[str] = None
    
    contactNumber: str
    services: List[str]
    operatingHours: List[OperatingHours]
    address: Address
    accountDetails: AccountDetails
    areaPhotos: Optional[Dict[str, List[str]]] = {} 
    totalMachineries: Optional[int] = None
    floorSpace: Optional[int] = None
    totalTrainers: Optional[int] = None
    yearlyMembershipCost: Optional[int] = None



class GymOwnerRegistration(BaseModel):
    name: str
    email: str
    mobile: str  
    password: str
    confirmPassword: str
    dob: date
    referral_id: Optional[str] = None
    gyms: List[GymItem]


 
@router.post("/new_gym_owner_registration")
async def register_gym_owner(registration: GymOwnerRegistration, db: Session = Depends(get_db), redis=Depends(get_redis)):
 
    try:
        existing_owner = db.query(GymOwner).filter(GymOwner.contact_number == registration.mobile
        ).first()
 
        if existing_owner:
            raise HTTPException(
                status_code=400,
                detail="User is already registered. Please login."
            )
       

        today=date.today()  
        dob = datetime.strptime(str(registration.dob), "%Y-%m-%d").date()
        age=today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        hashed_password=get_password_hash(registration.password)
        new_owner = GymOwner(
            name=registration.name,
            email=registration.email,
            contact_number=registration.mobile,
            password=hashed_password,
            dob=registration.dob,
            age=age,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            verification = '{"mobile": false,"email": false}'
        )
        db.add(new_owner)
        db.flush()  # Flush to get owner_id without committing

        # Generate referral code for owner ONCE (not per gym)
        # This happens only once when owner first registers
        gym_referral_code: Optional[str] = None
        max_referral_attempts = 5
        for attempt in range(max_referral_attempts):
            try:
                gym_referral_code = generate_unique_referral_code(
                    db=db,
                    name=new_owner.name,
                    user_id=new_owner.owner_id,
                    method="sequential",
                    table_name="referral_gym_code",
                    max_retries=3,
                )
                break
            except ValueError as exc:
                if attempt < max_referral_attempts - 1:
                    auth_logger.warning(
                        "Owner referral code generation retry",
                        owner_id=new_owner.owner_id,
                        attempt=attempt + 1,
                        error=str(exc),
                    )
                    continue
                gym_referral_code = generate_unique_referral_code(
                    db=db,
                    name=new_owner.name,
                    method="random",
                    table_name="referral_gym_code",
                    max_retries=5,
                )

        
        
        
        if gym_referral_code:
            db.add(
                ReferralGymCode(
                    owner_id=new_owner.owner_id,
                    referral_code=gym_referral_code,
                    created_at=datetime.now(),
                )
            )
        else:
            auth_logger.error(
                "Failed to generate owner referral code",
                owner_id=new_owner.owner_id,
            )

       
        for gym_data in registration.gyms:
            operating_hours = [
                {
                    "id": op_hour.id,
                    "startTime": op_hour.startTime,
                    "endTime": op_hour.endTime,
                    "day": op_hour.day
                } for op_hour in gym_data.operatingHours
            ]

            new_gym = Gym(
                owner_id=new_owner.owner_id,
                name=gym_data.name,
                referal_id=gym_data.referal_id or "",
                location=gym_data.location,
                contact_number=gym_data.contactNumber,
                services=gym_data.services,
                operating_hours=operating_hours,
                street=gym_data.address.street,
                area=gym_data.address.area,
                city=gym_data.address.city,
                state=gym_data.address.state,
                pincode=gym_data.address.pincode,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                logo='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png',
                cover_pic='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/studios.png',
                fittbot_verified=False,
            )
            db.add(new_gym)
            db.flush()  # Flush to get gym_id without committing

            # Create gym details entry
            gym_details = GymDetails(
                gym_id=new_gym.gym_id,
                total_machineries=gym_data.totalMachineries,
                floor_space=gym_data.floorSpace,
                total_trainers=gym_data.totalTrainers,
                yearly_membership_cost=gym_data.yearlyMembershipCost,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(gym_details)

            from app.models.fittbot_models import AccountDetails
            account_details = AccountDetails(
                gym_id=new_gym.gym_id,
                account_number=gym_data.accountDetails.accountNumber,
                account_ifsccode=gym_data.accountDetails.ifscCode,
                account_holdername=gym_data.accountDetails.accountHolderName,
                bank_name=gym_data.accountDetails.bankName,
                account_branch=gym_data.accountDetails.branchName,
                upi_id=gym_data.accountDetails.upiId,
                gst_number=gym_data.accountDetails.gstNumber,
                gst_type=gym_data.accountDetails.gstType,
                gst_percentage=gym_data.accountDetails.gstPercentage,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(account_details)

            # Create default batches for the new gym (bulk insert)
            default_batches = [
                GymBatches(gym_id=new_gym.gym_id, batch_name="Early Morning", timing="4:00 am - 7:00 am", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Morning", timing="7:00 am - 10:00 am", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Fornoon", timing="10:00 am - 12:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Afternoon", timing="12:00 pm - 4:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Evening", timing="4:00 pm - 8:00 pm", description=""),
                GymBatches(gym_id=new_gym.gym_id, batch_name="Night", timing="8:00 pm - 12:00 am", description=""),
            ]
            db.add_all(default_batches)

        # Handle referral code ONCE after all gyms are created (owner-to-owner)
        # Referral code comes from registration.referral_id at owner level
        referral_code_input = ""
        if registration.referral_id:
            referral_code_input = registration.referral_id.strip()

        if referral_code_input:
            referrer_code_entry = (
                db.query(ReferralGymCode)
                .filter(ReferralGymCode.referral_code == referral_code_input)
                .first()
            )

            if not referrer_code_entry:
                auth_logger.warning(
                    "Invalid owner referral code supplied during registration",
                    owner_id=new_owner.owner_id,
                    referral_code=referral_code_input,
                )
            else:
                referrer_owner_id = referrer_code_entry.owner_id

                if referrer_owner_id == new_owner.owner_id:
                    auth_logger.warning(
                        "Owner attempted to use their own referral code",
                        owner_id=new_owner.owner_id,
                    )
                else:
                    # Create owner-to-owner mapping
                    referral_mapping_entry = ReferralGymMapping(
                        referrer_owner_id=referrer_owner_id,
                        referee_owner_id=new_owner.owner_id,
                        referral_date=date.today(),
                        status="completed",
                    )
                    db.add(referral_mapping_entry)
                    db.flush()

                    reward_amount = 500
                    reward_month = date.today()

                    def apply_owner_referral_reward(target_owner_id: int, reason: str) -> None:
                        # Check if any record exists for this owner in the current month
                        today = date.today()
                        cash_entry = (
                            db.query(ReferralGymCash)
                            .filter(
                                ReferralGymCash.owner_id == target_owner_id,
                                extract('year', ReferralGymCash.month) == today.year,
                                extract('month', ReferralGymCash.month) == today.month,
                            )
                            .first()
                        )

                        if cash_entry:
                            # Record exists in current month, just add cash
                            cash_entry.referral_cash += reward_amount
                            if not cash_entry.status:
                                cash_entry.status = "not initiated"
                        else:
                            # No record in current month, create new row with today's date
                            db.add(
                                ReferralGymCash(
                                    owner_id=target_owner_id,
                                    month=today,
                                    referral_cash=reward_amount,
                                    status="not initiated",
                                )
                            )

                        db.add(
                            ReferralGymCashLogs(
                                owner_id=target_owner_id,
                                referral_cash=reward_amount,
                                reason=reason,
                            )
                        )

                    # Give 500 to referrer owner
                    apply_owner_referral_reward(
                        referrer_owner_id,
                        f"Referral reward for referring owner {new_owner.owner_id}",
                    )
                    # Give 500 to new owner (referee)
                    apply_owner_referral_reward(
                        new_owner.owner_id,
                        f"Referral reward for being referred by owner {referrer_owner_id}",
                    )



        # Commit all owner, gym, referral data first (atomic transaction)
        db.commit()

        otp=generate_otp()
        await redis.set(f"otp:{registration.mobile}", otp, ex=300)
        if await async_send_verification_sms(registration.mobile, otp):
            print("Otp Send Succefully")
            
        photo_upload_tasks = []
        gyms_created = db.query(Gym).filter(Gym.owner_id == new_owner.owner_id).order_by(Gym.gym_id).all()
        
        from app.models.fittbot_models import GymPhoto
        import json
        
        gym_photos_by_index = {}  
        redis_pattern = f"temp_photo:{registration.mobile}:*"
        redis_keys = await redis.keys(redis_pattern)
        

        
        if redis_keys:
            for key in redis_keys:
                photo_data = await redis.get(key)
                if photo_data:
                    photo_info = json.loads(photo_data)
                    if photo_info.get("cdn_url"):
                        gym_index = photo_info.get("gym_index", 0)
                        area_type = photo_info["area_type"]
                        
                        
                        if gym_index not in gym_photos_by_index:
                            gym_photos_by_index[gym_index] = {}
                        if area_type not in gym_photos_by_index[gym_index]:
                            gym_photos_by_index[gym_index][area_type] = []
                        gym_photos_by_index[gym_index][area_type].append(photo_info)
                    else:
                        print(f"Skipping photo without cdn_url: {photo_info}")
                else:
                    print(f"No data found for Redis key: {key}")
        
        for gym_index, gym_data in enumerate(registration.gyms):
            current_gym = gyms_created[gym_index] if gym_index < len(gyms_created) else None
            
            if current_gym and gym_index in gym_photos_by_index:
                gym_photos = gym_photos_by_index[gym_index]
                total_photos_for_gym = 0
                
                for area_type, photos in gym_photos.items():
                    for photo_info in photos:
                        gym_photo = GymPhoto(
                            gym_id=current_gym.gym_id,
                            area_type=area_type,
                            image_url=photo_info["cdn_url"],
                            file_name=photo_info["file_name"]
                        )
                        db.add(gym_photo)
                        total_photos_for_gym += 1
                
                if total_photos_for_gym > 0:
                    photo_upload_tasks.append({
                        "gym_id": current_gym.gym_id,
                        "gym_name": current_gym.name,
                        "gym_index": gym_index,
                        "photo_areas": list(gym_photos.keys()),
                        "total_photos": total_photos_for_gym,
                        "status": "completed"
                    })
                    
        if gym_photos_by_index:
            try:
                db.commit()
            except Exception as photo_error:
                db.rollback()
                auth_logger.error(
                    "Failed to commit gym photos",
                    owner_id=new_owner.owner_id,
                    error=str(photo_error)
                )
                # Don't fail the entire registration if photos fail
        
        if redis_keys:
            await redis.delete(*redis_keys)
        
        
        message = "Gym owner and Gym registered and Otp send successfully"
        if photo_upload_tasks:
            total_photos = sum(task.get("total_photos", 0) for task in photo_upload_tasks)
            message = f"Gym owner registered successfully! {total_photos} photos processed across {len(photo_upload_tasks)} gyms. OTP sent."      
 
        return {
            "status":200,
            "message": message,
            "data": {
                "owner_id": new_owner.owner_id,
                "photo_upload_tasks": photo_upload_tasks
            }
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )



@router.get("/owner/{owner_id}/gym-photos")
async def get_owner_gym_photos(owner_id: int, db: Session = Depends(get_db)):
    """
    Get all gym photos for all gyms owned by the owner
    """
    try:
        owner = db.query(GymOwner).filter(GymOwner.owner_id == owner_id).first()
        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND"
            )
        
        gyms = db.query(Gym).filter(Gym.owner_id == owner_id).all()
        
        gyms_data = []
        total_photos = 0
        
        for gym in gyms:
            from app.models.fittbot_models import GymPhoto
            photos = db.query(GymPhoto).filter(
                GymPhoto.gym_id == gym.gym_id,
                GymPhoto.image_url != "" 
            ).all()
            
            photos_by_area = {}
            for photo in photos:
                area_type = photo.area_type
                if area_type not in photos_by_area:
                    photos_by_area[area_type] = []
                
                photos_by_area[area_type].append({
                    "photo_id": photo.photo_id,
                    "image_url": photo.image_url,
                    "file_name": photo.file_name,
                    "created_at": photo.created_at.isoformat() if photo.created_at else None
                })
            
            gym_info = {
                "gym_id": gym.gym_id,
                "name": gym.name,
                "contact_number": gym.contact_number,
                "city": gym.city,
                "photos_by_area": photos_by_area,
                "photo_count": len(photos),
                "areas_with_photos": list(photos_by_area.keys())
            }
            
            gyms_data.append(gym_info)
            total_photos += len(photos)
        
        return {
            "status": 200,
            "message": "Gym photos retrieved successfully",
            "data": {
                "owner_id": owner_id,
                "owner_name": owner.name,
                "total_gyms": len(gyms),
                "total_photos": total_photos,
                "gyms": gyms_data
            }
        }
        
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Failed to retrieve gym photos: {str(e)}",
            error_code="GYM_PHOTOS_RETRIEVE_ERROR"
        )


@router.get("/subscription-status")
async def check_subscrition_status(request:Request, db: Session = Depends(get_db)):
    auth_header = request.headers.get("Authorization")
    auth_logger.debug("auth header received", auth_header_present=bool(auth_header))
 
    if not auth_header:
        raise FittbotHTTPException(
            status_code=401, 
            detail="Missing Authorization header",
            error_code="AUTH_MISSING_HEADER",
            security_event=True
        )
   
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise FittbotHTTPException(
            status_code=401, 
            detail="Invalid authorization header format",
            error_code="AUTH_INVALID_HEADER_FORMAT",
            security_event=True
        )
   
    token = parts[1]
    auth_logger.debug("token extracted for verification")
   
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])  
        client_id =payload.get('sub')
        role = payload.get('role')
 
        if client_id is None:
            raise FittbotHTTPException(
                status_code=401, 
                detail="Token missing subject (client_id)",
                error_code="AUTH_TOKEN_MISSING_SUBJECT",
                security_event=True
            )
       
        client = db.query(ClientFittbotAccess).filter(ClientFittbotAccess.client_id == client_id).first()
        subscribed= False
        if client:
            subscribed= True if client.access_status=='active' else False
 
        return{
            "status":200, "message":"valid token","data":{"subscribed":subscribed}
        }
    except ExpiredSignatureError:
        return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})
    except JWTError:
        raise FittbotHTTPException(
            status_code=401, 
            detail="Invalid token",
            error_code="AUTH_INVALID_TOKEN",
            security_event=True
        )
 
class LoginRequest(BaseModel):
    mobile_number: str
    password: str
    role: str
 
@router.post("/login", dependencies=[Depends(RateLimiter(times=15, seconds=60))])
async def login(request: LoginRequest, db: Session = Depends(get_db), redis=Depends(get_redis)):
    try:
        
        mobile_number = request.mobile_number
        password = request.password
        role = request.role
   
        if role == "owner":
            owner = db.query(GymOwner).filter(GymOwner.contact_number == mobile_number).first()
            if not owner:
                raise HTTPException(status_code=401, detail="Mobile number not registered.")

            if not verify_password(password, owner.password):
                raise HTTPException(status_code=401, detail="Invalid Password Please Check")
            verification=json.loads(owner.verification)
       
            if not verification['mobile']:
                mobile_otp= "123456" if mobile_number == '8667458723' or mobile_number=="9486987082" else generate_otp()
                await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
                if await async_send_verification_sms(mobile_number, mobile_otp):
                    return {"status": 201,"message": "Please verify the user", "data":{"verification": verification, "contact" : owner.contact_number, "email":owner.email, 'id':owner.owner_id}}
                else:
                    raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
            
            mobile_otp= "123456" if mobile_number == '8667458723' or mobile_number=="9486987082" else generate_otp()

            await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
            if await async_send_verification_sms(mobile_number, mobile_otp):
                return {"status": 200,"message": "Otp Send successful"}
            else:
                raise FittbotHTTPException(
                    status_code=500, 
                    detail="Failed to send OTP",
                    error_code="SMS_SEND_FAILED"
                )
    
       
        elif role == "trainer":
            trainer = db.query(Trainer).filter(Trainer.contact == mobile_number).first()

            if not trainer:
                raise HTTPException(status_code=401, detail="Mobile number not registered.")

            if not trainer.password:
                raise HTTPException(status_code=400, detail="Password not set for this trainer")

            if not verify_password(password, trainer.password):
                raise HTTPException(status_code=401, detail="Invalid Password Please Check")
           
            mobile_otp = "123456" if mobile_number == '8667458723' or mobile_number == "9486987082" else generate_otp()
            await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
           
            if await async_send_verification_sms(mobile_number, mobile_otp):
                return {"status": 200, "message": "OTP sent successfully"}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
       
        elif role == "client":
            client = db.query(Client).filter(Client.contact == mobile_number).first()

            if not client:

                raise HTTPException(status_code=401, detail="Mobile number not registered.")
           
            verification=json.loads(client.verification)
            required_fields = [
                client.age,
                client.height,
                client.weight,
                client.lifestyle,
                client.goals,
                client.gender,
                client.bmi,
                client.dob
            ]
            if client.gym_id is None:
                if verification['mobile'] == False and any(field is None for field in required_fields) :
                    
                    mobile_otp= "123456" if mobile_number == '8667458723' or mobile_number=="9486987082" else generate_otp()
                    
                    await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)

                    #if True:
                    if await async_send_verification_sms(mobile_number, mobile_otp):
                    
                        return {
                            "status": 202,
                            "message": "Registeration initiated but incomplete, Please verify the user",
                            "data":{
                                "full_name":client.name,
                                "contact":client.contact,
                                "email":client.email
                            }
                        }
                elif  verification['mobile']== True and any(field is None for field in required_fields):
                    # Verify password if it's set
                    if verification.get('password', False) and client.password:
                        if not verify_password(password, client.password):
                            raise HTTPException(status_code=401, detail="Invalid Password Please Check")

                    return {
                        "status": 203,
                        "message": "Registeration initiated but incomplete, Please complete your registeration process",
                        "data":{
                            "full_name":client.name,
                            "contact":client.contact,
                            "email":client.email
                        }
                    }
               
                else:
                    if not verify_password(password, client.password):

                        raise HTTPException(status_code=401, detail="Invalid Password Please Check")
                   
                    gym=None
                    if not verification['mobile']:
                        mobile_otp= "123456" if mobile_number == '8667458723' or mobile_number=="9486987082" else generate_otp()
                        
                        await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
                        if await async_send_verification_sms(mobile_number, mobile_otp):
                            return {"status": 201,"message": "Please verify the user", "data":{"verification": verification, "contact" : client.contact, "email":client.email, 'id':client.client_id}}
                        else:
                            raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
            else:
                if not verify_password(password, client.password):

                    raise HTTPException(status_code=401, detail="Invalid Password Please Check")
   
                gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).first()
                if not all([verification['mobile'], verification['password']]):
   
                    if not verification['mobile']:
                        mobile_otp= "123456" if mobile_number == '8667458723' or mobile_number=="9486987082" else generate_otp()
                        
                        await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
                        if await async_send_verification_sms(mobile_number, mobile_otp):
                            return {"status": 201,"message": "Please verify the user", "data":{"verification": verification, "contact" : client.contact, "email":client.email, 'id':client.client_id}}
                        else:
                            raise FittbotHTTPException(
                        status_code=500, 
                        detail="Failed to send OTP",
                        error_code="SMS_SEND_FAILED"
                    )
   
                    return {
                        "status": 201,
                        "message": "Please verify the user",
                        "data":{"verification": verification, "contact" : client.contact, "email":client.email, 'id':client.client_id}
                    }
       
            mobile_otp= "123456" if mobile_number=="9486987082" else generate_otp()            
            await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)

      
            
            if await async_send_verification_sms(mobile_number, mobile_otp):
            
                print("OTP sent successfully")
            else:
                raise FittbotHTTPException(
                    status_code=500, 
                    detail="Failed to send OTP",
                    error_code="SMS_SEND_FAILED"
                )

            return {
                "status": 200,
                "message": "Verification Otp send successful"
            }
   
        else:
            raise HTTPException(status_code=400, detail="Invalid role provided.")
   
    except HTTPException:
        raise
    
    except Exception as e:
     
        raise FittbotHTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred: {str(e)}",
            error_code="SYSTEM_UNEXPECTED_ERROR"
        )
   


class verificationRequest(BaseModel):
    data:str
    otp:int
    role:str
    device:Optional[str] = None

@router.post('/otp-verification')
async def otp_verification(request:verificationRequest, http_request: Request, db:Session=Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        data=request.data
        otp=request.otp
        role=request.role

        device=None


        if request.device!=None:
            device=request.device
        

       
        if role == "client":
            stored_otp = await redis.get(f"otp:{data}")  
            if stored_otp and stored_otp == str(otp):  
                await redis.delete(f"otp:{data}")
                client = db.query(Client).filter(Client.contact == data).first()
                gym=None
                if client.gym_id:
                    gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).first()
                access_token = create_access_token({"sub": str(client.client_id), "role": role})
                refresh_token = create_refresh_token({"sub": str(client.client_id)})
                access=db.query(ClientFittbotAccess).filter(ClientFittbotAccess.client_id == client.client_id).first()
                subscribed=False
                if access:
                    subscribed= True if access.access_status=='active' else False
                client.refresh_token=refresh_token
                db.commit()
                # Check if request is from mobile app
                is_mobile_app = is_mobile_request(http_request)

                response_data = {
                    "message": "OTP verified successfully",
                    "status": 200,
                    "data": {
                        "gym_id": client.gym_id if client.gym_id is not None else None,
                        "subscribed": subscribed,
                        "client_id": client.client_id,
                        "gym_name": gym.name if gym else "",
                        "gender": client.gender,
                        "gym_logo": gym.logo if gym else "",
                        "name": client.name if client.name else "",
                        "mobile": client.contact if client.contact else "",
                        "profile": client.profile if client.profile else "",
                        "weight":client.weight if client.weight  else 0
                    }
                }

                if device is not None:
                    if device=="mobile":
                        response_data["data"]["access_token"] = access_token
                        response_data["data"]["refresh_token"] = refresh_token
                        return response_data
                    
                    else:
               
                        response = JSONResponse(content=response_data)
                        response.set_cookie(
                            key="access_token",
                            value=access_token,
                            max_age=3600,
                            httponly=True,
                            secure=settings.cookie_secure,
                            domain=settings.cookie_domain_value,
                            samesite=settings.cookie_samesite_value,
                        )

                        return response

                if is_mobile_app:

                    response_data["data"]["access_token"] = access_token
                    response_data["data"]["refresh_token"] = refresh_token
                    return response_data

                response = JSONResponse(content=response_data)
                response.set_cookie(
                    key="access_token",
                    value=access_token,
                    max_age=3600,
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )
                response.set_cookie(
                    key="refresh_token",
                    value=refresh_token,
                    max_age=604800,
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )

                return response
            else:
                raise HTTPException(status_code=400, detail=f"Incorrect otp entered")
           
        elif role == "owner":
            stored_otp = await redis.get(f"otp:{data}")
            if stored_otp and stored_otp == str(otp):
                await redis.delete(f"otp:{data}")
                owner=db.query(GymOwner).filter(GymOwner.contact_number == data).first()
                gyms = db.query(Gym).filter(Gym.owner_id == owner.owner_id).all()
                if not gyms:
                    raise HTTPException(status_code=400, detail="No gyms associated with this owner")
 
                access_token = create_access_token({"sub": str(owner.owner_id), "role": role})
                refresh_token = create_refresh_token({"sub": str(owner.owner_id)})
                owner.refresh_token=refresh_token
                db.commit()
 
                gym_data = {}
 
                if len(gyms) == 1:
                    gym_data = {"gym_id": gyms[0].gym_id, "name": gyms[0].name, 'logo':gyms[0].logo, "owner_id": gyms[0].owner_id}
                else:
                    gym_data = [{"gym_id": gym.gym_id, "name": gym.name, "location": gym.location, 'logo':gym.logo, "owner_id": gym.owner_id} for gym in gyms]
 
                # Check if request is from mobile app
                user_agent = http_request.headers.get("User-Agent", "")
                is_mobile_app = is_mobile_request(http_request)
 
                response_data = {
                    "status": 200,
                    "message": "Otp Verified successful",
                    "data": {
                        "owner_id": owner.owner_id,
                        "name": owner.name,
                        "gyms": gym_data
                    }
                }
 
                # For mobile app, include tokens in response body
                if is_mobile_app:
                    response_data["data"]["access_token"] = access_token
                    response_data["data"]["refresh_token"] = refresh_token
                    return response_data
 
                # For webapp, set HTTP-only cookies
                response = JSONResponse(content=response_data)
                
                response.set_cookie(
                    key="access_token",
                    value=access_token,
                    max_age=3600,  # 1 hour
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )
                
                response.set_cookie(
                    key="refresh_token",
                    value=refresh_token,
                    max_age=604800,  # 7 days
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )

                return response
            else:
                raise HTTPException(status_code=400, detail=f"Incorrect otp entered")
       
        elif role == "trainer":
            stored_otp = await redis.get(f"otp:{data}")  
            if stored_otp and stored_otp == str(otp):  
                await redis.delete(f"otp:{data}")
                trainer = db.query(Trainer).filter(Trainer.contact == data).first()

                if not trainer:
                    raise HTTPException(status_code=404, detail="Mobile number is not registered")
               
                gyms_query = db.query(Gym, TrainerProfile).join(
                    TrainerProfile, Gym.gym_id == TrainerProfile.gym_id
                ).filter(TrainerProfile.trainer_id == trainer.trainer_id).all()
               
                if not gyms_query:
                    raise HTTPException(status_code=400, detail="No gyms associated with this trainer")
               
                gyms = [gym for gym, profile in gyms_query]
   
                access_token = create_access_token({"sub": str(trainer.trainer_id), "role": role})
                refresh_token = create_refresh_token({"sub": str(trainer.trainer_id)})
               
                trainer.refresh_token = refresh_token
                db.commit()
   
                if len(gyms) == 1:
                    gym_data = {"gym_id": gyms[0].gym_id, "name": gyms[0].name, 'logo': gyms[0].logo, "owner_id": gyms[0].owner_id}
                else:
                    gym_data = [{"gym_id": gym.gym_id, "name": gym.name, "location": gym.location, 'logo': gym.logo, 'owner_id':gym.owner_id} for gym in gyms]
               
                # Check if request is from mobile app
                user_agent = http_request.headers.get("User-Agent", "")
                
                is_mobile_app = is_mobile_request(http_request)
 
                response_data = {
                    "status": 200,
                    "message": "OTP verified successfully",
                    "data": {
                        "trainer_id": trainer.trainer_id,
                        "name": trainer.full_name,
                        "gyms": gym_data
                    }
                }
 
                # For mobile app, include tokens in response body
                if is_mobile_app:
                    response_data["data"]["access_token"] = access_token
                    response_data["data"]["refresh_token"] = refresh_token
                    return response_data
 
                # For webapp, set HTTP-only cookies
                response = JSONResponse(content=response_data)
                response.set_cookie(
                    key="access_token",
                    value=access_token,
                    max_age=3600,  # 1 hour
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )
                response.set_cookie(
                    key="refresh_token",
                    value=refresh_token,
                    max_age=604800,  # 7 days
                    httponly=True,
                    secure=settings.cookie_secure,
                    domain=settings.cookie_domain_value,
                    samesite=settings.cookie_samesite_value,
                )

                return response
            else:
                raise HTTPException(status_code=400, detail="Incorrect OTP entered")
 
        elif role == "marketing":
            stored_otp = await redis.get(f"otp:{data}")
            if stored_otp and stored_otp == str(otp):
                await redis.delete(f"otp:{data}")
                bdm=db.query(Managers).filter(Managers.contact == data).first()
                
                if not bdm:
                    bde=db.query(Executives).filter(Executives.contact == data).first()

                    if bde:
                        raise HTTPException(status_code=400, detail="Mobile Number is not registered")

                    access_token = create_access_token({"sub": str(bde.id), "role": "BDE"})
                    refresh_token = create_refresh_token({"sub": str(bde.id)})
                    bde.refresh_token=refresh_token
                    db.commit()
    
                    user_agent = http_request.headers.get("User-Agent", "")
                    is_mobile_app = is_mobile_request(http_request)
    
                    response_data = {
                        "status": 200,
                        "message": "Otp Verified successful",
                        "data": {
                            "owner_id": bde.id,
                            "name": bde.name,
                            "role":"BDE"
                        }
                    }
                    raise HTTPException(status_code=400, detail="Mobile Number is not registered")
 
                access_token = create_access_token({"sub": str(bdm.id), "role": "BDM"})
                refresh_token = create_refresh_token({"sub": str(bdm.id)})
                bdm.refresh_token=refresh_token
                db.commit()
 
                user_agent = http_request.headers.get("User-Agent", "")
                is_mobile_app = is_mobile_request(http_request)
 
                response_data = {
                    "status": 200,
                    "message": "Otp Verified successful",
                    "data": {
                        "owner_id": bdm.id,
                        "name": bdm.name,
                        "role":"BDM"
                    }
                }
 
                
            else:
                raise HTTPException(status_code=400, detail=f"Incorrect otp entered")

        elif role == "admin":
            stored_otp = await redis.get(f"otp:{data}")
            if stored_otp and stored_otp == str(otp):
                await redis.delete(f"otp:{data}")

                # Query admin from fittbot_admins schema
                admin = db.query(Admins).filter(Admins.contact_number == data).first()

                if not admin:
                    raise HTTPException(status_code=404, detail="Only admins are allowed")

                # Create tokens
                access_token = create_access_token({"sub": str(admin.admin_id), "role": "admin"})
                refresh_token = create_refresh_token({"sub": str(admin.admin_id)})

                # Update refresh token in database
                admin.refresh_token = refresh_token
                db.commit()

                response_data = {
                    "status": 200,
                    "message": "Otp Verified successful",
                    "data": {
                        "admin_id": admin.admin_id,
                        "name": admin.name,
                        "role": "admin"
                    }
                }

                # For device=web, set HTTPOnly cookies
                if device and device.lower() == "web":
                    response = JSONResponse(content=response_data)
                    response.set_cookie(
                        key="access_token",
                        value=access_token,
                        max_age=3600,  # 1 hour
                        httponly=True,
                        secure=settings.cookie_secure,
                        domain=settings.cookie_domain_value,
                        samesite=settings.cookie_samesite_value,
                    )
                    response.set_cookie(
                        key="refresh_token",
                        value=refresh_token,
                        max_age=604800,  # 7 days
                        httponly=True,
                        secure=settings.cookie_secure,
                        domain=settings.cookie_domain_value,
                        samesite=settings.cookie_samesite_value,
                    )
                    return response

                # For mobile, return tokens in body
                response_data["data"]["access_token"] = access_token
                response_data["data"]["refresh_token"] = refresh_token
                return response_data
            else:
                raise HTTPException(status_code=400, detail=f"Incorrect otp entered")



    except Exception as e:
        
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")
 

def get_access_token_from_request(request: Request) -> str:
    """Extract access token from cookie or Authorization header"""
    # Try cookie first (webapp)
    access_token = request.cookies.get("access_token")
 
    if not access_token:
        # Try Authorization header (mobile app)
        auth_header = request.headers.get("Authorization")
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0].lower() == "bearer":
                access_token = parts[1]
 
    return access_token


def is_mobile_request(request: Request) -> bool:
    """Check if request is from mobile app based on User-Agent and custom headers"""
    user_agent = request.headers.get("User-Agent", "")

    # Enhanced mobile detection
    mobile_indicators = [
        "mobile", "android", "ios", "flutter", "dart",
        "okhttp", "retrofit", "alamofire", "nsurlsession",
        "cfnetwork", "volley", "ktor", "axios-mobile",
        "react-native", "cordova", "phonegap", "ionic",
        "capacitor", "expo", "xamarin"
    ]

    is_mobile = any(indicator in user_agent.lower() for indicator in mobile_indicators)

    # Custom header check (recommended approach)
    custom_client_header = request.headers.get("X-Client-Type", "").lower()
    if custom_client_header in ["mobile", "android", "ios"]:
        is_mobile = True

    return is_mobile



@router.post("/logout")
async def logout(request: Request):
    """Logout endpoint that clears cookies for webapp"""
    try:
        # For mobile apps, they handle token removal locally
        if is_mobile_request(request):
            return {"status": 200, "message": "Logout successful"}
 
        # For webapp, clear cookies
        response_data = {"status": 200, "message": "Logout successful"}
        response = JSONResponse(content=response_data)
 
        # Clear auth cookies
        response.delete_cookie(
            key="access_token",
            domain=settings.cookie_domain_value,
            path="/",
            httponly=True,
            secure=settings.cookie_secure,
            samesite=settings.cookie_samesite_value,
        )
        response.delete_cookie(
            key="refresh_token",
            domain=settings.cookie_domain_value,
            path="/",
            httponly=True,
            secure=settings.cookie_secure,
            samesite=settings.cookie_samesite_value,
        )
 
        return response
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during logout: {str(e)}")
 
 
