# app/routers/profile_pic_router.py

import os
import time
from typing import Optional
import boto3
from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from redis.asyncio import Redis
from app.utils.security import verify_password, get_password_hash
from app.utils.redis_config import get_redis
from app.utils.otp import generate_otp, async_send_verification_sms
from app.models.database import get_db
from app.fittbot_api.v1.client.client_api.home.calculate_macros import calculate_macros,calculate_bmr,activity_multipliers
from app.models.fittbot_models import Client
from app.utils.logging_utils import FittbotHTTPException
import json
from datetime import datetime, date
from app.utils.check_subscriptions import get_client_tier

# --- Models / Schemas / Utils your code already uses (paths may vary in your project) ---
from app.models.fittbot_models import (
    Client,Avatar,
    Gym,
    GymBatches,
    GymPlans,
    ClientTarget,
    GymOwner,
)

AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
AVATAR_PREFIX = "Profile_pics/"
AVATAR_MAX_SIZE = 1 * 1024 * 1024  # 1 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)

router = APIRouter(prefix="/profile", tags=["Profile"])


async def delete_keys_by_pattern(redis: Redis, pattern: str) -> None:
    """Delete all keys matching the given pattern."""
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)


def generate_avatar_upload_url(
    client_id: int,
    extension: str,
    content_type: str = "image/jpeg",
):
    """
    Create a browser POST policy for direct S3 upload + return CDN URL.
    """

    if not content_type.startswith("image/"):
        raise FittbotHTTPException(
            status_code=400,
            detail="Invalid content type; must start with image/",
            error_code="INVALID_CONTENT_TYPE",
            log_data={"content_type": content_type},
        )

    if not extension:
        raise FittbotHTTPException(
            status_code=400,
            detail="File extension is required",
            error_code="MISSING_FILE_EXTENSION",
            log_data={"client_id": client_id},
        )

    key = f"{AVATAR_PREFIX}user-{client_id}.{extension}"
    version = int(time.time() * 1000)

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
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to generate presigned upload form",
            error_code="S3_PRESIGN_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "key": key},
        )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"

    return {
        "upload": presigned,  # POST policy for browser direct upload
        "cdn_url": cdn_url,
        "version": version,
    }


@router.get("/upload-url")
async def create_upload_url(
    request: Request,
    client_id: int,
    extension: str,
):
    """
    Returns a presigned POST payload and the final CDN URL to use after upload.
    """
    try:
        url_data = generate_avatar_upload_url(client_id, extension)
        return {"status": 200, "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="PROFILE_UPLOAD_URL_GENERATION_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "extension": extension},
        )


class ConfirmBody(BaseModel):
    cdn_url: str
    client_id: int


@router.post("/confirm")
async def confirm_avatar(
    request: Request,
    body: ConfirmBody,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Persists the uploaded avatar URL onto the client's profile and clears related caches.
    """
    try:
        client = db.query(Client).filter(Client.client_id == body.client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": body.client_id},
            )

        client.profile = body.cdn_url
        db.commit()
        db.refresh(client)

        # Invalidate any cached clientdata blobs
        client_data_pattern = await redis.keys("gym:*:clientdata")
        if client_data_pattern:
            await redis.delete(*client_data_pattern)

        return {
            "status": 200,
            "message": "Avatar updated",
            "data": client.profile,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to update profile picture",
            error_code="PROFILE_UPDATE_ERROR",
            log_data={
                "exc": repr(e),
                "client_id": getattr(body, "client_id", None),
                "cdn_url_prefix": (body.cdn_url[:60] + "...") if getattr(body, "cdn_url", None) else None,
            },
        )



class ClientSchema(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    email: Optional[str] = None
    contact: Optional[str] = None
    height: Optional[float] = None
    lifestyle: Optional[str] = None
    medical_issues: Optional[str] = None
    goals: Optional[str] = None
    gender: Optional[str] = None
    dob: Optional[date] = None
    profile: Optional[str] = None

    class Config:
        from_attributes = True


@router.get("/profile_data")
async def get_data(
    client_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client = (
            db.query(Client)
            .filter(Client.client_id == client_id)
            .first()
            if client_id
            else None
        )

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        client_data = ClientSchema.model_validate(client)
        # Preserve your original mapping behavior
        client_data.lifestyle = client.lifestyle
        client_data.goals = client.goals

        # Convert client_data to dict and replace None with empty strings
        client_data_dict = client_data.model_dump()
        for key, value in client_data_dict.items():
            if value is None:
                client_data_dict[key] = "" if key != "dob" else None

        tier = get_client_tier(db, client_id)
        gym_data = {}

        if tier.endswith("_gym") and client.gym_id:
            gym = db.query(Gym).filter(Gym.gym_id == client.gym_id).first()
            batch = (
                db.query(GymBatches)
                .filter(GymBatches.batch_id == client.batch_id)
                .first()
                if client.batch_id
                else None
            )
            training = (
                db.query(GymPlans)
                .filter(GymPlans.id == client.training_id)
                .first()
                if client.training_id
                else None
            )

            if gym:
                gym_data = {
                    "gym_location": gym.location,
                    "gym_name": gym.name,
                    "gym_logo": gym.logo,
                    "gym_cover_pic": gym.cover_pic,
                    "batch_name": batch.batch_name if batch else None,
                    "batch_timing": batch.timing if batch else None,
                    "training_plans": training.plans if training else None,
                    "training_duration": training.duration if training and training.duration else None,
                    "training_amount": training.amount if training and training.amount else None,
                }

        response_payload = {
            "status": 200,
            "success": True,
            "message": "Data retrieved successfully",
            "data": {
                "client_data": client_data_dict,
            },
        }

        response_payload["data"]["gym_data"] = gym_data

        return response_payload

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching profile: {str(e)}",
            error_code="PROFILE_FETCH_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    email: Optional[str] = None
    contact: Optional[str] = None
    dob: Optional[str] = None
    newPassword: Optional[str] = None
    oldPassword: Optional[str] = None
    height:Optional[float]=None
    client_id: int
    lifestyle: Optional [str]=None
    medical_issues: Optional[str]=None
    goals: Optional[str] =None
    gender: Optional[str]=None
    role: str
    method: str


@router.put("/update_profile")
async def update_profile(
    request: UpdateProfileRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        if request.method == "profile":
            if request.role != "client":
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Only clients can update profiles.",
                    error_code="PROFILE_UPDATE_ROLE_INVALID",
                    log_data={"role": request.role, "client_id": request.client_id},
                )

            client = db.query(Client).filter(Client.client_id == request.client_id).first()
            if not client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Client not found.",
                    error_code="CLIENT_NOT_FOUND",
                    log_data={"client_id": request.client_id},
                )

            is_changed = False

            if request.contact:
                if not client.contact == request.contact:
                    
                    existing_client = db.query(Client).filter(
                        (Client.contact == request.contact)
                    ).first()
                    if existing_client:
                        raise FittbotHTTPException(
                            status_code=400,
                            detail="Mobile number already registered with different account",
                            error_code="MOBILE_ALREADY_REGISTERED",
                            log_data={"client_id": request.client_id, "contact": request.contact},
                        )
                    client.verification = '{"mobile": false, "password" : true}'
                    is_changed = True
                client.contact = request.contact

            if request.dob:
                if not client.dob == request.dob:
                    today = date.today()
                    dob = datetime.strptime(str(request.dob), "%Y-%m-%d").date()
                    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                    client.age = age
                client.dob = request.dob

            if request.email:

                client.email = request.email

            if request.name:
                client.name = request.name
            if request.location:
                client.location = request.location
            if request.lifestyle:
                client.lifestyle = request.lifestyle
            if request.medical_issues:
                client.medical_issues = request.medical_issues
            if request.goals:
                client.goals = request.goals
            if request.gender:
                client.gender = request.gender
            if request.height:
                client.height = request.height

            tier = get_client_tier(db, request.client_id)
            gym_id = client.gym_id if client.gym_id and tier.endswith("_gym") else None
            try:
                db.commit()

                # Only calculate BMR/TDEE if all required fields are present
                if client.weight and client.height and client.age and client.lifestyle:
                    bmr = calculate_bmr(client.weight, client.height, client.age)
                    tdee = bmr * activity_multipliers.get(client.lifestyle, 1.2)

                    if client.goals == "weight_loss":
                        tdee -= 500
                    elif client.goals == "weight_gain":
                        tdee += 500

                    protein, carbs, fat, _, _ = calculate_macros(tdee, client.goals or "maintenance")

                    client_target = db.query(ClientTarget).filter(
                        ClientTarget.client_id == client.client_id
                    ).first()

                    if client_target:
                        client_target.calories = int(tdee)
                        client_target.protein = protein
                        client_target.carbs = carbs
                        client_target.fat = fat
                        client_target.updated_at = datetime.now()
                        db.commit()
                    else:
                        client_target = ClientTarget(
                            client_id=client.client_id,
                            calories=int(tdee),
                            protein=protein,
                            carbs=carbs,
                            fat=fat,
                            updated_at=datetime.now(),
                        )
                        db.add(client_target)
                        db.commit()

                # Invalidate redis keys (unchanged logic)
                target_actual_keys_pattern = "*:initial_target_actual"
                target_actual_keys = await redis.keys(target_actual_keys_pattern)
                if target_actual_keys:
                    await redis.delete(*target_actual_keys)

                client_status_key_pattern = "*:initialstatus"
                client_status_key = await redis.keys(client_status_key_pattern)
                if client_status_key:
                    await redis.delete(*client_status_key)

                pattern = "*:target_actual"
                keys = await redis.keys(pattern)
                if keys:
                    await redis.delete(*keys)

            except FittbotHTTPException:
                raise
            except Exception as e:
                db.rollback()
                raise FittbotHTTPException(
                    status_code=500,
                    detail=f"Error updating profile: {str(e)}",
                    error_code="PROFILE_UPDATE_ERROR",
                    log_data={"client_id": request.client_id, "error": str(e)},
                )

        elif request.method == "password":
            if request.role != "client":
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Only client can change passwords.",
                    error_code="PASSWORD_CHANGE_ROLE_INVALID",
                    log_data={"role": request.role, "client_id": request.client_id},
                )

            client = db.query(Client).filter(Client.client_id == request.client_id).first()
            if not client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="Client not found.",
                    error_code="CLIENT_NOT_FOUND",
                    log_data={"client_id": request.client_id},
                )

            if not verify_password(request.oldPassword, client.password):
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Incorrect old password.",
                    error_code="INCORRECT_OLD_PASSWORD",
                    log_data={"client_id": request.client_id},
                )
            else:
                hashed_password = get_password_hash(request.newPassword)
                client.password = hashed_password

            try:
                db.commit()
            except Exception as e:
                db.rollback()
                raise FittbotHTTPException(
                    status_code=500,
                    detail=f"Error updating password: {str(e)}",
                    error_code="PASSWORD_UPDATE_ERROR",
                    log_data={"client_id": request.client_id, "error": str(e)},
                )

            tier = get_client_tier(db, request.client_id)
            gym_id = client.gym_id if client.gym_id and tier.endswith("_gym") else None
            is_changed = False  # keep logic consistent with your original code

        # common cache invalidations (unchanged)
        gym_segment = str(gym_id) if gym_id is not None else "*"
        client_status_pattern = f"{request.client_id}:{gym_segment}:status"

        if is_changed:
            mobile_otp = generate_otp()
            await redis.set(f"otp:{client.contact}", mobile_otp, ex=300)
            if await async_send_verification_sms(client.contact, mobile_otp):
                print(f"Verification OTP send successfully to {client.contact}")

        await delete_keys_by_pattern(redis, client_status_pattern)
        await delete_keys_by_pattern(redis, f"gym:{gym_segment}:posts")
        await delete_keys_by_pattern(redis, f"gym:{gym_segment}:clientdata")

        return {
            "status": 200,
            "message": "Profile updated successfully.",
            "is_changed": is_changed,
            "data": {
                "verification": json.loads(client.verification) if client.verification else {"mobile": False, "password": False},
                "contact": client.contact or "",
                "id": client.client_id,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        print(e)
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An unexpected error occurred {e}",
            error_code="PROFILE_UPDATE_UNEXPECTED",
            log_data={"client_id": getattr(request, "client_id", None), "error": str(e)},
        )


@router.get("/get_fittbot_avatars")
async def get_fittbot_avatars(client_id: int, db: Session = Depends(get_db)):
    try:
        client = db.query(Client).filter(Client.client_id == client_id).first()

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found.",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        avatars = db.query(Avatar).filter(Avatar.gender == client.gender).all()
        avatar_list = [{"id": avatar.id, "avatarurl": avatar.avatarurl} for avatar in avatars]

        return {"status": 200, "data": avatar_list}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch Fittbot avatars",
            error_code="FITT_BOT_AVATARS_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


class updateAvatarRequest(BaseModel):
    client_id: int
    profile: str


@router.put("/update_avatar")
async def update_avatar(
    req: updateAvatarRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client = db.query(Client).filter(Client.client_id == req.client_id).first()

        if not client:
            raise FittbotHTTPException(
                status_code=400,
                detail="Client not found",
                error_code="AVATAR_UPDATE_CLIENT_NOT_FOUND",
                log_data={"client_id": req.client_id},
            )

        client.profile = req.profile
        db.commit()

        tier = get_client_tier(db, client.client_id)
        gym_segment = str(client.gym_id) if client.gym_id and tier.endswith("_gym") else "*"
        await delete_keys_by_pattern(redis, f"{client.client_id}:{gym_segment}:status")
        await delete_keys_by_pattern(redis, f"{client.client_id}:{gym_segment}:profile")

        return {"status": 200, "message": "Avatar updated successfully"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error Updating Avatar: {str(e)}",
            error_code="AVATAR_UPDATE_ERROR",
            log_data={"client_id": req.client_id, "error": str(e)},
        )
