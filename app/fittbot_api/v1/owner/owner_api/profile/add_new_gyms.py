# app/routers/new_gyms.py

from datetime import datetime
from typing import List, Optional, Dict, Any
import json

from fastapi import APIRouter, Depends
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Gym, AccountDetails, GymPhoto, GymOwner, GymDetails, GymBatches,GymPlans
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis
from redis.asyncio import Redis


class AddressIn(BaseModel):
    street: str
    area: str
    city: str
    state: str
    pincode: str


class PhotoIn(BaseModel):
    photo_id: str
    area_type: str
    url: str


class GymIn(BaseModel):
    gymName: str
    contact_number: str
    services: List[str]
    operating_hours: List[Dict[str, Any]]
    address: AddressIn
    account_number: str
    ifsc_code: str
    account_holder_name: str
    bank_name: str
    branch_name: str
    upi_id: Optional[str] = None
    gst_number: Optional[str] = None
    gst_type: str = "nogst"
    gst_percentage: str = "18"
    photos: Optional[List[PhotoIn]] = []
    total_machineries: Optional[int] = None
    floor_space: Optional[int] = None
    total_trainers: Optional[int] = None
    yearly_membership_cost: Optional[int] = None

    @field_validator("gymName", "contact_number")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise FittbotHTTPException(
                status_code=400,
                detail="Gym name and contact number must be non-empty",
                error_code="VALIDATION_ERROR",
                log_data={"field": "gymName/contact_number", "value": v},
            )
        return v.strip()


class GymBulkCreate(BaseModel):
    owner_id: int
    gyms: List[GymIn]


class GymOut(BaseModel):
    gym_id: int
    owner_id: int
    gymName: str
    contact_number: str
    services: List[str]
    operating_hours: List[Dict[str, Any]]
    address: AddressIn
    photos_count: int = 0
    total_machineries: Optional[int] = None
    floor_space: Optional[int] = None
    total_trainers: Optional[int] = None
    yearly_membership_cost: Optional[int] = None


router = APIRouter(prefix="/new_gyms", tags=["Gyms"])


@router.post("/add_gyms")
async def bulk_create_gyms(
    registration: GymBulkCreate,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Creates new gyms with account details. Photos are fetched from Redis temp storage
    instead of being sent in the request body.
    """
    try:
        if not registration.gyms:
            raise FittbotHTTPException(
                status_code=400,
                detail="No gyms provided",
                error_code="NO_GYMS",
                log_data={"owner_id": registration.owner_id},
            )

        # Get owner contact from database to match Redis keys
        owner = db.query(GymOwner).filter(GymOwner.owner_id == registration.owner_id).first()
        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": registration.owner_id},
            )

        # Get photos from Redis (if any)
        gym_photos_by_index = {}
        redis_pattern = f"temp_photo:{owner.contact_number}:*"
        redis_keys = await redis.keys(redis_pattern)

        print(f"Looking for Redis keys with pattern: {redis_pattern}")
        print(f"Found {len(redis_keys)} Redis keys: {redis_keys}")

        if redis_keys:
            for key in redis_keys:
                photo_data = await redis.get(key)
                if photo_data:
                    photo_info = json.loads(photo_data)
                    print(f"Processing Redis key {key}: {photo_info}")
                    if photo_info.get("cdn_url"):  # Only confirmed photos
                        gym_index = photo_info.get("gym_index", 0)
                        area_type = photo_info["area_type"]

                        print(f"Adding photo for gym_index {gym_index}, area_type {area_type}")

                        if gym_index not in gym_photos_by_index:
                            gym_photos_by_index[gym_index] = {}
                        if area_type not in gym_photos_by_index[gym_index]:
                            gym_photos_by_index[gym_index][area_type] = []
                        gym_photos_by_index[gym_index][area_type].append(photo_info)
                    else:
                        print(f"Skipping photo without cdn_url: {photo_info}")
                else:
                    print(f"No data found for Redis key: {key}")

        print(f"Final gym_photos_by_index: {gym_photos_by_index}")

        now = datetime.now()
        created_gyms: List[GymOut] = []
        total_photos_processed = 0

        for gym_index, gym_data in enumerate(registration.gyms):
            # Create gym record
            new_gym = Gym(
                owner_id=registration.owner_id,
                name=gym_data.gymName,
                contact_number=gym_data.contact_number,
                services=gym_data.services,
                operating_hours=gym_data.operating_hours,
                # Address fields
                street=gym_data.address.street,
                area=gym_data.address.area,
                city=gym_data.address.city,
                state=gym_data.address.state,
                pincode=gym_data.address.pincode,
                # Set location as city for compatibility
                location=gym_data.address.city,
                created_at=now,
                updated_at=now,
                fittbot_verified=False,
                logo="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/default_logo.png",
                #logo='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/volume.png',
                cover_pic='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default_posters/studios.png',
                referal_id="",
            )
            db.add(new_gym)
            db.flush()  # Get the gym_id without committing

            # Create account details
            account_details = AccountDetails(
                gym_id=new_gym.gym_id,
                account_number=gym_data.account_number,
                bank_name=gym_data.bank_name,
                account_ifsccode=gym_data.ifsc_code,
                account_branch=gym_data.branch_name,
                account_holdername=gym_data.account_holder_name,
                upi_id=gym_data.upi_id or "",
                gst_number=gym_data.gst_number or "",
                gst_type=gym_data.gst_type,
                gst_percentage=gym_data.gst_percentage,
                created_at=now,
                updated_at=now,
            )
            db.add(account_details)

            # Create gym details
            gym_details = GymDetails(
                gym_id=new_gym.gym_id,
                total_machineries=gym_data.total_machineries,
                floor_space=gym_data.floor_space,
                total_trainers=gym_data.total_trainers,
                yearly_membership_cost=gym_data.yearly_membership_cost,
                created_at=now,
                updated_at=now
            )
            db.add(gym_details)

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

            # Handle photos from Redis for this specific gym index
            photos_created = 0
            if gym_index in gym_photos_by_index:
                gym_photos = gym_photos_by_index[gym_index]
                for area_type, photos in gym_photos.items():
                    for photo_info in photos:
                        try:
                            gym_photo = GymPhoto(
                                gym_id=new_gym.gym_id,
                                area_type=area_type,
                                image_url=photo_info["cdn_url"],
                                file_name=photo_info["file_name"],
                                created_at=now,
                                updated_at=now,
                            )
                            db.add(gym_photo)
                            photos_created += 1
                            total_photos_processed += 1
                        except Exception as photo_error:
                            print(f"Error processing photo for gym {gym_index}: {photo_error}")
                            continue

            # Prepare response data
            created_gym = GymOut(
                gym_id=new_gym.gym_id,
                owner_id=new_gym.owner_id,
                gymName=new_gym.name,
                contact_number=new_gym.contact_number,
                services=new_gym.services or [],
                operating_hours=new_gym.operating_hours or [],
                address=AddressIn(
                    street=new_gym.street or "",
                    area=new_gym.area or "",
                    city=new_gym.city or "",
                    state=new_gym.state or "",
                    pincode=new_gym.pincode or ""
                ),
                photos_count=photos_created,
                total_machineries=gym_data.total_machineries,
                floor_space=gym_data.floor_space,
                total_trainers=gym_data.total_trainers,
                yearly_membership_cost=gym_data.yearly_membership_cost
            )
            created_gyms.append(created_gym)

        # Commit all changes
        db.commit()

        # Clean up Redis temp photos after successful creation
        if redis_keys:
            await redis.delete(*redis_keys)
            print(f"Cleaned up {len(redis_keys)} temporary photo records from Redis")

        # Refresh gym objects to get final state
        for created_gym in created_gyms:
            gym_obj = db.query(Gym).filter(Gym.gym_id == created_gym.gym_id).first()
            if gym_obj:
                db.refresh(gym_obj)

        return {
            "status": 200,
            "message": f"Successfully added {len(created_gyms)} gyms with account details and {total_photos_processed} photos from Redis",
            "data": [gym.model_dump() for gym in created_gyms],
            "summary": {
                "gyms_created": len(created_gyms),
                "total_photos": total_photos_processed,
                "photos_per_gym": [gym.photos_count for gym in created_gyms]
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while creating gyms with account details and photos",
            error_code="GYM_BULK_CREATE_ERROR",
            log_data={
                "owner_id": registration.owner_id if registration else None,
                "gyms_count": len(registration.gyms) if registration and registration.gyms else 0,
                "error": repr(e),
            },
        )


class RemovePhotoRequest(BaseModel):
    gym_id: int
    photo_id: str
    owner_id: int


@router.delete("/remove_gym_photo")
async def remove_gym_photo(
    request: RemovePhotoRequest,
    db: Session = Depends(get_db),
):
    """
    Remove a specific gym photo by photo ID
    """
    try:
        # Verify the gym belongs to the owner
        gym = db.query(Gym).filter(
            Gym.gym_id == request.gym_id,
            Gym.owner_id == request.owner_id
        ).first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found or doesn't belong to this owner",
                error_code="GYM_NOT_FOUND",
                log_data={
                    "gym_id": request.gym_id,
                    "owner_id": request.owner_id
                },
            )

        # Find and delete the photo
        gym_photo = db.query(GymPhoto).filter(
            GymPhoto.gym_id == request.gym_id,
            GymPhoto.photo_id == request.photo_id
        ).first()

        if not gym_photo:
            raise FittbotHTTPException(
                status_code=404,
                detail="Photo not found",
                error_code="PHOTO_NOT_FOUND",
                log_data={
                    "gym_id": request.gym_id,
                    "photo_id": request.photo_id
                },
            )

        # Store photo info for response
        photo_info = {
            "photo_id": gym_photo.photo_id,
            "area_type": gym_photo.area_type,
            "file_name": gym_photo.file_name,
            "image_url": gym_photo.image_url
        }

        # Delete the photo record
        db.delete(gym_photo)
        db.commit()

        return {
            "status": 200,
            "message": "Photo removed successfully",
            "data": {
                "removed_photo": photo_info,
                "gym_id": request.gym_id
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while removing the photo",
            error_code="PHOTO_REMOVAL_ERROR",
            log_data={
                "gym_id": request.gym_id,
                "photo_id": request.photo_id,
                "error": repr(e),
            },
        )


class RemoveMultiplePhotosRequest(BaseModel):
    gym_id: int
    photo_ids: List[str]
    owner_id: int


@router.delete("/remove_multiple_gym_photos")
async def remove_multiple_gym_photos(
    request: RemoveMultiplePhotosRequest,
    db: Session = Depends(get_db),
):
    """
    Remove multiple gym photos by photo IDs
    """
    try:
        # Verify the gym belongs to the owner
        gym = db.query(Gym).filter(
            Gym.gym_id == request.gym_id,
            Gym.owner_id == request.owner_id
        ).first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found or doesn't belong to this owner",
                error_code="GYM_NOT_FOUND",
                log_data={
                    "gym_id": request.gym_id,
                    "owner_id": request.owner_id
                },
            )

        # Find photos to remove
        gym_photos = db.query(GymPhoto).filter(
            GymPhoto.gym_id == request.gym_id,
            GymPhoto.photo_id.in_(request.photo_ids)
        ).all()

        if not gym_photos:
            raise FittbotHTTPException(
                status_code=404,
                detail="No photos found to remove",
                error_code="PHOTOS_NOT_FOUND",
                log_data={
                    "gym_id": request.gym_id,
                    "photo_ids": request.photo_ids
                },
            )

        # Store photo info for response
        removed_photos = []
        for photo in gym_photos:
            removed_photos.append({
                "photo_id": photo.photo_id,
                "area_type": photo.area_type,
                "file_name": photo.file_name,
                "image_url": photo.image_url
            })

        # Delete all photos
        for photo in gym_photos:
            db.delete(photo)

        db.commit()

        return {
            "status": 200,
            "message": f"Successfully removed {len(removed_photos)} photos",
            "data": {
                "removed_photos": removed_photos,
                "gym_id": request.gym_id,
                "removed_count": len(removed_photos)
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while removing photos",
            error_code="MULTIPLE_PHOTOS_REMOVAL_ERROR",
            log_data={
                "gym_id": request.gym_id,
                "photo_ids": request.photo_ids,
                "error": repr(e),
            },
        )


@router.post("/add_gyms_with_registration_photos")
async def add_gyms_with_registration_photos(
    registration: GymBulkCreate,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Special endpoint for creating gyms with photos from registration process.
    This pulls photos from Redis temp storage and associates them with the new gyms.
    """
    try:
        if not registration.gyms:
            raise FittbotHTTPException(
                status_code=400,
                detail="No gyms provided",
                error_code="NO_GYMS",
                log_data={"owner_id": registration.owner_id},
            )

        # Get owner contact from database to match Redis keys
        owner = db.query(GymOwner).filter(GymOwner.owner_id == registration.owner_id).first()
        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": registration.owner_id},
            )

        # Get photos from Redis (if any)
        gym_photos_by_index = {}
        redis_pattern = f"temp_photo:{owner.contact_number}:*"
        redis_keys = await redis.keys(redis_pattern)

        if redis_keys:
            for key in redis_keys:
                photo_data = await redis.get(key)
                if photo_data:
                    photo_info = json.loads(photo_data)
                    if photo_info.get("cdn_url"):  # Only confirmed photos
                        gym_index = photo_info.get("gym_index", 0)
                        area_type = photo_info["area_type"]

                        if gym_index not in gym_photos_by_index:
                            gym_photos_by_index[gym_index] = {}
                        if area_type not in gym_photos_by_index[gym_index]:
                            gym_photos_by_index[gym_index][area_type] = []
                        gym_photos_by_index[gym_index][area_type].append(photo_info)

        now = datetime.now()
        created_gyms: List[GymOut] = []

        for gym_index, gym_data in enumerate(registration.gyms):
            # Create gym record
            new_gym = Gym(
                owner_id=registration.owner_id,
                name=gym_data.gymName,
                contact_number=gym_data.contact_number,
                services=gym_data.services,
                operating_hours=gym_data.operating_hours,
                # Address fields
                street=gym_data.address.street,
                area=gym_data.address.area,
                city=gym_data.address.city,
                state=gym_data.address.state,
                pincode=gym_data.address.pincode,
                location=gym_data.address.city,
                created_at=now,
                updated_at=now,
                fittbot_verified=False,
                logo="",
                cover_pic="",
                referal_id="",
            )
            db.add(new_gym)
            db.flush()

            # Create account details
            account_details = AccountDetails(
                gym_id=new_gym.gym_id,
                account_number=gym_data.account_number,
                bank_name=gym_data.bank_name,
                account_ifsccode=gym_data.ifsc_code,
                account_branch=gym_data.branch_name,
                account_holdername=gym_data.account_holder_name,
                upi_id=gym_data.upi_id or "",
                gst_number=gym_data.gst_number or "",
                gst_type=gym_data.gst_type,
                gst_percentage=gym_data.gst_percentage,
                created_at=now,
                updated_at=now,
            )
            db.add(account_details)

            # Create gym details
            gym_details = GymDetails(
                gym_id=new_gym.gym_id,
                total_machineries=gym_data.total_machineries,
                floor_space=gym_data.floor_space,
                total_trainers=gym_data.total_trainers,
                yearly_membership_cost=gym_data.yearly_membership_cost,
                created_at=now,
                updated_at=now
            )
            db.add(gym_details)

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

            # Handle photos from Redis for this specific gym index
            photos_created = 0
            if gym_index in gym_photos_by_index:
                gym_photos = gym_photos_by_index[gym_index]
                for area_type, photos in gym_photos.items():
                    for photo_info in photos:
                        gym_photo = GymPhoto(
                            gym_id=new_gym.gym_id,
                            area_type=area_type,
                            image_url=photo_info["cdn_url"],
                            file_name=photo_info["file_name"],
                            created_at=now,
                            updated_at=now,
                        )
                        db.add(gym_photo)
                        photos_created += 1

            # Prepare response data
            created_gym = GymOut(
                gym_id=new_gym.gym_id,
                owner_id=new_gym.owner_id,
                gymName=new_gym.name,
                contact_number=new_gym.contact_number,
                services=new_gym.services or [],
                operating_hours=new_gym.operating_hours or [],
                address=AddressIn(
                    street=new_gym.street or "",
                    area=new_gym.area or "",
                    city=new_gym.city or "",
                    state=new_gym.state or "",
                    pincode=new_gym.pincode or ""
                ),
                photos_count=photos_created,
                total_machineries=gym_data.total_machineries,
                floor_space=gym_data.floor_space,
                total_trainers=gym_data.total_trainers,
                yearly_membership_cost=gym_data.yearly_membership_cost
            )
            created_gyms.append(created_gym)

        # Commit all changes
        db.commit()

        # Clean up Redis temp photos after successful creation
        if redis_keys:
            await redis.delete(*redis_keys)
            print(f"Cleaned up {len(redis_keys)} temporary photo records from Redis")

        total_photos = sum(gym.photos_count for gym in created_gyms)
        return {
            "status": 200,
            "message": f"Successfully added {len(created_gyms)} gyms with account details and {total_photos} photos",
            "data": [gym.model_dump() for gym in created_gyms],
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="An error occurred while creating gyms with registration photos",
            error_code="GYM_REGISTRATION_CREATE_ERROR",
            log_data={
                "owner_id": registration.owner_id if registration else None,
                "gyms_count": len(registration.gyms) if registration and registration.gyms else 0,
                "error": repr(e),
            },
        )



class DataRequest(BaseModel):
    gym_id:int
    owner_id:int

@router.post("/add_new_gym")
async def add_another_gym(data:DataRequest,db: Session = Depends(get_db)):
    try:
        gym_id=data.gym_id

        get_data= db.query(Gym).filter(Gym.gym_id==gym_id).first()
    
        if get_data is None:
            return{
                "status":200,
                "data":[]
            }
        else:
            data=[]
            for new_data in get_data:
                data.append(new_data.name)

            return{
                "status":200,
                "data":data
            }

    except Exception as e:
        raise FittbotHTTPException(status_code=500,detail="internal server error")
    

@router.delete("/delete_existing_data")
async def delete_existing_ref(id:int,db: Session = Depends(get_db)):
    try:
        data=db.query(GymPlans).filter(GymPlans.id==id).first()
        if data:
            db.delete(data)
            db.commit()
            return{
                "status":200,
                "message":"Plans deleted succesfully"
            }

        else:
            return{
                "status":200,
                "message":"no data found"
            }
    except Exception as e:
        raise FittbotHTTPException(status_code=500,detail="deletion error occured")
    
