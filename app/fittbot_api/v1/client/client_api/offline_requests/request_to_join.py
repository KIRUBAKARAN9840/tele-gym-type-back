import time
import boto3
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from app.models.database import get_db
from app.models.fittbot_models import FittbotGymMembership, Client, GymJoinRequest
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/offline_requests", tags=["OfflineRequests"])

# AWS S3 Configuration
AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
JOIN_REQUEST_PREFIX = "join_request_dp/"
MAX_FILE_SIZE = 1 * 1024 * 1024  # 1 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)


def generate_join_request_upload_url(client_id: int, extension: str, content_type: str = "image/jpeg"):
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

    key = f"{JOIN_REQUEST_PREFIX}join-{client_id}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, MAX_FILE_SIZE],
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
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": version,
    }

@router.get("/upload-url")
async def get_upload_url(client_id: int, extension: str):

    try:
        url_data = generate_join_request_upload_url(client_id, extension)
        return {"status": 200, "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="JOIN_REQUEST_UPLOAD_URL_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "extension": extension},
        )



@router.get("/join")
async def request_to_join(client_id: int, db: Session = Depends(get_db)):
    try:
        if not client_id:
            raise FittbotHTTPException(
                status_code=401,
                detail="Client not authenticated",
                error_code="CLIENT_NOT_AUTHENTICATED"
            )

        # Check if client has any active membership in fittbot_gym_membership
        active_membership = db.query(FittbotGymMembership).filter(
            FittbotGymMembership.client_id == str(client_id),
            FittbotGymMembership.status == "active"
        ).first()

        if active_membership:
            return {
                "status": 200,
                "already_member": True
            }

        # Check if there's already a pending join request for this client and gym
        existing_request = db.query(GymJoinRequest).filter(
            GymJoinRequest.client_id == client_id,
            GymJoinRequest.status == "pending"
        ).first()



        if existing_request:
            return {
                "status": 200,
                "already_member": False,
                "already_sent_request": True
            }



        # Fetch client details for non-members
        client = db.query(Client).filter(Client.client_id == client_id).first()
        return {
            "status": 200,
            "already_member": False,
            "already_sent_request": False,
            "client_name": client.name if client else None,
            "mobile_number": client.contact if client else None
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to check membership status",
            error_code="MEMBERSHIP_CHECK_ERROR",
            log_data={"exc": repr(e)},
        )




class ConfirmDpBody(BaseModel):
    client_id: int
    cdn_url: str


@router.post("/confirm-dp")
async def confirm_join_request_dp(body: ConfirmDpBody, db: Session = Depends(get_db)):
    """
    Confirms the uploaded dp URL for a join request (updates existing request if exists).
    """
    try:
        # Check if there's an existing pending join request for this client
        join_request = db.query(GymJoinRequest).filter(
            GymJoinRequest.client_id == body.client_id,
            GymJoinRequest.status == "pending"
        ).first()

        if join_request:
            join_request.dp = body.cdn_url
            db.commit()
            db.refresh(join_request)

        return {
            "status": 200,
            "message": "DP uploaded successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to confirm dp upload",
            error_code="JOIN_REQUEST_DP_CONFIRM_ERROR",
            log_data={"exc": repr(e), "client_id": body.client_id},
        )


class AddJoinRequestBody(BaseModel):
    client_id: int
    gym_id: int
    name: str
    mobile_number: str
    alternate_mobile_number: Optional[str] = None
    dp: Optional[str] = None


@router.post("/add")
async def add_join_request(body: AddJoinRequestBody, db: Session = Depends(get_db)):

    try:
        # Check if client exists
        client = db.query(Client).filter(Client.client_id == body.client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": body.client_id},
            )

        # Check if there's already a pending request for this client and gym
        existing_request = db.query(GymJoinRequest).filter(
            GymJoinRequest.client_id == body.client_id,
            GymJoinRequest.gym_id == body.gym_id,
            GymJoinRequest.status == "pending"
        ).first()

        if existing_request:
            # Update existing request
            existing_request.name = body.name
            existing_request.mobile_number = body.mobile_number
            existing_request.alternate_mobile_number = body.alternate_mobile_number
            if body.dp:
                existing_request.dp = body.dp
            db.commit()
            db.refresh(existing_request)

            return {
                "status": 200,
                "message": "Join request updated successfully",
                "data": {
                    "id": existing_request.id,
                    "client_id": existing_request.client_id,
                    "gym_id": existing_request.gym_id,
                    "name": existing_request.name,
                    "mobile_number": existing_request.mobile_number,
                    "alternate_mobile_number": existing_request.alternate_mobile_number,
                    "dp": existing_request.dp,
                    "status": existing_request.status,
                }
            }

        # Create new join request
        new_request = GymJoinRequest(
            client_id=body.client_id,
            gym_id=body.gym_id,
            name=body.name,
            mobile_number=body.mobile_number,
            alternate_mobile_number=body.alternate_mobile_number,
            dp=body.dp,
            status="pending"
        )
        db.add(new_request)
        db.commit()
        db.refresh(new_request)

        return {
            "status": 200,
            "message": "Join request created successfully",
            "data": {
                "id": new_request.id,
                "client_id": new_request.client_id,
                "gym_id": new_request.gym_id,
                "name": new_request.name,
                "mobile_number": new_request.mobile_number,
                "alternate_mobile_number": new_request.alternate_mobile_number,
                "dp": new_request.dp,
                "status": new_request.status,
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to create join request",
            error_code="JOIN_REQUEST_CREATE_ERROR",
            log_data={"exc": repr(e), "client_id": body.client_id, "gym_id": body.gym_id},
        )
