from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    UploadFile,
    File,
    Form,
    Query,
)
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel
import os
import uuid
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import mimetypes
import time
import json
from dotenv import load_dotenv

from app.models.database import get_db
from app.models.marketingmodels import Executives, Managers, LocalGymDocs, GymDatabase

load_dotenv()

router = APIRouter(tags=["Local Gym Documents"], prefix="/marketing/local_document")

S3_BUCKET = os.getenv("S3_BUCKET", "fittbot-uploads")
S3_REGION = os.getenv("S3_REGION", "ap-south-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=S3_REGION,
        )

    except Exception as e:
        print(f"❌ Failed to initialize AWS S3 client for local docs: {e}")
        s3_client = None
else:
    print("❌ AWS credentials not configured for local docs upload")


class LocalGymDocumentResponse(BaseModel):
    id: int
    gym_id: int
    gym_name: Optional[str] = None
    contact_number: Optional[str] = None
    location: Optional[str] = None
    aadhaar_front_url: Optional[str] = None
    aadhaar_back_url: Optional[str] = None
    pan_url: Optional[str] = None
    bankbook_url: Optional[str] = None
    plan_1: Optional[Any] = None
    plan_2: Optional[Any] = None
    plan_3: Optional[Any] = None
    plan_4: Optional[Any] = None
    plan_5: Optional[Any] = None
    updated_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class LocalGymDocumentListResponse(BaseModel):
    success: bool
    message: str
    data: List[LocalGymDocumentResponse]
    total_count: int


class DocumentUploadResponse(BaseModel):
    success: bool
    message: str
    document_url: Optional[str] = None


class DocumentDeleteResponse(BaseModel):
    success: bool
    message: str


def upload_to_s3(file: UploadFile, gym_id: int, document_type: str) -> str:
    if not s3_client:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS S3 not configured",
        )

    file_extension = os.path.splitext(file.filename)[1]
    unique_filename = f"local_gym_{gym_id}_{document_type}_{uuid.uuid4().hex}{file_extension}"
    s3_key = f"local-gym-documents/{gym_id}/{document_type}/{unique_filename}"

    max_retries = 3
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            file.file.seek(0)
            s3_client.upload_fileobj(
                file.file,
                S3_BUCKET,
                s3_key,
                ExtraArgs={
                    "ContentType": file.content_type
                    or mimetypes.guess_type(file.filename)[0]
                },
            )
            return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
        except (NoCredentialsError, ClientError) as e:
            if attempt == max_retries:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to upload to S3: {str(e)}",
                )
            time.sleep(retry_delay)
            retry_delay *= 2
        except Exception as e:
            if attempt == max_retries:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Upload failed: {str(e)}",
                )
            time.sleep(retry_delay)
            retry_delay *= 2

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to upload document to S3",
    )


def delete_from_s3(file_url: str) -> bool:
    if not s3_client or not file_url:
        return False

    try:
        if S3_BUCKET in file_url:
            s3_key = file_url.split(f"{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/")[-1]
        else:
            return False

        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except Exception:
        return False


def get_user_name(db: Session, user_id: int, role: str) -> str:
    try:
        if role.upper() == "BDE":
            executive = db.query(Executives).filter(Executives.id == user_id).first()
            if executive:
                return executive.name
        elif role.upper() == "BDM":
            manager = db.query(Managers).filter(Managers.id == user_id).first()
            if manager:
                return manager.name
    except Exception:
        pass

    return f"{role}_{user_id}"


def normalize_plan_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return value


def ensure_local_doc_record(db: Session, gym_id: int) -> LocalGymDocs:
    record = db.query(LocalGymDocs).filter(LocalGymDocs.gym_id == gym_id).first()
    if record:
        return record

    record = LocalGymDocs(
        gym_id=gym_id,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    db.add(record)
    db.flush()
    return record


@router.get("/list", response_model=LocalGymDocumentListResponse)
async def get_local_gyms(
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
):
    try:
        query = (
            db.query(
                GymDatabase.id.label("gym_id"),
                GymDatabase.gym_name.label("gym_name"),
                GymDatabase.contact_phone.label("gym_contact"),
                GymDatabase.address.label("gym_address"),
                GymDatabase.area.label("gym_area"),
                GymDatabase.city.label("gym_city"),
                GymDatabase.state.label("gym_state"),
                GymDatabase.pincode.label("gym_pincode"),
                LocalGymDocs.id.label("doc_id"),
                LocalGymDocs.aadhaar_url.label("aadhaar_url"),
                LocalGymDocs.aadhaar_back.label("aadhaar_back"),
                LocalGymDocs.pan_url.label("pan_url"),
                LocalGymDocs.bankbook_url.label("bankbook_url"),
                LocalGymDocs.plan_1,
                LocalGymDocs.plan_2,
                LocalGymDocs.plan_3,
                LocalGymDocs.plan_4,
                LocalGymDocs.plan_5,
                LocalGymDocs.updated_by.label("updated_by"),
                LocalGymDocs.created_at.label("doc_created_at"),
                LocalGymDocs.updated_at.label("doc_updated_at"),
            )
            .outerjoin(LocalGymDocs, GymDatabase.id == LocalGymDocs.gym_id)
            .order_by(desc(GymDatabase.created_at))
        )

        if search:
            query = query.filter(
                GymDatabase.gym_name.ilike(f"%{search}%")
                | GymDatabase.area.ilike(f"%{search}%")
                | GymDatabase.city.ilike(f"%{search}%")
            )

        results = query.offset(offset).limit(limit).all()

        document_list: List[LocalGymDocumentResponse] = []
        for row in results:
            location_parts = [
                part
                for part in [
                    row.gym_address,
                    row.gym_area,
                    row.gym_city,
                    row.gym_state,
                    row.gym_pincode,
                ]
                if part
            ]
            combined_location = ", ".join(location_parts) if location_parts else None

            document_list.append(
                LocalGymDocumentResponse(
                    id=row.doc_id if row.doc_id else 0,
                    gym_id=row.gym_id,
                    gym_name=row.gym_name,
                    contact_number=row.gym_contact,
                    location=combined_location,
                    aadhaar_front_url=row.aadhaar_url,
                    aadhaar_back_url=row.aadhaar_back,
                    pan_url=row.pan_url,
                    bankbook_url=row.bankbook_url,
                    plan_1=row.plan_1,
                    plan_2=row.plan_2,
                    plan_3=row.plan_3,
                    plan_4=row.plan_4,
                    plan_5=row.plan_5,
                    updated_by=row.updated_by,
                    created_at=row.doc_created_at,
                    updated_at=row.doc_updated_at,
                )
            )

        return LocalGymDocumentListResponse(
            success=True,
            message=f"Retrieved {len(document_list)} gyms successfully",
            data=document_list,
            total_count=len(document_list),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch local gym documents: {str(e)}",
        ) from e


@router.post("/upload-document", response_model=DocumentUploadResponse)
async def upload_local_gym_document(
    gym_id: int = Form(...),
    document_type: str = Form(...),
    user_id: int = Form(...),
    role: str = Form(...),
    file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    try:
        valid_documents = ["aadhar_front", "aadhar_back", "pan", "bankbook"]
        valid_plans = ["plan_1", "plan_2", "plan_3", "plan_4", "plan_5"]

        print("document_type",document_type)

        if document_type not in valid_documents + valid_plans:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid document type. Must be a supported document or plan_1 to plan_5",
            )

        gym_exists = (
            db.query(GymDatabase.id).filter(GymDatabase.id == gym_id).first()
        )
        if not gym_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Gym not found"
            )

        current_time = datetime.now()
        user_name = get_user_name(db, user_id, role)
        user_identifier = f"{role} - {user_name} (ID: {user_id})"

        doc_record = ensure_local_doc_record(db, gym_id)
        attribute_map = {
            "aadhar_front": "aadhaar_url",
            "aadhar_back": "aadhaar_back",
            "pan": "pan_url",
            "bankbook": "bankbook_url",
        }
        plan_map = {plan: plan for plan in valid_plans}

        document_url: Optional[str] = None

        if document_type in attribute_map:
            if not file:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="File upload is required for document types",
                )
            document_url = upload_to_s3(file, gym_id, document_type)
            setattr(doc_record, attribute_map[document_type], document_url)
        else:
            target_attr = plan_map[document_type]
            if file:
                document_url = upload_to_s3(file, gym_id, document_type)
                setattr(doc_record, target_attr, document_url)


        doc_record.updated_by = user_identifier
        doc_record.updated_at = current_time

        db.commit()

        return DocumentUploadResponse(
            success=True,
            message=f"{document_type.replace('_', ' ').title()} saved successfully",
            document_url=document_url,
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload document: {str(e)}",
        ) from e


@router.delete("/delete-document", response_model=DocumentDeleteResponse)
async def delete_local_gym_document(
    gym_id: int = Query(...),
    document_type: str = Query(...),
    user_id: int = Query(...),
    role: str = Query(...),
    db: Session = Depends(get_db),
):
    try:
        valid_documents = ["aadhar_front", "aadhar_back", "pan", "bankbook"]
        valid_plans = ["plan_1", "plan_2", "plan_3", "plan_4", "plan_5"]

        if document_type not in valid_documents + valid_plans:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid document type. Must be a supported document or plan_1 to plan_5",
            )

        gym_exists = (
            db.query(GymDatabase.id).filter(GymDatabase.id == gym_id).first()
        )
        if not gym_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Gym not found"
            )

        doc_record = (
            db.query(LocalGymDocs).filter(LocalGymDocs.gym_id == gym_id).first()
        )
        if not doc_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Local documents not found for this gym",
            )

        attribute_map = {
            "aadhar_front": "aadhaar_url",
            "aadhar_back": "aadhaar_back",
            "pan": "pan_url",
            "bankbook": "bankbook_url",
        }
        plan_map = {plan: plan for plan in valid_plans}

        current_url: Optional[str] = None

        if document_type in attribute_map:
            attr_name = attribute_map[document_type]
        else:
            attr_name = plan_map[document_type]

        current_value = getattr(doc_record, attr_name)
        setattr(doc_record, attr_name, None)

        current_url = (
            current_value
            if isinstance(current_value, str)
            and f"{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com" in current_value
            else None
        )

        if current_url:
            delete_from_s3(current_url)

        current_time = datetime.now()
        user_name = get_user_name(db, user_id, role)
        doc_record.updated_by = f"{role} - {user_name} (ID: {user_id})"
        doc_record.updated_at = current_time

        db.commit()

        return DocumentDeleteResponse(
            success=True,
            message=f"{document_type.replace('_', ' ').title()} deleted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete document: {str(e)}",
        ) from e


@router.get("/gym-details/{gym_id}", response_model=LocalGymDocumentListResponse)
async def get_local_gym_details(
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        gym_data = (
            db.query(
                GymDatabase.id,
                GymDatabase.gym_name,
                GymDatabase.contact_phone,
                GymDatabase.address,
                GymDatabase.area,
                GymDatabase.city,
                GymDatabase.state,
                GymDatabase.pincode,
            )
            .filter(GymDatabase.id == gym_id)
            .first()
        )

        if not gym_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Gym not found"
            )

        doc_record = (
            db.query(LocalGymDocs).filter(LocalGymDocs.gym_id == gym_id).first()
        )

        location_parts = [
            part
            for part in [
                gym_data.address,
                gym_data.area,
                gym_data.city,
                gym_data.state,
                gym_data.pincode,
            ]
            if part
        ]
        combined_location = ", ".join(location_parts) if location_parts else None

        doc_response = LocalGymDocumentResponse(
            id=doc_record.id if doc_record else 0,
            gym_id=gym_id,
            gym_name=gym_data.gym_name,
            contact_number=gym_data.contact_phone,
            location=combined_location,
            aadhaar_front_url=doc_record.aadhaar_url if doc_record else None,
            aadhaar_back_url=doc_record.aadhaar_back if doc_record else None,
            pan_url=doc_record.pan_url if doc_record else None,
            bankbook_url=doc_record.bankbook_url if doc_record else None,
            plan_1=doc_record.plan_1 if doc_record else None,
            plan_2=doc_record.plan_2 if doc_record else None,
            plan_3=doc_record.plan_3 if doc_record else None,
            plan_4=doc_record.plan_4 if doc_record else None,
            plan_5=doc_record.plan_5 if doc_record else None,
            updated_by=doc_record.updated_by if doc_record else None,
            created_at=doc_record.created_at if doc_record else None,
            updated_at=doc_record.updated_at if doc_record else None,
        )

        response=LocalGymDocumentListResponse(
            success=True,
            message="Local gym verification details retrieved successfully",
            data=[doc_response],
            total_count=1,
        )

        print("responseeee is",response)

        return LocalGymDocumentListResponse(
            success=True,
            message="Local gym verification details retrieved successfully",
            data=[doc_response],
            total_count=1,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch gym details: {str(e)}",
        ) from e
