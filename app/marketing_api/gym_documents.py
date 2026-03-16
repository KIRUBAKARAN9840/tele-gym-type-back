from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models.database import get_db
from app.models.marketingmodels import  Executives, Managers
from app.models.fittbot_models import Gym,GymVerificationDocument
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os
import uuid
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import mimetypes
import time

load_dotenv()

router = APIRouter(tags=["Gym Documents"], prefix="/marketing/gym-documents")

# AWS S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "fittbot-uploads")
S3_REGION = os.getenv("S3_REGION", "ap-south-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Initialize S3 client
s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=S3_REGION
        )

    except Exception as e:
        print(f"❌ Failed to initialize AWS S3 client: {e}")
        s3_client = None
else:
    print("❌ AWS credentials not configured")

# Pydantic models for request/response
class GymVerificationDocumentResponse(BaseModel):
    id: int
    gym_id: int
    gym_name: Optional[str] = None
    contact_number: Optional[str] = None
    location: Optional[str] = None
    aadhaar_front_url: Optional[str] = None
    aadhaar_back_url:Optional[str] = None
    pan_url: Optional[str] = None
    bankbook_url: Optional[str] = None
    updated_by: Optional[str] = None
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True

class GymResponse(BaseModel):
    id: int
    gym_name: str
    contact_number: Optional[str]
    location: Optional[str]
    street_area: Optional[str]
    aadhar_url: Optional[str] = None
    pan_url: Optional[str] = None
    bankbook_url: Optional[str] = None
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str] = None
    last_updated_by: Optional[str] = None

    class Config:
        from_attributes = True

class GymListResponse(BaseModel):
    success: bool
    message: str
    data: List[GymResponse]
    total_count: int

class GymVerificationDocumentListResponse(BaseModel):
    success: bool
    message: str
    data: List[GymVerificationDocumentResponse]
    total_count: int

class DocumentUploadRequest(BaseModel):
    gym_id: int
    document_type: str  # "aadhar", "pan", "bankbook"
    file_url: str

class DocumentUploadResponse(BaseModel):
    success: bool
    message: str
    document_url: Optional[str] = None

class DocumentDeleteResponse(BaseModel):
    success: bool
    message: str

def upload_to_s3(file: UploadFile, gym_id: int, document_type: str) -> str:
    """Upload file to AWS S3 and return the URL with retry logic"""
    if not s3_client:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS S3 not configured"
        )

    file_extension = os.path.splitext(file.filename)[1]
    unique_filename = f"gym_{gym_id}_{document_type}_{uuid.uuid4().hex}{file_extension}"
    s3_key = f"gym-documents/{gym_id}/{document_type}/{unique_filename}"

    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔄 Attempt {attempt}/{max_retries} - Uploading file to S3...")

            # Reset file pointer to beginning for retry attempts
            file.file.seek(0)

            # Upload file to S3
            s3_client.upload_fileobj(
                file.file,
                S3_BUCKET,
                s3_key,
                ExtraArgs={
                    'ContentType': file.content_type or mimetypes.guess_type(file.filename)[0]
                }
            )

            # Generate the URL
            file_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"

            print(f"✅ File uploaded successfully: {s3_key}")
            return file_url

        except NoCredentialsError:
            print(f"❌ AWS credentials not found")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="AWS credentials not found"
            )

        except ClientError as e:
            print(f"⚠️  S3 ClientError on attempt {attempt}/{max_retries}: {str(e)}")
            if attempt < max_retries:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"❌ All {max_retries} attempts failed for S3 upload")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to upload to S3 after {max_retries} attempts: {str(e)}"
                )

        except Exception as e:
            print(f"⚠️  Upload error on attempt {attempt}/{max_retries}: {str(e)}")
            if attempt < max_retries:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"❌ All {max_retries} attempts failed")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Upload failed after {max_retries} attempts: {str(e)}"
                )

def delete_from_s3(file_url: str) -> bool:
    """Delete file from AWS S3"""
    if not s3_client:
        return False

    try:
        # Extract S3 key from URL
        if S3_BUCKET in file_url:
            s3_key = file_url.split(f"{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/")[-1]
        else:
            return False

        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"✅ File deleted successfully: {s3_key}")
        return True

    except Exception as e:
        print(f"❌ Failed to delete file from S3: {e}")
        return False

def get_user_name(db: Session, user_id: int, role: str) -> str:
    """
    Get user name based on role and user_id
    """
    try:
        if role.upper() == 'BDE':
            # Look for BDE in executives table
            executive = db.query(Executives).filter(Executives.id == user_id).first()
            if executive:
                return executive.name
        elif role.upper() == 'BDM':
            # Look for BDM in managers table
            manager = db.query(Managers).filter(Managers.id == user_id).first()
            if manager:
                return manager.name

        # Fallback: return role + ID if name not found
        return f"{role}_{user_id}"
    except Exception as e:
        print(f"Error getting user name: {e}")
        return f"{role}_{user_id}"

@router.get("/list", response_model=GymVerificationDocumentListResponse)
async def get_gyms(
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None
):
    """
    Get all gyms with their verification documents (if any) using LEFT JOIN
    """
    try:
        # Query with LEFT JOIN to show all gyms, even without verification documents
        query = db.query(
            Gym.gym_id.label('gym_id'),
            Gym.name.label('gym_name'),
            Gym.contact_number.label('gym_contact'),
            Gym.street.label('gym_street'),
            Gym.area.label('gym_area'),
            Gym.city.label('gym_city'),
            Gym.state.label('gym_state'),
            Gym.pincode.label('gym_pincode'),
            GymVerificationDocument.id.label('doc_id'),
            GymVerificationDocument.aadhaar_url.label('aadhaar_url'),
            GymVerificationDocument.aadhaar_back.label('aadhaar_back'),
            GymVerificationDocument.pan_url.label('pan_url'),
            GymVerificationDocument.bankbook_url.label('bankbook_url'),
            GymVerificationDocument.updated_by.label('updated_by'),
            GymVerificationDocument.created_at.label('doc_created_at'),
            GymVerificationDocument.updated_at.label('doc_updated_at')
        ).outerjoin(
            GymVerificationDocument, Gym.gym_id == GymVerificationDocument.gym_id
        )

        # Add search filter if provided
        if search:
            query = query.filter(
                Gym.name.ilike(f"%{search}%") |
                Gym.area.ilike(f"%{search}%") |
                Gym.city.ilike(f"%{search}%")
            )

        results = query.order_by(desc(Gym.created_at)).offset(offset).limit(limit).all()

        # Convert to response format
        document_list = []
        for row in results:
            # Combine location fields into a single string
            location_parts = []
            if row.gym_street:
                location_parts.append(row.gym_street)
            if row.gym_area:
                location_parts.append(row.gym_area)
            if row.gym_city:
                location_parts.append(row.gym_city)
            if row.gym_state:
                location_parts.append(row.gym_state)
            if row.gym_pincode:
                location_parts.append(row.gym_pincode)

            combined_location = ", ".join(location_parts) if location_parts else None

            doc_response = GymVerificationDocumentResponse(
                id=row.doc_id if row.doc_id else 0,
                gym_id=row.gym_id,
                gym_name=row.gym_name,
                contact_number=row.gym_contact,
                location=combined_location,
                aadhaar_front_url=row.aadhaar_url if row.aadhaar_url else None,
                aadhaar_back_url=row.aadhaar_back if row.aadhaar_back else None,
                pan_url=row.pan_url,
                bankbook_url=row.bankbook_url,
                updated_by=row.updated_by,
                created_at=row.doc_created_at,
                updated_at=row.doc_updated_at
            )
            document_list.append(doc_response)

        return GymVerificationDocumentListResponse(
            success=True,
            message=f'Retrieved {len(document_list)} gyms successfully',
            data=document_list,
            total_count=len(document_list)
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch gym verification documents: {str(e)}"
        )

@router.post("/upload-document", response_model=DocumentUploadResponse)
async def upload_gym_document(
    file: UploadFile = File(...),
    gym_id: int = Form(...),
    document_type: str = Form(...),
    user_id: int = Form(...),
    role: str = Form(...),
    db: Session = Depends(get_db)
):
    """
    Upload gym document (Aadhar, PAN, Bank Book)
    """
    try:
        print("gym_id",gym_id)
        # Validate document type
        if document_type not in ["aadhar_back","aadhar_front", "pan", "bankbook"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid document type. Must be 'aadhar', 'pan', or 'bankbook'"
            )

        # Check if gym exists (only query gym_id to avoid missing column errors)
        gym_exists = db.query(Gym.gym_id).filter(Gym.gym_id == gym_id).first()
        if not gym_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Gym not found"
            )

        # Upload file to S3
        file_url = upload_to_s3(file, gym_id, document_type)

        # Set tracking information
        current_time = datetime.now()
        user_name = get_user_name(db, user_id, role)
        user_identifier = f"{role} - {user_name} (ID: {user_id})"

        # Check if verification document record exists for this gym
        verification_doc = db.query(GymVerificationDocument).filter(
            GymVerificationDocument.gym_id == gym_id
        ).first()

        if not verification_doc:
            # Create new verification document record
            verification_doc = GymVerificationDocument(
                gym_id=gym_id,
                updated_by=user_identifier,
                created_at=current_time,
                updated_at=current_time
            )
            db.add(verification_doc)

        # Update document URL based on type
        if document_type == "aadhar_front":
            verification_doc.aadhaar_url = file_url
        elif document_type == "pan":
            verification_doc.pan_url = file_url
        elif document_type == "bankbook":
            verification_doc.bankbook_url = file_url
        elif document_type=="aadhar_back":
            verification_doc.aadhaar_back=file_url

        # Update tracking information
        verification_doc.updated_by = user_identifier
        verification_doc.updated_at = current_time

        db.commit()

        response=DocumentUploadResponse(
            success=True,
            message=f"{document_type.title()} document uploaded successfully",
            document_url=file_url
        )

        print("response",response)

        return DocumentUploadResponse(
            success=True,
            message=f"{document_type.title()} document uploaded successfully",
            document_url=file_url
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload document: {str(e)}"
        )

@router.delete("/delete-document", response_model=DocumentDeleteResponse)
async def delete_gym_document(
    gym_id: int = Query(...),
    document_type: str = Query(...),
    user_id: int = Query(...),
    role: str = Query(...),
    db: Session = Depends(get_db)
):
    """
    Delete gym document
    """
    try:
        # Validate document type
        if document_type not in ["aadhar_front","aadhar_back", "pan", "bankbook"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid document type. Must be 'aadhar', 'pan', or 'bankbook'"
            )

        # Check if gym exists (only query gym_id to avoid missing column errors)
        gym_exists = db.query(Gym.gym_id).filter(Gym.gym_id == gym_id).first()
        if not gym_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Gym not found"
            )

        # Check if verification document record exists
        verification_doc = db.query(GymVerificationDocument).filter(
            GymVerificationDocument.gym_id == gym_id
        ).first()

        if not verification_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No verification documents found for this gym"
            )

        # Get current document URL
        current_url = None
        if document_type == "aadhar_front":
            current_url = verification_doc.aadhaar_url
            verification_doc.aadhaar_url = None
        if document_type == "aadhar_back":
            current_url = verification_doc.aadhaar_back
            verification_doc.aadhaar_back = None
        elif document_type == "pan":
            current_url = verification_doc.pan_url
            verification_doc.pan_url = None
        elif document_type == "bankbook":
            current_url = verification_doc.bankbook_url
            verification_doc.bankbook_url = None

        # Delete from S3 if URL exists
        if current_url:
            delete_from_s3(current_url)

        # Set tracking information for deletion
        current_time = datetime.now()
        user_name = get_user_name(db, user_id, role)
        user_identifier = f"{role} - {user_name} (ID: {user_id})"

        verification_doc.updated_by = user_identifier
        verification_doc.updated_at = current_time

        db.commit()

        return DocumentDeleteResponse(
            success=True,
            message=f"{document_type.title()} document deleted successfully"
        )

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete document: {str(e)}"
        )

@router.get("/gym-details/{gym_id}")
async def get_gym_details(
    gym_id: int,
    db: Session = Depends(get_db)
):

    print("gym is",gym_id)
    try:
        # Query only specific columns from Gym to avoid missing column errors
        gym_data = db.query(
            Gym.gym_id,
            Gym.name,
            Gym.contact_number,
            Gym.street,
            Gym.area,
            Gym.city,
            Gym.state,
            Gym.pincode
        ).filter(Gym.gym_id == gym_id).first()

        if not gym_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Gym not found"
            )

        # Get verification document for this gym
        verification_doc = db.query(GymVerificationDocument).filter(
            GymVerificationDocument.gym_id == gym_id
        ).first()

        # Combine location fields into a single string
        location_parts = []
        if gym_data.street:
            location_parts.append(gym_data.street)
        if gym_data.area:
            location_parts.append(gym_data.area)
        if gym_data.city:
            location_parts.append(gym_data.city)
        if gym_data.state:
            location_parts.append(gym_data.state)
        if gym_data.pincode:
            location_parts.append(gym_data.pincode)

        combined_location = ", ".join(location_parts) if location_parts else None

        # If no verification document exists, return empty document URLs
        doc_response = GymVerificationDocumentResponse(
            id=verification_doc.id if verification_doc else 0,
            gym_id=gym_id,
            gym_name=gym_data.name,
            contact_number=gym_data.contact_number,
            location=combined_location,
            aadhaar_front_url=verification_doc.aadhaar_url if verification_doc else None,
            aadhaar_back_url=verification_doc.aadhaar_back if verification_doc else None,
            pan_url=verification_doc.pan_url if verification_doc else None,
            bankbook_url=verification_doc.bankbook_url if verification_doc else None,
            updated_by=verification_doc.updated_by if verification_doc else None,
            created_at=verification_doc.created_at if verification_doc else None,
            updated_at=verification_doc.updated_at if verification_doc else None
        )

        response= GymVerificationDocumentListResponse(
            success=True,
            message='Gym verification details retrieved successfully',
            data=[doc_response],
            total_count=1
        )

        print("ressss",response)

        return GymVerificationDocumentListResponse(
            success=True,
            message='Gym verification details retrieved successfully',
            data=[doc_response],
            total_count=1
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch gym details: {str(e)}"
        )
    
