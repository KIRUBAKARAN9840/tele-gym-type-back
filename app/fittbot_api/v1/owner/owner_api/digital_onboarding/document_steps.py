import time
import boto3

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    AccountDetails,
    Gym,
    GymVerificationDocument,
    GymOnboardingPics,
    GymPrefilledAgreement,
    GymLocation,
)
from app.utils.logging_utils import auth_logger, FittbotHTTPException

PHOTO_MAX_SIZE = 10 * 1024 * 1024  # 5 MB

router = APIRouter(prefix="/owner_registration", tags=["Registration Steps"])

# S3 Configuration
AWS_REGION = "ap-south-2"
BUCKET_NAME = "fittbot-uploads"
DOCUMENT_MAX_SIZE = 15 * 1024 * 1024  # 5 MB

_s3 = boto3.client("s3", region_name=AWS_REGION)


class DataType(str, Enum):
    operating_hours = "operating_hours"
    services = "services"


class DocumentType(str, Enum):
    pancard = "pan_card"
    passbook = "bank_document"


class UpdateGymDataRequest(BaseModel):
    data_type: DataType
    gym_id: int
    data: Any


class UpdateGymDataResponse(BaseModel):
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


class AccountDetailsRequest(BaseModel):
    gym_id: int
    accountNumber: str
    confirmAccountNumber: str
    ifscCode: str
    accountHolderName: str
    bankName: str
    branchName: str
    upiId: Optional[str] = None
    panNumber: Optional[str] = None
    gstNumber: Optional[str] = None
    gstType: str = "no_gst"
    gstPercentage: str = "18"


class AccountDetailsResponse(BaseModel):
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


class DocumentConfirmRequest(BaseModel):
    gym_id: int
    column_name: DocumentType
    cdn_url: str


class DocumentConfirmResponse(BaseModel):
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


def generate_document_upload_url(gym_id: int, document_type: str, extension: str, content_type: str = "image/jpeg"):
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
            log_data={"gym_id": gym_id},
        )

    if document_type not in ["pan_card", "bank_document"]:
        raise FittbotHTTPException(
            status_code=400,
            detail="Invalid document_type. Must be one of: pancard, passbook",
            error_code="INVALID_DOCUMENT_TYPE",
            log_data={"document_type": document_type},
        )

    # Key layout: gym_verification_documents/<gym_id>/<document_type>.<extension>
    key = f"gym_verification_documents/{gym_id}/{document_type}.{extension}"
    version = int(time.time() * 1000)

    fields = {"Content-Type": content_type}
    conditions = [
        {"Content-Type": content_type},
        ["content-length-range", 1, DOCUMENT_MAX_SIZE],
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
            log_data={"exc": repr(e), "gym_id": gym_id, "key": key},
        )

    presigned["url"] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
    cdn_url = f"{presigned['url']}{key}?v={version}"

    return {
        "upload": presigned,
        "cdn_url": cdn_url,
        "version": version,
    }


class DocumentStepsResponse(BaseModel):
    status: int
    message: str
    data: Optional[Dict[str, Any]] = None


@router.get("/document-steps", response_model=DocumentStepsResponse)
async def get_document_steps(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):

    try:
        
        stmt = select(AccountDetails).where(AccountDetails.gym_id == gym_id)
        result = await db.execute(stmt)
        account_details = result.scalar_one_or_none()
        account_details_completed = account_details is not None

        # 2. Check gyms table for services and operating_hours
        stmt = select(Gym).where(Gym.gym_id == gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        services_completed = gym.services is not None and len(gym.services) > 0 if gym.services else False
        operating_hours_completed = gym.operating_hours is not None and len(gym.operating_hours) > 0 if gym.operating_hours else False

        # 3. Check gym_verification_documents table
        stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == gym_id)
        result = await db.execute(stmt)
        verification_doc = result.scalar_one_or_none()

        # Agreement status
        agreement_completed = verification_doc.agreement if verification_doc and verification_doc.agreement else False

        # Pancard status (pan_url)
        pancard_completed = verification_doc.pan_url is not None and len(verification_doc.pan_url) > 0 if verification_doc else False

        # Passbook status (bankbook_url)
        passbook_completed = verification_doc.bankbook_url is not None and len(verification_doc.bankbook_url) > 0 if verification_doc else False

        # 4. Check gym_onboarding_pics table
        stmt = select(GymOnboardingPics).where(GymOnboardingPics.gym_id == gym_id)
        result = await db.execute(stmt)
        onboarding_pics = result.scalar_one_or_none()

        # Build documents list with pancard and passbook only
        documents = [
            {"pancard": pancard_completed},
            {"passbook": passbook_completed}
        ]

        # Build onboarding pics list separately
        onboarding_pics_status = []
        if onboarding_pics:
            pic_columns = [
                "machinery_1",
                "machinery_2",
                "treadmill_area",
                "cardio_area",
                "dumbell_area",
                "reception_area"
            ]
            for col in pic_columns:
                value = getattr(onboarding_pics, col, None)
                onboarding_pics_status.append({
                    col: value is not None and len(value) > 0 if value else False
                })
        else:
            
            onboarding_pics_status = [
                {"machinery_1": False},
                {"machinery_2": False},
                {"treadmill_area": False},
                {"cardio_area": False},
                {"dumbell_area": False},
                {"reception_area": False}
            ]

        response_data = {
            "account_details": account_details_completed,
            "services": services_completed,
            "operating_hours": operating_hours_completed,
            "agreement": agreement_completed,
            "documents": documents,
            "onboarding_pics": onboarding_pics_status
        }

        return DocumentStepsResponse(
            status=200,
            message="Document steps status retrieved successfully",
            data=response_data
        )

    except HTTPException:
        raise

    except Exception as e:
        auth_logger.error(f"Error getting document steps: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while getting document steps: {str(e)}",
            error_code="DOCUMENT_STEPS_ERROR"
        )


@router.post("/update-gym-data", response_model=UpdateGymDataResponse)
async def update_gym_data(
    request: UpdateGymDataRequest,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        stmt = select(Gym).where(Gym.gym_id == request.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        if request.data_type == DataType.operating_hours:
            gym.operating_hours = request.data
        elif request.data_type == DataType.services:
            gym.services = request.data

        await db.commit()
        await db.refresh(gym)



        return UpdateGymDataResponse(
            status=200,
            message=f"{request.data_type.value} updated successfully",
            data={request.data_type.value: request.data}
        )

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error updating gym data: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while updating gym data: {str(e)}",
            error_code="UPDATE_GYM_DATA_ERROR"
        )


@router.post("/account-details", response_model=AccountDetailsResponse)
async def add_account_details(
    request: AccountDetailsRequest,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        if request.accountNumber != request.confirmAccountNumber:
            raise HTTPException(
                status_code=400,
                detail="Account numbers do not match"
            )

        stmt = select(Gym).where(Gym.gym_id == request.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        stmt = select(AccountDetails).where(AccountDetails.gym_id == request.gym_id)
        result = await db.execute(stmt)
        existing_account = result.scalar_one_or_none()

        if existing_account:
            existing_account.account_number = request.accountNumber
            existing_account.account_ifsccode = request.ifscCode
            existing_account.account_holdername = request.accountHolderName
            existing_account.bank_name = request.bankName
            existing_account.account_branch = request.branchName
            existing_account.upi_id = request.upiId
            existing_account.pan_number = request.panNumber
            existing_account.gst_number = request.gstNumber
            existing_account.gst_type = request.gstType
            existing_account.gst_percentage = request.gstPercentage

            await db.commit()
            await db.refresh(existing_account)


            return AccountDetailsResponse(
                status=200,
                message="Account details updated successfully",
                data={"account_id": existing_account.account_id}
            )
        else:
            from datetime import datetime
            new_account = AccountDetails(
                gym_id=request.gym_id,
                account_number=request.accountNumber,
                account_ifsccode=request.ifscCode,
                account_holdername=request.accountHolderName,
                bank_name=request.bankName,
                account_branch=request.branchName,
                upi_id=request.upiId,
                pan_number=request.panNumber,
                gst_number=request.gstNumber,
                gst_type=request.gstType,
                gst_percentage=request.gstPercentage,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_account)
            await db.commit()
            await db.refresh(new_account)



            return AccountDetailsResponse(
                status=200,
                message="Account details added successfully",
                data={"account_id": new_account.account_id}
            )

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error adding account details: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while adding account details: {str(e)}",
            error_code="ACCOUNT_DETAILS_ERROR"
        )


@router.get("/document-upload")
async def get_document_upload_url(gym_id: int, scope: str, extension: str):

    try:
        url_data = generate_document_upload_url(gym_id, scope, extension)
        return {"status": 200, "data": url_data}
    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="DOCUMENT_UPLOAD_URL_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id, "document_type": scope, "extension": extension},
        )


@router.post("/document-confirm", response_model=DocumentConfirmResponse)
async def confirm_document_upload(
    request: DocumentConfirmRequest,
    db: AsyncSession = Depends(get_async_db)
):

    try:
        stmt = select(Gym).where(Gym.gym_id == request.gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == request.gym_id)
        result = await db.execute(stmt)
        verification_doc = result.scalar_one_or_none()

        if verification_doc:
            if request.column_name == DocumentType.pancard:
                verification_doc.pan_url = request.cdn_url
            elif request.column_name == DocumentType.passbook:
                verification_doc.bankbook_url = request.cdn_url

            await db.commit()
            await db.refresh(verification_doc)



            return DocumentConfirmResponse(
                status=200,
                message=f"{request.column_name.value} uploaded successfully",
                data={
                    "gym_id": request.gym_id,
                    "document_type": request.column_name.value,
                    "url": request.cdn_url
                }
            )
        else:
            from datetime import datetime
            new_verification_doc = GymVerificationDocument(
                gym_id=request.gym_id,
                pan_url=request.cdn_url if request.column_name == DocumentType.pancard else None,
                bankbook_url=request.cdn_url if request.column_name == DocumentType.passbook else None,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.add(new_verification_doc)
            await db.commit()
            await db.refresh(new_verification_doc)



            return DocumentConfirmResponse(
                status=200,
                message=f"{request.column_name.value} uploaded successfully",
                data={
                    "gym_id": request.gym_id,
                    "column_name": request.column_name.value,
                    "url": request.cdn_url
                }
            )

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error confirming document: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while confirming document: {str(e)}",
            error_code="DOCUMENT_CONFIRM_ERROR"
        )


DOCUMENT_COLUMNS = ["pan_card", "bank_document"]

DOCUMENT_TITLES = {
    "pan_card": "pan_card",
    "passbook": "bank_documnet"
}

DOCUMENT_DB_MAPPING = {
    "pan_card": "pan_url",
    "bank_document": "bankbook_url"
}


@router.get("/get-documents")
async def get_documents(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    try:
        stmt = select(Gym).where(Gym.gym_id == gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        stmt = select(GymVerificationDocument).where(GymVerificationDocument.gym_id == gym_id)
        result = await db.execute(stmt)
        verification_doc = result.scalar_one_or_none()

        documents = []
        for idx, key in enumerate(DOCUMENT_COLUMNS, start=1):
            db_column = DOCUMENT_DB_MAPPING.get(key)
            url = None
            if verification_doc and db_column:
                url = getattr(verification_doc, db_column, None)

            documents.append({
                "id": idx,
                "key": key,
                "image_url": url
            })

        return {
            "status": 200,
            "message": "Documents retrieved successfully",
            "data": {
                "gym_id": gym_id,
                "documents": documents
            }
        }

    except HTTPException:
        raise

    except Exception as e:
        auth_logger.error(f"Error getting documents: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while getting documents: {str(e)}",
            error_code="GET_DOCUMENTS_ERROR"
        )


@router.get("/get-prefilled-agreement")
async def get_prefilled_agreement(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get prefilled agreement PDF link for a gym if it exists.
    Returns the S3 link for downloading the agreement.
    """
    try:
        stmt = select(Gym).where(Gym.gym_id == gym_id)
        result = await db.execute(stmt)
        gym = result.scalar_one_or_none()

        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")

        stmt = select(GymPrefilledAgreement).where(GymPrefilledAgreement.gym_id == gym_id)
        result = await db.execute(stmt)
        prefilled_agreement = result.scalar_one_or_none()

        if not prefilled_agreement:
            return {
                "status": 200,
                "message": "No prefilled agreement found for this gym",
                "data": {
                    "gym_id": gym_id,
                    "has_agreement": False,
                    "s3_link": None
                }
            }

        return {
            "status": 200,
            "message": "Prefilled agreement retrieved successfully",
            "data": {
                "gym_id": gym_id,
                "has_agreement": True,
                "s3_link": prefilled_agreement.s3_link,
                "updated_at": prefilled_agreement.updated_at.isoformat() if prefilled_agreement.updated_at else None
            }
        }

    except HTTPException:
        raise

    except Exception as e:
        auth_logger.error(f"Error getting prefilled agreement: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while getting prefilled agreement: {str(e)}",
            error_code="GET_PREFILLED_AGREEMENT_ERROR"
        )


class UploadCurrentPicConfirmRequest(BaseModel):
    gym_id: int
    latitude: float
    longitude: float
    gym_pic_url: str


@router.get("/upload_current_pic")
async def upload_current_pic(gym_id: int, extension: str):

    try:
        if not extension:
            raise FittbotHTTPException(
                status_code=400,
                detail="File extension is required",
                error_code="MISSING_FILE_EXTENSION",
                log_data={"gym_id": gym_id},
            )

        content_type = "image/jpeg"
        if extension.lower() in ("png",):
            content_type = "image/png"
        elif extension.lower() in ("webp",):
            content_type = "image/webp"

        key = f"owner_current_pics/{gym_id}/current_pic.{extension}"
        version = int(time.time() * 1000)

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
            "status": 200,
            "data": {
                "upload": presigned,
                "cdn_url": cdn_url,
                "version": version,
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate upload URL",
            error_code="CURRENT_PIC_UPLOAD_URL_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id, "extension": extension},
        )


@router.post("/upload_current_pic_confirm")
async def upload_current_pic_confirm(
    request: UploadCurrentPicConfirmRequest,
    db: AsyncSession = Depends(get_async_db),
):

    try:

        stmt = select(GymLocation).where(GymLocation.gym_id == request.gym_id)
        result = await db.execute(stmt)
        existing = result.scalar_one_or_none()

 

        if existing:
            existing.latitude = request.latitude
            existing.longitude = request.longitude
            existing.gym_pic = request.gym_pic_url
        else:
            new_location = GymLocation(
                gym_id=request.gym_id,
                latitude=request.latitude,
                longitude=request.longitude,
                gym_pic=request.gym_pic_url,
            )
            db.add(new_location)

        await db.commit()

        return {
            "status": 200,
            "message": "Gym location and photo saved successfully",
            "data": {
                "gym_id": request.gym_id,
                "latitude": float(request.latitude),
                "longitude": float(request.longitude),
                "gym_pic": request.gym_pic_url,
            }
        }

    except HTTPException:
        await db.rollback()
        raise

    except Exception as e:
        await db.rollback()
        auth_logger.error(f"Error saving gym location: {str(e)}")
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occurred while saving gym location: {str(e)}",
            error_code="UPLOAD_CURRENT_PIC_ERROR",
        )


