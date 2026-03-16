import json
import random
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pydantic import BaseModel
from typing import Optional

from app.models.async_database import get_async_db
from app.models.fittbot_models import Gym, GymOwner, AccountDetails, AccountDetailsEditRequest
from app.utils.logging_utils import FittbotHTTPException
from datetime import datetime
from app.utils.redis_config import get_redis
from app.utils.otp import generate_otp, async_send_verification_sms


class UpdateOwnerPersonalDetailsRequest(BaseModel):
    owner_id: int
    name: Optional[str] = None
    email: Optional[str] = None
    contact_number: Optional[str] = None


class CheckMobileExistsRequest(BaseModel):
    mobile: str
    owner_id: int


class SendMobileChangeOtpRequest(BaseModel):
    owner_id: int
    mobile: str
    step: str  # "current" or "new"


class VerifyMobileChangeOtpRequest(BaseModel):
    owner_id: int
    mobile: str
    otp: str
    step: str  # "current" or "new"


class UpdateMobileNumberRequest(BaseModel):
    owner_id: int
    new_mobile: str


class GymAddressUpdate(BaseModel):
    street: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None


class UpdateGymBasicDetailsRequest(BaseModel):
    owner_id: int
    gym_id: int
    name: Optional[str] = None
    contact_number: Optional[str] = None
    services: Optional[List[str]] = None
    operating_hours: Optional[List[Dict[str, Any]]] = None
    address: Optional[GymAddressUpdate] = None


router = APIRouter(prefix="/owner/profile", tags=["OwnerProfile"])


def _normalize_services(raw_services: Any) -> List[str]:

    if not raw_services:
        return []

    if isinstance(raw_services, list):
        return [str(service) for service in raw_services if service]

    if isinstance(raw_services, str):
        try:
            parsed = json.loads(raw_services)
            if isinstance(parsed, list):
                return [str(service) for service in parsed if service]
        except json.JSONDecodeError:
            pass
        # Fallback to comma separated string
        return [part.strip() for part in raw_services.split(",") if part.strip()]

    return []


def _normalize_operating_hours(raw_hours: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_hours, list):
        return raw_hours
    if isinstance(raw_hours, str):
        try:
            parsed = json.loads(raw_hours)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return []
    return []


@router.get("/gym_basic_details")
async def get_gym_basic_details(
    owner_id: int,
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        if not isinstance(owner_id, int) or owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
                log_data={"owner_id": owner_id},
            )

        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        # Async query using select
        result = await db.execute(
            select(Gym).where(Gym.gym_id == gym_id)
        )
        gym = result.scalars().first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": gym_id},
            )

        if gym.owner_id != owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="You do not have access to this gym",
                error_code="GYM_ACCESS_DENIED",
                log_data={"gym_id": gym_id, "owner_id": owner_id},
            )

        services = _normalize_services(gym.services)
        operating_hours = _normalize_operating_hours(gym.operating_hours)

        response = {
            "gym_id": gym.gym_id,
            "name": gym.name,
            "location": gym.location,
            "contact_number": gym.contact_number,
            "services": services,
            "operating_hours": operating_hours,
            "address": {
                "street": gym.street,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
            },
            "logo": gym.logo or None,
            "cover_pic": gym.cover_pic or None,
        }

        return {
            "status": 200,
            "message": "Gym details fetched successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch gym details",
            error_code="GYM_DETAILS_FETCH_ERROR",
            log_data={"gym_id": gym_id, "owner_id": owner_id, "error": repr(exc)},
        )


@router.put("/gym_basic_details")
async def update_gym_basic_details(
    request: UpdateGymBasicDetailsRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Update gym basic details: name, contact_number, services, operating_hours, address.
    """
    try:
        if not isinstance(request.owner_id, int) or request.owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
                log_data={"owner_id": request.owner_id},
            )

        if not isinstance(request.gym_id, int) or request.gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": request.gym_id},
            )

        result = await db.execute(
            select(Gym).where(Gym.gym_id == request.gym_id)
        )
        gym = result.scalars().first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": request.gym_id},
            )

        if gym.owner_id != request.owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="You do not have access to this gym",
                error_code="GYM_ACCESS_DENIED",
                log_data={"gym_id": request.gym_id, "owner_id": request.owner_id},
            )

        # Update fields if provided
        if request.name is not None:
            gym.name = request.name
        if request.contact_number is not None:
            gym.contact_number = request.contact_number
        if request.services is not None:
            gym.services = json.dumps(request.services)
        if request.operating_hours is not None:
            gym.operating_hours = json.dumps(request.operating_hours)
        if request.address is not None:
            if request.address.street is not None:
                gym.street = request.address.street
            if request.address.area is not None:
                gym.area = request.address.area
            if request.address.city is not None:
                gym.city = request.address.city
            if request.address.state is not None:
                gym.state = request.address.state
            if request.address.pincode is not None:
                gym.pincode = request.address.pincode

        await db.commit()
        await db.refresh(gym)

        services = _normalize_services(gym.services)
        operating_hours = _normalize_operating_hours(gym.operating_hours)

        response = {
            "gym_id": gym.gym_id,
            "name": gym.name,
            "contact_number": gym.contact_number,
            "services": services,
            "operating_hours": operating_hours,
            "address": {
                "street": gym.street,
                "area": gym.area,
                "city": gym.city,
                "state": gym.state,
                "pincode": gym.pincode,
            },
        }

        return {
            "status": 200,
            "message": "Gym details updated successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update gym details",
            error_code="GYM_DETAILS_UPDATE_ERROR",
            log_data={"gym_id": request.gym_id, "owner_id": request.owner_id, "error": repr(exc)},
        )


@router.get("/owner_personal_details")
async def get_owner_personal_details(
    owner_id: int,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        if not isinstance(owner_id, int) or owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
                log_data={"owner_id": owner_id},
            )

        result = await db.execute(
            select(GymOwner).where(GymOwner.owner_id == owner_id)
        )
        owner = result.scalars().first()

        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": owner_id},
            )

        response = {
            "owner_id": owner.owner_id,
            "name": owner.name,
            "email": owner.email,
            "contact_number": owner.contact_number,
        }

        return {
            "status": 200,
            "message": "Owner personal details fetched successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch owner personal details",
            error_code="OWNER_DETAILS_FETCH_ERROR",
            log_data={"owner_id": owner_id, "error": repr(exc)},
        )


@router.put("/owner_personal_details")
async def update_owner_personal_details(
    request: UpdateOwnerPersonalDetailsRequest,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        if not isinstance(request.owner_id, int) or request.owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
                log_data={"owner_id": request.owner_id},
            )

        result = await db.execute(
            select(GymOwner).where(GymOwner.owner_id == request.owner_id)
        )
        owner = result.scalars().first()

        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": request.owner_id},
            )


        if request.name is not None:
            owner.name = request.name
        if request.email is not None:
            owner.email = request.email
        if request.contact_number is not None:
            # Check if contact number is already in use by another owner
            existing = await db.execute(
                select(GymOwner).where(
                    GymOwner.contact_number == request.contact_number,
                    GymOwner.owner_id != request.owner_id
                )
            )
            if existing.scalars().first():
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Contact number already in use",
                    error_code="CONTACT_NUMBER_EXISTS",
                    log_data={"contact_number": request.contact_number},
                )
            owner.contact_number = request.contact_number

        await db.commit()
        await db.refresh(owner)

        response = {
            "owner_id": owner.owner_id,
            "name": owner.name,
            "email": owner.email,
            "contact_number": owner.contact_number,
        }

        return {
            "status": 200,
            "message": "Owner personal details updated successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update owner personal details",
            error_code="OWNER_DETAILS_UPDATE_ERROR",
            log_data={"owner_id": request.owner_id, "error": repr(exc)},
        )


@router.post("/check_mobile_exists")
async def check_mobile_exists(
    request: CheckMobileExistsRequest,
    db: AsyncSession = Depends(get_async_db),
):

    try:
        result = await db.execute(
            select(GymOwner).where(
                GymOwner.contact_number == request.mobile,
                GymOwner.owner_id != request.owner_id
            )
        )
        existing = result.scalars().first()

        return {
            "status": 200,
            "exists": existing is not None,
            "message": "Mobile number already registered" if existing else "Mobile number is available",
        }

    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to check mobile number",
            error_code="CHECK_MOBILE_ERROR",
            log_data={"error": repr(exc)},
        )


@router.post("/send_mobile_change_otp")
async def send_mobile_change_otp(
    request: SendMobileChangeOtpRequest,
    db: AsyncSession = Depends(get_async_db),
    redis=Depends(get_redis),
):

    try:

        result = await db.execute(
            select(GymOwner).where(GymOwner.owner_id == request.owner_id)
        )
        owner = result.scalars().first()

        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
                log_data={"owner_id": request.owner_id},
            )

        # For 'current' step, verify the mobile matches owner's current number
        if request.step == "current":
            if owner.contact_number != request.mobile:
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Mobile number does not match current number",
                    error_code="MOBILE_MISMATCH",
                )

        # For 'new' step, check if new number already exists
        if request.step == "new":
            existing = await db.execute(
                select(GymOwner).where(
                    GymOwner.contact_number == request.mobile,
                    GymOwner.owner_id != request.owner_id
                )
            )
            if existing.scalars().first():
                raise FittbotHTTPException(
                    status_code=400,
                    detail="This mobile number is already registered",
                    error_code="MOBILE_EXISTS",
                )

        # Generate and store OTP
        otp = generate_otp()
        otp_key = f"mobile_change_otp:{request.owner_id}:{request.step}:{request.mobile}"
        await redis.set(otp_key, otp, ex=300)  # 5 minutes expiry

        # Send OTP via SMS
        if await async_send_verification_sms(request.mobile, otp):
            return {
                "status": 200,
                "message": f"OTP sent successfully to {request.mobile[:3]}****{request.mobile[-3:]}",
            }
        else:
            raise FittbotHTTPException(
                status_code=500,
                detail="Failed to send OTP",
                error_code="SMS_SEND_FAILED",
            )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to send OTP",
            error_code="SEND_OTP_ERROR",
            log_data={"error": repr(exc)},
        )


@router.post("/verify_mobile_change_otp")
async def verify_mobile_change_otp(
    request: VerifyMobileChangeOtpRequest,
    redis=Depends(get_redis),
):

    try:
        otp_key = f"mobile_change_otp:{request.owner_id}:{request.step}:{request.mobile}"
        stored_otp = await redis.get(otp_key)

        if not stored_otp:
            raise FittbotHTTPException(
                status_code=400,
                detail="OTP expired or not found. Please request a new OTP.",
                error_code="OTP_EXPIRED",
            )

        if stored_otp != request.otp:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid OTP. Please try again.",
                error_code="INVALID_OTP",
            )

        await redis.delete(otp_key)


        verified_key = f"mobile_change_verified:{request.owner_id}:{request.step}"
        await redis.set(verified_key, request.mobile, ex=600)

        return {
            "status": 200,
            "message": "OTP verified successfully",
            "step": request.step,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to verify OTP",
            error_code="VERIFY_OTP_ERROR",
            log_data={"error": repr(exc)},
        )


@router.post("/update_mobile_number")
async def update_mobile_number(
    request: UpdateMobileNumberRequest,
    db: AsyncSession = Depends(get_async_db),
    redis=Depends(get_redis),
):
    """
    Update mobile number after both OTPs are verified.
    """
    try:
        # Check if both steps are verified
        current_verified_key = f"mobile_change_verified:{request.owner_id}:current"
        new_verified_key = f"mobile_change_verified:{request.owner_id}:new"

        current_verified = await redis.get(current_verified_key)
        new_verified = await redis.get(new_verified_key)

        if not current_verified:
            raise FittbotHTTPException(
                status_code=400,
                detail="Current mobile number not verified. Please verify first.",
                error_code="CURRENT_NOT_VERIFIED",
            )

        if not new_verified:
            raise FittbotHTTPException(
                status_code=400,
                detail="New mobile number not verified. Please verify first.",
                error_code="NEW_NOT_VERIFIED",
            )

        if new_verified != request.new_mobile:
            raise FittbotHTTPException(
                status_code=400,
                detail="New mobile number does not match verified number.",
                error_code="MOBILE_MISMATCH",
            )

        # Update the mobile number
        result = await db.execute(
            select(GymOwner).where(GymOwner.owner_id == request.owner_id)
        )
        owner = result.scalars().first()

        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND",
            )

        # Double check the new number is not already in use
        existing = await db.execute(
            select(GymOwner).where(
                GymOwner.contact_number == request.new_mobile,
                GymOwner.owner_id != request.owner_id
            )
        )
        if existing.scalars().first():
            raise FittbotHTTPException(
                status_code=400,
                detail="This mobile number is already registered",
                error_code="MOBILE_EXISTS",
            )

        owner.contact_number = request.new_mobile
        await db.commit()
        await db.refresh(owner)

        # Clean up verification keys
        await redis.delete(current_verified_key)
        await redis.delete(new_verified_key)

        return {
            "status": 200,
            "message": "Mobile number updated successfully",
            "data": {
                "owner_id": owner.owner_id,
                "contact_number": owner.contact_number,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update mobile number",
            error_code="UPDATE_MOBILE_ERROR",
            log_data={"error": repr(exc)},
        )


# ====================== PAYMENT DETAILS APIS ======================

class PaymentDetailsEditRequest(BaseModel):
    owner_id: int
    gym_id: int
    old_data: Dict[str, Any]
    new_data: Dict[str, Any]


class UpdateUpiGstRequest(BaseModel):
    owner_id: int
    gym_id: int
    upi_id: Optional[str] = None
    gst_type: Optional[str] = None
    gst_number: Optional[str] = None
    gst_percentage: Optional[str] = None


@router.put("/payment_details_upi_gst")
async def update_upi_gst_details(
    request: UpdateUpiGstRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Directly update UPI and GST fields without verification.
    These fields can be edited freely by the owner.
    """
    try:
        if not isinstance(request.owner_id, int) or request.owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
            )

        if not isinstance(request.gym_id, int) or request.gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
            )

        # Verify gym belongs to owner
        gym_result = await db.execute(
            select(Gym).where(Gym.gym_id == request.gym_id)
        )
        gym = gym_result.scalars().first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
            )

        if gym.owner_id != request.owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="You do not have access to this gym",
                error_code="GYM_ACCESS_DENIED",
            )

        # Get or create account details
        result = await db.execute(
            select(AccountDetails).where(AccountDetails.gym_id == request.gym_id)
        )
        account = result.scalars().first()

        if not account:
            # Create new account details record
            account = AccountDetails(
                gym_id=request.gym_id,
                upi_id=request.upi_id or "",
                gst_type=request.gst_type or "",
                gst_number=request.gst_number or "",
                gst_percentage=request.gst_percentage or "18",
            )
            db.add(account)
        else:
            # Update existing record
            if request.upi_id is not None:
                account.upi_id = request.upi_id
            if request.gst_type is not None:
                account.gst_type = request.gst_type
            if request.gst_number is not None:
                account.gst_number = request.gst_number
            if request.gst_percentage is not None:
                account.gst_percentage = request.gst_percentage
            account.updated_at = datetime.now()

        await db.commit()
        await db.refresh(account)

        return {
            "status": 200,
            "message": "UPI and GST details updated successfully",
            "data": {
                "upi_id": account.upi_id or "",
                "gst_type": account.gst_type or "",
                "gst_number": account.gst_number or "",
                "gst_percentage": account.gst_percentage or "18",
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update UPI and GST details",
            error_code="UPDATE_UPI_GST_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(exc)},
        )


@router.get("/payment_details")
async def get_payment_details(
    owner_id: int,
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Get payment/account details for a gym.
    """
    try:
        if not isinstance(owner_id, int) or owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
            )

        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
            )

        # Verify gym belongs to owner
        gym_result = await db.execute(
            select(Gym).where(Gym.gym_id == gym_id)
        )
        gym = gym_result.scalars().first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
            )

        if gym.owner_id != owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="You do not have access to this gym",
                error_code="GYM_ACCESS_DENIED",
            )

        # Get account details
        result = await db.execute(
            select(AccountDetails).where(AccountDetails.gym_id == gym_id)
        )
        account = result.scalars().first()

        # Check for pending edit request
        pending_result = await db.execute(
            select(AccountDetailsEditRequest).where(
                AccountDetailsEditRequest.gym_id == gym_id,
                AccountDetailsEditRequest.query_solved == False
            )
        )
        pending_request = pending_result.scalars().first()

        if account:
            response = {
                "account_id": account.account_id,
                "gym_id": account.gym_id,
                "account_number": account.account_number or "",
                "bank_name": account.bank_name or "",
                "account_ifsccode": account.account_ifsccode or "",
                "account_branch": account.account_branch or "",
                "account_holdername": account.account_holdername or "",
                "upi_id": account.upi_id or "",
                "pan_number": account.pan_number or "",
                "gst_number": account.gst_number or "",
                "gst_type": account.gst_type or "",
                "gst_percentage": account.gst_percentage or "18",
            }
        else:
            response = {
                "account_id": None,
                "gym_id": gym_id,
                "account_number": "",
                "bank_name": "",
                "account_ifsccode": "",
                "account_branch": "",
                "account_holdername": "",
                "upi_id": "",
                "pan_number": "",
                "gst_number": "",
                "gst_type": "",
                "gst_percentage": "18",
            }

        pending_request_data = None
        if pending_request:
            pending_request_data = {
                "id": pending_request.id,
                "requested_time": pending_request.requested_time.isoformat() if pending_request.requested_time else None,
                "old_data": pending_request.old_json,
                "new_data": pending_request.new_json,
            }

        return {
            "status": 200,
            "message": "Payment details fetched successfully",
            "data": response,
            "pending_edit_request": pending_request_data,
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch payment details",
            error_code="PAYMENT_DETAILS_FETCH_ERROR",
            log_data={"gym_id": gym_id, "error": repr(exc)},
        )


@router.post("/payment_details_edit_request")
async def submit_payment_details_edit_request(
    request: PaymentDetailsEditRequest,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Submit a request to edit payment details.
    Changes require admin verification before being applied.
    """
    try:
        if not isinstance(request.owner_id, int) or request.owner_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid owner_id",
                error_code="INVALID_OWNER_ID",
            )

        if not isinstance(request.gym_id, int) or request.gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
            )

        # Verify gym belongs to owner
        gym_result = await db.execute(
            select(Gym).where(Gym.gym_id == request.gym_id)
        )
        gym = gym_result.scalars().first()

        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
            )

        if gym.owner_id != request.owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="You do not have access to this gym",
                error_code="GYM_ACCESS_DENIED",
            )

        # Check if there's already a pending request
        pending_result = await db.execute(
            select(AccountDetailsEditRequest).where(
                AccountDetailsEditRequest.gym_id == request.gym_id,
                AccountDetailsEditRequest.query_solved == False
            )
        )
        pending_request = pending_result.scalars().first()

        if pending_request:
            raise FittbotHTTPException(
                status_code=400,
                detail="You already have a pending edit request. Please wait for it to be processed.",
                error_code="PENDING_REQUEST_EXISTS",
            )

        # Create new edit request
        new_request = AccountDetailsEditRequest(
            gym_id=request.gym_id,
            owner_id=request.owner_id,
            old_json=request.old_data,
            new_json=request.new_data,
            query_solved=False,
            requested_time=datetime.now(),
        )
        db.add(new_request)
        await db.commit()
        await db.refresh(new_request)

        return {
            "status": 200,
            "message": "Edit request submitted successfully. Our team will verify and update your details within 24-48 hours.",
            "data": {
                "request_id": new_request.id,
                "requested_time": new_request.requested_time.isoformat(),
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to submit edit request",
            error_code="SUBMIT_EDIT_REQUEST_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(exc)},
        )


@router.get("/payment_details_edit_request_status")
async def get_payment_details_edit_request_status(
    owner_id: int,
    gym_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    """
    Get the status of pending edit requests for payment details.
    """
    try:
        # Verify gym belongs to owner
        gym_result = await db.execute(
            select(Gym).where(Gym.gym_id == gym_id)
        )
        gym = gym_result.scalars().first()

        if not gym or gym.owner_id != owner_id:
            raise FittbotHTTPException(
                status_code=403,
                detail="Access denied",
                error_code="ACCESS_DENIED",
            )

        # Get pending request
        result = await db.execute(
            select(AccountDetailsEditRequest).where(
                AccountDetailsEditRequest.gym_id == gym_id,
                AccountDetailsEditRequest.query_solved == False
            )
        )
        pending_request = result.scalars().first()

        if not pending_request:
            return {
                "status": 200,
                "has_pending_request": False,
                "message": "No pending edit requests",
                "data": None,
            }

        return {
            "status": 200,
            "has_pending_request": True,
            "message": "You have a pending edit request",
            "data": {
                "request_id": pending_request.id,
                "old_data": pending_request.old_json,
                "new_data": pending_request.new_json,
                "requested_time": pending_request.requested_time.isoformat() if pending_request.requested_time else None,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch edit request status",
            error_code="FETCH_STATUS_ERROR",
            log_data={"gym_id": gym_id, "error": repr(exc)},
        )
