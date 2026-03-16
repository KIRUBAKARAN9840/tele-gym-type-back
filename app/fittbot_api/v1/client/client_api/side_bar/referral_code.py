from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.fittbot_models import ReferralCode
from app.utils.logging_utils import FittbotHTTPException


router = APIRouter(prefix="/referral", tags=["Referral Code"])


@router.get("/get")
async def get_referral_code(client_id: int, db: Session = Depends(get_db)):

    try:

        referral_record = db.query(ReferralCode).filter(
            ReferralCode.client_id == client_id
        ).first()

        if not referral_record:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"No referral code found for client_id {client_id}",
                error_code="REFERRAL_CODE_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        return {
            "status": 200,
            "referral_code": referral_record.referral_code

        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching referral code: {str(e)}",
            error_code="GET_REFERRAL_CODE_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )
