from __future__ import annotations

from datetime import datetime
from typing import List, Dict
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import ReferralGymCode, ReferralGymCash, GymOwner
from app.utils.logging_utils import FittbotHTTPException
from app.utils.referral_code_generator import generate_unique_referral_code


router = APIRouter(prefix="/gym_referral", tags=["Gymowner"])


@router.get("/get")
async def get_referral_data(
    owner_id: int = Query(..., description="Owner identifier"),
    db: Session = Depends(get_db),
):

    try:

        print("owner is",owner_id)

        # Get owner to access name for referral code generation
        owner = db.query(GymOwner).filter(GymOwner.owner_id == owner_id).first()
        if not owner:
            raise FittbotHTTPException(
                status_code=404,
                detail="Owner not found",
                error_code="OWNER_NOT_FOUND"
            )

        referral_code_record = (
            db.query(ReferralGymCode)
            .filter(ReferralGymCode.owner_id == owner_id)
            .first()
        )

        # If referral code doesn't exist, create it
        if not referral_code_record:
            try:
                # Generate referral code using sequential method (same as auth registration)
                gym_referral_code = generate_unique_referral_code(
                    db=db,
                    name=owner.name,
                    user_id=owner_id,
                    method="sequential",
                    table_name="referral_gym_code",
                    max_retries=3,
                )
            except ValueError:
                # Fallback to random method if sequential fails
                gym_referral_code = generate_unique_referral_code(
                    db=db,
                    name=owner.name,
                    method="random",
                    table_name="referral_gym_code",
                    max_retries=5,
                )

            # Create new referral code record
            referral_code_record = ReferralGymCode(
                owner_id=owner_id,
                referral_code=gym_referral_code,
                created_at=datetime.now(),
            )
            db.add(referral_code_record)
            db.commit()
            db.refresh(referral_code_record)

        referral_code = referral_code_record.referral_code
      
        cash_records: List[ReferralGymCash] = (
            db.query(ReferralGymCash)
            .filter(ReferralGymCash.owner_id == owner_id)
            .all()
        )

        month_data: Dict[str, Dict] = defaultdict(lambda: {"cash": 0, "status": "active"})

        for record in cash_records:
            # Handle if record.month is already a date object or a string
            if isinstance(record.month, str):
                try:
                    date_obj = datetime.strptime(record.month, "%Y-%m-%d")
                    month_year = date_obj.strftime("%b-%y")  # Format: Nov-25
                except ValueError:
                    month_year = record.month[:7] if len(record.month) >= 7 else record.month
            else:
                # record.month is already a date object
                month_year = record.month.strftime("%b-%y")  # Format: Nov-25

            month_data[month_year]["cash"] += record.referral_cash
            month_data[month_year]["status"] = record.status

       
        monthly_data = []
        total_cash = 0

        for month_year in sorted(month_data.keys(), reverse=True):
            cash_amount = month_data[month_year]["cash"]
            status = month_data[month_year]["status"]

            monthly_data.append({
                "month_year": month_year,
                "cash": cash_amount,
                "status": status,
            })
            total_cash += cash_amount

        total_count = len(monthly_data)
        data={
                "referral_code": referral_code,
                "monthly_data": monthly_data,
                "total_cash": total_cash,
                "total_count": total_count,
            }
        

        print("dataaa is",data)

        # Return response
        return {
            "status": 200,
            "message": "Referral data fetched successfully",
            "data": {
                "referral_code": referral_code,
                "monthly_data": monthly_data,
                "total_cash": total_cash,
                "total_count": total_count,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch referral data",
            error_code="REFERRAL_DATA_FETCH_FAILED",
            log_data={"owner_id": owner_id, "error": repr(exc)},
        ) from exc
