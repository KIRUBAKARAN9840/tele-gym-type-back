# app/api/v1/devices/smart_watch.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import SmartWatch
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/smartwatch",tags=["Smart Watch"])


@router.get("/get_interest")
async def get_smart_watch_intrest(client_id: int, db: Session = Depends(get_db)):
    try:
        data = db.query(SmartWatch).filter(SmartWatch.client_id == client_id).first()
        interest = data.interested if data else False

        return {
            "status": 200,
            "message": "Data retrived successfully",
            "data": interest,
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        # Keep logic unchanged; normalize error with FittbotHTTPException
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occured, {str(e)}",
            error_code="SMART_WATCH_GET_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )


class InterestRequest(BaseModel):
    client_id: int
    interest: bool


@router.post("/show_interest")
async def add_interest(request: InterestRequest, db: Session = Depends(get_db)):
    try:
        client_id = request.client_id
        interest = request.interest

        existing = (
            db.query(SmartWatch).filter(SmartWatch.client_id == client_id).first()
        )
        if existing:
            # Preserve original behavior: set to True regardless of incoming value
            existing.interested = True
        else:
            data = SmartWatch(client_id=client_id, interested=interest)
            db.add(data)

        db.commit()

        return {"status": 200, "message": "Thank you for showing the interest"}
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occured, {str(e)}",
            error_code="SMART_WATCH_POST_ERROR",
            log_data={
                "client_id": request.client_id,
                "interest": request.interest,
                "error": str(e),
            },
        )
