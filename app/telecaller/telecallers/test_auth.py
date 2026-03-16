from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.telecaller.dependencies import get_current_telecaller

router = APIRouter()

@router.get("/test-auth")
async def test_authentication(
    telecaller = Depends(get_current_telecaller),
    db: Session = Depends(get_db)
):
    """Test if authentication is working"""
    return {
        "message": "Authentication successful",
        "telecaller_id": telecaller.id,
        "telecaller_name": telecaller.name,
        "mobile": telecaller.mobile_number
    }