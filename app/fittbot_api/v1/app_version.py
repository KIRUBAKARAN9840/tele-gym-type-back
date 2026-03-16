from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.fittbot_models import AppVersion, Client


router = APIRouter(prefix="/app", tags=["App Version"])


def _parse_version(value: Optional[str]) -> list[int]:
    if not value:
        return [0]
    parts = []
    for segment in str(value).split("."):
        try:
            parts.append(int(segment))
        except ValueError:
            # Strip non numeric suffixes (e.g. "1a")
            digits = "".join(ch for ch in segment if ch.isdigit())
            parts.append(int(digits) if digits else 0)
    return parts or [0]


def _is_version_lower(current: str, minimum: str) -> bool:
    current_parts = _parse_version(current)
    minimum_parts = _parse_version(minimum)
    length = max(len(current_parts), len(minimum_parts))
    current_parts += [0] * (length - len(current_parts))
    minimum_parts += [0] * (length - len(minimum_parts))
    return current_parts < minimum_parts


@router.get("/version")
async def get_version_status(
    
    current_version: str = Query(..., description="Current app version"),
    platform: Optional[str] = Query(None, description="Platform identifier (android/ios)"),
    app: Optional[str]=Query(None, description="Platform identifier (android/ios)"),
    db: Session = Depends(get_db),
):



    if app=="business":
      

        # Debug: Check all records in the table
        all_records = db.query(AppVersion).all()
        record= db.query(AppVersion).filter(AppVersion.platform==app).first()

   
        if not record:

         
            return {
                "status":200,
                "force_update": False
            }
        
        needs_min_update = False
        if record.min_supported_version:
            needs_min_update = _is_version_lower(
                current_version, record.min_supported_version
            )



        force_update = record.force_update or needs_min_update





        response = {
            "status":200,
            "force_update": bool(force_update)
        }

        return response

    app="fittbot"
                
    record= db.query(AppVersion).filter(AppVersion.platform==app).first()
    
    if not record:
        return {
            "status":200,
            "force_update": False
        }
    

    needs_min_update = False
    if record.min_supported_version:
        needs_min_update = _is_version_lower(
            current_version, record.min_supported_version
        )

    force_update = record.force_update or needs_min_update



    response = {
        "status":200,
        "force_update": bool(force_update)
    }

    return response
