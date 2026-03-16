from __future__ import annotations
import orjson
from typing import Dict, Any, List, Iterable, Tuple
from urllib.parse import quote_plus

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import select,JSON as SAJSON

from app.models.database import get_db
from app.models.fittbot_models import ClientWorkoutTemplate, QRCode

router = APIRouter(prefix="/workout_template", tags=["workout_template"])

DAYS6 = ["monday","tuesday","wednesday","thursday","friday","saturday"]
DAYS_ALL = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

EXERCISE_IMG_BASE = "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/exercises"


MG_IMG_MALE = {
    "ABS":      "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/ABS_M_NEW.png",
    "Leg":      "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/LEGS_M_NEW.png",
    "Back":     "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/BACK_M_NEW.png",
    "Chest":    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/CHEST_M_NEW.png",
    "Biceps":   "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/BICEPS_M_NEW.png",
    "Cardio":   "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/CARDIO_M_NEW.png",
    "Triceps":  "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/TRICEPS_M_NEW.png",
    "Forearms": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/FOREARM_M_NEW.png",
    "Shoulder": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/MALE/SHOULDERS_M_NEW.png",
}
MG_IMG_FEMALE = {
    "ABS":      "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/ABS_F.png",
    "Leg":      "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/LEGS_F.png",
    "Back":     "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/BACK_F.png",
    "Chest":    "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/CHEST_F.png",
    "Biceps":   "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/BICEPS_F.png",
    "Cardio":   "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/CARDIO_F.png",
    "Triceps":  "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/TRICEPS_F.png",
    "Forearms": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/FOREARM_F.png",
    "Shoulder": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/muscle_group/FEMALE/SHOULDERS_F.png",
}

# Map raw muscle_group values to your canonical keys shown in the example payload
def _canonical_group(mg: str) -> str:
    m = (mg or "").strip()
    # normalize common variants → your canonical keys
    table = {
        "Abs": "ABS", "Core": "Core",
        "Back": "Back",
        "Chest": "Chest",
        "Biceps": "Biceps",
        "Triceps": "Triceps",
        "Forearm": "Forearms", "Forearms": "Forearms",
        "Shoulder": "Shoulder", "Shoulders": "Shoulder",
        "Leg": "Leg", "Legs": "Leg",
        "Quads": "Leg", "Hamstrings": "Leg", "Calves": "Leg", "Glutes": "Leg",
        "Cardio": "Cardio",
    }
    return table.get(m, m or "Other")

def _exercise_img(name: str) -> str:
    # derive image path like your examples (spaces → '+', extension .png)
    return f"{EXERCISE_IMG_BASE}/{quote_plus((name or '').strip())}.png"

class StructurizeAndSaveRequest(BaseModel):
    client_id: int = Field(..., description="Client id")
    # Pass the full LLM template (the one you already save). We read ids from it.
    template: Dict[str, Any]

def _gather_ids(template: Dict[str, Any]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    days = (template or {}).get("days") or {}
    # Use all day keys present in the template, not just DAYS6
    for d in days.keys():
        out[d] = []
        for ex in (days.get(d, {}).get("exercises") or []):
            eid = ex.get("id")
            if isinstance(eid, int):
                out[d].append(eid)
    return out

def _fetch_qr_rows(db: Session, ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    """Fetch exercises from JSON catalog by IDs instead of database"""
    if not ids:
        return {}

    # Load catalog from JSON
    from .exercise_catalog_db import load_catalog
    catalog = load_catalog(db)

    id_map = {}
    for eid in set(ids):
        exercise_data = catalog.get('by_id', {}).get(eid)
        if exercise_data:
            id_map[eid] = exercise_data

    return id_map


def _to_str(x) -> str:
    return "" if x is None else str(x)

def _build_day_payload(day_ids: List[int], id_map: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build the required JSON structure for ONE day with all image paths from JSON:
      {
        "<Group>": {
           "isCardio": bool,
           "exercises": [
              {
                "name": str,
                "gifPath": str,       # Male gif
                "imgPath": str,       # Male image
                "gifPathFemale": str, # Female gif
                "imgPathFemale": str  # Female image
              },
              ...
           ],
           "imagePath": str,
           "isBodyWeight": bool,
           "isMuscleGroup": bool,
           "imagepath_female": str
        },
        ...
      }
    """
    buckets: Dict[str, Dict[str, Any]] = {}

    for eid in (day_ids or []):
        exercise_data = id_map.get(eid)
        if not exercise_data:
            continue

        gkey = _canonical_group(exercise_data.get('muscle_group', ''))

        if gkey not in buckets:
            buckets[gkey] = {
                "isCardio": bool(exercise_data.get("isCardio", False)),
                "exercises": [],
                "imagePath": _to_str(MG_IMG_MALE.get(gkey, "")) or None,
                "isBodyWeight": bool(exercise_data.get("isBodyWeight", False)),
                "isMuscleGroup": True,  # Always true for muscle groups
                "imagepath_female": _to_str(MG_IMG_FEMALE.get(gkey, "")) or None,
            }
        else:
            # Combine booleans (OR)
            buckets[gkey]["isCardio"] = buckets[gkey]["isCardio"] or bool(exercise_data.get("isCardio", False))
            buckets[gkey]["isBodyWeight"] = buckets[gkey]["isBodyWeight"] or bool(exercise_data.get("isBodyWeight", False))

        # Exercise item with ALL image paths from JSON
        name = _to_str(exercise_data.get("name", ""))

        # Get all 4 image paths directly from JSON
        gif_male = exercise_data.get("gifUrl", "")
        img_male = exercise_data.get("imgUrl", "")
        gif_female = exercise_data.get("gifPathFemale", "")
        img_female = exercise_data.get("imgPathFemale", "")

        buckets[gkey]["exercises"].append({
            "name": name,
            "gifPath": gif_male,           # Male gif path from JSON
            "imgPath": img_male,           # Male image path from JSON
            "gifPathFemale": gif_female,   # Female gif path from JSON
            "imgPathFemale": img_female,   # Female image path from JSON
        })

    # Guarantee shape even if no ids
    for gkey, gval in list(buckets.items()):
        if not isinstance(gval.get("exercises"), list):
            gval["exercises"] = []

    return buckets



def _persist_payload(db: Session, client_id: int, day: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Persist payload as a real JSON object if the column is JSON,
    otherwise serialize exactly once to text.
    """
    # Detect column type once
    col = ClientWorkoutTemplate.__table__.c.exercise_data
    is_json_col = isinstance(col.type, SAJSON)

    existing = (
        db.query(ClientWorkoutTemplate)
          .filter(
             ClientWorkoutTemplate.client_id == client_id,
             ClientWorkoutTemplate.template_name == day
          ).first()
    )

    if existing:
        existing.exercise_data = payload if is_json_col else orjson.dumps(payload).decode()
        db.add(existing)
        return {"template_name": day, "exercise_data": payload}
    else:
        rec = ClientWorkoutTemplate(
            client_id     = client_id,
            template_name = day,
            exercise_data = payload if is_json_col else orjson.dumps(payload).decode()
        )
        db.add(rec)
        return {"template_name": day, "exercise_data": payload}

@router.post("/structurize_and_save")
async def structurize_and_save(
    request: StructurizeAndSaveRequest,
    db: Session = Depends(get_db)
):

    if not request.template or not isinstance(request.template, dict):
        raise HTTPException(400, "template is required and must be an object")

    # 1) Collect ids by day and fetch all rows in one shot
    per_day_ids = _gather_ids(request.template)
    all_ids: List[int] = [eid for ids in per_day_ids.values() for eid in ids]
    id_map = _fetch_qr_rows(db, all_ids)

    # 2) Build payload per day and upsert
    results: List[Dict[str, Any]] = []
    template_days = request.template.get("days", {})
    for day_key in per_day_ids.keys():  # Use dynamic day keys instead of fixed DAYS6
        # Get the actual day title from the template instead of using the day key
        day_data = template_days.get(day_key, {})
        day_title = day_data.get('title', day_key.title())  # Fallback to capitalized day key if no title

        payload = _build_day_payload(per_day_ids.get(day_key, []), id_map)
        results.append(_persist_payload(db, request.client_id, day_title, payload))

    # 3) Commit once
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"DB error while saving structured templates: {e}")

    return {"status": 200, "message": "Structured templates saved", "data": results}