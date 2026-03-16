from __future__ import annotations
import orjson
from typing import Dict, Any, List, Iterable

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import JSON as SAJSON

from app.models.database import get_db
from app.models.fittbot_models import ClientWorkoutTemplate

from .exercise_catalog_json import load_catalog

router = APIRouter(prefix="/workout_template_json", tags=["workout_template_json"])

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

# Map raw muscle_group values to canonical keys
def _canonical_group(mg: str) -> str:
    m = (mg or "").strip()
    table = {
        "Abs": "ABS", "Core": "ABS", "ABS": "ABS",
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

class StructurizeAndSaveRequestJSON(BaseModel):
    client_id: int = Field(..., description="Client id")
    template: Dict[str, Any]

def _gather_ids(template: Dict[str, Any]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    days = (template or {}).get("days") or {}
    for d in days.keys():
        out[d] = []
        for ex in (days.get(d, {}).get("exercises") or []):
            eid = ex.get("id")
            if isinstance(eid, int):
                out[d].append(eid)
    return out

def _fetch_catalog_exercises(ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    """Fetch exercises from JSON catalog by IDs"""
    if not ids:
        return {}

    catalog = load_catalog()
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
    Build the required JSON structure for ONE day with all image paths (male/female):
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
                "imagePath": _to_str(MG_IMG_MALE.get(gkey, "")),
                "isBodyWeight": bool(exercise_data.get("isBodyWeight", False)),
                "isMuscleGroup": True,
                "imagepath_female": _to_str(MG_IMG_FEMALE.get(gkey, "")),
            }
        else:
            # Combine booleans (OR)
            buckets[gkey]["isCardio"] = buckets[gkey]["isCardio"] or bool(exercise_data.get("isCardio", False))
            buckets[gkey]["isBodyWeight"] = buckets[gkey]["isBodyWeight"] or bool(exercise_data.get("isBodyWeight", False))

        # Exercise item with ALL image paths
        name = _to_str(exercise_data.get("name", ""))

        # Male paths
        gif_male = _to_str(exercise_data.get("gifUrl", "")) or None
        img_male = _to_str(exercise_data.get("imgUrl", "")) or None

        # Female paths
        gif_female = _to_str(exercise_data.get("gifPathFemale", "")) or None
        img_female = _to_str(exercise_data.get("imgPathFemale", "")) or None

        buckets[gkey]["exercises"].append({
            "name": name,
            "gifPath": gif_male,           # Male gif path
            "imgPath": img_male,           # Male image path
            "gifPathFemale": gif_female,   # Female gif path
            "imgPathFemale": img_female,   # Female image path
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
async def structurize_and_save_json(
    request: StructurizeAndSaveRequestJSON,
    db: Session = Depends(get_db)
):
    """
    Structurize and save workout template using JSON catalog
    Ensures all image paths (male/female, gif/img) are included
    """

    if not request.template or not isinstance(request.template, dict):
        raise HTTPException(400, "template is required and must be an object")

    # 1) Collect ids by day and fetch all exercises from JSON catalog
    per_day_ids = _gather_ids(request.template)
    all_ids: List[int] = [eid for ids in per_day_ids.values() for eid in ids]
    id_map = _fetch_catalog_exercises(all_ids)

    # 2) Build payload per day and upsert
    results: List[Dict[str, Any]] = []
    template_days = request.template.get("days", {})

    for day_key in per_day_ids.keys():
        # Get the actual day title from the template
        day_data = template_days.get(day_key, {})
        day_title = day_data.get('title', day_key.title())

        payload = _build_day_payload(per_day_ids.get(day_key, []), id_map)
        results.append(_persist_payload(db, request.client_id, day_title, payload))

    # 3) Commit once
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"DB error while saving structured templates: {e}")

    return {"status": 200, "message": "Structured templates saved (JSON-based)", "data": results}
