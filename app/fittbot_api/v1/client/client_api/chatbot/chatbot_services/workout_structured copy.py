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
    for d in DAYS6:
        out[d] = []
        for ex in (days.get(d, {}).get("exercises") or []):
            eid = ex.get("id")
            if isinstance(eid, int):
                out[d].append(eid)
    return out

def _fetch_qr_rows(db: Session, ids: Iterable[int]) -> Dict[int, QRCode]:
    if not ids:
        return {}
    q = db.execute(
        select(QRCode).where(QRCode.id.in_(list(set(ids))))
    ).scalars().all()
    return {r.id: r for r in q}


def _to_str(x) -> str:
    return "" if x is None else str(x)

def _build_day_payload(day_ids: List[int], id_map: Dict[int, "QRCode"]) -> Dict[str, Any]:
    """
    Build the required JSON structure for ONE day (a plain Python dict):
      {
        "<Group>": {
           "isCardio": bool,
           "exercises": [{"name": str, "gifPath": str, "imgPath": str}, ...],
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
        row = id_map.get(eid)
        if not row:
            continue

        gkey = _canonical_group(row.muscle_group)

        if gkey not in buckets:
            buckets[gkey] = {
                "isCardio": bool(getattr(row, "isCardio", False)),
                "exercises": [],                           # ALWAYS an array
                "imagePath": _to_str(MG_IMG_MALE.get(gkey, "")),
                "isBodyWeight": bool(getattr(row, "isBodyWeight", False)),
                "isMuscleGroup": bool(getattr(row, "isMuscleGroup", True)),
                "imagepath_female": _to_str(MG_IMG_FEMALE.get(gkey, "")),
            }
        else:
            # combine booleans (OR) so group flags remain true if any exercise is true
            buckets[gkey]["isCardio"]      = buckets[gkey]["isCardio"] or bool(getattr(row, "isCardio", False))
            buckets[gkey]["isBodyWeight"]  = buckets[gkey]["isBodyWeight"] or bool(getattr(row, "isBodyWeight", False))
            buckets[gkey]["isMuscleGroup"] = buckets[gkey]["isMuscleGroup"] or bool(getattr(row, "isMuscleGroup", True))

        # exercise item (strings only; avoid None)
        name = _to_str(getattr(row, "exercises", ""))
        gif  = _to_str(getattr(row, "gifUrl", "")) or None   # keep null if truly empty
        img  = _exercise_img(name)                           # or use row.imgUrl if you add it

        buckets[gkey]["exercises"].append({
            "name":    name,
            "gifPath": gif,
            "imgPath": img,
        })

    # guarantee shape even if no ids (prevents undefined access in UI)
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
    """
    Takes the saved template (with exercise IDs),
    creates ONE row per day (monday..saturday) in client_workout_template,
    with exercise_data shaped exactly like the provided example.
    If (client_id, day) exists → overwrite/update.
    """
    if not request.template or not isinstance(request.template, dict):
        raise HTTPException(400, "template is required and must be an object")

    # 1) Collect ids by day and fetch all rows in one shot
    per_day_ids = _gather_ids(request.template)
    all_ids: List[int] = [eid for ids in per_day_ids.values() for eid in ids]
    id_map = _fetch_qr_rows(db, all_ids)

    # 2) Build payload per day and upsert
    results: List[Dict[str, Any]] = []
    for day in DAYS6:
        payload = _build_day_payload(per_day_ids.get(day, []), id_map)
        results.append(_persist_payload(db, request.client_id, day, payload))

    # 3) Commit once
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"DB error while saving structured templates: {e}")

    return {"status": 200, "message": "Structured templates saved", "data": results}