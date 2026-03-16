import json
from datetime import datetime, date, time as dtime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import EquipmentWorkout, ActualWorkout
from app.utils.logging_utils import FittbotHTTPException


router = APIRouter(prefix="/equipment", tags=["Equipment"])


def _load_equipment_catalog(db: Session) -> Dict[str, Any]:
    """Fetch and merge equipment definitions stored in EquipmentWorkout."""
    records = db.query(EquipmentWorkout).order_by(EquipmentWorkout.id.asc()).all()
    if not records:
        raise FittbotHTTPException(
            status_code=404,
            detail="Equipment catalog not configured.",
            error_code="EQUIPMENT_NOT_CONFIGURED",
        )

    catalog: Dict[str, Any] = {}
    for record in records:
        payload = record.equipment
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                continue
        if isinstance(payload, dict):
            catalog.update(payload)

    if not catalog:
        raise FittbotHTTPException(
            status_code=404,
            detail="Equipment catalog is empty.",
            error_code="EQUIPMENT_CATALOG_EMPTY",
        )

    return catalog


@router.get("/catalog")
async def get_equipment_catalog(db: Session = Depends(get_db)):
    """Return all equipment names with their images in a lightweight list."""
    try:
        catalog = _load_equipment_catalog(db)
        equipment_list = _enumerate_equipment(catalog)

        if not equipment_list:
            raise FittbotHTTPException(
                status_code=404,
                detail="No equipment entries available.",
                error_code="EQUIPMENT_LIST_EMPTY",
            )

        return {"status": 200, "data": equipment_list}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to load equipment catalog.",
            error_code="EQUIPMENT_LIST_ERROR",
            log_data={"error": str(exc)},
        )


@router.get("/exercises")
async def get_equipment_exercises(
    equipment_name: str = Query(..., min_length=1, description="Equipment name to fetch exercises for"),
    db: Session = Depends(get_db),
):
    """Return the exercise list for a specific equipment."""
    try:
        catalog = _load_equipment_catalog(db)
        matched_key = next(
            (key for key in catalog.keys() if key.lower() == equipment_name.lower()),
            None,
        )

        if not matched_key:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Equipment '{equipment_name}' not found.",
                error_code="EQUIPMENT_NOT_FOUND",
                log_data={"equipment_name": equipment_name},
            )

        exercises = _enumerate_exercises_from_catalog_entry(catalog.get(matched_key, {}))

        print("exercises", exercises)

        return {"status": 200, "data": exercises}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to load equipment exercises.",
            error_code="EQUIPMENT_EXERCISE_ERROR",
            log_data={"equipment_name": equipment_name, "error": str(exc)},
        )


@router.get("/history")
async def get_equipment_exercises_with_history(
    equipment_name: str = Query(..., min_length=1, description="Equipment name to fetch exercises for"),
    client_id: int = Query(..., description="Client ID to fetch last performed exercise data"),
    db: Session = Depends(get_db),
):

    try:
        catalog = _load_equipment_catalog(db)
        matched_key = next(
            (key for key in catalog.keys() if key.lower() == equipment_name.lower()),
            None,
        )

        if not matched_key:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Equipment '{equipment_name}' not found.",
                error_code="EQUIPMENT_NOT_FOUND",
                log_data={"equipment_name": equipment_name},
            )

        exercises = _enumerate_exercises_from_catalog_entry(catalog.get(matched_key, {}))

        if not exercises:
            return {"status": 200, "data": []}

        history_lookup = _build_exercise_history(db, client_id, {item["name"].lower(): item["name"] for item in exercises})

        filtered_exercises: List[Dict[str, Any]] = []
        for item in exercises:
            lookup_key = item["name"].lower()
            history = history_lookup.get(lookup_key)
            if history:
                item["exercise_data"] = history
                filtered_exercises.append(item)

        print("exercises are",filtered_exercises)

        return {"status": 200, "data": filtered_exercises}

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to load equipment exercise history.",
            error_code="EQUIPMENT_EXERCISE_HISTORY_ERROR",
            log_data={"equipment_name": equipment_name, "client_id": client_id, "error": str(exc)},
        )


def _enumerate_equipment(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    equipment_list: List[Dict[str, Any]] = []
    for idx, (name, meta) in enumerate(
        sorted(catalog.items(), key=lambda item: item[0].lower()),
        start=1
    ):
        image_url = meta.get("image") if isinstance(meta, dict) else None
        equipment_list.append({
            "id": idx,
            "name": name,
            "image": image_url,
        })
    return equipment_list


def _enumerate_exercises_from_catalog_entry(entry: Any) -> List[Dict[str, Any]]:
    exercises = entry.get("exercises") if isinstance(entry, dict) else []
    if not isinstance(exercises, list):
        exercises = []

    enumerated: List[Dict[str, Any]] = []
    for idx, exercise in enumerate(exercises, start=1):
        if isinstance(exercise, dict):
            item = exercise.copy()
        else:
            item = {"name": str(exercise)}
        item.setdefault("name", item.get("label", f"Exercise {idx}"))
        item["id"] = idx
        enumerated.append(item)
    return enumerated


def _parse_iso_datetime(value: Optional[str], fallback: datetime) -> datetime:
    if not value:
        return fallback

    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return fallback


from datetime import datetime, date, time as dtime, timezone
from typing import Dict, Any, Optional
import json

# ---- helpers ----

def _to_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Return a timezone-aware UTC datetime. If naive, assume UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _parse_iso_datetime(value: Optional[str], fallback_base: datetime) -> datetime:
    """
    Parse an ISO datetime string into an aware UTC datetime.
    - If `value` is None/empty or unparsable, returns `fallback_base`.
    - If parsed datetime is naive, assume UTC.
    """
    if not value:
        return fallback_base
    try:
        # Accept both "...Z" and with offsets
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        dt = _to_aware_utc(dt)
        return dt if dt is not None else fallback_base
    except Exception:
        return fallback_base

# ---- patched core ----

def _build_exercise_history(
    db: Session,
    client_id: int,
    exercise_name_lookup: Dict[str, str]
) -> Dict[str, Dict[str, Any]]:
    if not exercise_name_lookup:
        return {}

    records = (
        db.query(ActualWorkout)
        .filter(ActualWorkout.client_id == client_id)
        .order_by(ActualWorkout.date.asc())
        .all()
    )

    if not records:
        return {}

    history: Dict[str, Dict[str, Any]] = {}

    for record in records:
        record_date: date = record.date
        # Make the base timestamp AWARE (UTC) to avoid naive/aware comparisons
        base_timestamp = datetime.combine(record_date, dtime.min).replace(tzinfo=timezone.utc)

        raw_details = record.workout_details
        if not raw_details:
            continue

        details = raw_details
        if isinstance(raw_details, str):
            try:
                details = json.loads(raw_details)
            except json.JSONDecodeError:
                continue

        if not isinstance(details, list):
            continue

        for entry in details:
            if not isinstance(entry, dict):
                continue

            for muscle_group, exercises in entry.items():
                if not isinstance(exercises, list):
                    continue

                for exercise in exercises:
                    if not isinstance(exercise, dict):
                        continue

                    name = exercise.get("name")
                    if not name:
                        continue

                    name_key = name.lower()
                    if name_key not in exercise_name_lookup:
                        continue

                    sets = exercise.get("sets") or []
                    latest_ts = base_timestamp  # start with an aware baseline

                    for set_entry in sets:
                        if not isinstance(set_entry, dict):
                            continue

                        end_ts = _parse_iso_datetime(
                            set_entry.get("endTime") or set_entry.get("end_time"),
                            base_timestamp
                        )
                        start_ts = _parse_iso_datetime(
                            set_entry.get("startTime") or set_entry.get("start_time"),
                            base_timestamp
                        )

                        # All are aware UTC now, safe to compare
                        candidate_ts = max(end_ts, start_ts, latest_ts)
                        if candidate_ts > latest_ts:
                            latest_ts = candidate_ts

                    previous = history.get(name_key)
                    if not previous or latest_ts > previous["_timestamp"]:
                        history[name_key] = {
                            "exercise": exercise_name_lookup[name_key],
                            "muscle_group": muscle_group,
                            "last_performed_at": latest_ts.isoformat(),  # UTC ISO
                            "sets": sets,
                            "_timestamp": latest_ts,  # internal, aware UTC
                        }

    # strip internal field
    for value in history.values():
        value.pop("_timestamp", None)

    return history
