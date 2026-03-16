from __future__ import annotations
import difflib, threading, json, os
from typing import Dict, Any, List, Set, Optional

_LOCK = threading.Lock()
_CACHE: Optional[Dict[str, Any]] = None  # process cache

def _norm(s: str) -> str:
    s = (s or "").lower()
    return "".join(ch for ch in s if ch.isalnum() or ch.isspace()).strip()

# Light synonyms (so "Core" maps to "Abs", etc.)
_MUSCLE_SYNONYM = {
    "core": "abs",
    "abdominals": "abs",
    "mid-back": "back",
    "upper back": "back",
    "lower back": "back",
    "quadriceps": "quads",
    "quads": "quads",
    "hamstring": "hamstrings",
    "hamstrings": "hamstrings",
    "glute": "glutes",
    "glutes": "glutes",
    "calf": "calves",
    "calves": "calves",
    "bicep": "biceps",
    "biceps": "biceps",
    "tricep": "triceps",
    "triceps": "triceps",
    "chest": "chest",
    "shoulder": "shoulders",
    "shoulders": "shoulders",
    "back": "back",
    "legs": "legs",
    "leg": "legs",
    "abs": "abs",
    "cardio": "cardio",
    "forearm": "forearms",
    "forearms": "forearms"
}

def _muscle_key(m: str) -> str:
    k = (m or "").strip().lower()
    return _MUSCLE_SYNONYM.get(k, k)

def load_catalog() -> Dict[str, Any]:
    """
    Load exercise catalog from workouts.json file. Builds catalog structure with:
      - by_id: Map of exercise ID to exercise data
      - by_muscle: Map of muscle group to list of exercise IDs
      - name_to_id: Map of normalized exercise name to ID
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    with _LOCK:
        if _CACHE is not None:
            return _CACHE

        by_id: Dict[int, Dict[str, Any]] = {}
        by_muscle: Dict[str, List[int]] = {}
        name_to_id: Dict[str, int] = {}

        try:
            # Get the path to workouts.json
            current_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(current_dir, "workouts.json")

            # Load from workouts.json file
            with open(json_path, 'r', encoding='utf-8') as f:
                workout_data = json.load(f)

            # Generate exercises from JSON structure
            eid_counter = 1
            for muscle_group, group_data in workout_data.items():
                if not isinstance(group_data, dict) or "exercises" not in group_data:
                    continue

                isCardio = group_data.get("isCardio", False)
                # Infer isBodyWeight based on muscle group or exercise type
                # Cardio and bodyweight exercises typically don't use weights
                isBodyWeight = isCardio or muscle_group.upper() in ["ABS", "CARDIO"]

                for exercise in group_data.get("exercises", []):
                    name = exercise.get("name", "").strip()
                    if not name:
                        continue

                    # Get all image paths from JSON
                    gifPath = exercise.get("gifPath", "").strip()
                    imgPath = exercise.get("imgPath", "").strip()
                    gifPathFemale = exercise.get("gifPathFemale", "").strip()
                    imgPathFemale = exercise.get("imgPathFemale", "").strip()

                    ex = {
                        "id": eid_counter,
                        "name": name,
                        "muscle_group": muscle_group,
                        "gifUrl": gifPath,  # Male gif
                        "imgUrl": imgPath,  # Male image
                        "gifPathFemale": gifPathFemale,  # Female gif
                        "imgPathFemale": imgPathFemale,  # Female image
                        "isCardio": isCardio,
                        "isBodyWeight": isBodyWeight,
                    }
                    by_id[eid_counter] = ex
                    by_muscle.setdefault(_muscle_key(muscle_group), []).append(eid_counter)
                    name_to_id[_norm(name)] = eid_counter

                    eid_counter += 1

            _CACHE = {"by_id": by_id, "by_muscle": by_muscle, "name_to_id": name_to_id}
            print(f"✅ Loaded {len(by_id)} exercises from workouts.json")
            return _CACHE

        except Exception as e:
            print(f"🚨 Error loading catalog from workouts.json: {e}")
            # Return a comprehensive fallback catalog with common exercises
            _CACHE = {
                "by_id": {
                    1: {"id": 1, "name": "Push-ups", "muscle_group": "Chest", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": True},
                    2: {"id": 2, "name": "Squats", "muscle_group": "Leg", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": True},
                    3: {"id": 3, "name": "Plank", "muscle_group": "ABS", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": True},
                    4: {"id": 4, "name": "Lunges", "muscle_group": "Leg", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": True},
                    5: {"id": 5, "name": "Pull-ups", "muscle_group": "Back", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": True},
                    6: {"id": 6, "name": "Burpees", "muscle_group": "Cardio", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": True, "isBodyWeight": True},
                    7: {"id": 7, "name": "Bench Press", "muscle_group": "Chest", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": False},
                    8: {"id": 8, "name": "Deadlift", "muscle_group": "Back", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": False},
                    9: {"id": 9, "name": "Shoulder Press", "muscle_group": "Shoulder", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": False},
                    10: {"id": 10, "name": "Bicep Curls", "muscle_group": "Biceps", "gifUrl": "", "imgUrl": "", "gifPathFemale": "", "imgPathFemale": "", "isCardio": False, "isBodyWeight": False},
                },
                "by_muscle": {
                    "chest": [1, 7], "legs": [2, 4], "abs": [3], "back": [5, 8],
                    "cardio": [6], "shoulders": [9], "biceps": [10]
                },
                "name_to_id": {
                    "pushups": 1, "push ups": 1, "squats": 2, "plank": 3, "lunges": 4, "pullups": 5, "pull ups": 5,
                    "burpees": 6, "bench press": 7, "deadlift": 8, "shoulder press": 9, "bicep curls": 10
                }
            }
            return _CACHE

def id_for_name(name: str, catalog: Dict[str, Any]) -> Optional[int]:
    key = _norm(name)
    nid = catalog["name_to_id"].get(key)
    if nid is not None:
        return nid
    # fuzzy
    keys = list(catalog["name_to_id"].keys())
    best = difflib.get_close_matches(key, keys, n=1, cutoff=0.77)
    return catalog["name_to_id"][best[0]] if best else None

def pick_from_muscles(muscles: List[str], catalog: Dict[str, Any], used_ids: Set[int], n: int) -> List[int]:
    pool: List[int] = []
    for m in (muscles or []):
        pool += catalog["by_muscle"].get(_muscle_key(m), [])
    # unique preserve order
    seen: Set[int] = set()
    pool = [eid for eid in pool if not (eid in seen or seen.add(eid))]

    picked: List[int] = []
    for eid in pool:
        if eid not in used_ids:
            picked.append(eid)
            if len(picked) >= n:
                break
    if len(picked) < n:
        # global fallback (rare)
        for eid in catalog["by_id"].keys():
            if eid not in used_ids and eid not in picked:
                picked.append(eid)
                if len(picked) >= n:
                    break
    return picked
