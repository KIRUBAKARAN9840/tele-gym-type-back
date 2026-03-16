# JSON-Based Workout Template System

## Overview

This is a complete workout template creation system that loads exercises from `workouts.json` instead of the database. It ensures all image paths (male/female, gif/img) are properly loaded and preserved throughout the workflow.

## Files Created

### 1. `exercise_catalog_json.py`
**Purpose**: Core catalog loader - reads exercises from `workouts.json` and builds lookup structures.

**Key Features**:
- Loads all exercises from `workouts.json` file
- Creates three data structures for fast lookups:
  - `by_id`: Exercise ID → Exercise data
  - `by_muscle`: Muscle group → List of exercise IDs
  - `name_to_id`: Normalized name → Exercise ID
- Thread-safe caching (loads only once)
- Preserves all image paths (gifUrl, imgUrl, gifPathFemale, imgPathFemale)
- Fuzzy name matching for typo handling

**Functions**:
- `load_catalog()` - Loads and caches the exercise catalog
- `id_for_name(name, catalog)` - Finds exercise by name (with fuzzy matching)
- `pick_from_muscles(muscles, catalog, used_ids, n)` - Selects N exercises from muscle groups

---

### 2. `database_exercise_manager_json.py`
**Purpose**: Exercise management and validation using JSON catalog.

**Key Features**:
- Get exercises by muscle group
- Validate exercise names (with fuzzy matching)
- Find similar exercises for typos
- Generate balanced workout templates
- All functions preserve image paths

**Main Class**: `DatabaseExerciseManagerJSON`

**Key Methods**:
- `get_available_exercises_by_muscle(muscle_group)` - Get all exercises for a muscle
- `validate_exercise_exists(exercise_name)` - Check if exercise exists
- `find_similar_exercises(exercise_name, limit=3)` - Find similar exercises
- `select_balanced_exercises(muscle_groups, total=6)` - Select balanced exercises

---

### 3. `workout_llm_helper_json.py`
**Purpose**: Workout template generation using JSON catalog.

**Key Features**:
- Generate complete workout templates from user profiles
- No database dependency - uses JSON catalog only
- Supports different fitness goals (muscle gain, weight loss, strength, endurance)
- Bulk modifications (add/replace exercises)
- Custom muscle split generation

**Main Functions**:
- `llm_generate_template_from_profile_json_only(profile)` - Generate complete template
- `apply_bulk_modifications_json(template, operation, ...)` - Add/replace exercises
- `generate_custom_muscle_split_json(template_names, muscle_groups_per_day)` - Custom splits

---

### 4. `workout_structured_json.py`
**Purpose**: Structure and save workout templates to database with all image paths.

**Key Features**:
- Converts workout templates to structured format
- Includes all image paths (male/female, gif/img)
- Saves to `client_workout_template` table
- FastAPI endpoint: `/workout_template_json/structurize_and_save`

**Endpoint**: `POST /workout_template_json/structurize_and_save`

**Request Body**:
```json
{
  "client_id": 123,
  "template": {
    "name": "5-Day Program",
    "goal": "muscle gain",
    "days": {
      "chest_day": {
        "title": "Chest Day",
        "muscle_groups": ["chest"],
        "exercises": [
          {
            "id": 102,
            "name": "Dumbbell Flat Bench Press",
            "sets": 3,
            "reps": 10,
            "gifUrl": "https://...",
            "imgUrl": "https://...",
            "gifPathFemale": "https://...",
            "imgPathFemale": "https://..."
          }
        ]
      }
    }
  }
}
```

---

## How It Works

### 1. **Loading Exercises** (exercise_catalog_json.py)

```python
from exercise_catalog_json import load_catalog

# Load catalog (cached after first call)
catalog = load_catalog()

# Access exercises
print(f"Total exercises: {len(catalog['by_id'])}")
print(f"Muscle groups: {list(catalog['by_muscle'].keys())}")

# Get exercise by ID
exercise = catalog['by_id'][102]
print(f"Name: {exercise['name']}")
print(f"Male Gif: {exercise['gifUrl']}")
print(f"Female Gif: {exercise['gifPathFemale']}")
```

### 2. **Getting Exercises by Muscle Group**

```python
from database_exercise_manager_json import DatabaseExerciseManagerJSON

# Get all chest exercises
chest_exercises = DatabaseExerciseManagerJSON.get_available_exercises_by_muscle('chest')

for ex in chest_exercises:
    print(f"{ex['name']}")
    print(f"  Male Gif: {ex['gifUrl']}")
    print(f"  Female Gif: {ex['gifPathFemale']}")
    print(f"  Male Img: {ex['imgUrl']}")
    print(f"  Female Img: {ex['imgPathFemale']}")
```

### 3. **Generating Workout Template**

```python
from workout_llm_helper_json import llm_generate_template_from_profile_json_only

# Define user profile
profile = {
    "template_names": ["Chest Day", "Back Day", "Leg Day"],
    "client_goal": "muscle gain"
}

# Generate template (NO DATABASE REQUIRED!)
template, message = llm_generate_template_from_profile_json_only(profile)

# Template contains:
# - Full exercise details with all image paths
# - Sets/reps based on exercise type
# - Muscle group assignments
print(template)
```

### 4. **Saving to Database**

```python
# Use the FastAPI endpoint
import requests

response = requests.post(
    "http://your-api/workout_template_json/structurize_and_save",
    json={
        "client_id": 123,
        "template": template  # Template from step 3
    }
)

# Template is saved with all image paths preserved
```

---

## Image Path Structure

Each exercise in the JSON has **4 image paths**:

```python
{
  "name": "Dumbbell Flat Bench Press",
  "gifUrl": "https://fittbot-uploads.s3.../gif_male/flatdumbbellchestpress.gif",
  "imgUrl": "https://fittbot-uploads.s3.../img_male/flatdumbellchestpress.png",
  "gifPathFemale": "https://fittbot-uploads.s3.../gif_female/dumbellflatbenchpress.gif",
  "imgPathFemale": "https://fittbot-uploads.s3.../img_female/dumbellflatbenchpress.png"
}
```

All these paths are:
1. ✅ Loaded from `workouts.json`
2. ✅ Preserved during template generation
3. ✅ Included in structured payload
4. ✅ Saved to database

---

## Testing

Run the test script to verify everything works:

```bash
cd /path/to/chatbot_services
python3 test_json_workout.py
```

**Tests Include**:
1. ✅ Load catalog from JSON
2. ✅ Get exercises by muscle group
3. ✅ Generate complete workout template
4. ✅ Exercise validation and fuzzy matching
5. ✅ Structured payload with all image paths

---

## Key Differences from Database Version

| Feature | Database Version | JSON Version |
|---------|-----------------|--------------|
| Data Source | `qr_code` table | `workouts.json` file |
| Requires DB | Yes | No |
| Image Paths | 2 (gifUrl, imgUrl) | 4 (male/female gif/img) |
| Loading Speed | Database query | File read (cached) |
| Portability | Needs DB connection | Standalone |
| Image Path Issues | May have missing paths | All paths from JSON |

---

## Advantages of JSON System

1. **No Database Dependency** - Works without database connection
2. **All Image Paths** - Includes male/female gif/img paths
3. **Faster Loading** - File read is faster than DB query
4. **Portable** - Can work in any environment with the JSON file
5. **Easy Testing** - No need to set up database for tests
6. **Consistent Data** - JSON file is the single source of truth

---

## Integration with Existing System

To integrate with your existing workout template chatbot:

### Option 1: Replace Database Version
```python
# OLD (database version)
from .exercise_catalog_db import load_catalog
from .database_exercise_manager import DatabaseExerciseManager
from .workout_llm_helper import llm_generate_template_from_profile_database_only

catalog = load_catalog(db)  # Needs database

# NEW (JSON version)
from .exercise_catalog_json import load_catalog
from .database_exercise_manager_json import DatabaseExerciseManagerJSON
from .workout_llm_helper_json import llm_generate_template_from_profile_json_only

catalog = load_catalog()  # No database needed!
```

### Option 2: Use Both (Fallback)
```python
try:
    # Try database version first
    from .exercise_catalog_db import load_catalog
    catalog = load_catalog(db)
except Exception as e:
    # Fall back to JSON version
    from .exercise_catalog_json import load_catalog
    catalog = load_catalog()
```

---

## Muscle Groups Supported

From `workouts.json`:
- ABS
- Back
- Biceps
- Cardio
- Chest
- Cycling
- Forearms
- Leg/Legs
- Shoulders
- Treadmill
- Triceps

All muscle groups have multiple exercises with full image paths.

---

## Example: Complete Workflow

```python
# 1. Load catalog (one-time, cached)
from exercise_catalog_json import load_catalog
catalog = load_catalog()
print(f"✅ Loaded {len(catalog['by_id'])} exercises")

# 2. Generate workout template
from workout_llm_helper_json import llm_generate_template_from_profile_json_only

profile = {
    "template_names": ["Upper Body", "Lower Body", "Full Body"],
    "client_goal": "muscle gain"
}

template, msg = llm_generate_template_from_profile_json_only(profile)
print(f"✅ {msg}")

# 3. Verify image paths are present
for day_key, day_data in template['days'].items():
    print(f"\n{day_data['title']}:")
    for ex in day_data['exercises']:
        has_all_paths = all([
            ex.get('gifUrl'),
            ex.get('imgUrl'),
            ex.get('gifPathFemale'),
            ex.get('imgPathFemale')
        ])
        print(f"  {ex['name']}: {'✅ All paths' if has_all_paths else '❌ Missing paths'}")

# 4. Save to database (via API endpoint)
# POST /workout_template_json/structurize_and_save
# All image paths are preserved in the database!
```

---

## Troubleshooting

### "workouts.json not found"
- Ensure `workouts.json` is in the same directory as `exercise_catalog_json.py`
- Check file permissions

### "No exercises loaded"
- Verify `workouts.json` is valid JSON
- Check the JSON structure matches expected format

### "Import errors"
- Both relative and absolute imports are supported
- Use `from .exercise_catalog_json import ...` in modules
- Use `from exercise_catalog_json import ...` in scripts

---

## Summary

✅ **Created**: Complete JSON-based workout system
✅ **No Database**: Works without database connection
✅ **All Image Paths**: Includes male/female gif/img paths
✅ **Tested**: All tests passing
✅ **Production Ready**: Can be integrated immediately

The system mirrors the database version exactly but uses `workouts.json` as the data source, ensuring all image paths are properly loaded and preserved throughout the workout template creation process.
