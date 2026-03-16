from __future__ import annotations
from typing import Dict, Any, Tuple, List

try:
    from .exercise_catalog_json import load_catalog, id_for_name, pick_from_muscles
    from .database_exercise_manager_json import DatabaseExerciseManagerJSON
except ImportError:
    from exercise_catalog_json import load_catalog, id_for_name, pick_from_muscles
    from database_exercise_manager_json import DatabaseExerciseManagerJSON


def llm_generate_template_from_profile_json_only(profile: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Generate template using ONLY JSON exercises - new JSON-first approach
    No database connection required - loads from workouts.json
    """

    try:
        # Extract profile information
        template_names = profile.get("template_names", ["Day 1", "Day 2", "Day 3"])
        template_count = len(template_names)
        goal = profile.get("client_goal", "muscle gain")

        # Define muscle group mappings for different goals
        muscle_group_programs = {
            "muscle gain": ["chest", "back", "legs", "shoulders", "biceps", "triceps"],
            "weight loss": ["cardio", "legs", "abs", "back"],
            "strength": ["chest", "back", "legs", "shoulders"],
            "endurance": ["cardio", "legs", "abs", "back"]
        }

        # Get appropriate muscle groups for the goal
        target_muscle_groups = muscle_group_programs.get(goal.lower(), ["chest", "back", "legs", "shoulders"])

        # Create template structure
        template = {
            "name": f"{template_count}-Day Program",
            "goal": goal,
            "days": {}
        }

        # Generate exercises for each day
        for i, day_name in enumerate(template_names):
            day_key = day_name.lower().replace(' ', '_').replace('-', '_')

            # Assign muscle groups cyclically
            assigned_muscle_groups = []
            exercises_per_day = 6  # Always 6 exercises per day for optimal workout volume

            if template_count <= len(target_muscle_groups):
                # One muscle group per day
                muscle_group = target_muscle_groups[i % len(target_muscle_groups)]
                assigned_muscle_groups = [muscle_group]
            else:
                # Multiple muscle groups per day
                groups_per_day = max(1, len(target_muscle_groups) // template_count)
                start_idx = (i * groups_per_day) % len(target_muscle_groups)
                assigned_muscle_groups = target_muscle_groups[start_idx:start_idx + groups_per_day]

            # Get exercises from JSON for these muscle groups
            day_exercises = []
            for muscle_group in assigned_muscle_groups:
                muscle_exercises = DatabaseExerciseManagerJSON.get_available_exercises_by_muscle(muscle_group)

                # Select exercises for this muscle group
                exercises_for_muscle = min(exercises_per_day // len(assigned_muscle_groups) + 1, len(muscle_exercises))
                selected_exercises = muscle_exercises[:exercises_for_muscle]

                for exercise in selected_exercises:
                    # Add default sets/reps
                    exercise_copy = exercise.copy()

                    # Keep all image paths from JSON
                    # These will be used when structuring the final template
                    if exercise.get('isCardio'):
                        exercise_copy['sets'] = 1
                        exercise_copy['reps'] = '20 minutes'
                    else:
                        exercise_copy['sets'] = 3
                        exercise_copy['reps'] = 12 if exercise.get('isBodyWeight') else 10

                    day_exercises.append(exercise_copy)

            # Ensure minimum exercises per day
            while len(day_exercises) < 6:
                # Add more exercises from any available muscle group
                additional_muscle = target_muscle_groups[len(day_exercises) % len(target_muscle_groups)]
                additional_exercises = DatabaseExerciseManagerJSON.get_available_exercises_by_muscle(additional_muscle)

                if additional_exercises:
                    # Find an exercise not already in the day
                    existing_names = [ex['name'] for ex in day_exercises]
                    for exercise in additional_exercises:
                        if exercise['name'] not in existing_names:
                            exercise_copy = exercise.copy()
                            exercise_copy['sets'] = 3
                            exercise_copy['reps'] = 12 if exercise.get('isBodyWeight') else 10
                            day_exercises.append(exercise_copy)
                            break
                else:
                    break  # No more exercises available

            # Use the day name from template_names (AI-generated or user-specified)
            day_title = day_name.title() if day_name else f"Day {i + 1}"

            template["days"][day_key] = {
                "title": day_title,
                "muscle_groups": assigned_muscle_groups,
                "exercises": day_exercises[:6]  # Limit to 6 exercises per day
            }

        return template, f"Generated {template_count}-day workout template based on your {goal} goal"

    except Exception as e:
        print(f"🚨 Error in llm_generate_template_from_profile_json_only: {e}")
        import traceback
        traceback.print_exc()

        # Return minimal fallback
        template_names = profile.get("template_names", ["Day 1"])
        fallback_template = {
            "name": "Fallback Workout",
            "goal": "muscle gain",
            "days": {}
        }

        for name in template_names:
            day_key = name.lower().replace(' ', '_')
            fallback_template["days"][day_key] = {
                "title": name.title(),
                "muscle_groups": ["full body"],
                "exercises": [
                    {
                        "id": 1,
                        "name": "Push-ups",
                        "sets": 3,
                        "reps": 10,
                        "muscle_group": "Chest",
                        "gifUrl": "",
                        "imgUrl": "",
                        "gifPathFemale": "",
                        "imgPathFemale": "",
                        "isCardio": False,
                        "isBodyWeight": True
                    },
                    {
                        "id": 2,
                        "name": "Squats",
                        "sets": 3,
                        "reps": 12,
                        "muscle_group": "Leg",
                        "gifUrl": "",
                        "imgUrl": "",
                        "gifPathFemale": "",
                        "imgPathFemale": "",
                        "isCardio": False,
                        "isBodyWeight": True
                    }
                ]
            }

        return fallback_template, "Fallback template generated"


def apply_bulk_modifications_json(
    template: Dict[str, Any],
    operation: str,
    target_days: str = 'all',
    specific_count: int = None,
    muscles: List[str] = None,
    add_count: int = 2
) -> Tuple[Dict[str, Any], str]:
    """
    Apply bulk modifications to workout template using JSON catalog
    Operations: add_exercises, replace_all
    """

    catalog = load_catalog()
    if not catalog:
        return template, "Could not load exercise catalog from JSON"

    updated = template.copy()
    days = updated.get('days', {})
    day_keys = list(days.keys())

    # Determine which days to modify
    target_day_keys = []
    if target_days == 'all':
        target_day_keys = day_keys
    elif target_days == 'specific_count' and specific_count:
        target_day_keys = day_keys[:min(specific_count, len(day_keys))]

    if not target_day_keys:
        return template, "No valid days found to modify"

    # Better muscle group mapping
    muscle_mapping = {
        'legs': ['legs', 'leg'],
        'leg': ['legs', 'leg'],
        'upper': ['chest', 'back', 'shoulders', 'biceps', 'triceps'],
        'chest': ['chest'],
        'back': ['back'],
        'shoulders': ['shoulders', 'shoulder'],
        'arms': ['biceps', 'triceps'],
        'biceps': ['biceps'],
        'triceps': ['triceps'],
        'core': ['abs'],
        'abs': ['abs'],
        'cardio': ['cardio'],
        'forearms': ['forearms']
    }

    if operation == 'add_exercises':
        # Add exercises to specified days
        for dk in target_day_keys:
            day_data = days.get(dk, {})
            existing_ex = day_data.get('exercises', [])
            used_ids = {ex.get('id') for ex in existing_ex if ex.get('id')}

            # Determine muscles for this day
            day_muscles = muscles or day_data.get('muscle_groups', ['chest'])
            expanded_muscles = []
            for m in day_muscles:
                expanded_muscles.extend(muscle_mapping.get(m.lower(), [m.lower()]))

            # Pick new exercises
            new_ids = pick_from_muscles(expanded_muscles, catalog, used_ids, add_count)
            for eid in new_ids:
                ex_data = catalog['by_id'].get(eid)
                if ex_data:
                    new_ex = {
                        'id': eid,
                        'name': ex_data['name'],
                        'muscle_group': ex_data['muscle_group'],
                        'gifUrl': ex_data.get('gifUrl', ''),
                        'imgUrl': ex_data.get('imgUrl', ''),
                        'gifPathFemale': ex_data.get('gifPathFemale', ''),
                        'imgPathFemale': ex_data.get('imgPathFemale', ''),
                        'isCardio': ex_data.get('isCardio', False),
                        'isBodyWeight': ex_data.get('isBodyWeight', False),
                        'sets': 1 if ex_data.get('isCardio') else 3,
                        'reps': '20 minutes' if ex_data.get('isCardio') else 10
                    }
                    existing_ex.append(new_ex)

            days[dk]['exercises'] = existing_ex

        return updated, f"Added {add_count} exercise(s) to {len(target_day_keys)} day(s)"

    elif operation == 'replace_all':
        # Replace all exercises for specified days
        for dk in target_day_keys:
            day_data = days.get(dk, {})
            day_muscles = muscles or day_data.get('muscle_groups', ['chest'])
            expanded_muscles = []
            for m in day_muscles:
                expanded_muscles.extend(muscle_mapping.get(m.lower(), [m.lower()]))

            # Pick fresh exercises
            new_ids = pick_from_muscles(expanded_muscles, catalog, set(), 6)
            new_exercises = []
            for eid in new_ids:
                ex_data = catalog['by_id'].get(eid)
                if ex_data:
                    new_ex = {
                        'id': eid,
                        'name': ex_data['name'],
                        'muscle_group': ex_data['muscle_group'],
                        'gifUrl': ex_data.get('gifUrl', ''),
                        'imgUrl': ex_data.get('imgUrl', ''),
                        'gifPathFemale': ex_data.get('gifPathFemale', ''),
                        'imgPathFemale': ex_data.get('imgPathFemale', ''),
                        'isCardio': ex_data.get('isCardio', False),
                        'isBodyWeight': ex_data.get('isBodyWeight', False),
                        'sets': 1 if ex_data.get('isCardio') else 3,
                        'reps': '20 minutes' if ex_data.get('isCardio') else 10
                    }
                    new_exercises.append(new_ex)

            days[dk]['exercises'] = new_exercises

        return updated, f"Replaced all exercises for {len(target_day_keys)} day(s)"

    return template, "Unknown operation"


def generate_custom_muscle_split_json(
    template_names: List[str],
    muscle_groups_per_day: List[List[str]]
) -> Tuple[Dict[str, Any], str]:
    """
    Generate a custom muscle split template using JSON catalog
    """

    catalog = load_catalog()
    if not catalog:
        return {}, "Could not load exercise catalog from JSON"

    # Better muscle mapping
    muscle_mapping = {
        'legs': ['legs', 'leg'],
        'leg': ['legs', 'leg'],
        'upper': ['chest', 'back', 'shoulders', 'biceps', 'triceps'],
        'chest': ['chest'],
        'back': ['back'],
        'shoulders': ['shoulders', 'shoulder'],
        'arms': ['biceps', 'triceps'],
        'biceps': ['biceps'],
        'triceps': ['triceps'],
        'core': ['abs'],
        'abs': ['abs'],
        'cardio': ['cardio']
    }

    template = {
        "name": f"Custom Muscle Split ({len(template_names)} days)",
        "goal": "muscle gain",
        "days": {}
    }

    for i, day_name in enumerate(template_names):
        day_key = day_name.lower().replace(' ', '_')
        assigned_muscles = muscle_groups_per_day[i] if i < len(muscle_groups_per_day) else ["chest"]

        # Expand muscle groups
        expanded_muscles = []
        for m in assigned_muscles:
            expanded_muscles.extend(muscle_mapping.get(m.lower(), [m.lower()]))

        # Pick 6 exercises
        exercise_ids = pick_from_muscles(expanded_muscles, catalog, set(), 6)
        exercises = []
        for eid in exercise_ids:
            ex_data = catalog['by_id'].get(eid)
            if ex_data:
                exercises.append({
                    'id': eid,
                    'name': ex_data['name'],
                    'muscle_group': ex_data['muscle_group'],
                    'gifUrl': ex_data.get('gifUrl', ''),
                    'imgUrl': ex_data.get('imgUrl', ''),
                    'gifPathFemale': ex_data.get('gifPathFemale', ''),
                    'imgPathFemale': ex_data.get('imgPathFemale', ''),
                    'isCardio': ex_data.get('isCardio', False),
                    'isBodyWeight': ex_data.get('isBodyWeight', False),
                    'sets': 1 if ex_data.get('isCardio') else 3,
                    'reps': '20 minutes' if ex_data.get('isCardio') else 10
                })

        template["days"][day_key] = {
            "title": day_name.title(),
            "muscle_groups": assigned_muscles,
            "exercises": exercises
        }

    return template, f"Generated custom {len(template_names)}-day muscle split"
