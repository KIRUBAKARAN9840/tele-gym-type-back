"""
JSON-first exercise management system
Ensures all exercises come from workouts.json and provides AI-powered suggestions
"""

from typing import Dict, Any, List, Optional, Tuple
import re
import difflib

try:
    from .exercise_catalog_json import load_catalog, id_for_name, pick_from_muscles
except ImportError:
    from exercise_catalog_json import load_catalog, id_for_name, pick_from_muscles


class DatabaseExerciseManagerJSON:
    """Manages exercise selection and validation using only JSON exercises"""

    @staticmethod
    def get_available_exercises_by_muscle(muscle_group: str) -> List[Dict[str, Any]]:
        """Get all available exercises for a specific muscle group from JSON"""
        catalog = load_catalog()

        # Normalize muscle group name
        muscle_key = muscle_group.lower().strip()
        muscle_synonyms = {
            'chest': 'chest',
            'back': 'back',
            'legs': 'legs',
            'leg': 'legs',
            'shoulders': 'shoulders',
            'shoulder': 'shoulders',
            'biceps': 'biceps',
            'bicep': 'biceps',
            'triceps': 'triceps',
            'tricep': 'triceps',
            'core': 'abs',
            'abs': 'abs',
            'cardio': 'cardio',
            'full body': 'full body',
            'quads': 'legs',
            'hamstrings': 'hamstrings',
            'calves': 'calves',
            'glutes': 'legs',
            'forearms': 'forearms',
            'forearm': 'forearms'
        }

        normalized_muscle = muscle_synonyms.get(muscle_key, muscle_key)

        # Get exercise IDs for this muscle group
        exercise_ids = catalog.get('by_muscle', {}).get(normalized_muscle, [])

        # Build exercise list with full details including all image paths
        exercises = []
        for exercise_id in exercise_ids:
            exercise_data = catalog.get('by_id', {}).get(exercise_id)
            if exercise_data:
                exercises.append({
                    'id': exercise_id,
                    'name': exercise_data['name'],
                    'muscle_group': exercise_data['muscle_group'],
                    'gifUrl': exercise_data.get('gifUrl', ''),  # Male gif
                    'imgUrl': exercise_data.get('imgUrl', ''),  # Male image
                    'gifPathFemale': exercise_data.get('gifPathFemale', ''),  # Female gif
                    'imgPathFemale': exercise_data.get('imgPathFemale', ''),  # Female image
                    'isCardio': exercise_data.get('isCardio', False),
                    'isBodyWeight': exercise_data.get('isBodyWeight', False)
                })

        return exercises

    @staticmethod
    def validate_exercise_exists(exercise_name: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Check if an exercise exists in the JSON catalog"""
        catalog = load_catalog()
        exercise_id = id_for_name(exercise_name, catalog)

        if exercise_id:
            exercise_data = catalog.get('by_id', {}).get(exercise_id)
            if exercise_data:
                return True, {
                    'id': exercise_id,
                    'name': exercise_data['name'],
                    'muscle_group': exercise_data['muscle_group'],
                    'gifUrl': exercise_data.get('gifUrl', ''),
                    'imgUrl': exercise_data.get('imgUrl', ''),
                    'gifPathFemale': exercise_data.get('gifPathFemale', ''),
                    'imgPathFemale': exercise_data.get('imgPathFemale', ''),
                    'isCardio': exercise_data.get('isCardio', False),
                    'isBodyWeight': exercise_data.get('isBodyWeight', False)
                }

        return False, None

    @staticmethod
    def find_similar_exercises(exercise_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Find similar exercises in the JSON catalog when exact match fails"""
        catalog = load_catalog()
        all_exercise_names = list(catalog.get('name_to_id', {}).keys())

        # Use difflib to find similar names
        similar_names = difflib.get_close_matches(
            exercise_name.lower().strip(),
            all_exercise_names,
            n=limit,
            cutoff=0.5
        )

        similar_exercises = []
        for name in similar_names:
            exercise_id = catalog.get('name_to_id', {}).get(name)
            if exercise_id:
                exercise_data = catalog.get('by_id', {}).get(exercise_id)
                if exercise_data:
                    similar_exercises.append({
                        'id': exercise_id,
                        'name': exercise_data['name'],
                        'muscle_group': exercise_data['muscle_group'],
                        'gifUrl': exercise_data.get('gifUrl', ''),
                        'imgUrl': exercise_data.get('imgUrl', ''),
                        'gifPathFemale': exercise_data.get('gifPathFemale', ''),
                        'imgPathFemale': exercise_data.get('imgPathFemale', ''),
                        'isCardio': exercise_data.get('isCardio', False),
                        'isBodyWeight': exercise_data.get('isBodyWeight', False),
                        'similarity': difflib.SequenceMatcher(None, exercise_name.lower(), name).ratio()
                    })

        # Sort by similarity score
        similar_exercises.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_exercises

    @staticmethod
    def generate_muscle_group_template(muscle_groups: List[str], exercises_per_group: int = 3) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a template with exercises for specific muscle groups"""
        template_exercises = {}

        for muscle_group in muscle_groups:
            available_exercises = DatabaseExerciseManagerJSON.get_available_exercises_by_muscle(muscle_group)

            if available_exercises:
                # Select the first N exercises for this muscle group
                selected_exercises = available_exercises[:exercises_per_group]

                # Add default sets/reps based on exercise type
                for exercise in selected_exercises:
                    if exercise.get('isCardio'):
                        exercise['sets'] = 1
                        exercise['reps'] = '20 minutes'
                    else:
                        exercise['sets'] = 3
                        exercise['reps'] = 12 if exercise.get('isBodyWeight') else 10

                template_exercises[muscle_group] = selected_exercises
            else:
                print(f"⚠️ No exercises found for muscle group: {muscle_group}")
                template_exercises[muscle_group] = []

        return template_exercises

    @staticmethod
    def validate_user_exercise_request(user_request: str) -> Dict[str, Any]:
        """Validate and process user exercise requests using AI and JSON lookup"""

        # Extract exercise names from user request using simple patterns
        exercise_patterns = [
            r'add\s+([a-zA-Z\s-]+?)(?:\s+to|\s+for|\s*$)',
            r'include\s+([a-zA-Z\s-]+?)(?:\s+to|\s+for|\s*$)',
            r'replace.*with\s+([a-zA-Z\s-]+?)(?:\s+to|\s+for|\s*$)',
            r'change.*to\s+([a-zA-Z\s-]+?)(?:\s+to|\s+for|\s*$)',
        ]

        extracted_exercises = []
        for pattern in exercise_patterns:
            matches = re.finditer(pattern, user_request.lower())
            for match in matches:
                exercise_name = match.group(1).strip()
                if len(exercise_name) > 2:  # Filter out very short matches
                    extracted_exercises.append(exercise_name)

        if not extracted_exercises:
            # Try to extract any capitalized words that might be exercise names
            words = user_request.split()
            potential_exercises = []
            current_exercise = []

            for word in words:
                if word.lower() in ['add', 'include', 'replace', 'change', 'with', 'to', 'for', 'and', 'or']:
                    if current_exercise:
                        potential_exercises.append(' '.join(current_exercise))
                        current_exercise = []
                elif len(word) > 2 and word.isalpha():
                    current_exercise.append(word)

            if current_exercise:
                potential_exercises.append(' '.join(current_exercise))

            extracted_exercises.extend(potential_exercises)

        # Validate each extracted exercise
        validation_results = {
            'valid_exercises': [],
            'invalid_exercises': [],
            'suggestions': {},
            'can_fulfill': True,
            'message': ''
        }

        for exercise_name in extracted_exercises:
            exists, exercise_data = DatabaseExerciseManagerJSON.validate_exercise_exists(exercise_name)

            if exists:
                validation_results['valid_exercises'].append(exercise_data)
            else:
                validation_results['invalid_exercises'].append(exercise_name)

                # Find similar exercises
                similar = DatabaseExerciseManagerJSON.find_similar_exercises(exercise_name, limit=3)
                if similar:
                    validation_results['suggestions'][exercise_name] = similar

        # Generate response message
        if validation_results['valid_exercises']:
            valid_names = [ex['name'] for ex in validation_results['valid_exercises']]
            validation_results['message'] = f"✅ Found these exercises: {', '.join(valid_names)}"

        if validation_results['invalid_exercises']:
            validation_results['can_fulfill'] = False
            invalid_names = validation_results['invalid_exercises']

            suggestions_text = []
            for invalid_name in invalid_names:
                if invalid_name in validation_results['suggestions']:
                    similar_names = [ex['name'] for ex in validation_results['suggestions'][invalid_name]]
                    suggestions_text.append(f"'{invalid_name}' → Try: {', '.join(similar_names)}")

            if suggestions_text:
                validation_results['message'] += f"\n\n⚠️ Not found: {', '.join(invalid_names)}\n💡 Suggestions:\n" + '\n'.join(suggestions_text)
            else:
                validation_results['message'] += f"\n\n❌ Cannot find: {', '.join(invalid_names)}. Please try different exercise names."

        return validation_results

    @staticmethod
    def get_exercise_by_id(exercise_id: int) -> Optional[Dict[str, Any]]:
        """Get exercise details by ID"""
        catalog = load_catalog()
        exercise_data = catalog.get('by_id', {}).get(exercise_id)

        if exercise_data:
            return {
                'id': exercise_id,
                'name': exercise_data['name'],
                'muscle_group': exercise_data['muscle_group'],
                'gifUrl': exercise_data.get('gifUrl', ''),
                'imgUrl': exercise_data.get('imgUrl', ''),
                'gifPathFemale': exercise_data.get('gifPathFemale', ''),
                'imgPathFemale': exercise_data.get('imgPathFemale', ''),
                'isCardio': exercise_data.get('isCardio', False),
                'isBodyWeight': exercise_data.get('isBodyWeight', False)
            }

        return None

    @staticmethod
    def select_balanced_exercises(muscle_groups: List[str], total_exercises: int = 6) -> List[Dict[str, Any]]:
        """Select a balanced set of exercises across muscle groups"""
        catalog = load_catalog()
        selected_exercises = []
        used_ids = set()

        # Calculate exercises per muscle group
        exercises_per_group = max(1, total_exercises // len(muscle_groups))
        remaining_exercises = total_exercises % len(muscle_groups)

        for i, muscle_group in enumerate(muscle_groups):
            # Add one extra exercise to first few groups if there are remainders
            target_count = exercises_per_group + (1 if i < remaining_exercises else 0)

            # Get available exercises for this muscle group
            muscle_exercise_ids = pick_from_muscles([muscle_group], catalog, used_ids, target_count)

            for exercise_id in muscle_exercise_ids:
                exercise_data = catalog.get('by_id', {}).get(exercise_id)
                if exercise_data and exercise_id not in used_ids:
                    selected_exercises.append({
                        'id': exercise_id,
                        'name': exercise_data['name'],
                        'muscle_group': exercise_data['muscle_group'],
                        'gifUrl': exercise_data.get('gifUrl', ''),
                        'imgUrl': exercise_data.get('imgUrl', ''),
                        'gifPathFemale': exercise_data.get('gifPathFemale', ''),
                        'imgPathFemale': exercise_data.get('imgPathFemale', ''),
                        'isCardio': exercise_data.get('isCardio', False),
                        'isBodyWeight': exercise_data.get('isBodyWeight', False),
                        'sets': 1 if exercise_data.get('isCardio') else 3,
                        'reps': '20 minutes' if exercise_data.get('isCardio') else (12 if exercise_data.get('isBodyWeight') else 10)
                    })
                    used_ids.add(exercise_id)

                    if len(selected_exercises) >= total_exercises:
                        break

            if len(selected_exercises) >= total_exercises:
                break

        return selected_exercises
