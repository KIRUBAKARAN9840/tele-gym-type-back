"""
AI-powered exercise validation and suggestion system
Ensures all user requests for exercises are validated against the database
"""

import json
import re
from typing import Dict, Any, List, Tuple, Optional
from sqlalchemy.orm import Session
from .database_exercise_manager import DatabaseExerciseManager


class AIExerciseValidator:
    """AI-powered exercise validation with intelligent suggestions"""

    @staticmethod
    def validate_and_suggest_exercises(oai, model: str, user_request: str, db: Session) -> Dict[str, Any]:
        """Use AI to extract exercise names and validate them against database"""

        # First, use AI to extract exercise names intelligently
        ai_extracted = AIExerciseValidator._ai_extract_exercise_names(oai, model, user_request)

        # Then validate each extracted exercise
        validation_result = {
            'can_fulfill': True,
            'validated_exercises': [],
            'invalid_exercises': [],
            'suggestions': [],
            'ai_message': '',
            'user_friendly_message': ''
        }

        for exercise_name in ai_extracted.get('exercise_names', []):
            exists, exercise_data = DatabaseExerciseManager.validate_exercise_exists(db, exercise_name)

            if exists:
                validation_result['validated_exercises'].append(exercise_data)
            else:
                validation_result['invalid_exercises'].append(exercise_name)
                validation_result['can_fulfill'] = False

                # Find similar exercises
                similar = DatabaseExerciseManager.find_similar_exercises(db, exercise_name, limit=3)
                if similar:
                    validation_result['suggestions'].extend([{
                        'requested': exercise_name,
                        'alternatives': similar
                    }])

        # Generate user-friendly response
        validation_result['user_friendly_message'] = AIExerciseValidator._generate_user_message(validation_result)

        return validation_result

    @staticmethod
    def _ai_extract_exercise_names(oai, model: str, user_request: str) -> Dict[str, Any]:
        """Use AI to intelligently extract exercise names from user request"""

        system_prompt = """You are an expert fitness assistant. Extract exercise names from user requests.

IMPORTANT RULES:
1. Extract ONLY exercise names, not muscle groups
2. Handle typos and variations intelligently
3. Recognize common exercise aliases
4. Return exercise names in standard gym terminology

Examples:
- "add pushups" → ["Push-ups"]
- "include bench press and dumbell press" → ["Bench Press", "Dumbbell Press"]
- "replace squats with lunges" → ["Squats", "Lunges"]
- "more chest exercises" → [] (this is muscle group, not specific exercise)

Respond in JSON: {"exercise_names": ["Exercise 1", "Exercise 2"], "reasoning": "explanation"}"""

        user_prompt = f"Extract exercise names from: '{user_request}'"

        try:
            resp = oai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1
            )

            result = json.loads(resp.choices[0].message.content or "{}")
            return {
                "exercise_names": result.get("exercise_names", []),
                "reasoning": result.get("reasoning", "")
            }

        except Exception as e:
            print(f"AI exercise extraction failed: {e}")
            # Fallback to simple pattern matching
            return AIExerciseValidator._fallback_extract_exercises(user_request)

    @staticmethod
    def _fallback_extract_exercises(user_request: str) -> Dict[str, Any]:
        """Fallback exercise extraction using patterns"""

        exercise_patterns = [
            r'(?:add|include|give|put)\s+([a-zA-Z\s-]{3,30}?)(?:\s+(?:to|for|on|exercise)|$)',
            r'(?:replace|change|swap).*?(?:with|to)\s+([a-zA-Z\s-]{3,30}?)(?:\s|$)',
            r'(?:instead\s+of|rather\s+than)\s+([a-zA-Z\s-]{3,30}?)(?:\s|$)',
            r'([a-zA-Z\s-]{3,30}?)\s+(?:exercise|workout)',
        ]

        extracted = []
        user_lower = user_request.lower()

        for pattern in exercise_patterns:
            matches = re.finditer(pattern, user_lower)
            for match in matches:
                exercise_name = match.group(1).strip()

                # Filter out common non-exercise words
                skip_words = ['more', 'some', 'different', 'better', 'new', 'other', 'muscle', 'group', 'workout', 'exercise']
                if exercise_name.lower() not in skip_words and len(exercise_name) > 2:
                    # Clean up the name
                    cleaned = ' '.join(word.capitalize() for word in exercise_name.split())
                    if cleaned not in extracted:
                        extracted.append(cleaned)

        return {
            "exercise_names": extracted,
            "reasoning": "Pattern-based extraction"
        }

    @staticmethod
    def _generate_user_message(validation_result: Dict[str, Any]) -> str:
        """Generate user-friendly message about exercise validation"""

        if validation_result['can_fulfill']:
            valid_names = [ex['name'] for ex in validation_result['validated_exercises']]
            return f"✅ Perfect! I can add these exercises: {', '.join(valid_names)}"

        # Handle invalid exercises
        message_parts = []

        if validation_result['validated_exercises']:
            valid_names = [ex['name'] for ex in validation_result['validated_exercises']]
            message_parts.append(f"✅ I can add: {', '.join(valid_names)}")

        if validation_result['invalid_exercises']:
            invalid_names = validation_result['invalid_exercises']
            message_parts.append(f"❌ Sorry, I can't find: {', '.join(invalid_names)}")

            if validation_result['suggestions']:
                message_parts.append("\n💡 Here are some alternatives:")
                for suggestion in validation_result['suggestions']:
                    alternatives = [alt['name'] for alt in suggestion['alternatives'][:2]]
                    message_parts.append(f"Instead of '{suggestion['requested']}', try: {', '.join(alternatives)}")

        return '\n'.join(message_parts)

    @staticmethod
    def suggest_muscle_group_exercises(oai, model: str, muscle_group: str, db: Session, count: int = 3) -> List[Dict[str, Any]]:
        """AI-powered suggestion of exercises for a muscle group"""

        # Get available exercises for the muscle group
        available_exercises = DatabaseExerciseManager.get_available_exercises_by_muscle(db, muscle_group)

        if not available_exercises:
            return []

        # Use AI to select the best exercises for the request
        system_prompt = f"""You are selecting the best {muscle_group} exercises from a database.

RULES:
1. Select {count} exercises that provide good muscle group coverage
2. Prefer compound movements over isolation
3. Consider difficulty progression
4. Avoid redundant exercises

Available exercises: {[ex['name'] for ex in available_exercises]}

Respond in JSON: {{"selected_exercises": ["Exercise 1", "Exercise 2"], "reasoning": "why these are good"}}"""

        try:
            resp = oai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Select best {count} {muscle_group} exercises"}
                ],
                temperature=0.2
            )

            result = json.loads(resp.choices[0].message.content or "{}")
            selected_names = result.get("selected_exercises", [])

            # Return the full exercise data for selected exercises
            selected_exercises = []
            for exercise in available_exercises:
                if exercise['name'] in selected_names:
                    selected_exercises.append(exercise)

            # If AI didn't select enough, fill with remaining exercises
            while len(selected_exercises) < count and len(selected_exercises) < len(available_exercises):
                for exercise in available_exercises:
                    if exercise not in selected_exercises:
                        selected_exercises.append(exercise)
                        break

            return selected_exercises[:count]

        except Exception as e:
            print(f"AI exercise selection failed: {e}")
            # Return first N exercises as fallback
            return available_exercises[:count]

    @staticmethod
    def handle_exercise_replacement_request(oai, model: str, user_request: str, current_exercises: List[Dict], db: Session) -> Dict[str, Any]:
        """Handle replacement requests like 'replace X with Y' """

        # Extract what to replace and what to replace it with
        replacement_patterns = [
            r'replace\s+([^w]+)\s+with\s+([^\.]+)',
            r'change\s+([^t]+)\s+to\s+([^\.]+)',
            r'swap\s+([^f]+)\s+for\s+([^\.]+)',
        ]

        target_exercise = None
        replacement_exercise = None

        for pattern in replacement_patterns:
            match = re.search(pattern, user_request.lower())
            if match:
                target_exercise = match.group(1).strip()
                replacement_exercise = match.group(2).strip()
                break

        if not target_exercise or not replacement_exercise:
            return {'success': False, 'message': 'Could not understand replacement request'}

        # Validate replacement exercise exists
        exists, exercise_data = DatabaseExerciseManager.validate_exercise_exists(db, replacement_exercise)

        if not exists:
            similar = DatabaseExerciseManager.find_similar_exercises(db, replacement_exercise, limit=3)
            if similar:
                suggestions = [ex['name'] for ex in similar]
                return {
                    'success': False,
                    'message': f"❌ '{replacement_exercise}' not found. Try: {', '.join(suggestions)}"
                }
            else:
                return {
                    'success': False,
                    'message': f"❌ '{replacement_exercise}' not found in our exercise database"
                }

        # Find and replace the target exercise
        updated_exercises = []
        replacement_made = False

        for exercise in current_exercises:
            if target_exercise.lower() in exercise.get('name', '').lower():
                # Replace with the validated exercise
                new_exercise = exercise_data.copy()
                new_exercise['sets'] = exercise.get('sets', 3)
                new_exercise['reps'] = exercise.get('reps', 10)
                updated_exercises.append(new_exercise)
                replacement_made = True
            else:
                updated_exercises.append(exercise)

        if replacement_made:
            return {
                'success': True,
                'updated_exercises': updated_exercises,
                'message': f"✅ Replaced {target_exercise} with {exercise_data['name']}"
            }
        else:
            return {
                'success': False,
                'message': f"❌ Could not find '{target_exercise}' to replace"
            }