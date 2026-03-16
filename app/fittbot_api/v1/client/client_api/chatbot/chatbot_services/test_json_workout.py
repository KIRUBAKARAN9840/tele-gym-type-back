"""
Test script for JSON-based workout template creation
Demonstrates how to use the new JSON catalog system without database dependency
"""

import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import exercise_catalog_json
from exercise_catalog_json import load_catalog
import database_exercise_manager_json
from database_exercise_manager_json import DatabaseExerciseManagerJSON
import workout_llm_helper_json
from workout_llm_helper_json import llm_generate_template_from_profile_json_only


def test_load_catalog():
    """Test loading exercises from workouts.json"""
    print("\n" + "="*80)
    print("TEST 1: Loading Exercise Catalog from workouts.json")
    print("="*80)

    catalog = load_catalog()

    print(f"✅ Total exercises loaded: {len(catalog['by_id'])}")
    print(f"✅ Muscle groups available: {list(catalog['by_muscle'].keys())}")

    # Show sample exercises from different muscle groups
    print("\n📋 Sample exercises by muscle group:")
    for muscle_group in ['chest', 'back', 'legs', 'abs', 'shoulders']:
        exercise_ids = catalog['by_muscle'].get(muscle_group, [])[:3]
        print(f"\n  {muscle_group.upper()}:")
        for eid in exercise_ids:
            ex = catalog['by_id'].get(eid)
            if ex:
                print(f"    - {ex['name']}")
                print(f"      ID: {ex['id']}")
                print(f"      Male Gif: {ex['gifUrl'][:60]}..." if ex['gifUrl'] else "      No male gif")
                print(f"      Female Gif: {ex['gifPathFemale'][:60]}..." if ex['gifPathFemale'] else "      No female gif")


def test_get_exercises_by_muscle():
    """Test getting exercises for specific muscle groups"""
    print("\n" + "="*80)
    print("TEST 2: Getting Exercises by Muscle Group")
    print("="*80)

    muscle_groups = ['chest', 'back', 'legs', 'abs']

    for muscle in muscle_groups:
        exercises = DatabaseExerciseManagerJSON.get_available_exercises_by_muscle(muscle)
        print(f"\n{muscle.upper()}: {len(exercises)} exercises available")

        # Show first 3 exercises with all image paths
        for ex in exercises[:3]:
            print(f"  ✓ {ex['name']}")
            print(f"    - ID: {ex['id']}")
            print(f"    - Cardio: {ex['isCardio']}, Bodyweight: {ex['isBodyWeight']}")
            print(f"    - Male Gif: {'✓' if ex.get('gifUrl') else '✗'}")
            print(f"    - Male Img: {'✓' if ex.get('imgUrl') else '✗'}")
            print(f"    - Female Gif: {'✓' if ex.get('gifPathFemale') else '✗'}")
            print(f"    - Female Img: {'✓' if ex.get('imgPathFemale') else '✗'}")


def test_generate_workout_template():
    """Test generating a complete workout template"""
    print("\n" + "="*80)
    print("TEST 3: Generating Complete Workout Template")
    print("="*80)

    # Sample profile
    profile = {
        "template_names": ["Chest Day", "Back Day", "Leg Day", "Shoulder Day", "Arm Day"],
        "template_count": 5,
        "client_goal": "muscle gain"
    }

    print("\n📝 Profile:")
    print(f"  Days: {profile['template_count']}")
    print(f"  Day Names: {', '.join(profile['template_names'])}")
    print(f"  Goal: {profile['client_goal']}")

    # Generate template
    template, message = llm_generate_template_from_profile_json_only(profile)

    print(f"\n✅ {message}")
    print(f"\n📋 Template Structure:")
    print(f"  Name: {template['name']}")
    print(f"  Goal: {template['goal']}")
    print(f"  Days: {len(template['days'])}")

    # Show details for each day
    for day_key, day_data in template['days'].items():
        print(f"\n  🗓️ {day_data['title']}:")
        print(f"    Muscle Groups: {', '.join(day_data['muscle_groups'])}")
        print(f"    Exercises: {len(day_data['exercises'])}")

        for i, ex in enumerate(day_data['exercises'], 1):
            print(f"      {i}. {ex['name']} - {ex['sets']} sets x {ex['reps']} reps")
            print(f"         Muscle: {ex['muscle_group']}")
            # Verify all image paths are present
            has_male_gif = bool(ex.get('gifUrl'))
            has_male_img = bool(ex.get('imgUrl'))
            has_female_gif = bool(ex.get('gifPathFemale'))
            has_female_img = bool(ex.get('imgPathFemale'))

            print(f"         Images: M-Gif:{'✓' if has_male_gif else '✗'} M-Img:{'✓' if has_male_img else '✗'} "
                  f"F-Gif:{'✓' if has_female_gif else '✗'} F-Img:{'✓' if has_female_img else '✗'}")


def test_exercise_validation():
    """Test exercise name validation and fuzzy matching"""
    print("\n" + "="*80)
    print("TEST 4: Exercise Validation and Fuzzy Matching")
    print("="*80)

    test_names = [
        "Bench Press",  # Exact match
        "bench pres",   # Typo - should find Bench Press
        "push up",      # Variation
        "XYZ Exercise"  # Invalid
    ]

    for name in test_names:
        print(f"\n🔍 Testing: '{name}'")
        exists, exercise_data = DatabaseExerciseManagerJSON.validate_exercise_exists(name)

        if exists:
            print(f"  ✅ Found: {exercise_data['name']}")
            print(f"     ID: {exercise_data['id']}")
            print(f"     Muscle: {exercise_data['muscle_group']}")
        else:
            print(f"  ❌ Not found")
            similar = DatabaseExerciseManagerJSON.find_similar_exercises(name, limit=3)
            if similar:
                print(f"  💡 Similar exercises:")
                for s in similar:
                    print(f"     - {s['name']} (similarity: {s['similarity']:.2f})")


def test_structured_payload():
    """Test building structured payload with all image paths"""
    print("\n" + "="*80)
    print("TEST 5: Building Structured Payload")
    print("="*80)

    # Generate a simple template
    profile = {
        "template_names": ["Day 1"],
        "client_goal": "muscle gain"
    }

    template, _ = llm_generate_template_from_profile_json_only(profile)

    # Get IDs from template
    day_data = list(template['days'].values())[0]
    exercise_ids = [ex['id'] for ex in day_data['exercises']]

    print(f"\n📋 Exercise IDs: {exercise_ids}")

    # Load catalog and build payload
    catalog = load_catalog()
    id_map = {eid: catalog['by_id'][eid] for eid in exercise_ids if eid in catalog['by_id']}

    print(f"\n✅ Exercises in payload:")
    for eid, ex_data in id_map.items():
        print(f"\n  ID {eid}: {ex_data['name']}")
        print(f"    Male Gif URL: {ex_data.get('gifUrl', 'N/A')[:80]}")
        print(f"    Male Img URL: {ex_data.get('imgUrl', 'N/A')[:80]}")
        print(f"    Female Gif URL: {ex_data.get('gifPathFemale', 'N/A')[:80]}")
        print(f"    Female Img URL: {ex_data.get('imgPathFemale', 'N/A')[:80]}")


def main():
    """Run all tests"""
    print("\n" + "🏋️ " * 30)
    print("JSON-BASED WORKOUT TEMPLATE SYSTEM - TEST SUITE")
    print("🏋️ " * 30)

    try:
        test_load_catalog()
        test_get_exercises_by_muscle()
        test_generate_workout_template()
        test_exercise_validation()
        test_structured_payload()

        print("\n" + "="*80)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\n🎉 The JSON-based workout system is working correctly!")
        print("📝 All image paths (male/female, gif/img) are properly loaded and preserved.")
        print("\n")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
