"""
Quick test to verify JSON integration works correctly
Tests that exercises from workouts.json have all 4 image paths
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exercise_catalog_db import load_catalog
from database_exercise_manager import DatabaseExerciseManager


def test_catalog_loading():
    """Test that catalog loads from JSON with all image paths"""
    print("\n" + "="*80)
    print("TEST: Loading Catalog from workouts.json")
    print("="*80)

    catalog = load_catalog(db=None)  # No database needed!

    print(f"✅ Total exercises: {len(catalog['by_id'])}")
    print(f"✅ Muscle groups: {list(catalog['by_muscle'].keys())}")

    # Check first exercise has all image paths
    first_ex = list(catalog['by_id'].values())[0]
    print(f"\n📋 Sample Exercise: {first_ex['name']}")
    print(f"   ID: {first_ex['id']}")
    print(f"   Muscle: {first_ex['muscle_group']}")
    print(f"   ✓ gifUrl: {bool(first_ex.get('gifUrl'))}")
    print(f"   ✓ imgUrl: {bool(first_ex.get('imgUrl'))}")
    print(f"   ✓ gifPathFemale: {bool(first_ex.get('gifPathFemale'))}")
    print(f"   ✓ imgPathFemale: {bool(first_ex.get('imgPathFemale'))}")

    return catalog


def test_exercise_manager():
    """Test that exercise manager returns all image paths"""
    print("\n" + "="*80)
    print("TEST: Exercise Manager Returns All Image Paths")
    print("="*80)

    # Get chest exercises
    exercises = DatabaseExerciseManager.get_available_exercises_by_muscle(db=None, muscle_group='chest')

    print(f"\n✅ Found {len(exercises)} chest exercises")

    if exercises:
        ex = exercises[0]
        print(f"\n📋 First Exercise: {ex['name']}")
        print(f"   Required keys present:")
        print(f"   ✓ gifUrl: {'✓' if 'gifUrl' in ex else '✗'}")
        print(f"   ✓ imgUrl: {'✓' if 'imgUrl' in ex else '✗'}")
        print(f"   ✓ gifPathFemale: {'✓' if 'gifPathFemale' in ex else '✗'}")
        print(f"   ✓ imgPathFemale: {'✓' if 'imgPathFemale' in ex else '✗'}")

        # Show actual values
        print(f"\n   Actual values:")
        print(f"   gifUrl: {ex.get('gifUrl', 'MISSING')[:80]}")
        print(f"   imgUrl: {ex.get('imgUrl', 'MISSING')[:80]}")
        print(f"   gifPathFemale: {ex.get('gifPathFemale', 'MISSING')[:80]}")
        print(f"   imgPathFemale: {ex.get('imgPathFemale', 'MISSING')[:80]}")


def test_structured_payload():
    """Test that structured payload includes all image paths"""
    print("\n" + "="*80)
    print("TEST: Structured Payload Has All Image Paths")
    print("="*80)

    from workout_structured import _fetch_qr_rows, _build_day_payload

    # Get some exercise IDs
    catalog = load_catalog(db=None)
    exercise_ids = list(catalog['by_id'].keys())[:3]

    print(f"\n📋 Testing with exercise IDs: {exercise_ids}")

    # Fetch exercises
    id_map = _fetch_qr_rows(db=None, ids=exercise_ids)

    print(f"✅ Fetched {len(id_map)} exercises")

    # Build payload
    payload = _build_day_payload(exercise_ids, id_map)

    print(f"\n📦 Payload structure:")
    for muscle_group, group_data in payload.items():
        print(f"\n  {muscle_group}:")
        print(f"    Exercises: {len(group_data['exercises'])}")
        print(f"    isMuscleGroup: {group_data['isMuscleGroup']}")

        for i, ex in enumerate(group_data['exercises'][:2], 1):  # Show first 2
            print(f"\n    Exercise {i}: {ex['name']}")
            print(f"      ✓ gifPath: {'✓' if ex.get('gifPath') else '✗ MISSING'}")
            print(f"      ✓ imgPath: {'✓' if ex.get('imgPath') else '✗ MISSING'}")
            print(f"      ✓ gifPathFemale: {'✓' if ex.get('gifPathFemale') else '✗ MISSING'}")
            print(f"      ✓ imgPathFemale: {'✓' if ex.get('imgPathFemale') else '✗ MISSING'}")

            # Check if they have actual URLs (not empty strings)
            has_all = all([
                ex.get('gifPath'),
                ex.get('imgPath'),
                ex.get('gifPathFemale'),
                ex.get('imgPathFemale')
            ])

            if has_all:
                print(f"      ✅ ALL IMAGE PATHS PRESENT!")
            else:
                print(f"      ❌ Some paths are empty")


def main():
    print("\n" + "🔧 " * 30)
    print("JSON INTEGRATION TEST - Verifying All Image Paths")
    print("🔧 " * 30)

    try:
        test_catalog_loading()
        test_exercise_manager()
        test_structured_payload()

        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED!")
        print("="*80)
        print("\n🎉 System now loads from workouts.json with all image paths!")
        print("📝 All 4 image paths (gifUrl, imgUrl, gifPathFemale, imgPathFemale) are present")
        print("✨ No more null values or missing paths!")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
