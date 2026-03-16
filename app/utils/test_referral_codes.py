"""
Test script to demonstrate referral code generation
Run this to see example codes without database
"""

from referral_code_generator import (
    generate_referral_code_sequential,
    generate_referral_code_hash_based,
    generate_referral_code_random,
    get_code_statistics,
    base36_encode
)


def test_sequential_generation():
    """Test sequential code generation"""
    print("=" * 70)
    print("SEQUENTIAL CODE GENERATION (Recommended for Production)")
    print("=" * 70)

    test_user_ids = [1, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000]

    print(f"\n{'User ID':<15} {'Referral Code':<15} {'Note'}")
    print("-" * 70)

    for user_id in test_user_ids:
        code = generate_referral_code_sequential(user_id)
        print(f"{user_id:<15} {code:<15} User #{user_id:,}")

    print("\nNote: Each run produces different codes due to random salt")
    print("Run this script multiple times to see variation\n")


def test_hash_generation():
    """Test hash-based code generation"""
    print("=" * 70)
    print("HASH-BASED CODE GENERATION")
    print("=" * 70)

    test_identifiers = [
        "john@example.com",
        "jane@example.com",
        "mike@example.com",
        "+1234567890",
        "+9876543210",
        "550e8400-e29b-41d4-a716-446655440000"
    ]

    print(f"\n{'Identifier':<40} {'Referral Code'}")
    print("-" * 70)

    for identifier in test_identifiers:
        code = generate_referral_code_hash_based(identifier)
        print(f"{identifier:<40} {code}")

    print("\nNote: Hash codes are deterministic - same input = same output")
    print("(unless you change the secret salt)\n")


def test_random_generation():
    """Test random code generation"""
    print("=" * 70)
    print("RANDOM CODE GENERATION")
    print("=" * 70)

    print(f"\n{'Attempt':<10} {'Referral Code':<15} {'Note'}")
    print("-" * 70)

    for i in range(10):
        code = generate_referral_code_random()
        print(f"{i+1:<10} {code:<15} Completely random")

    print("\nNote: Each code is randomly generated")
    print("Extremely low collision probability (1 in 78 billion)\n")


def test_statistics():
    """Display code statistics"""
    print("=" * 70)
    print("REFERRAL CODE SYSTEM STATISTICS")
    print("=" * 70)

    stats = get_code_statistics()

    print(f"\nFormat: {stats['prefix']} + {stats['characters_after_prefix']} alphanumeric characters")
    print(f"Character set: {stats['character_set']}")
    print(f"Total combinations: {stats['total_combinations_formatted']}")
    print(f"Human readable: {stats['human_readable']}")
    print(f"Supports users: {stats['supports_users']}")
    print(f"Recommended method: {stats['recommended_method']}")

    print(f"\nExample codes:")
    for example in stats['example_codes']:
        print(f"  - {example}")

    print(f"\nCollision probability at 1B users: {stats['collision_probability_at_1B_users']}")
    print("With retry logic: Nearly 0% failure rate\n")


def test_base36_encoding():
    """Test base36 encoding"""
    print("=" * 70)
    print("BASE36 ENCODING DEMONSTRATION")
    print("=" * 70)

    test_numbers = [0, 1, 10, 36, 100, 1000, 10000, 100000, 1000000, 10000000]

    print(f"\n{'Decimal':<15} {'Base36':<15} {'With Padding'}")
    print("-" * 70)

    for num in test_numbers:
        base36 = base36_encode(num)
        padded = base36.zfill(7)
        print(f"{num:<15} {base36:<15} {padded}")

    print("\nBase36 uses: 0-9 (10 digits) + A-Z (26 letters) = 36 characters")
    print("More compact than decimal, more readable than hex\n")


def compare_methods():
    """Compare all three methods"""
    print("=" * 70)
    print("METHOD COMPARISON - Same User (ID: 12345, email: user@example.com)")
    print("=" * 70)

    user_id = 12345
    email = "user@example.com"

    print(f"\n{'Method':<20} {'Code Generated':<15} {'Characteristics'}")
    print("-" * 70)

    # Sequential
    seq_code = generate_referral_code_sequential(user_id)
    print(f"{'Sequential':<20} {seq_code:<15} Changes each time (random salt)")

    # Hash-based
    hash_code = generate_referral_code_hash_based(email)
    print(f"{'Hash-based':<20} {hash_code:<15} Always same for same email")

    # Random
    rand_code = generate_referral_code_random()
    print(f"{'Random':<20} {rand_code:<15} Completely random")

    print("\nRecommendation:")
    print("  ✓ Use SEQUENTIAL for user registration (guaranteed unique with retries)")
    print("  ✓ Use HASH for predictable codes (e.g., testing)")
    print("  ✓ Use RANDOM as fallback\n")


def test_collision_scenarios():
    """Test collision scenarios"""
    print("=" * 70)
    print("COLLISION PROBABILITY SIMULATION")
    print("=" * 70)

    # Generate codes for sequential users
    print("\nGenerating codes for 100 sequential users:")
    print(f"{'User ID':<10} {'Code':<15} {'User ID':<10} {'Code':<15}")
    print("-" * 70)

    codes_set = set()
    collisions = 0

    for i in range(1, 101):
        code = generate_referral_code_sequential(i)
        if code in codes_set:
            collisions += 1
        codes_set.add(code)

        if i <= 50:  # Show first 50
            partner = i + 50
            code2 = generate_referral_code_sequential(partner) if i <= 50 else ""
            print(f"{i:<10} {code:<15} {partner if i <= 50 else '':<10} {code2:<15}")

    print(f"\nGenerated {len(codes_set)} unique codes from 100 users")
    print(f"Collisions detected: {collisions}")
    print(f"Uniqueness rate: {(len(codes_set) / 100) * 100:.1f}%")

    print("\nNote: In production, the retry logic handles any collisions")
    print("Maximum 5 retries ensures virtually 0% failure rate\n")


if __name__ == "__main__":
    print("\n")
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║                FITTBOT REFERRAL CODE GENERATOR TEST                ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    print("\n")

    # Run all tests
    test_statistics()
    input("Press Enter to continue to Sequential Generation test...")

    test_sequential_generation()
    input("Press Enter to continue to Hash-based Generation test...")

    test_hash_generation()
    input("Press Enter to continue to Random Generation test...")

    test_random_generation()
    input("Press Enter to continue to Base36 Encoding demonstration...")

    test_base36_encoding()
    input("Press Enter to continue to Method Comparison...")

    compare_methods()
    input("Press Enter to continue to Collision Simulation...")

    test_collision_scenarios()

    print("=" * 70)
    print("ALL TESTS COMPLETED!")
    print("=" * 70)
    print("\nNext Steps:")
    print("  1. Run database migration")
    print("  2. Backfill existing users")
    print("  3. Test with actual registration API")
    print("=" * 70)
    print("\n")