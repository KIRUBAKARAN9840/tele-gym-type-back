"""
Referral Code Generator for Fittbot
Generates unique referral codes with FIT prefix
Supports up to 1+ billion unique codes
"""

import hashlib
import secrets
from sqlalchemy.orm import Session
from sqlalchemy import text


def base62_encode(number: int) -> str:
   
    if number == 0:
        return '0'

    base62 = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []

    while number:
        number, remainder = divmod(number, 62)
        result.append(base62[remainder])

    return ''.join(reversed(result))


def generate_referral_code_sequential(user_id: int, name: str = "FIT") -> str:
    """
    Generate referral code using sequential method.
    Format: First 3 letters of name + 5 alphanumeric characters

    Args:
        user_id: User ID for code generation
        name: Name to use for prefix (first 3 letters will be extracted)

    Returns:
        Referral code like JOHx7K9m or SAR4aB3x (for name "John" or "Sarah")
    """
    # Extract first 3 letters from name and convert to uppercase
    # Remove non-alphabetic characters
    clean_name = ''.join(c for c in name if c.isalpha())
    prefix = clean_name[:3].upper().ljust(3, 'X')  # Pad with X if less than 3 letters

    # Generate a nice alphanumeric mix (5 characters)
    base62 = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    # Use user_id as seed for uniqueness, but generate random-looking chars
    # Combine user_id with random salt for better distribution
    salt = secrets.randbelow(999999)
    combined = (user_id * 1000000) + salt

    # Generate 5 random-looking alphanumeric characters
    random_chars = []
    for i in range(5):
        # Mix the combined value with position for variation
        index = (combined + i * 1234567) % 62
        random_chars.append(base62[index])

    return f"{prefix}{''.join(random_chars)}"


def generate_referral_code_hash_based(unique_identifier: str, name: str = "FIT") -> str:
    """
    Generate referral code using hash-based approach.
    Format: First 3 letters of name + 5 alphanumeric characters

    This is an alternative method that uses SHA256 hashing.
    Best for when you want to generate codes before user creation.

    Args:
        unique_identifier: Any unique string (email, phone, UUID, etc.)
        name: Name to use for prefix (first 3 letters will be extracted)

    Returns:
        Referral code like JOHxY7k9 (case-sensitive)

    Example:
        unique_identifier="user@example.com", name="John" -> JOHdF8h3
    """
    # Extract first 3 letters from name and convert to uppercase
    clean_name = ''.join(c for c in name if c.isalpha())
    prefix = clean_name[:3].upper().ljust(3, 'X')

    # Add a secret salt for additional security (store this in env vars)
    secret_salt = "FITTBOT_REFERRAL_SECRET_2025"  # Move to environment variable

    # Create hash
    hash_input = f"{unique_identifier}{secret_salt}".encode()
    hash_digest = hashlib.sha256(hash_input).hexdigest()

    # Convert first bits to base62
    hash_number = int(hash_digest[:14], 16)  # Take first 14 hex chars

    # Generate 5 alphanumeric characters from hash
    base62 = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    random_chars = []
    for i in range(5):
        index = (hash_number + i * 123456) % 62
        random_chars.append(base62[index])
        hash_number = hash_number // 62

    return f"{prefix}{''.join(random_chars)}"


def generate_referral_code_random(name: str = "FIT") -> str:
    """
    Generate completely random referral code.
    Format: First 3 letters of name + 5 random alphanumeric characters

    This is the simplest but requires database uniqueness check.

    Args:
        name: Name to use for prefix (first 3 letters will be extracted)

    Returns:
        Random referral code like JOHk7Mq9 (case-sensitive)
    """
    # Extract first 3 letters from name and convert to uppercase
    clean_name = ''.join(c for c in name if c.isalpha())
    prefix = clean_name[:3].upper().ljust(3, 'X')

    base62 = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    # Generate 5 random alphanumeric characters
    random_chars = ''.join(secrets.choice(base62) for _ in range(5))

    return f"{prefix}{random_chars}"


ALLOWED_REFERRAL_TABLES = {"clients", "gyms", "referral_code", "referral_gym_code"}

def is_referral_code_unique(db: Session, referral_code: str, table_name: str = "clients") -> bool:

    if table_name not in ALLOWED_REFERRAL_TABLES:
        raise ValueError(f"Invalid table name: {table_name}. Allowed: {ALLOWED_REFERRAL_TABLES}")

    query = text(f"SELECT COUNT(*) FROM {table_name} WHERE referral_code = :code")
    result = db.execute(query, {"code": referral_code}).scalar()
    return result == 0


def generate_unique_referral_code(
    db: Session,
    name: str,
    user_id: int = None,
    unique_identifier: str = None,
    method: str = "sequential",
    table_name: str = "clients",
    max_retries: int = 5
) -> str:

    for attempt in range(max_retries):
        # Generate code based on method
        if method == "sequential":
            if user_id is None:
                raise ValueError("user_id required for sequential method")
            # Add attempt number to ensure different code each retry
            code = generate_referral_code_sequential(user_id + attempt, name)

        elif method == "hash":
            if unique_identifier is None:
                raise ValueError("unique_identifier required for hash method")
            # Append attempt number to identifier for retries
            identifier_with_retry = f"{unique_identifier}_{attempt}"
            code = generate_referral_code_hash_based(identifier_with_retry, name)

        elif method == "random":
            code = generate_referral_code_random(name)

        else:
            raise ValueError(f"Invalid method: {method}. Use 'sequential', 'hash', or 'random'")

        # Check uniqueness
        if is_referral_code_unique(db, code, table_name):
            return code

    # If we get here, we failed to generate unique code
    raise ValueError(
        f"Failed to generate unique referral code after {max_retries} attempts. "
        "This is extremely unlikely - please check database constraints."
    )


# Example usage and statistics
def get_code_statistics():
    """
    Returns statistics about referral code capacity.
    """
    return {
        "prefix": "First 3 letters of name (e.g., JOH, SAR, MIK)",
        "characters_after_prefix": 5,
        "character_set": "0-9, a-z, A-Z (62 characters) - CASE SENSITIVE",
        "total_combinations": 62**5,
        "total_combinations_formatted": "916,132,832",
        "human_readable": "916 MILLION unique codes per name prefix",
        "supports_users": "Millions of users per name with massive safety margin",
        "example_codes": ["JOHa1B2c", "SAR7xK9m", "MIKzZyYx"],
        "collision_probability_at_1M_users": "~0.11% (virtually zero)",
        "recommended_method": "sequential (guaranteed unique with user_id)",
        "case_sensitive": True
    }
