"""ID generation utilities"""

import time
import random
from typing import Optional


def generate_id(prefix: str) -> str:
    """
    Generate unique ID with timestamp and random suffix
    Format: {prefix}_{timestamp}_{random}
    """
    timestamp = int(time.time())
    random_suffix = random.randint(1000, 9999)
    return f"{prefix}_{timestamp}_{random_suffix}"


def generate_short_id(prefix: str, length: int = 8) -> str:
    """
    Generate shorter ID for specific use cases
    """
    import string
    chars = string.ascii_lowercase + string.digits
    suffix = ''.join(random.choice(chars) for _ in range(length))
    return f"{prefix}_{suffix}"


def generate_batch_ref(prefix: str = "batch") -> str:
    """
    Generate batch reference ID
    """
    import datetime
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    random_hex = f"{random.randint(0, 0xffff):04x}"
    return f"{prefix}_{timestamp}_{random_hex}"