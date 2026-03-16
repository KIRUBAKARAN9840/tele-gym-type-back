"""Payment utilities package"""

import uuid
from datetime import datetime

def generate_unique_id(prefix: str = "") -> str:
    """Generate unique ID with timestamp and UUID"""
    timestamp = int(datetime.now().timestamp())
    unique_id = str(uuid.uuid4())[:8]
    if prefix:
        return f"{prefix}_{timestamp}_{unique_id}"
    return f"{timestamp}_{unique_id}"

from .async_db_wrapper import run_sync_db_operation, shutdown_db_executor, get_thread_pool_stats
from .idempotency import require_idempotency
from .webhook_verifier import verify_webhook_signature

__all__ = [
    "generate_unique_id",
    "require_idempotency",
    "verify_webhook_signature",
    "run_sync_db_operation",
    "shutdown_db_executor",
    "get_thread_pool_stats",
]
