"""Idempotency utilities for request deduplication"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session

from ..models import IdempotencyKey
from ..config.settings import get_payment_settings


def require_idempotency(db: Session, key: str, request_payload: Dict[str, Any]) -> None:
    """Check idempotency and prevent duplicate requests"""
    settings = get_payment_settings()
    
    # Generate request hash
    request_hash = hashlib.sha256(
        json.dumps(request_payload, sort_keys=True).encode()
    ).hexdigest()
    
    # Check if key exists
    existing_key = db.query(IdempotencyKey).filter(IdempotencyKey.key == key).first()
    
    if existing_key:
        # Check if expired
        if existing_key.is_expired:
            # Delete expired key and continue
            db.delete(existing_key)
            db.commit()
        else:
            # Key exists and not expired - this is a duplicate
            if existing_key.has_response:
                # Return cached response if available
                raise HTTPException(
                    status_code=existing_key.response_status or 409,
                    detail=existing_key.get_response_text() or "Duplicate request"
                )
            else:
                # Key exists but no cached response
                raise HTTPException(
                    status_code=409, 
                    detail="Duplicate request (Idempotency-Key exists)"
                )
    
    # Create new idempotency key
    expires_at = datetime.now(timezone.utc) + settings.idempotency_ttl_delta
    
    idem_key = IdempotencyKey(
        key=key,
        request_hash=request_hash,
        expires_at=expires_at
    )
    
    db.add(idem_key)
    db.commit()


def cache_response(
    db: Session, 
    key: str, 
    status_code: int, 
    response_body: bytes
) -> None:
    """Cache response for idempotency key"""
    idem_key = db.query(IdempotencyKey).filter(IdempotencyKey.key == key).first()
    
    if idem_key:
        idem_key.set_response(status_code, response_body)
        db.commit()