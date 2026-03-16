"""Idempotency models for request deduplication"""

from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import String, Integer, LargeBinary, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class IdempotencyKey(Base, TimestampMixin):
    """Idempotency keys for request deduplication"""
    
    __tablename__ = "idempotency_keys"
    __table_args__ = (
        Index('idx_idempotency_keys_expires_at', 'expires_at'),
        Index('idx_idempotency_keys_request_hash', 'request_hash'),
        {"schema": "payments"}
    )
    
    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    request_hash: Mapped[Optional[str]] = mapped_column(String(64))  # SHA256 hash
    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    response_body: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    def __repr__(self) -> str:
        return f"<IdempotencyKey(key={self.key}, status={self.response_status})>"
    
    @property
    def is_expired(self) -> bool:
        """Check if idempotency key has expired"""
        if not self.expires_at:
            return False
        
        return datetime.now(timezone.utc) > self.expires_at
    
    @property
    def has_response(self) -> bool:
        """Check if key has a cached response"""
        return self.response_status is not None and self.response_body is not None
    
    @property
    def is_successful_response(self) -> bool:
        """Check if cached response indicates success"""
        return bool(self.response_status and 200 <= self.response_status < 300)
    
    def set_response(self, status_code: int, response_body: bytes) -> None:
        """Store response for idempotency"""
        self.response_status = status_code
        self.response_body = response_body
    
    def get_response_text(self) -> Optional[str]:
        """Get response body as text"""
        if not self.response_body:
            return None
        
        try:
            return self.response_body.decode('utf-8')
        except UnicodeDecodeError:
            return None