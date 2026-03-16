"""Webhook event models"""

from typing import Optional, Dict, Any
from sqlalchemy import String, Boolean, Text, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import TimestampMixin
from app.models.database import Base


class WebhookEvent(Base, TimestampMixin):
    """Webhook events from payment providers"""
    
    __tablename__ = "webhook_events"
    __table_args__ = (
        Index('idx_webhook_events_provider', 'provider'),
        Index('idx_webhook_events_event_type', 'event_type'),
        Index('idx_webhook_events_external_id', 'external_event_id'),
        Index('idx_webhook_events_processed', 'processed'),
        Index('idx_webhook_events_verified', 'verified'),
        Index('idx_webhook_events_created_at', 'created_at'),
        {"schema": "payments"}
    )
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    external_event_id: Mapped[Optional[str]] = mapped_column(String(100))
    signature: Mapped[Optional[str]] = mapped_column(String(500))
    verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    payload_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    
    def __repr__(self) -> str:
        return f"<WebhookEvent(id={self.id}, provider={self.provider}, event_type={self.event_type})>"
    
    @property
    def is_verified(self) -> bool:
        """Check if webhook signature was verified"""
        return self.verified
    
    @property
    def is_processed(self) -> bool:
        """Check if webhook was successfully processed"""
        return self.processed
    
    @property
    def has_error(self) -> bool:
        """Check if webhook processing had errors"""
        return bool(self.error_message)
    
    @property
    def processing_status(self) -> str:
        """Get human-readable processing status"""
        if not self.verified:
            return "Not Verified"
        elif self.has_error:
            return "Failed"
        elif self.processed:
            return "Processed"
        else:
            return "Pending"
    
    def mark_as_processed(self) -> None:
        """Mark webhook as successfully processed"""
        self.processed = True
        self.error_message = None
    
    def mark_as_failed(self, error: str) -> None:
        """Mark webhook as failed with error message"""
        self.processed = False
        self.error_message = error
    
    def mark_as_verified(self) -> None:
        """Mark webhook signature as verified"""
        self.verified = True