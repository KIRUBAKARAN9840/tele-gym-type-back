"""Refund related schemas"""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field

from ..models.enums import Provider


class RefundCreateRequest(BaseModel):
    """Schema for refund creation request"""
    refund_id: str = Field(..., min_length=1, max_length=100)
    payment_id: str = Field(..., min_length=1, max_length=100)
    entitlement_id: Optional[str] = Field(None, max_length=100)
    amount_minor: int = Field(..., ge=0)
    provider: Provider
    reason: Optional[str] = None
    processed_at: Optional[datetime] = None
    
    class Config:
        use_enum_values = True


class RefundResponse(BaseModel):
    """Schema for refund response"""
    refund_id: str