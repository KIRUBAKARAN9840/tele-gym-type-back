"""Check-in related Pydantic schemas"""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class ScanRequest(BaseModel):
    """Schema for check-in scan request"""
    entitlement_id: str = Field(..., min_length=1, max_length=100)
    gym_id: str = Field(..., min_length=1, max_length=100)
    scan_at: datetime
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class CheckinResponse(BaseModel):
    """Schema for check-in response"""
    checkin_id: str
    entitlement_id: str
    gym_id: str
    customer_id: str
    scanned_at: datetime
    status: str
    payout_line_id: Optional[str] = None
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }