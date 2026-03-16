"""Payout related schemas"""

from typing import List
from datetime import date
from pydantic import BaseModel, Field

from ..models.enums import PayoutMode


class PayoutRunRequest(BaseModel):
    """Schema for payout run request"""
    date: date
    payout_mode: PayoutMode = PayoutMode.UPI
    
    class Config:
        use_enum_values = True


class PayoutBatchResponse(BaseModel):
    """Schema for payout batch response"""
    batches: List[str]
    status: str