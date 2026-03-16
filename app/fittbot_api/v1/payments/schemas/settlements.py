"""Settlement related schemas"""

from typing import List, Dict
from datetime import date
from pydantic import BaseModel, Field

from ..models.enums import Provider


class ReconImportRequest(BaseModel):
    """Schema for reconciliation import request"""
    settlement_date: date
    provider: Provider = Provider.razorpay_pg
    items: List[Dict[str, str]] = Field(..., min_items=1)
    
    class Config:
        use_enum_values = True


class SettlementResponse(BaseModel):
    """Schema for settlement response"""
    settlement_id: str
    items: int