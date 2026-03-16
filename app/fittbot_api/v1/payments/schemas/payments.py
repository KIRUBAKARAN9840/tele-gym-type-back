"""Payment-related Pydantic schemas"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field

from ..models.enums import Provider


class VerifyPaymentRequest(BaseModel):
    """Schema for payment verification"""
    payment_id: str = Field(..., min_length=1, max_length=100)
    order_id: str = Field(..., min_length=1, max_length=100)
    provider_order_id: Optional[str] = Field(None, max_length=100)
    amount_minor: int = Field(..., ge=0)
    customer_id: str = Field(..., min_length=1, max_length=100)
    provider: Provider = Provider.razorpay_pg
    captured_at: Optional[datetime] = None
    provider_payment_id: Optional[str] = Field(None, max_length=100)
    signature: Optional[str] = None
    
    class Config:
        use_enum_values = True


class PaymentResponse(BaseModel):
    """Schema for payment response"""
    payment_id: str
    order_id: str
    customer_id: str
    amount_minor: int
    currency: str
    provider: str
    status: str
    entitlements: List[str] = []
    
    @property
    def amount_rupees(self) -> float:
        return self.amount_minor / 100.0
    
    class Config:
        from_attributes = True