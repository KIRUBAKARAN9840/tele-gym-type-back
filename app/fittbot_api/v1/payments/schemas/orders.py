"""Order-related Pydantic schemas"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

from ..models.enums import Provider, ItemType


class CreateOrderItem(BaseModel):
    """Schema for creating an order item"""
    sku: Optional[str] = None
    item_type: ItemType
    qty: int = Field(default=1, ge=1)
    gym_id: Optional[str] = None
    trainer_id: Optional[str] = None
    title: Optional[str] = None
    unit_price_minor: Optional[int] = Field(None, ge=0)
    metadata: Optional[Dict[str, Any]] = None
    
    class Config:
        use_enum_values = True


class CreateOrderRequest(BaseModel):
    """Schema for creating an order"""
    order_id: str = Field(..., min_length=1, max_length=100)
    customer_id: str = Field(..., min_length=1, max_length=100)
    provider: Provider
    currency: str = Field(default="INR", min_length=3, max_length=3)
    items: List[CreateOrderItem] = Field(..., min_items=1)
    
    class Config:
        use_enum_values = True


class OrderItemResponse(BaseModel):
    """Schema for order item response"""
    id: str
    item_type: str
    sku: Optional[str]
    gym_id: Optional[str]
    trainer_id: Optional[str]
    title: Optional[str]
    unit_price_minor: int
    qty: int
    metadata: Optional[Dict[str, Any]]
    
    @property
    def unit_price_rupees(self) -> float:
        return self.unit_price_minor / 100.0
    
    @property
    def total_amount_minor(self) -> int:
        return self.unit_price_minor * self.qty
    
    @property
    def total_amount_rupees(self) -> float:
        return self.total_amount_minor / 100.0
    
    class Config:
        from_attributes = True


class OrderResponse(BaseModel):
    """Schema for order response"""
    id: str
    customer_id: str
    currency: str
    provider: str
    gross_amount_minor: int
    status: str
    items: List[OrderItemResponse] = []
    
    @property
    def gross_amount_rupees(self) -> float:
        return self.gross_amount_minor / 100.0
    
    class Config:
        from_attributes = True