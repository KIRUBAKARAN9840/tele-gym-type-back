"""Webhook related schemas"""

from typing import Dict, Any
from pydantic import BaseModel


class RazorpayXWebhook(BaseModel):
    """Schema for RazorpayX webhook payload"""
    event: str
    payload: Dict[str, Any]


class RevenueCatWebhook(BaseModel):
    """Schema for RevenueCat webhook payload"""
    event: Dict[str, Any]