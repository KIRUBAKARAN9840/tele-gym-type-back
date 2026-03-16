from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SessionType(str, Enum):
    """Session booking type - custom slots per date or same time for all dates"""
    custom = "custom"
    same_time = "same_time"


class CommandStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class SubscriptionCheckoutRequest(BaseModel):
    plan_sku: str = Field(..., description="Catalog SKU that maps to Razorpay plan_id")
    user_id: Optional[str] = Field(
        default=None,
        description="Optional override, defaults to authenticated user",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Optional client-supplied idempotency key (UUID recommended)",
    )


class SubscriptionCheckoutAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


class SubscriptionVerifyRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_subscription_id: str
    razorpay_signature: str
    user_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class SubscriptionVerifyAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


class CommandStatusResponse(BaseModel):
    request_id: str
    status: CommandStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    updated_at_epoch: int


class SubscriptionCheckoutCommand(BaseModel):
    command_id: Optional[str] = None
    user_id: str
    plan_sku: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None


class SubscriptionVerifyCommand(BaseModel):
    command_id: Optional[str] = None
    razorpay_payment_id: str
    razorpay_subscription_id: str
    razorpay_signature: str
    user_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class RazorpayWebhookPayload(BaseModel):
    event: str
    payload: Dict[str, Any]
    created_at: Optional[int] = None
    signature: Optional[str] = None
    webhook_id: Optional[str] = None


class RevenueCatCreateOrderRequest(BaseModel):
    product_sku: str
    currency: str = "INR"
    client_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class RevenueCatVerifyRequest(BaseModel):
    client_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class RevenueCatCommandAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


class RevenueCatOrderCommand(BaseModel):
    client_id: str
    product_sku: str
    currency: str = "INR"


class RevenueCatVerifyCommand(BaseModel):
    client_id: str


class RevenueCatWebhookCommand(BaseModel):
    signature: str
    raw_body: str


class DailyPassCommandAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


class GymMembershipVerifyRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_order_id: str
    razorpay_signature: str
    reward: Optional[bool] = False
    reward_applied: Optional[int] = 0


class GymMembershipCommandAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


# Session booking payments
class CustomSlotEntry(BaseModel):
    """Single slot entry for custom booking"""
    start_time: str  # e.g., "05:00 PM"
    schedule_id: int


class SessionCheckoutRequest(BaseModel):

    gym_id: int
    client_id: int
    session_id: int
    trainer_id: Optional[int] = None
    sessions_count: int
    reward: bool = False
    idempotency_key: Optional[str] = None
    # REMOVED: is_offer_eligible - now calculated server-side for security

    session_type: SessionType = SessionType.same_time  # "custom" or "same_time"
    scheduled_dates: List[str] = Field(default_factory=list)  # ["2025-12-20", "2025-12-26", ...]
    default_slot: Optional[str] = None  # "02:00 PM" - used when session_type is "same_time"
    custom_slot: Optional[Dict[str, List[Dict[str, Any]]]] = None  # {"2025-12-20": [{"start_time": "...", "schedule_id": ...}]}




class SessionCheckoutAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2


class SessionVerifyRequest(BaseModel):
    razorpay_payment_id: str
    razorpay_order_id: str
    razorpay_signature: str
    # Optional - fetched from SessionPurchase using razorpay_order_id (matches dailypass pattern)
    gym_id: Optional[int] = None
    client_id: Optional[int] = None
    session_id: Optional[int] = None
    trainer_id: Optional[int] = None
    reward: bool = False
    idempotency_key: Optional[str] = None


class SessionVerifyAccepted(BaseModel):
    request_id: str
    status: CommandStatus
    status_url: str
    retry_after_seconds: int = 2
