"""Payment system enums and constants"""

try:
    from enum import StrEnum
except ImportError:  # Python < 3.11
    from enum import Enum

    class StrEnum(str, Enum):
        """Backport of enum.StrEnum for older Python versions."""

        def __new__(cls, value, *args, **kwargs):
            if not isinstance(value, str):
                value = str(value)
            obj = str.__new__(cls, value)
            obj._value_ = value
            return obj


class Provider(StrEnum):
    """Payment providers supported by the system"""
    razorpay_pg = "razorpay_pg"
    revenuecat = "revenuecat"
    google_play = "google_play"


class ItemType(StrEnum):
    """Types of items that can be purchased"""
    daily_pass = "daily_pass"
    direct_booking = "direct_booking"
    pt_session = "pt_session"
    app_subscription = "app_subscription"
    gym_membership = "gym_membership"


class EntType(StrEnum):
    """Entitlement types"""
    visit = "visit"
    session = "session"
    app = "app"
    membership = "membership"


class StatusOrder(StrEnum):
    """Order status states"""
    pending = "pending"
    paid = "paid"
    part_refunded = "part_refunded"
    refunded = "refunded"
    canceled = "canceled"


class StatusPayment(StrEnum):
    """Payment status states"""
    captured = "captured"
    failed = "failed"
    refunded = "refunded"


class StatusEnt(StrEnum):
    """Entitlement status states"""
    pending = "pending"
    active = "active"
    used = "used"
    expired = "expired"
    refunded = "refunded"
    revoked = "revoked"


class StatusCheckin(StrEnum):
    """Check-in status states"""
    ok = "ok"
    duplicate = "duplicate"
    fraud_suspect = "fraud_suspect"


class StatusPayoutLine(StrEnum):
    """Payout line status states"""
    pending = "pending"
    batched = "batched"
    paid = "paid"
    failed = "failed"


class PayoutMode(StrEnum):
    """Supported payout modes for RazorpayX"""
    IMPS = "IMPS"
    UPI = "UPI"
    NEFT = "NEFT"


class WebhookProvider(StrEnum):
    """Webhook providers"""
    razorpay_pg = "razorpay_pg"
    razorpayx = "razorpayx"
    revenuecat = "revenuecat"
    google_play = "google_play"


class RefundStatus(StrEnum):
    """Refund status states"""
    initiated = "initiated"
    processed = "processed"
    failed = "failed"


class DisputeStatus(StrEnum):
    """Dispute status states"""
    open = "open"
    won = "won"
    lost = "lost"
    canceled = "canceled"


class PayoutBatchStatus(StrEnum):
    """Payout batch status states"""
    processing = "processing"
    paid = "paid"
    failed = "failed"
    queued = "queued"


class SubscriptionStatus(StrEnum):
    """Subscription status states"""
    active = "active"
    expired = "expired"
    refunded = "refunded"
    revoked = "revoked"
    canceled = "canceled"
    renewed = "renewed"
