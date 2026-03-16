from sqlalchemy import (
    Column, Integer, String, DateTime, Enum, JSON, Index,
    ForeignKey, func
)
from app.models.database import Base
from datetime import datetime


class ClientActivityEvent(Base):
    """Raw event log for client activity - append-only."""
    __tablename__ = "client_activity_events"
    __table_args__ = (
        # Covers: booking lookup (client+event+product ORDER BY created_at DESC LIMIT 1)
        Index("ix_activity_client_event_product_created", "client_id", "event_type", "product_type", "created_at"),
        # Covers: recent purchase count check (client+event+created_at range)
        Index("ix_activity_client_event_created", "client_id", "event_type", "created_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(
        Integer,
        ForeignKey("clients.client_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type = Column(
        Enum(
            "gym_viewed",
            "dailypass_viewed",
            "session_viewed",
            "membership_viewed",
            "checkout_initiated",
            "checkout_completed",
            "checkout_failed",
        ),
        nullable=False,
    )
    gym_id = Column(Integer, nullable=True)
    product_type = Column(
        Enum("dailypass", "session", "membership", "subscription"),
        nullable=True,
    )
    product_details = Column(JSON, nullable=True)
    source = Column(String(50), nullable=True)
    command_id = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)


class ClientActivitySummary(Base):
    """Aggregated activity per client-gym pair. Upserted by Celery tasks."""
    __tablename__ = "client_activity_summary"
    __table_args__ = (
        Index("uq_activity_summary_client_gym", "client_id", "gym_id", unique=True),
        Index("ix_activity_summary_lead_status", "lead_status"),
        Index("ix_activity_summary_lead_score", "lead_score"),
        Index("ix_activity_summary_last_viewed", "last_viewed_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(
        Integer,
        ForeignKey("clients.client_id", ondelete="CASCADE"),
        nullable=False,
    )
    gym_id = Column(Integer, nullable=False)
    total_views = Column(Integer, default=0, nullable=False)
    last_viewed_at = Column(DateTime, nullable=True)
    checkout_attempts = Column(Integer, default=0, nullable=False)
    last_checkout_at = Column(DateTime, nullable=True)
    purchases = Column(Integer, default=0, nullable=False)
    last_purchase_at = Column(DateTime, nullable=True)
    interested_products = Column(JSON, nullable=True)
    lead_score = Column(Integer, default=0, nullable=False)
    lead_status = Column(
        Enum("cold", "warm", "hot", "converted"),
        default="cold",
        nullable=False,
    )
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.now,
        onupdate=datetime.now,
        nullable=False,
    )


class ClientWhatsAppLog(Base):
    """Track all WhatsApp messages sent to clients."""
    __tablename__ = "client_whatsapp_log"
    __table_args__ = (
        Index("ix_wa_log_client_created", "client_id", "created_at"),
        Index("ix_wa_log_client_trigger", "client_id", "trigger_type", "created_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(
        Integer,
        ForeignKey("clients.client_id", ondelete="CASCADE"),
        nullable=False,
    )
    trigger_type = Column(
        Enum("abandoned_checkout", "repeated_browsing", "booking_confirmation", "manual_telecaller", "promotional"),
        nullable=False,
    )
    template_name = Column(String(100), nullable=False)
    variables = Column(JSON, nullable=True)
    gym_id = Column(Integer, nullable=True)
    whatsapp_guid = Column(String(255), nullable=True)
    status = Column(
        Enum("sent", "failed", "delivered", "read"),
        default="sent",
        nullable=False,
    )
    sent_by = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
