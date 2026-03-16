"""
SQLAlchemy Models for Webhook Idempotency System
Models for the three new idempotency tables in payments schema
"""

from sqlalchemy import Column, String, DateTime, Text, Integer, Boolean, DECIMAL, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import uuid

# Use the same base as other payment models
try:
    from .base import Base
except ImportError:
    # Fallback if base doesn't exist
    Base = declarative_base()

class WebhookProcessingLog(Base):
    """
    Core idempotency table - prevents duplicate webhook processing
    
    This is the KEY table that prevents duplicate webhook processing.
    Every webhook processing attempt gets logged here with a unique event_id.
    """
    __tablename__ = "webhook_processing_logs"
    __table_args__ = {'schema': 'payments'}
    
    # Primary key
    id = Column(String(255), primary_key=True)
    
    # Idempotency fields (MOST IMPORTANT)
    event_id = Column(String(255), unique=True, nullable=False, index=True,
                     comment="RevenueCat event ID - prevents duplicate processing")
    event_type = Column(String(255), nullable=False, index=True)
    
    # Processing details
    customer_id = Column(String(255), nullable=False, index=True,
                        comment="Customer who triggered webhook")
    status = Column(String(255), nullable=False, default='processing', index=True)
    
    # Timing fields
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime, nullable=True, index=True)
    processing_duration_ms = Column(Integer, nullable=True,
                                  comment="Processing time in milliseconds")
    
    # Data fields for debugging
    raw_event_data = Column(Text, nullable=True,
                          comment="Full webhook JSON for debugging")
    result_summary = Column(Text, nullable=True,
                          comment="What was created/updated")
    error_message = Column(Text, nullable=True,
                         comment="Error details if failed")
    
    # Metadata fields
    retry_count = Column(Integer, default=0)
    is_recovery_event = Column(Boolean, default=False, index=True,
                             comment="TRUE if from recovery system")
    webhook_source = Column(String(50), default='revenuecat')
    
    # Audit timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Additional indexes for common queries
    __table_args__ = (
        Index('idx_customer_event_type', 'customer_id', 'event_type'),
        Index('idx_status_started', 'status', 'started_at'),
        Index('idx_customer_status_date', 'customer_id', 'status', 'started_at'),
        {'schema': 'payments'}
    )
    
    def __repr__(self):
        return f"<WebhookProcessingLog {self.event_id}: {self.event_type} - {self.status}>"
    
    @classmethod
    def generate_id(cls):
        """Generate a unique ID for processing log"""
        return f"whl_{int(datetime.now().timestamp())}"
    
    @classmethod
    def create_event_id(cls, event_data):
        """Create event ID from webhook data"""
        # Use RevenueCat's event ID if available, otherwise generate one
        if 'id' in event_data:
            return event_data['id']
        
        customer_id = event_data.get('app_user_id', 'unknown')
        transaction_id = event_data.get('transaction_id', 'unknown')
        timestamp = int(datetime.now().timestamp())
        
        return f"{customer_id}_{transaction_id}_{timestamp}"

class WebhookRecoveryLog(Base):
    """
    Recovery tracking table - logs webhook recovery actions
    
    This table tracks when the webhook recovery system takes action
    to recover missed or failed webhooks.
    """
    __tablename__ = "webhook_recovery_logs"
    __table_args__ = {'schema': 'payments'}
    
    # Primary key
    id = Column(String(255), primary_key=True)
    
    # Recovery target
    order_id = Column(String(255), nullable=True, index=True,
                     comment="Order that was recovered")
    customer_id = Column(String(255), nullable=False, index=True)
    
    # Recovery details
    recovery_reason = Column(String(100), nullable=False, index=True,
                           comment="missed_webhook, payment_failed, billing_error, etc.")
    original_webhook_type = Column(String(50), nullable=True,
                                 comment="INITIAL_PURCHASE, RENEWAL, etc.")
    recovery_action = Column(String(100), nullable=False,
                           comment="simulated_webhook, marked_as_failed, manual_intervention")
    
    # Timing
    recovered_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    original_order_created_at = Column(DateTime, nullable=True)
    
    # Data
    recovery_data = Column(Text, nullable=True,
                         comment="JSON recovery details")
    revenuecat_customer_info = Column(Text, nullable=True,
                                    comment="Customer info from RevenueCat API")
    
    # Success tracking
    recovery_successful = Column(Boolean, default=True, index=True)
    recovery_error_message = Column(Text, nullable=True)
    
    # Audit timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Additional indexes
    __table_args__ = (
        Index('idx_customer_recovered', 'customer_id', 'recovered_at'),
        {'schema': 'payments'}
    )
    
    def __repr__(self):
        return f"<WebhookRecoveryLog {self.id}: {self.recovery_reason} for {self.customer_id}>"
    
    @classmethod
    def generate_id(cls):
        """Generate a unique ID for recovery log"""
        return f"wrl_{int(datetime.now().timestamp())}"
    
    @property
    def recovery_delay_minutes(self):
        """Calculate recovery delay in minutes"""
        if self.original_order_created_at and self.recovered_at:
            delta = self.recovered_at - self.original_order_created_at
            return int(delta.total_seconds() / 60)
        return None

class WebhookMonitoringStats(Base):
    """
    Monitoring statistics table - hourly aggregated webhook stats
    
    This table stores hourly statistics for monitoring webhook system health,
    success rates, and performance metrics.
    """
    __tablename__ = "webhook_monitoring_stats"
    __table_args__ = {'schema': 'payments'}
    
    # Primary key
    id = Column(String(255), primary_key=True)
    date_hour = Column(DateTime, nullable=False, unique=True, index=True,
                      comment="Hour being tracked (YYYY-MM-DD HH:00:00)")
    
    # Processing counts
    total_webhooks_received = Column(Integer, default=0)
    webhooks_processed_successfully = Column(Integer, default=0)
    webhooks_failed = Column(Integer, default=0)
    duplicate_webhooks_blocked = Column(Integer, default=0)
    
    # Event type breakdown
    initial_purchase_count = Column(Integer, default=0)
    renewal_count = Column(Integer, default=0)
    cancellation_count = Column(Integer, default=0)
    expiration_count = Column(Integer, default=0)
    
    # Recovery stats
    recovery_events_processed = Column(Integer, default=0)
    orders_recovered = Column(Integer, default=0)
    
    # Performance stats
    avg_processing_time_ms = Column(DECIMAL(10, 2), default=0)
    max_processing_time_ms = Column(Integer, default=0)
    
    # Audit timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<WebhookMonitoringStats {self.date_hour}: {self.total_webhooks_received} total>"
    
    @classmethod
    def generate_id(cls, date_hour=None):
        """Generate ID for stats record"""
        if not date_hour:
            date_hour = datetime.now()
        return f"stats_{date_hour.strftime('%Y%m%d_%H')}"
    
    @classmethod
    def get_current_hour_stats(cls, db_session):
        """Get or create stats record for current hour using upsert to prevent deadlocks"""
        from sqlalchemy.exc import IntegrityError
        from sqlalchemy import text

        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        stats_id = cls.generate_id(current_hour)

        # First try to get existing record
        stats = db_session.query(cls).filter(cls.id == stats_id).first()

        if stats:
            return stats

        # Use INSERT ... ON DUPLICATE KEY UPDATE to handle concurrent inserts
        try:
            db_session.execute(
                text("""
                    INSERT INTO payments.webhook_monitoring_stats
                    (id, date_hour, total_webhooks_received, webhooks_processed_successfully,
                     webhooks_failed, duplicate_webhooks_blocked, initial_purchase_count,
                     renewal_count, cancellation_count, expiration_count,
                     recovery_events_processed, orders_recovered, avg_processing_time_ms,
                     max_processing_time_ms, created_at, updated_at)
                    VALUES (:id, :date_hour, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE updated_at = NOW()
                """),
                {"id": stats_id, "date_hour": current_hour}
            )
            db_session.flush()
        except IntegrityError:
            # Another worker already inserted, rollback and continue
            db_session.rollback()
        except Exception:
            # Ignore any other errors - stats are non-critical
            pass

        # Fetch the record (either we created it or another worker did)
        stats = db_session.query(cls).filter(cls.id == stats_id).first()

        # If still not found (shouldn't happen), create a transient instance
        if not stats:
            stats = cls(
                id=stats_id,
                date_hour=current_hour
            )

        return stats
    
    @property
    def success_rate(self):
        """Calculate success rate percentage"""
        if self.total_webhooks_received == 0:
            return 0.0
        return (self.webhooks_processed_successfully / self.total_webhooks_received) * 100
    
    @property
    def failure_rate(self):
        """Calculate failure rate percentage"""
        if self.total_webhooks_received == 0:
            return 0.0
        return (self.webhooks_failed / self.total_webhooks_received) * 100

# Export all models
__all__ = [
    'WebhookProcessingLog',
    'WebhookRecoveryLog', 
    'WebhookMonitoringStats'
]