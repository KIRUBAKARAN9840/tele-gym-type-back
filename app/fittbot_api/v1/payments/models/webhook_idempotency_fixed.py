"""
FIXED SQLAlchemy Models for Webhook Idempotency System
Uses your main database 'latest' and proper Base class from app.models.database
"""

from sqlalchemy import Column, String, DateTime, Text, Integer, Boolean, DECIMAL, UniqueConstraint, Index
from datetime import datetime
from app.models.database import Base  # Use your main app's Base class

class WebhookProcessingLog(Base):
    """
    Core idempotency table - prevents duplicate webhook processing
    
    FIXED: Uses main 'latest' database, NOT separate 'payments' schema
    """
    __tablename__ = "webhook_processing_logs"
    # REMOVED: __table_args__ = {'schema': 'payments'} - this was wrong!
    
    # Primary key
    id = Column(String(255), primary_key=True)
    
    # Idempotency fields (MOST IMPORTANT)
    event_id = Column(String(255), unique=True, nullable=False, index=True,
                     comment="RevenueCat event ID - prevents duplicate processing")
    event_type = Column(String(255), nullable=False, index=True)
    
    # Processing details
    customer_id = Column(String(255), nullable=False, index=True,
                        comment="Customer who triggered webhook")
    status = Column(String(20), nullable=False, default='processing', index=True,
                   comment="processing, completed, failed")
    
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
    
    FIXED: Uses main 'latest' database, NOT separate 'payments' schema
    """
    __tablename__ = "webhook_recovery_logs"
    # REMOVED: __table_args__ = {'schema': 'payments'} - this was wrong!
    
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
    
    FIXED: Uses main 'latest' database, NOT separate 'payments' schema
    """
    __tablename__ = "webhook_monitoring_stats"
    # REMOVED: __table_args__ = {'schema': 'payments'} - this was wrong!
    
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
        """Get or create stats record for current hour"""
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        stats_id = cls.generate_id(current_hour)
        
        stats = db_session.query(cls).filter(cls.id == stats_id).first()
        
        if not stats:
            stats = cls(
                id=stats_id,
                date_hour=current_hour
            )
            db_session.add(stats)
            db_session.flush()
        
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