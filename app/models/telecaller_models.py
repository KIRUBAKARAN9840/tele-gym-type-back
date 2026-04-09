from sqlalchemy import Column, Integer, String, DateTime, Enum, Boolean, ForeignKey, Text, Index, Date, Float, JSON
from sqlalchemy.orm import relationship
from app.models.database import Base
from datetime import datetime
import uuid

class Manager(Base):
    __tablename__ = "managers"
    __table_args__ = {"schema": "telecaller"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False, index=True)
    mobile_number = Column(String(20), unique=True, nullable=False, index=True)
    status = Column(Enum("active", "inactive", default="active"), nullable=False)
    verified = Column(Boolean, default=False, nullable=False)
    is_super_admin = Column(Integer, default=0, nullable=False)  # 0 = normal manager, 1 = super admin
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # OTP session fields
    otp_session_token = Column(String(255), unique=True, nullable=True)
    otp_session_expires_at = Column(DateTime, nullable=True)
    refresh_token = Column(String(255), unique=True, nullable=True)  # For token refresh flow
    last_login_at = Column(DateTime, nullable=True)
    login_attempts = Column(Integer, default=0, nullable=True)
    locked_until = Column(DateTime, nullable=True)

    # Relationships
    telecallers = relationship("Telecaller", back_populates="manager")
    gym_assignments = relationship("GymAssignment", back_populates="manager")
    gym_assignment_history = relationship("GymAssignmentHistory", back_populates="manager")
    gym_call_logs = relationship("GymCallLogs", back_populates="manager")

class Telecaller(Base):
    __tablename__ = "telecallers"
    __table_args__ = {"schema": "telecaller"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False, index=True)
    mobile_number = Column(String(20), unique=True, nullable=False, index=True)
    status = Column(Enum("active", "inactive", default="active"), nullable=False)
    verified = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # OTP session fields
    otp_session_token = Column(String(255), unique=True, nullable=True)
    otp_session_expires_at = Column(DateTime, nullable=True)
    refresh_token = Column(String(255), unique=True, nullable=True)  # For token refresh flow
    last_login_at = Column(DateTime, nullable=True)
    login_attempts = Column(Integer, default=0, nullable=True)
    locked_until = Column(DateTime, nullable=True)

    # Language information
    language_known = Column(JSON, nullable=True)  # Stores list of languages known by telecaller

    # Relationships
    manager = relationship("Manager", back_populates="telecallers")
    gym_assignments = relationship("GymAssignment", back_populates="telecaller")
    gym_assignment_history = relationship("GymAssignmentHistory", back_populates="telecaller")
    gym_call_logs = relationship("GymCallLogs", back_populates="telecaller", foreign_keys="[GymCallLogs.telecaller_id]")
    assigned_gym_call_logs = relationship("GymCallLogs", foreign_keys="[GymCallLogs.assigned_telecaller_id]")

class GymAssignment(Base):
    __tablename__ = "gym_assignments"
    __table_args__ = (
        Index("ix_gym_assignments_gym_id", "gym_id", unique=True),
        Index("ix_gym_assignments_manager_id", "manager_id"),
        Index("ix_gym_assignments_telecaller_id", "telecaller_id"),
        {"schema": "telecaller"}
    )

    gym_id = Column(Integer, ForeignKey("telecaller.gym_database.id", ondelete="CASCADE"), primary_key=True)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), primary_key=True)
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=False)
    assigned_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    target_date = Column(Date, nullable=True)  # Date for which the gym is assigned
    status = Column(Enum("active", "inactive", default="active"), nullable=False)

    # Relationships
    telecaller = relationship("Telecaller", back_populates="gym_assignments")
    manager = relationship("Manager", back_populates="gym_assignments")

class GymAssignmentHistory(Base):
    __tablename__ = "gym_assignment_history"
    __table_args__ = (
        Index("ix_gym_assign_history_gym_id", "gym_id"),
        Index("ix_gym_assign_history_manager_id", "manager_id"),
        Index("ix_gym_assign_history_telecaller_id", "telecaller_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("telecaller.gym_database.id", ondelete="CASCADE"), nullable=False)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="SET NULL"), nullable=True)
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=False)
    action = Column(Enum("assigned", "unassigned", "reassigned"), nullable=False)
    action_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    remarks = Column(Text, nullable=True)

    # Relationships
    telecaller = relationship("Telecaller", back_populates="gym_assignment_history")
    manager = relationship("Manager", back_populates="gym_assignment_history")

class GymCallLogs(Base):
    __tablename__ = "gym_call_logs"
    __table_args__ = (
        Index("ix_gym_call_logs_gym_id", "gym_id"),
        Index("ix_gym_call_logs_telecaller_id", "telecaller_id"),
        Index("ix_gym_call_logs_manager_id", "manager_id"),
        Index("ix_gym_call_logs_status", "call_status"),
        Index("ix_gym_call_logs_assigned_telecaller_id", "assigned_telecaller_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("telecaller.gym_database.id", ondelete="CASCADE"), nullable=False)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), nullable=False)
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=False)
    assigned_telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="SET NULL"), nullable=True)
    call_status = Column(Enum("pending", "contacted", "interested", "not_interested", "follow_up_required", "follow_up", "rejected", "converted", "no_response","out_of_service", "closed", "delegated"), default="pending", nullable=False)
    remarks = Column(Text, nullable=False)  # Made mandatory
    follow_up_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    followup_alert = Column(DateTime, nullable=True)

    # New fields for call log form
    interest_level = Column(String(20), nullable=True)  # e.g., "High", "Medium", "Low"
    total_members = Column(Integer, nullable=True)
    new_contact_number = Column(String(30), nullable=True)
    feature_explained = Column(Boolean, default=False, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller", back_populates="gym_call_logs", foreign_keys=[telecaller_id])
    manager = relationship("Manager", back_populates="gym_call_logs")
    converted_status = relationship("ConvertedStatus", back_populates="gym_call_log", uselist=False)
    assigned_to_telecaller = relationship("Telecaller", foreign_keys=[assigned_telecaller_id], overlaps="assigned_gym_call_logs")



class PerformanceMetrics(Base):
    __tablename__ = "performance_metrics"
    __table_args__ = (
        Index("ix_performance_metrics_telecaller_date", "telecaller_id", "date"),
        Index("ix_performance_metrics_manager_date", "manager_id", "date"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), nullable=False)
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, default=datetime.utcnow().date(), nullable=False)

    # Daily metrics
    calls_made = Column(Integer, default=0, nullable=False)
    calls_connected = Column(Integer, default=0, nullable=False)
    gyms_interested = Column(Integer, default=0, nullable=False)
    gyms_converted = Column(Integer, default=0, nullable=False)
    followups_scheduled = Column(Integer, default=0, nullable=False)

    # Calculated metrics
    connection_rate = Column(Float, default=0.0, nullable=False)
    conversion_rate = Column(Float, default=0.0, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller")
    manager = relationship("Manager")

class ConvertedStatus(Base):
    __tablename__ = "converted_status"
    __table_args__ = (
        Index("idx_converted_status_telecaller", "telecaller_id"),
        Index("idx_converted_status_gym", "gym_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("telecaller.gym_database.id", ondelete="CASCADE"), nullable=False)
    gym_call_log_id = Column(Integer, ForeignKey("telecaller.gym_call_logs.id", ondelete="CASCADE"), nullable=True)

    # Conversion status flags
    document_uploaded = Column(Boolean, default=False, nullable=False)
    membership_plan_created = Column(Boolean, default=False, nullable=False)
    session_created = Column(Boolean, default=False, nullable=False)
    daily_pass_created = Column(Boolean, default=False, nullable=False)
    gym_studio_images_uploaded = Column(Boolean, default=False, nullable=False)
    agreement_signed = Column(Boolean, default=False, nullable=False)
    biometric_required = Column(Boolean, default=False, nullable=False)
    registered_place = Column(Enum("GYM", "OTHERS", name="registered_place_enum"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller")
    gym_call_log = relationship("GymCallLogs", back_populates="converted_status")


class GymDatabase(Base):
    __tablename__ = 'gym_database'
    __table_args__ = {"schema": "telecaller"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_name = Column(String(255))
    area = Column(String(255))
    city = Column(String(255))
    state = Column(String(255))
    pincode = Column(String(255))
    zone = Column(String(100))  # For storing zone classification (e.g., "South", "North", "East", "West")
    contact_person = Column(String(255), nullable=True)
    contact_phone = Column(String(20), nullable=True)
    address = Column(String(255))
    operating_hours = Column(JSON)
    approval_status = Column(Enum('pending', 'approved', 'rejected', name='approval_status_enum'), default='pending')
    type = Column(String(50), default='Gym')  # Type of fitness center: Gym, Yoga, Pilates, etc.
    submitted_by_manager = Column(Integer, ForeignKey('telecaller.managers.id', ondelete='SET NULL'), nullable=True)
    submitted_by_executive = Column(Integer, nullable=True)  # No FK - executives table not in telecaller schema
    submitter_type = Column(Enum('manager', 'executive', name='submitter_type_enum'), nullable=False)
    approved_by = Column(Integer, nullable=True)
    approval_date = Column(DateTime, nullable=True)
    rejection_reason = Column(Text, nullable=True)
    submission_notes = Column(Text, nullable=True)
    admin_notes = Column(Text, nullable=True)
    referal_id = Column(String(15))
    verified= Column(Boolean, default=False)
    location=Column(String(100))
    self_assigned=Column(Boolean, default=False)
    isprime = Column(Integer, default=0)  # 0 = non-prime, 1 = prime
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class ConvertedBy(Base):
    """Tracks which telecaller converted each gym"""
    __tablename__ = "converted_by"
    __table_args__ = (
        Index("idx_converted_by_gym_id", "gym_id", unique=True),
        Index("idx_converted_by_telecaller_id", "telecaller_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("fittbot_local.gyms.gym_id", ondelete="CASCADE"), nullable=False, unique=True)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller")


class LeaveApplication(Base):
    """Leave applications for managers and telecallers"""
    __tablename__ = "leave_applications"
    __table_args__ = (
        Index("idx_leave_applications_manager_id", "manager_id"),
        Index("idx_leave_applications_telecaller_id", "telecaller_id"),
        Index("idx_leave_applications_status", "status"),
        Index("idx_leave_applications_date_applied", "date_applied"),
        Index("idx_leave_applications_mobile_number", "mobile_number"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Foreign keys - nullable based on who is applying
    # If manager applies: manager_id is set, telecaller_id is NULL
    # If telecaller applies: both manager_id and telecaller_id are set
    manager_id = Column(Integer, ForeignKey("telecaller.managers.id", ondelete="CASCADE"), nullable=True)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), nullable=True)

    # Employee information (for keeping compatibility and quick access)
    mobile_number = Column(String(50), nullable=False, index=True)  # Employee mobile number
    name = Column(String(255), nullable=False)
    role = Column(String(50), nullable=False)  # "manager" or "telecaller"

    # Leave details
    reason = Column(Enum("Sick", "Vacation", "Personal", "Emergency", "Marriage", "Funeral", "Travel", "Festival", "Paternity", "Other", name="leave_reason_enum_telecaller"), nullable=False)
    message = Column(Text, nullable=True)
    leave_from = Column(Date, nullable=True)  # Leave start date
    leave_to = Column(Date, nullable=True)    # Leave end date

    # Status tracking
    status = Column(Enum("Pending", "Approved", "Rejected", name="leave_status_enum_telecaller"), default="Pending", nullable=False)
    date_applied = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    manager = relationship("Manager", foreign_keys=[manager_id])
    telecaller = relationship("Telecaller", foreign_keys=[telecaller_id])


class UserConversion(Base):
    """Tracks which telecaller converted each fittbot user/client"""
    __tablename__ = "user_conversion"
    __table_args__ = (
        Index("idx_user_conversion_client_id", "client_id", unique=True),
        Index("idx_user_conversion_telecaller_id", "telecaller_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(String(50), nullable=False, unique=True, index=True)  # Fittbot client_id
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="SET NULL"), nullable=True)
    purchased_plan = Column(String(255), nullable=True)  # Text field for purchased plan

    # Timestamps
    converted_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller", foreign_keys=[telecaller_id])


class TelecallerNotificationCursor(Base):
    """Tracks per-telecaller watermark for new user notifications.
    Each telecaller has a last_seen_at timestamp – any client created
    after that timestamp is considered 'unseen' for this telecaller."""
    __tablename__ = "telecaller_notification_cursor"
    __table_args__ = {"schema": "telecaller"}

    telecaller_id = Column(
        Integer,
        ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_seen_at = Column(DateTime, nullable=False, default=datetime.now)
    last_count = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller", foreign_keys=[telecaller_id])


class ClientCallFeedback(Base):
    """Tracks telecaller calls with clients – one row per call."""
    __tablename__ = "client_call_feedback"
    __table_args__ = (
        Index("idx_client_call_feedback_client_id", "client_id"),
        Index("idx_client_call_feedback_executive_id", "executive_id"),
        Index("idx_client_call_feedback_status", "status"),
        Index("idx_client_call_feedback_created_at", "created_at"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    executive_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="CASCADE"), nullable=False)
    feedback = Column(Text, nullable=False)
    status = Column(
        Enum("interested", "not_interested", "callback", "no_answer", "converted", "follow_up",
             name="client_call_status_enum"),
        nullable=False,
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    telecaller = relationship("Telecaller", foreign_keys=[executive_id])


class PurchasesByTelecaller(Base):
    """Tracks purchases entered by telecallers for converted clients."""
    __tablename__ = "purchases_by_telecaller"
    __table_args__ = (
        Index("idx_purchases_by_telecaller_client_id", "client_id"),
        Index("idx_purchases_by_telecaller_telecaller_id", "telecaller_id"),
        {"schema": "telecaller"}
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    telecaller_id = Column(Integer, ForeignKey("telecaller.telecallers.id", ondelete="SET NULL"), nullable=True)
    purchased_plan = Column(String(255), nullable=False)
    purchased_date = Column(Date, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationships
    telecaller = relationship("Telecaller")