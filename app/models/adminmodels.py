from sqlalchemy import Column, Integer, String, Date, DateTime, Enum, Boolean, JSON, ForeignKey, Float, Text, Time, UniqueConstraint
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship
from app.models.database import Base
from datetime import datetime
import uuid


class Employees(Base):
    __tablename__ = "employees"
    __table_args__ = {"schema": "fittbot_admins"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    contact = Column(String(20), unique=True, index=True, nullable=False)
    profile = Column(String(255), default='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png')
    password = Column(String(255), nullable=False)
    dob = Column(Date, nullable=False)
    age = Column(Integer)
    gender = Column(Enum("male", "female", "other"))
    department = Column(String(50))  
    designation = Column(String(50))  
    joined_date = Column(Date)
    role=Column(String(100))
    status = Column(Enum("active", "inactive"), default="active")
    uuid = Column(String(255), unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    employee_id = Column(String(50), unique=True, nullable=False)
    manager_role=Column(Boolean, default=False)
    access = Column(Boolean, default=True)
    expo_token = Column(MutableList.as_mutable(JSON))
    refresh_token = Column(String(255))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class Admins(Base):
    __tablename__ = "admins"
    __table_args__ = {"schema": "fittbot_admins"}

    admin_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=True)
    dob=Column(Date)
    age=Column(String(15))
    role=Column(String(100))
    otp=Column(String(45))
    expires_at= Column(DateTime)
    password= Column(String(255))
    contact_number = Column(String(15), unique=True, nullable=False)
    profile = Column(String(255))
    refresh_token=Column(String(255))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class EmployeeRoles(Base):
    __tablename__ = "employee_roles"
    __table_args__ = {"schema": "fittbot_admins"}

    role_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    department = Column(String(50), nullable=False)
    created_by = Column(Integer, ForeignKey("fittbot_admins.admins.admin_id", ondelete="SET NULL"), nullable=True)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("name", "department", name="uq_role_name_department"),
        {"schema": "fittbot_admins"}
    )


class EmployeeAssignments(Base):
    __tablename__ = "employee_assignments"
    __table_args__ = {"schema": "fittbot_admins"}

    assignment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    manager_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    employee_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    assigned_by = Column(Integer, ForeignKey("fittbot_admins.admins.admin_id", ondelete="SET NULL"), nullable=True)
    assignment_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("manager_id", "employee_id", name="uq_manager_employee_assignment"),
        {"schema": "fittbot_admins"}
    )


class TicketAssignment(Base):
    __tablename__ = "ticket_assignments"
    __table_args__ = (
        UniqueConstraint("ticket_id", "ticket_source", "status", name="uq_active_ticket_assignment"),
        {"schema": "fittbot_admins"}
    )

    assignment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ticket_id = Column(Integer, nullable=False, index=True)
    ticket_source = Column(Enum("Fittbot", "Fittbot Business", name="ticket_source"), nullable=False)
    employee_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    assigned_by = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="SET NULL"), nullable=True)
    assigned_date = Column(DateTime, default=datetime.now)
    status = Column(Enum("active", "completed", "reassigned", "inactive", name="assignment_status"), default="active")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    assigned_employee = relationship("Employees", foreign_keys=[employee_id], backref="ticket_assignments")
    assigner = relationship("Employees", foreign_keys=[assigned_by])


class TelecallingGymAssignment(Base):
    __tablename__ = "telecalling_gym_assignments"
    __table_args__ = {"schema": "fittbot_admins"}

    assignment_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    marketing_gym_id = Column(Integer, nullable=False, index=True)  # Reference to marketing_latest.gym_database.id
    fittbot_gym_id = Column(Integer, nullable=True, index=True)     # Reference to gyms.gym_id from fittbot_models
    referal_id = Column(String(15), nullable=False, index=True)     # Links marketing and fittbot gym records
    employee_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    assigned_by = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="SET NULL"), nullable=True)
    assignment_status = Column(Enum("active", "completed", "paused", "cancelled", name="telecalling_assignment_status"), default="active")
    assignment_date = Column(DateTime, default=datetime.now)
    target_clients = Column(Integer, nullable=True)  # Expected number of clients to convert
    priority = Column(Enum("low", "medium", "high", "urgent", name="assignment_priority"), default="medium")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    assigned_employee = relationship("Employees", foreign_keys=[employee_id], backref="telecalling_assignments")
    assigner = relationship("Employees", foreign_keys=[assigned_by])


class TelecallingRetentionTracking(Base):
    __tablename__ = "telecalling_retention_tracking"
    __table_args__ = {"schema": "fittbot_admins"}

    retention_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    assignment_id = Column(Integer, ForeignKey("fittbot_admins.telecalling_gym_assignments.assignment_id", ondelete="CASCADE"), nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)      # Reference to clients.client_id
    fittbot_gym_id = Column(Integer, nullable=False, index=True) # Reference to gyms.gym_id
    referal_id = Column(String(15), nullable=False, index=True)
    retention_period = Column(Enum("7_days", "30_days", "60_days", "90_days", "180_days", "365_days", name="retention_period_enum"), nullable=False)
    retention_status = Column(Enum("active", "churned", "at_risk", "renewed", name="retention_status_enum"), default="active")
    join_date = Column(DateTime, nullable=False)
    last_activity_date = Column(DateTime, nullable=True)
    churn_date = Column(DateTime, nullable=True)
    churn_reason = Column(Text, nullable=True)
    retention_score = Column(Float, nullable=True)  # 0.0 to 1.0 score
    follow_up_required = Column(Boolean, default=False)
    follow_up_date = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    assignment = relationship("TelecallingGymAssignment", backref="retention_tracking")


class Expenses(Base):
    __tablename__ = "expenses"
    __table_args__ = {"schema": "fittbot_admins"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    category = Column(Enum("operational", "marketing", name="expense_category"), nullable=False, index=True)
    expense_type = Column(String(100), nullable=False, index=True)
    amount = Column(Float, nullable=False)
    expense_date = Column(Date, nullable=False, index=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class TaxCompliance(Base):
    __tablename__ = "tax_compliance"
    __table_args__ = {"schema": "fittbot_admins"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    month = Column(String(7), nullable=False, unique=True, index=True)  # Format: 'YYYY-MM'
    gst_paid = Column(Float, default=0.0)
    tds_paid = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class OpeningBalance(Base):
    __tablename__ = "opening_balance"
    __table_args__ = {"schema": "fittbot_admins"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    financial_year = Column(String(9), nullable=False, unique=True, index=True)  # Format: '2020-2021'
    amount = Column(Float, nullable=False, default=0.0)  # Opening balance amount in rupees
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class SupportTicketAssignment(Base):
    __tablename__ = "support_ticket_assignments"
    __table_args__ = {"schema": "fittbot_admins"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ticket_id = Column(Integer, nullable=False, index=True)
    ticket_source = Column(String(50), nullable=False)  # "Fittbot" or "Fittbot Business"
    admin_id = Column(Integer, ForeignKey("fittbot_admins.admins.admin_id", ondelete="CASCADE"), nullable=False, index=True)
    assigned_at = Column(DateTime, default=datetime.now)