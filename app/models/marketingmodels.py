# marketingmodels.py

from sqlalchemy import Column, Integer, String, Date, DateTime, Enum, Boolean, JSON, ForeignKey, Float, Text, Time, Index
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import relationship
from app.models.database import Base
from datetime import datetime
import uuid

class Managers(Base):
    __tablename__ = "managers"
    __table_args__ = {"schema": "marketing_latest"}


    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    employee_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(25), index=True)
    email = Column(String(100), unique=True, index=True)
    contact = Column(String(20), unique=True, index=True)
    profile = Column(String(255), default='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png')
    password = Column(String(255), nullable=False)
    dob = Column(Date, nullable=False)
    age = Column(Integer)
    gender = Column(Enum("male", "female"))
    role = Column(String(30))
    joined_date = Column(Date)
    status = Column(Enum("active", "inactive"))
    uuid = Column(String(255), unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    emp_id = Column(String(50))
    access = Column(Boolean, default=True)
    expo_token = Column(MutableList.as_mutable(JSON))
    refresh_token = Column(String(255))
    assigned = Column(JSON)  # For storing location->zone assignments like {"Bangalore": ["South"], "Chennai": ["South", "North", "East"]}
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    


class Executives(Base):
    __tablename__ = "executives"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    employee_id = Column(Integer, ForeignKey("fittbot_admins.employees.id", ondelete="CASCADE"), nullable=False, index=True)
    manager_id = Column(Integer, ForeignKey("marketing_latest.managers.id",  ondelete="SET NULL"), nullable=True)
    name = Column(String(25), index=True)
    email = Column(String(100), unique=True, index=True)
    contact = Column(String(20), unique=True, index=True)
    profile = Column(String(255), default='https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png')
    password = Column(String(255), nullable=False)
    dob = Column(Date, nullable=False)
    age = Column(Integer)
    gender = Column(Enum("male", "female"))
    role = Column(String(30))
    joined_date = Column(Date)
    status = Column(Enum("active", "inactive"))
    uuid = Column(String(255), unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    emp_id = Column(String(50)) 
    access = Column(Boolean, default=True)
    expo_token = Column(MutableList.as_mutable(JSON))
    refresh_token = Column(String(255))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    parent = relationship("Managers", backref="executives", passive_deletes=True)


class GymVisits(Base):
    __tablename__ = 'gym_visits'
    __table_args__ = (
        Index("ix_gym_visits_referal_id", "referal_id"),
        Index("ix_gym_visits_manager_date", "manager_id", "created_at"),
        Index("ix_gym_visits_user_date", "user_id", "created_at"),
        {"schema": "marketing_latest"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("marketing_latest.executives.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    manager_id=Column(Integer)
    gym_id = Column(Integer, ForeignKey('marketing_latest.gym_database.id', ondelete='SET NULL'), nullable=True)
    start_date = Column(DateTime, default=datetime.now)
    gym_name = Column(String(255), nullable=False)
    gym_address = Column(Text)    
    referal_id = Column(String(15))
    assigned_date = Column(DateTime, nullable=True)
    assigned_on = Column(DateTime, nullable=True)
    contact_person = Column(String(255), nullable=False)
    contact_phone = Column(String(20), nullable=False)
    inquired_person1 = Column(String(255))
    inquired_phone1 = Column(String(20))
    inquired_person2 = Column(String(255))
    inquired_phone2 = Column(String(20))
    visit_type = Column(Enum(
        'sales_call', 
        'follow_up', 
        'partnership', 
        'promotion', 
        'review', 
        'demo', 
        'other',
        name='visit_type_enum'
    ), default='sales_call')
    status = Column(Enum('assigned', 'completed', 'cancelled', name='planned_visit_status'), default='assigned')
    notes = Column(Text)
    visit_purpose = Column(String(100))
    visit_purpose_other = Column(Text)
    check_in_time = Column(DateTime)
    check_in_location = Column(JSON) 
    exterior_photo = Column(String(500))  
    attendance_selfie = Column(String(500))
    facility_photos = Column(JSON)
    gym_size = Column(String(50))
    total_member_count = Column(Integer)
    active_member_count = Column(Integer)
    expected_member_count = Column(Integer)
    conversion_probability = Column(String(50))
    operating_hours = Column(JSON)  
    current_tech = Column(Text)
    people_met = Column(Text)
    meeting_duration = Column(String(50))
    presentation_given = Column(Boolean, default=False)
    demo_provided = Column(Boolean, default=False)
    interest_level = Column(Integer, default=0)  
    questions_asked = Column(Text)
    objections = Column(Text)
    decision_maker_present = Column(Boolean, default=False)
    decision_timeline = Column(String(100))
    competitors = Column(Text)
    pain_points = Column(Text)
    current_solutions = Column(Text)
    key_benefits = Column(Text)
    next_steps = Column(Text)
    materials_to_send = Column(Text)
    visit_outcome = Column(String(100))
    visit_summary = Column(Text)
    action_items = Column(Text)
    overall_rating = Column(Integer, default=0)
    final_status = Column(Enum('pending', 'followup', 'converted', 'rejected', 'scheduled'), default='pending')
    total_followup_attempts = Column(Integer, default=0)
    last_followup_date = Column(DateTime)
    last_followup_outcome = Column(String(100))
    conversion_stage = Column(Enum(
        'initial', 
        'proposal_sent', 
        'negotiation', 
        'contract_pending',
        'onboarding',
        'active',
        'at_risk',
        'churned',
        name='conversion_stage_enum'
    ), default='initial')
    next_follow_up_date = Column(DateTime)
    rejection_reason = Column(Text)
    next_meeting_date = Column(DateTime)
    follow_up_notes = Column(Text)
    conversion_notes = Column(Text)
    check_out_time = Column(DateTime)
    completed = Column(Boolean, default=False)
    current_step = Column(Integer, default=0)  
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    monthly_leads= Column(Integer,nullable=True)
    monthly_conversion=Column(Integer,nullable=True)
    checklist=Column(JSON,nullable=True)

    def to_dict(self):
        data = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            data[column.name] = value
        return data


class Feedback(Base):
    __tablename__ = 'feedback'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("marketing_latest.gym_visits.id", ondelete="CASCADE"), nullable=False)
    manager_id = Column(Integer, ForeignKey("marketing_latest.managers.id", ondelete="CASCADE"), nullable=False)
    executive_id = Column(Integer, ForeignKey("marketing_latest.executives.id", ondelete="CASCADE"), nullable=False)
    category = Column(Enum(
        'performance', 
        'communication', 
        'professionalism', 
        'preparation', 
        'followup', 
        'followup_agenda',
        'other',
        name='feedback_category_enum'
    ), nullable=False)
    rating = Column(Integer, nullable=False)  
    comments = Column(Text, nullable=False)
    suggestions = Column(Text)
    positive_points = Column(Text)
    improvement_areas = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    visit = relationship("GymVisits", backref="feedback")
    manager = relationship("Managers", backref="submitted_feedback")
    executive = relationship("Executives", backref="received_feedback")


class GymDatabase(Base):
    __tablename__ = 'gym_database'
    __table_args__ = {"schema": "marketing_latest"}

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
    submitted_by_manager = Column(Integer, ForeignKey('marketing_latest.managers.id', ondelete='SET NULL'), nullable=True)
    submitted_by_executive = Column(Integer, ForeignKey('marketing_latest.executives.id', ondelete='SET NULL'), nullable=True)
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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymAssignments(Base):
    __tablename__ = "gym_assignments"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id =Column(Integer, ForeignKey('marketing_latest.gym_database.id', ondelete="CASCADE", onupdate="CASCADE"))    
    referal_id = Column(String(15))
    executive_id = Column(Integer, ForeignKey('marketing_latest.executives.id', ondelete='SET NULL'), nullable=True)
    manager_id = Column(Integer, ForeignKey('marketing_latest.managers.id', ondelete='SET NULL'), nullable=True)
    status = Column(Enum('assigned', 'not_assigned'), default='not_assigned')
    conversion_status = Column(Enum('pending', 'followup', 'converted', 'rejected', 'scheduled'), default='pending')
    assigned_date = Column(DateTime, nullable=True)
    assigned_on = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymServices(Base):
    __tablename__ = 'gym_services'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey('marketing_latest.gym_database.id', ondelete='CASCADE', onupdate="CASCADE"))
    services = Column(JSON, nullable=True)
    amenities = Column(JSON, nullable=True)    
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymPics(Base):
    __tablename__ = 'gym_pics'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id =Column(Integer, ForeignKey('marketing_latest.gym_database.id', ondelete="CASCADE", onupdate="CASCADE"))
    facility_photos = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)

class GymMemberships(Base):
    __tablename__= 'gym_memberships'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id =Column(Integer, ForeignKey('marketing_latest.gym_database.id', ondelete="CASCADE", onupdate="CASCADE"))
    plans = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymDetailRequests(Base):
    __tablename__ = "gym_detail_requests"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("marketing_latest.gym_visits.id", ondelete="CASCADE"), nullable=False)
    manager_id = Column(Integer, ForeignKey("marketing_latest.managers.id", ondelete="CASCADE"), nullable=False)
    executive_id = Column(Integer, ForeignKey("marketing_latest.executives.id", ondelete="CASCADE"), nullable=False)
    gym_name = Column(String(255), nullable=False)
    gym_address = Column(Text)
    referal_id = Column(String(15))
    contact_person = Column(String(255))
    contact_phone = Column(String(20))
    request_reason = Column(Text)
    status = Column(Enum('pending', 'approved', 'rejected', name='request_status_enum'), default='pending')
    admin_notes = Column(Text)
    requested_at = Column(DateTime, default=datetime.now)
    reviewed_at = Column(DateTime)
    reviewed_by = Column(Integer)  
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    visit = relationship("GymVisits", backref="detail_requests")
    manager = relationship("Managers", backref="gym_detail_requests")
    executive = relationship("Executives", backref="gym_detail_requests")

    def to_dict(self):
        return {
            "id": self.id,
            "visit_id": self.visit_id,
            "manager_id": self.manager_id,
            "executive_id": self.executive_id,
            "gym_name": self.gym_name,
            "gym_address": self.gym_address,
            "referal_id":self.referal_id,
            "contact_person": self.contact_person,
            "contact_phone": self.contact_phone,
            "request_reason": self.request_reason,
            "status": self.status,
            "admin_notes": self.admin_notes,
            "requested_at": self.requested_at.isoformat() if self.requested_at else None,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "reviewed_by": self.reviewed_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    

class FollowupAttempts(Base):
    __tablename__ = 'followup_attempts'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("marketing_latest.gym_visits.id", ondelete="CASCADE"), nullable=False)
    attempt_number = Column(Integer, nullable=False)
    followup_date = Column(DateTime, nullable=False)
    followup_type = Column(String(255))
    contact_person = Column(String(255))
    notes = Column(Text)
    outcome = Column(String(255))
    next_action = Column(Text)
    next_followup_date = Column(DateTime)
    duration_minutes = Column(Integer)  
    interest_level = Column(Integer, default=0)  
    decision_maker_involved = Column(Boolean, default=False)
    budget_discussed = Column(Boolean, default=False)
    objections_raised = Column(Text)
    materials_requested = Column(Text)
    created_by = Column(Integer, ForeignKey("marketing_latest.executives.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    visit = relationship("GymVisits", backref="followup_attempts")
    created_by_user = relationship("Executives", backref="created_followups")


class PostConversionActivities(Base):
    __tablename__ = 'post_conversion_activities'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("marketing_latest.gym_visits.id", ondelete="CASCADE"), nullable=False)
    activity_type = Column(Enum(
        'contract_signing',
        'materials_sent',
        'demo_scheduled',
        'demo_completed',
        'training_scheduled', 
        'training_completed',
        'onboarding_started',
        'onboarding_completed',
        'first_payment_received',
        'installation_scheduled',
        'installation_completed',
        'go_live',
        'retention_check_30',
        'retention_check_60',
        'retention_check_90',
        'upsell_opportunity',
        'renewal_discussion',
        'support_ticket',
        'feedback_collection',
        name='activity_type_enum'
    ), nullable=False)
    activity_status = Column(Enum(
        'pending', 
        'in_progress', 
        'completed', 
        'cancelled',
        'overdue',
        'rescheduled',
        name='activity_status_enum'
    ), default='pending')
    priority = Column(Enum('low', 'medium', 'high', 'urgent'), default='medium')
    scheduled_date = Column(DateTime)
    completed_date = Column(DateTime)
    due_date = Column(DateTime)
    assigned_to = Column(Integer, ForeignKey("marketing_latest.executives.id"), nullable=True)
    assigned_by = Column(Integer, ForeignKey("marketing_latest.managers.id"), nullable=True)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    notes = Column(Text)
    outcome = Column(Text)  
    estimated_value = Column(Float) 
    actual_value = Column(Float)  
    documents_attached = Column(JSON)  
    reminder_sent = Column(Boolean, default=False)
    client_feedback = Column(Text)  
    internal_notes = Column(Text) 
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    visit = relationship("GymVisits", backref="post_conversion_activities")
    assigned_to_user = relationship("Executives", foreign_keys=[assigned_to], backref="assigned_activities")
    assigned_by_user = relationship("Managers", foreign_keys=[assigned_by], backref="created_activities")


class ActivityTimeline(Base):
    __tablename__ = 'activity_timeline'
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("marketing_latest.gym_visits.id", ondelete="CASCADE"), nullable=False)
    activity_type = Column(Enum(
        'visit_created',
        'visit_updated', 
        'status_changed',
        'followup_added',
        'followup_completed',
        'converted',
        'post_activity_added',
        'post_activity_completed',
        'feedback_received',
        'note_added',
        name='timeline_activity_enum'
    ), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    meta_data = Column(JSON)  
    performed_by = Column(Integer, ForeignKey("marketing_latest.executives.id"), nullable=True)
    performed_by_manager = Column(Integer, ForeignKey("marketing_latest.managers.id"), nullable=True)
    timestamp = Column(DateTime, default=datetime.now)
    
    visit = relationship("GymVisits", backref="timeline_activities")
    performed_by_user = relationship("Executives", foreign_keys=[performed_by])
    performed_by_manager_user = relationship("Managers", foreign_keys=[performed_by_manager])


class Attendance(Base):
    __tablename__ = "attendance"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    employee_id = Column(String(50), nullable=False, index=True)
    manager_id = Column(String(50), nullable=False, index=True)
    employee_name = Column(String(255), nullable=False)
    manager_name = Column(String(255), nullable=False)
    gym_name = Column(String(255), nullable=False)
    gym_address = Column(Text)
    punchin_time = Column(DateTime)
    punchin_location = Column(JSON)  # Store lat, lng, address as JSON
    punchout_time = Column(DateTime)
    punchout_location = Column(JSON)  # Store lat, lng, address as JSON
    status = Column(Enum("Active", "Completed", name="attendance_status"), default="Active")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class Leave(Base):
    __tablename__ = "leave"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    employee_id = Column(String(50), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    role = Column(String(50), nullable=False)
    reason = Column(Enum("Sick", "Vacation", "Personal", "Emergency", "Marriage", "Funeral", "Travel", "Festival", "Paternity", "Other", name="leave_reason_enum"), nullable=False)
    message = Column(Text)
    leave_from = Column(Date, nullable=True)  # Leave start date
    leave_to = Column(Date, nullable=True)    # Leave end date
    status = Column(Enum("Pending", "Approved", "Rejected", name="leave_status_enum"), default="Pending")
    date_applied = Column(DateTime, default=datetime.now)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


# class Gyms(Base):
#     """
#     Gym model for fittbot_local.gyms table
#     Used for document submission feature
#     """
#     __tablename__ = "gyms"
#     __table_args__ = {"schema": "fittbot_local"}

#     gym_id = Column(Integer, primary_key=True, autoincrement=True, comment='Primary key for gyms table')
#     owner_id = Column(Integer, nullable=True, comment='Foreign key to gym owners')
#     name = Column(String(200), nullable=False, comment='Gym name')
#     location = Column(String(255), nullable=True, comment='Gym location')
#     max_clients = Column(Integer, nullable=True, comment='Maximum number of clients allowed')
#     logo = Column(String(255), nullable=True, comment='Gym logo URL')
#     cover_pic = Column(String(255), nullable=True, comment='Gym cover picture URL')
#     subscription_end_date = Column(Date, nullable=True, comment='Gym subscription end date')
#     subscription_start_date = Column(Date, nullable=True, comment='Gym subscription start date')
#     created_at = Column(DateTime, nullable=True, default=datetime.now, comment='Record creation timestamp')
#     updated_at = Column(DateTime, nullable=True, default=datetime.now, onupdate=datetime.now, comment='Record update timestamp')
#     referal_id = Column(String(15), nullable=True, comment='Referral ID')
#     fittbot_verified = Column(Boolean, nullable=True, default=False, comment='Whether gym is verified by Fittbot')
#     dailypass = Column(Boolean, nullable=True, default=False, comment='Whether daily pass is available')
#     gym_timings = Column(JSON, nullable=True, comment='Gym operating timings in JSON format')
#     contact_number = Column(String(15), nullable=True, comment='Gym contact number')
#     services = Column(JSON, nullable=True, comment='Gym services in JSON format')
#     operating_hours = Column(JSON, nullable=True, comment='Operating hours in JSON format')
#     street = Column(String(255), nullable=True, comment='Street address')
#     area = Column(String(255), nullable=True, comment='Area name')
#     city = Column(String(100), nullable=True, comment='City name')
#     state = Column(String(100), nullable=True, comment='State name')
#     pincode = Column(String(10), nullable=True, comment='Postal code')

#     # Document URL columns for storing gym documents
#     aadhar_url = Column(String(512), nullable=True, comment='URL for Aadhar card document')
#     pan_url = Column(String(512), nullable=True, comment='URL for PAN card document')
#     bankbook_url = Column(String(512), nullable=True, comment='URL for Bank book document')

#     # Tracking columns for document management
#     created_by = Column(String(100), nullable=True, comment='User ID/Name of who created the record')
#     last_updated_by = Column(String(100), nullable=True, comment='User ID/Name of who last updated the record')

#     def __repr__(self):
#         return f"<Gyms(gym_id={self.gym_id}, name='{self.name}', location='{self.location}')>"

#     def to_dict(self):
#         """Convert gym object to dictionary"""
#         return {
#             'id': self.gym_id,
#             'gym_id': self.gym_id,
#             'gym_name': self.name,
#             'name': self.name,
#             'location': self.location,
#             'contact_number': self.contact_number,
#             'street_area': f"{self.street or ''}, {self.area or ''}".strip(', '),
#             'street': self.street,
#             'area': self.area,
#             'city': self.city,
#             'state': self.state,
#             'pincode': self.pincode,
#             'aadhar_url': self.aadhar_url,
#             'pan_url': self.pan_url,
#             'bankbook_url': self.bankbook_url,
#             'logo': self.logo,
#             'cover_pic': self.cover_pic,
#             'max_clients': self.max_clients,
#             'fittbot_verified': self.fittbot_verified,
#             'created_at': self.created_at.isoformat() if self.created_at else None,
#             'updated_at': self.updated_at.isoformat() if self.updated_at else None,
#             'created_by': self.created_by,
#             'last_updated_by': self.last_updated_by
#         }


class ManagerAttendance(Base):
    __tablename__ = "manager_attendance"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    employee_id = Column(String(50), nullable=False, index=True)  # BDM employee ID
    employee_name = Column(String(255), nullable=False)  # BDM employee name
    gym_name = Column(String(255), nullable=False)
    gym_address = Column(Text)
    punchin_time = Column(DateTime)
    punchin_location = Column(JSON)  # Store lat, lng, address as JSON
    punchout_time = Column(DateTime)
    punchout_location = Column(JSON)  # Store lat, lng, address as JSON
    status = Column(Enum("Active", "Completed", name="manager_attendance_status"), default="Active")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def to_dict(self):
        data = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            data[column.name] = value
        return data
    


class LocalGymDocs(Base):
    __tablename__ = "local_gym_docs"
    __table_args__ = {"schema": "marketing_latest"}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    aadhaar_url = Column(String(500), nullable=True)
    aadhaar_back= Column(String(500), nullable=True)
    pan_url = Column(String(500), nullable=True)
    bankbook_url = Column(String(500), nullable=True)
    plan_1 = Column(String(500), nullable=True)
    plan_2 = Column(String(500), nullable=True)
    plan_3 = Column(String(500), nullable=True)
    plan_4 = Column(String(500), nullable=True)
    plan_5 = Column(String(500), nullable=True)
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())  
