from sqlalchemy import Column, Integer,BigInteger,Table,UniqueConstraint, String, Float, Enum, Text, DateTime, ForeignKey, Date,Time,Boolean,JSON,Numeric, Index,func
from sqlalchemy.orm import relationship
from app.models.database import Base
from datetime import datetime
import uuid
from sqlalchemy.ext.mutable import MutableList,MutableDict
from sqlalchemy.ext.mutable import MutableDict 

SESSION_SCHEMA = "sessions"


class Gym(Base):
    __tablename__ = "gyms"

    gym_id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, nullable=True)  
    name = Column(String(200), nullable=False)
    location = Column(String(255), nullable=True)
    type = Column(String(20))
    max_clients = Column(Integer, nullable=True)
    logo = Column(String(255))
    cover_pic=Column(String(255))
    subscription_end_date = Column(Date)
    subscription_start_date = Column(Date)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    referal_id=Column(String(15))
    fittbot_verified=Column(Boolean, default=False)
    dailypass=Column(Boolean, default=False)
    gym_timings=Column(JSON)
    
    # New fields for registration
    contact_number = Column(String(15), nullable=True)
    services = Column(JSON, nullable=True)  # Array of services offered
    operating_hours = Column(JSON, nullable=True)  # Array of operating hour objects
    door_no = Column(String(50), nullable=True)
    building = Column(String(255), nullable=True)
    street = Column(String(255), nullable=True)
    area = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(String(10), nullable=True)
    fitness_type=Column(JSON, nullable=False)  # Array of fitness types (e.g., gym, yoga, crossfit)

    trainer_profiles = relationship("TrainerProfile", back_populates="gym")

    __table_args__ = (
        Index("idx_gym_verified_location", "fittbot_verified", "city", "area", "pincode"),
    )


class NewOffer(Base):
    """
    Gym-level offer flags to control special dailypass/session pricing.
    """
    __tablename__ = "new_offer"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, unique=True, index=True)
    dailypass = Column(Boolean, default=False, nullable=False)
    session = Column(Boolean, default=False, nullable=False)

class NoCostEmi(Base):
    __tablename__ = "no_cost_emi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, index=True)
    no_cost_emi = Column(Boolean, default=False, nullable=False)
    bnpl = Column(Boolean, default=False, nullable=False)

class Reminder(Base):
    __tablename__ = "reminders"
    
    reminder_id = Column(Integer, primary_key=True, index=True, nullable=False)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, index=True)
    gym_id = Column(Integer, nullable=True, index=True)
    reminder_time = Column(Time)
    details = Column(String(500), nullable=False)
    vibration_pattern = Column(JSON, nullable=True)
    reminder_type = Column(String(45))
    is_recurring = Column(Boolean, nullable=False, default=False)
    reminder_Sent=Column(Boolean, nullable=False, default=False)
    queued = Column(Boolean, default=False, nullable=False)
    sent_at = Column(DateTime)
    reminder_mode=Column(String(45))
    intimation_start_time=Column(Time)
    intimation_end_time=Column(Time)
    water_timing=Column(Float)
    water_amount=Column(Integer)
    gym_count=Column(Integer)
    diet_type=Column(String(45))
    title=Column(String(45))
    others_time=Column(DateTime)

class GymOwner(Base):
    __tablename__ = "gym_owners"

    owner_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    refresh_token=Column(String(255))
    contact_number = Column(String(15), unique=True, nullable=False)
    password = Column(String(255), nullable=False)
    profile = Column(String(255))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob=Column(Date)
    age=Column(String(15))
    verification=Column(JSON,nullable=False)
    expo_token = Column(MutableList.as_mutable(JSON))
    incomplete=Column(Boolean, nullable=False, default=False)

class DietTemplate(Base):
    __tablename__ = "diet_template"

    template_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    diet_variant = Column(String(45), nullable=False)
    time_slot = Column(String(20), nullable=False)
    meal_type = Column(String(50), nullable=False)
    diet_type = Column(String(50), nullable=False)
    calories = Column(Integer, nullable=False)
    protein = Column(Integer, nullable=False)
    fat = Column(Integer, nullable=False)
    carbs = Column(Integer, nullable=False)
    notes = Column(String(255), nullable=True)
    fiber = Column(Integer, nullable=True)
    sugar = Column(Integer, nullable=True)
    calcium=Column(Float, nullable=True)
    magnesium =Column(Float, nullable=True)
    potassium =Column(Float, nullable=True)
    Iodine=Column(Float, nullable=True)
    Iron=Column(Float, nullable=True)

class Client(Base):
    __tablename__ = "clients"
 
    client_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=True)
    profile=Column(String(255))
    location = Column(String(255),nullable=True)
    email = Column(String(100),nullable=False)
    contact = Column(String(15), nullable=False)
    password = Column(String(255), nullable=False)
    lifestyle = Column(Text)
    medical_issues = Column(Text)
    batch_id = Column(Integer, nullable=True)
    training_id = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    goals = Column(Text)
    gender = Column(String(20), nullable=True,default="Other")
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    access=Column(Boolean)
    joined_date = Column(Date, default=lambda: datetime.now().date())
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    dob=Column(Date, nullable=True)
    expiry=Column(Enum("joining_date", "start_of_the_month"))
    refresh_token=Column(String(255))
    verification=Column(JSON)
    uuid_client = Column(String(36), unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))
    device_token = Column(MutableList.as_mutable(JSON))
    data_sharing=Column(Boolean)
    pincode = Column(String(10))
    modal_shown = Column(Boolean, default=False)
    platform = Column(String(15))
    
class LiveCount(Base):
    __tablename__ = "live_count"
    id = Column(Integer, primary_key=True,nullable=True,autoincrement=True )
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"))
    count = Column(Integer, nullable=False, default=0)

class WorkoutTemplate(Base):
    __tablename__ = "workout_template"

    template_id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id=Column(Integer, nullable=False)
    template_name=Column(String(60), nullable=False)
    client_id = Column(Integer, nullable=False)
    day = Column(String(20), nullable=False)  
    workout_name = Column(String(100), nullable=False)
    sets = Column(Integer, nullable=False)
    reps = Column(Integer, nullable=False)
    weight_1 = Column(Integer)
    weight_2 = Column(Integer)
    weight_3 = Column(Integer)
    weight_4 = Column(Integer)
    muscle_group = Column(String(50))
    duration = Column(Integer)  
    rest_time = Column(Integer)  
    notes = Column(Text)

class Attendance(Base):
    __tablename__ = "attendance"
    
    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    in_time = Column(Time, nullable=False)
    out_time = Column(Time)
    muscle=Column(JSON)
    in_time_2 = Column(Time)
    out_time_2= Column(Time)
    muscle_2  = Column(JSON)
    in_time_3 = Column(Time)
    out_time_3= Column(Time)
    muscle_3 = Column(JSON)

    __table_args__ = (
        Index("ix_attendance_gym_date", "gym_id", "date"),
        Index("ix_attendance_client_date", "client_id", "date"),
    )

class FeeHistory(Base):
    __tablename__ = "fee_history"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, nullable=False)
    client_id = Column(Integer, nullable=False)
    fees_paid = Column(Float, nullable=False)
    payment_date = Column(Date, nullable=False)
    type= Column(String(45), nullable=False)

class Expenditure(Base):
    __tablename__ = "expenditures"

    expenditure_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, nullable=False)
    expenditure_type = Column(String(100), nullable=False) 
    amount = Column(Float, nullable=False)
    date = Column(Date, nullable=False)

class GymHourlyAgg(Base):
    __tablename__ = "gym_hourly_agg"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    col_4_6 = Column("4-6", Integer, default=0, nullable=False)  
    col_6_8 = Column("6-8", Integer, default=0, nullable=False)  
    col_8_10 = Column("8-10", Integer, default=0, nullable=False) 
    col_10_12 = Column("10-12", Integer, default=0, nullable=False)  
    col_12_14 = Column("12-14", Integer, default=0, nullable=False)  
    col_14_16 = Column("14-16", Integer, default=0, nullable=False)  
    col_16_18 = Column("16-18", Integer, default=0, nullable=False)  
    col_18_20 = Column("18-20", Integer, default=0, nullable=False)  
    col_20_22 = Column("20-22", Integer, default=0, nullable=False)  
    col_22_24 = Column("22-24", Integer, default=0, nullable=False) 

class GymAnalysis(Base):
    __tablename__ = "gym_analysis"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    analysis_type = Column(String(100), nullable=False) 
    analysis_name = Column(String(100), nullable=False) 
    value = Column(Float, nullable=False)
    analysis = Column(MutableDict.as_mutable(JSON), default=dict, nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class GymMonthlyData(Base):
    __tablename__ = "gym_monthly_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    month_year = Column(Date, nullable=False)  
    income = Column(Integer, nullable=False, default=0)
    expenditure = Column(Integer, nullable=False, default=0)
    new_entrants = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class GymPlans(Base):
    __tablename__ = "gym_plans"

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    gym_id = Column(Integer, ForeignKey("gyms.gym_id"), nullable=False)    
    plans = Column(String(50),  nullable=False)
    amount = Column(Integer, nullable=False)
    duration = Column(Integer, nullable=False)
    description = Column(String(255), nullable=True)
    services=Column(JSON, nullable=True)
    personal_training=Column(Boolean, default=False)
    bonus=Column(Integer,nullable=True)
    pause=Column(Integer,nullable=True)
    bonus_type=Column(String(45),nullable=True)
    pause_type=Column(String(45),nullable=True)
    original_amount=Column(Integer,nullable=True)
    plan_for= Column(String(45))
    buddy_count=Column(Integer,nullable=True)
    sessions_count=Column(Integer,nullable=True)

class GymBatches(Base):
    __tablename__ = "gym_batches"

    batch_id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id"), nullable=False)
    batch_name = Column(String(50), nullable=False)
    timing=Column(String(50), nullable=False)
    description = Column(String(255), nullable=True)

class Trainer(Base):
    __tablename__ = "trainers"
 
    trainer_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    full_name = Column(String(100), nullable=False)
    gender = Column(String(20), nullable=False)
    contact = Column(String(15),unique=True, nullable=False)
    email = Column(String(100), nullable=False)
    specializations = Column(JSON, nullable=True)  # Changed from specialization to specializations as JSON array
    experience = Column(Float, nullable=False)  
    certifications = Column(Text, nullable=True)  
    work_timings = Column(JSON, nullable=True)  # Changed from availability to work_timings as JSON array
    profile_image = Column(String(255), nullable=True)  
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    password = Column(String(255), nullable=False)
    refresh_token = Column(String(255), nullable=True)
 
    profiles = relationship("TrainerProfile", back_populates="trainer")

class TrainerProfile(Base):
    __tablename__ = "trainer_profiles"
    profile_id = Column(Integer, primary_key=True, autoincrement=True)
    trainer_id = Column(Integer, ForeignKey("trainers.trainer_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    full_name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=True)  
    specializations = Column(JSON, nullable=True)  # Changed from specialization to specializations as JSON array
    experience = Column(Float, nullable=True)
    certifications = Column(Text, nullable=True)
    work_timings = Column(JSON, nullable=True)  # Changed from availability to work_timings as JSON array
    profile_image = Column(String(255), nullable=True)
    can_view_client_data = Column(Boolean, default=False)
    personal_trainer = Column(Boolean, default=False)
 
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
 
    __table_args__ = (UniqueConstraint("trainer_id", "gym_id", name="uq_trainer_gym"),)
    trainer = relationship("Trainer", back_populates="profiles")
    gym = relationship("Gym", back_populates="trainer_profiles")

class TrainerAttendance(Base):
    __tablename__ = "trainer_attendance"
    
    attendance_id = Column(Integer, primary_key=True, autoincrement=True)
    trainer_id = Column(Integer, ForeignKey("trainers.trainer_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    punch_sessions = Column(JSON, nullable=True)  
    total_hours = Column(Float, default=0.0)
    status = Column(String(20), default="active")  
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    
    __table_args__ = (
        UniqueConstraint("trainer_id", "gym_id", "date", name="uq_trainer_gym_date"),
        Index("idx_trainer_date", "trainer_id", "date"),
        Index("idx_gym_date", "gym_id", "date"),
    )
    
    trainer = relationship("Trainer")
    gym = relationship("Gym")

class TemplateWorkout(Base):
    __tablename__ = "template_workout"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    name = Column(String(45), nullable=False)
    workoutPlan = Column(JSON, nullable=True)  
    notes = Column(Text, nullable=True)

class TemplateDiet(Base):
    __tablename__ = "template_diet"

    template_id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    template_name = Column(String(45), nullable=False)
    template_details = Column(JSON, nullable=True)
    notes = Column(Text, nullable=True)

class Post(Base):
    __tablename__ = "posts"

    post_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, nullable=False, index=True)
      
    client_id = Column(Integer, nullable=True, index=True)  
    content = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    is_pinned = Column(Boolean, default=False)
    status=Column(String(45))

    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")
    likes = relationship("Like", back_populates="post", cascade="all, delete-orphan")
    media = relationship("PostMedia", back_populates="post", cascade="all, delete-orphan")

class PostMedia(Base):
    __tablename__ = "post_media"

    media_id = Column(Integer, primary_key=True, index=True)
    post_id = Column(Integer, ForeignKey("posts.post_id", ondelete="CASCADE"), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_type = Column(String(50), nullable=False)
    file_path = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    status=Column(String(45))


    post = relationship("Post", back_populates="media")

class Comment(Base):
    __tablename__ = "comments"

    comment_id = Column(Integer, primary_key=True, index=True)
    post_id = Column(Integer, ForeignKey("posts.post_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True) 
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    post = relationship("Post", back_populates="comments")

class Like(Base):
    __tablename__ = "likes"

    like_id = Column(Integer, primary_key=True, index=True)
    post_id = Column(Integer, ForeignKey("posts.post_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=True, index=True)  
    created_at = Column(DateTime, default=datetime.now())

    post = relationship("Post", back_populates="likes")

class VoicePreference(Base):
    __tablename__ = "voice_preference"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, unique=True)
    preference = Column(String(1), nullable=False, default='1')  # '1' = voice ON, '0' = voice OFF
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class Food(Base):
    __tablename__ = "fittbot_food"
 
    id = Column(Integer, primary_key=True, index=True)
    categories = Column(String(100), nullable=False)
    item = Column(String(100), nullable=False)
    quantity=Column(String(45), nullable=False)
    pic = Column(Text)
    calories = Column(Integer, nullable=False)
    protein = Column(Float, nullable=False)
    carbs = Column(Float, nullable=False)
    fat = Column(Float, nullable=False)
    fiber = Column(Float, nullable=False)
    sugar = Column(Float, nullable=False)
    added_sugar=Column(Float, nullable=True)
    calcium=Column(Float, nullable=True)
    magnesium =Column(Float, nullable=True)
    potassium =Column(Float, nullable=True)
    sodium=Column(Float, nullable=True)
    iron=Column(Float, nullable=True)
    is_added=Column(Boolean, default=False)
    is_natural=Column(Boolean, default=False)
    is_manual=Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class CustomFood(Base):
    __tablename__ = "custom_food"
   
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False)
    name = Column(String(100), nullable=False)
    quantity = Column(String(45), nullable=False)
    calories = Column(Integer, nullable=False)
    protein = Column(Float, nullable=False)
    carbs = Column(Float, nullable=False)
    fat = Column(Float, nullable=False)
    fiber = Column(Float, nullable=True)
    sugar = Column(Float, nullable=True)
    pic = Column(Text, nullable=True)
    calcium=Column(Float, nullable=True)
    magnesium =Column(Float, nullable=True)
    potassium =Column(Float, nullable=True)
    Iodine=Column(Float, nullable=True)
    Iron=Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class ClientScheduler(Base):
    __tablename__ = "client_scheduler"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    assigned_trainer = Column(Integer, nullable=True)
    assigned_dietplan = Column(Integer, nullable=True)
    assigned_workoutplan = Column(Integer, nullable=True)

class Gym_Feedback(Base):
    __tablename__ = "feedback"
 
    id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    tag=Column(String(100),nullable=False)
    ratings=Column(Integer,nullable=False)
    feedback = Column(Text, nullable=True)
    timing = Column(DateTime, default=datetime.now())


class ClientToken(Base):
    __tablename__ = "support_tokens"
 
    id         = Column(Integer, primary_key=True, autoincrement=True)
    client_id  = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    token      = Column(String(255), nullable=False)
    subject    = Column(String(255))
    email      = Column(String(255))
    issue      = Column(Text)
    followed_up  = Column(Boolean, nullable=False, default=False)
    resolved  = Column(Boolean, nullable=False, default=False)
    comments = Column(Text)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    resolved_at = Column(DateTime(timezone=True))

class Feedback(Base):
    __tablename__ = "ffeedback"
 
    id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    tag=Column(String(100),nullable=False)
    ratings=Column(Integer,nullable=False)
    feedback = Column(Text, nullable=True)
    timing = Column(DateTime, default=datetime.now())


class MuscleAggregatedInsights(Base):
    __tablename__ = "muscle_aggregated_insights"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    client_id = Column(Integer, nullable=False)
    muscle_group = Column(String(100), nullable=True)
    total_volume = Column(Float, nullable=True)
    avg_weight = Column(Float, nullable=True)
    avg_reps = Column(Float, nullable=True)
    max_weight = Column(Float, nullable=True)
    max_reps = Column(Integer, nullable=True)
    rest_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())



class AggregatedInsights(Base):
    __tablename__ = 'aggregated_insights'

    id = Column(Integer, primary_key=True, autoincrement=True,nullable=False, unique=True)
    client_id = Column(Integer, nullable=False)
    week_start= Column(Date, nullable=False)
    total_volume = Column(Float, nullable=False)
    avg_weight = Column(Float, nullable=False)
    avg_reps = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ClientWeeklyPerformance(Base):
    __tablename__ = "client_weekly_performance"

    id = Column(Integer, primary_key=True, autoincrement=True,nullable=False, unique=True)
    client_id = Column(Integer, nullable=False)
    week_start = Column(Date, nullable=False)
    muscle_group = Column(String(50), nullable=True)
    total_volume = Column(Float, nullable=True)
    avg_weight = Column(Float, nullable=True)
    avg_reps = Column(Float, nullable=True)
    # workout_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ClientTarget(Base):
    __tablename__ = "client_targets"

    target_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False)
    calories = Column(Integer, nullable=True)
    protein = Column(Integer, nullable=True)
    carbs = Column(Integer, nullable=True)
    fat = Column(Integer, nullable=True)
    sugar = Column(Integer, nullable=True)
    fiber = Column(Integer, nullable=True)
    steps = Column(Integer, nullable=True)
    calories_to_burn = Column(Integer, nullable=True)
    water_intake = Column(Float, nullable=True)
    sleep_hours = Column(Float, nullable=True)
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    weight = Column(Integer, nullable=True)
    start_weight = Column(Float, nullable=True)
    calcium=Column(Float, nullable=True)
    magnesium =Column(Float, nullable=True)
    potassium =Column(Float, nullable=True)
    Iodine=Column(Float, nullable=True)
    Iron=Column(Float, nullable=True)


class ClientActual(Base):
    __tablename__ = "client_actual"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    weight = Column(Float)
    calories = Column(Integer)
    protein = Column(Integer)
    carbs = Column(Integer)
    fats = Column(Integer)
    sugar = Column(Integer)
    fiber = Column(Integer)
    steps = Column(Integer)
    burnt_calories = Column(Integer)
    water_intake = Column(Float)
    sleep_hours = Column(Float)
    target_calories = Column(Integer, nullable=True)
    target_protein = Column(Integer, nullable=True)
    target_fat = Column(Integer, nullable=True)
    target_carbs = Column(Integer, nullable=True)
    target_sleep_hrs = Column(Float, nullable=True)
    target_water_intake = Column(Float, nullable=True)
    target_steps = Column(Integer, nullable=True)
    calcium=Column(Float, nullable=True)
    magnesium =Column(Float, nullable=True)
    potassium =Column(Float, nullable=True)
    Iodine=Column(Float, nullable=True)
    Iron=Column(Float, nullable=True)
    target_sugar = Column(Integer, nullable=True)
    target_fiber = Column(Integer, nullable=True)
    target_calcium=Column(Float, nullable=True)
    target_magnesium =Column(Float, nullable=True)
    target_potassium =Column(Float, nullable=True)
    target_Iodine=Column(Float, nullable=True)
    target_Iron=Column(Float, nullable=True)



class ClientActualAggregatedWeekly(Base):
    __tablename__ = "client_actual_aggregated_weekly"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, primary_key=True, nullable=False)
    week_start = Column(Date, nullable=False)
    avg_weight = Column(Float)
    avg_calories = Column(Float)
    avg_protein = Column(Float)
    avg_carbs = Column(Float)
    avg_fats = Column(Float)
    total_steps = Column(Integer)
    total_burnt_calories = Column(Integer)
    avg_water_intake = Column(Float)
    avg_sleep_hours = Column(Float)
    avg_sugar = Column(Float)
    avg_fiber = Column(Float)
    avg_calcium=Column(Float, nullable=True)
    avg_magnesium =Column(Float, nullable=True)
    avg_potassium =Column(Float, nullable=True)
    avg_Iodine=Column(Float, nullable=True)
    avg_Iron=Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ClientActualAggregated(Base):
    __tablename__ = "client_actual_aggregated"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, primary_key=True, nullable=False)
    year = Column(Integer, nullable=False)
    avg_weight = Column(Float)
    avg_calories = Column(Float)
    avg_protein = Column(Float)
    avg_carbs = Column(Float)
    avg_fats = Column(Float)
    workout_time= Column(Integer)
    rest_time= Column(Integer)
    gym_time= Column(Integer)
    no_of_days_calories_met = Column(Integer)
    calories_surplus_days = Column(Integer)
    calories_deficit_days = Column(Integer)
    longest_streak = Column(Integer)
    current_streak = Column(Integer)
    average_protein_target = Column(Float)
    average_carbs_target = Column(Float)
    average_fat_target = Column(Float)
    avg_sugar = Column(Float)
    avg_fiber = Column(Float)
    avg_calcium=Column(Float, nullable=True)
    avg_magnesium =Column(Float, nullable=True)
    avg_potassium =Column(Float, nullable=True)
    avg_Iodine=Column(Float, nullable=True)
    avg_Iron=Column(Float, nullable=True)
    average_sugar_target = Column(Float)
    average_fiber_target = Column(Float)
    average_calcium_target=Column(Float, nullable=True)
    average_magnesium_target =Column(Float, nullable=True)
    average_potassium_target =Column(Float, nullable=True)
    average_Iodine_target=Column(Float, nullable=True)
    average_Iron_target=Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ClientGeneralAnalysis(Base):
    __tablename__ = "client_general_analysis"

    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    weight = Column(Float, nullable=True)
    sleep_hrs = Column(Float, nullable=True)
    attendance = Column(Integer, nullable=True)
    water_taken = Column(Float, nullable=True)
    steps_count = Column(Integer, nullable=True)
    burnt_calories = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ActualDiet(Base):
    __tablename__ = "actual_diet"

    record_id = Column(Integer, primary_key=True, autoincrement=True)  
    client_id = Column(Integer, nullable=False)  
    date = Column(Date, nullable=False) 
    diet_data = Column(JSON, nullable=True)  




class ActualWorkout(Base):
    __tablename__ = "actual_workout"

    record_id = Column(Integer, primary_key=True, autoincrement=True)  
    client_id = Column(Integer, nullable=False)  
    date = Column(Date, nullable=False)  
    workout_details = Column(JSON, nullable=True)


class FittbotWorkout(Base):
    __tablename__ = "fittbot_workout"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exercise_data = Column(JSON, nullable=False)



class ClientWorkoutTemplate(Base):
    __tablename__ = "client_workout_template"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    template_name = Column(String(255), nullable=False)
    exercise_data = Column(JSON, nullable=False)

class ClientDietTemplate(Base):
    __tablename__ = "client_diet_template"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    template_name = Column(String(255), nullable=False)
    diet_data = Column(JSON,nullable=False)


class Notification(Base):
    __tablename__ = "notifications"

    notification_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    message_id = Column(Integer, nullable=False)
    role = Column(Enum("owner", "trainer", "client"), nullable=False)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now())


class FcmToken(Base):
    __tablename__ = "fcm_tokens"

    token_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    fcm_token = Column(String(512), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now())

class GymLocation(Base):
    __tablename__ = "gym_location"
    id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, unique=True, index=True, nullable=False)
    latitude = Column(Numeric(10, 8), nullable=False)
    longitude = Column(Numeric(11, 8), nullable=False)
    gym_pic = Column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_gym_location_coordinates", "gym_id", "latitude", "longitude"),
    )

class QRCode(Base):
    __tablename__ = "qr_code"
    id = Column(Integer, primary_key=True, index=True)
    exercises = Column(String(255), nullable=False)
    muscle_group = Column(String(255),nullable=False)
    isMuscleGroup=Column(Boolean, nullable=False)
    isCardio=Column(Boolean, nullable=False)
    isBodyWeight=Column(Boolean, nullable=False)
    gif_path_m=Column(String(255), nullable=True)
    gif_path_f=Column(String(255), nullable=True)
    img_path_m=Column(String(255), nullable=True)
    img_path_f=Column(String(255), nullable=True)


class Message(Base):
    __tablename__ = "messages"

    message_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    sender_id = Column(Integer, nullable=False)
    recipient_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    sender_role = Column(Enum("owner", "trainer", "client"), nullable=False)
    recipient_role = Column(Enum("owner", "trainer", "client"), nullable=False)
    message = Column(Text, nullable=False)
    sent_at = Column(DateTime, default=datetime.now())
    is_read = Column(Boolean,default=False)


class GBMessage(Base):
    __tablename__ = "gb_message"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    session_id = Column(Integer, ForeignKey("gb_sessions.session_id", ondelete="CASCADE"), nullable=False)
    message = Column(Text, nullable=False)
    sent_at = Column(DateTime, default=datetime.now())

class New_Session(Base):
    __tablename__ = "gb_sessions"
    session_id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, autoincrement=True)
    workout_type = Column(JSON, nullable=False)
    session_time = Column(Time, nullable=False)
    session_date = Column(Date, nullable=False)
    host_id = Column(Integer, ForeignKey("clients.client_id"), nullable=False)
    participant_limit = Column(Integer, nullable=False)
    gender_preference = Column(String(20), nullable=False)
 
 
class Participant(Base):
    __tablename__ = "gb_participants"
    participant_id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("gb_sessions.session_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("clients.client_id"), nullable=False)
    proposed_time=Column(Time, nullable=False)
 
 
class JoinProposal(Base):
    __tablename__ = "gb_join_proposals"
    proposal_id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("gb_sessions.session_id"), nullable=False)
    proposer_id = Column(Integer, ForeignKey("clients.client_id"), nullable=False)
    proposed_time=Column(Time, nullable=False)


 
class RejectedProposal(Base):
    __tablename__ = "gb_rejected_proposals"
    rejected_id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("gb_sessions.session_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("clients.client_id"), nullable=False)




class RewardQuest(Base):
    __tablename__ = 'reward_quest'
    id = Column(Integer, primary_key=True, autoincrement=True)
    xp = Column(Integer, nullable=False)
    about = Column(String(255))
    description = Column(Text)
    tag = Column(String(45))

class RewardGym(Base):
    __tablename__ = 'reward_gym'
    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, nullable=False)
    xp = Column(Integer, nullable=False)
    gift = Column(String(500))
    image= Column(String(255))


class RewardClientHistory(Base):
    __tablename__ = 'reward_client_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    date = Column(Date)
    xp = Column(Integer, nullable=False)
    gift = Column(String(255))


class LeaderboardDaily(Base):
    __tablename__ = 'rewards_leaderboard_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=True)
    xp = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)

class LeaderboardMonthly(Base):
    __tablename__ = 'rewards_leaderboard_monthly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=True)
    xp = Column(Integer, nullable=False)
    month = Column(Date, nullable=False)

class LeaderboardOverall(Base):
    __tablename__ = 'rewards_leaderboard_overall'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=True)
    xp = Column(Integer, nullable=False)


class RewardBadge(Base):
    __tablename__ = 'rewards_badges'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    badge = Column(String(50), nullable=False)
    min_points = Column(Integer, nullable=False)
    max_points = Column(Integer, nullable=False)
    image_url = Column(String(255), nullable=False)
    level = Column(String(10), nullable=False)


class RewardPrizeHistory(Base):
    __tablename__ = 'reward_prize_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, nullable=False)
    client_id = Column(Integer, nullable=False)
    xp = Column(Integer, nullable=False)
    gift = Column(String(255), nullable=False )
    achieved_date = Column(DateTime,nullable=False)
    given_date = Column(DateTime,nullable=True)
    client_name = Column(String(50), nullable=False)
    is_given = Column(Boolean,nullable=False)
    profile= Column(String(155))



class CalorieEvent(Base):
    __tablename__ = "calorie_event"
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=True)\
    
    event_date = Column(Date, nullable=True)
    calories_added = Column(Integer, nullable=True)
    workout_added=Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())




class DailyGymHourlyAgg(Base):
    __tablename__ = "daily_gym_hourly_agg"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    agg_date = Column(Date, nullable=False)

    col_4_6 = Column("4-6", Integer, default=0, nullable=False)
    col_6_8 = Column("6-8", Integer, default=0, nullable=False)
    col_8_10 = Column("8-10", Integer, default=0, nullable=False)
    col_10_12 = Column("10-12", Integer, default=0, nullable=False)
    col_12_14 = Column("12-14", Integer, default=0, nullable=False)
    col_14_16 = Column("14-16", Integer, default=0, nullable=False)
    col_16_18 = Column("16-18", Integer, default=0, nullable=False)
    col_18_20 = Column("18-20", Integer, default=0, nullable=False)
    col_20_22 = Column("20-22", Integer, default=0, nullable=False)
    col_22_24 = Column("22-24", Integer, default=0, nullable=False)

    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())



class Avatar(Base):
    __tablename__ = "avatar"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gender = Column(String(45), nullable=False)
    avatarurl = Column(String(255), nullable=False)

class Report(Base):
    __tablename__ ="report"
 
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    user_role = Column(String(20), nullable=False)
    reported_id = Column(Integer, nullable=False)
    reported_role = Column(String(20), nullable=False)
    post_id = Column(Integer, nullable=False)
    reason = Column(Text, nullable=False)
    post_content = Column(Text, nullable=False)
    status = Column( Boolean, nullable=False)

class BlockedUsers(Base):
    __tablename__ = 'blocked_users'
   
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    user_role= Column(String(45),nullable=False)
    blocked_user_id = Column(JSON, nullable=False)


class Preference(Base):
    __tablename__ ="preference"

    preference_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    notifications = Column(Boolean, nullable=False, default=False)
    remainders = Column(Boolean, nullable=False, default=False)
    data_sharing =Column(Boolean, nullable=False, default=False)
    newsletters = Column(Boolean, nullable=False, default=False)
    promos_and_offers = Column(Boolean, nullable=False, default=False)


class RazorpayOrder(Base):
    __tablename__ = "razorpay_orders"

    id         = Column(BigInteger, primary_key=True)
    client_id = Column(Integer)
    plan = Column(Integer)
    order_id   = Column(String(40), unique=True, index=True)
    amount     = Column(Integer)          
    currency   = Column(String(4), default="INR")
    status     = Column(Enum("created", "authorized", "paid", "failed", "refunded", name="order_status"))
    payment_id = Column(String(40), unique=True, nullable=True)
    receipt    = Column(String(64))
    payment_method   = Column(String(32))
    acquirer_ref     = Column(String(64))
    failure_code     = Column(String(32))
    failure_desc     = Column(String(255))
    signature_verified = Column(Boolean, default=False)
    captured_at      = Column(DateTime)
    verified_at      = Column(DateTime)


class RazorpayPayment(Base):
    __tablename__ = "razorpay_payments"

    id = Column(BigInteger, primary_key=True)
    razorpay_event_key = Column(String(120), unique=True, index=True)
    event_type         = Column(String(64))
    payload            = Column(JSON)
    signature          = Column(String(128))



class ClientFittbotAccess(Base):

    __tablename__ = "client_fittbot_access"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    client_id     = Column(Integer,
                           ForeignKey("clients.client_id", ondelete="CASCADE"),
                           nullable=False)
    paid_date     = Column(DateTime, nullable=False)
    plan          = Column(String(100), nullable=False)
    access_status = Column(
        Enum("active", "inactive", name="access_status"),
        nullable=False,
        default="active"
    )
    # registeration_date=Column(DateTime, nullable=True)
    fittbot_plan= Column(Integer, nullable=False)
    free_trial=Column(String(20))
    start_date= Column(Date)
    days_left=Column(Integer)



class AboutToExpire(Base):
    __tablename__ = "about_to_expire"
 
    expiry_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"))
    gym_id = Column(Integer, ForeignKey("gyms.gym_id"), nullable=False)
    gym_client_id=Column(String(45))
    admission_number=Column(String(100))
    expires_in=Column(Integer)
    client_name = Column(String(255))
    gym_name = Column(String(255))
    gym_logo = Column(String(255), nullable=True)
    gym_contact = Column(String(255), nullable=True)
    gym_location = Column(String(255), nullable=True)
    plan_id = Column(Integer, ForeignKey("gym_plans.id", ondelete="CASCADE"))
    plan_description = Column(String(255), nullable=True)
    fees = Column(Float, nullable=True)
    discount = Column(Float, nullable=True)
    discounted_fees = Column(Float, nullable =True)
    due_date = Column(DateTime)
    invoice_number = Column(String(255), nullable=True)
    client_contact = Column(String(20), nullable=True)
    bank_details = Column(String(255), nullable=True)
    ifsc_code = Column(String(255), nullable=True)
    bank_name = Column(String(255))
    upi_id = Column(String(255))
    branch = Column(String(255))
    account_holder_name = Column(String(255), nullable=True)
    gst_number=Column(String(55), default=None)
    paid = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    mail_status = Column(Boolean, default=False)
    expired = Column(Boolean, default=False)
    email = Column(String(55),nullable=False)
 
 
class AccountDetails(Base):
    __tablename__ = "account_details"

    account_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE") ,nullable=False)
    account_number = Column(String(255))
    bank_name = Column(String(255))
    account_ifsccode = Column(String(45))
    account_branch = Column(String(255))
    account_holdername = Column(String(255))
    upi_id = Column(String(255), nullable=True)
    gst_number = Column(String(55),default=None)
    pan_number=Column(String(45),default=None)

    # Additional fields for registration
    gst_type = Column(String(20), nullable=True)  # inclusive, exclusive, nogst
    gst_percentage = Column(String(5), default="18", nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class AccountDetailsEditRequest(Base):
    __tablename__ = "account_details_edit_requests"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    old_json = Column(JSON, nullable=False)  # Original payment details before edit
    new_json = Column(JSON, nullable=False)  # Requested new payment details
    query_solved = Column(Boolean, default=False, nullable=False)
    requested_time = Column(DateTime, default=datetime.now(), nullable=False)
    resolved_time = Column(DateTime, nullable=True)
    admin_remarks = Column(String(500), nullable=True)  # Optional remarks from admin


class EstimateDiscount(Base):
    __tablename__ = "estimate_discount"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    membership_id = Column(Integer, nullable=False, index=True)
    discount_amount = Column(Numeric(10, 2), nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class GymEnquiry(Base):
    __tablename__ = "gym_enquiry"
 
    enquiry_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete = "CASCADE"), nullable=False)  
    name = Column(String(255), nullable=False)
    contact = Column(String(20), nullable=False)
    email = Column(String(255), nullable=False)
    convenientTime = Column(String(255))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    status = Column(String(255), default="pending")
    statusReason = Column(String(255))
    message = Column(Text, nullable=True)
 
 

 
class FeesReceipt(Base):
    __tablename__ = "fees_receipt"

    receipt_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=True)  # For regular clients
    manual_client_id = Column(Integer, ForeignKey("manual_clients.id", ondelete="CASCADE"), nullable=True)  # For manual CRM clients
    gym_id = Column(Integer, ForeignKey("gyms.gym_id"), nullable=False)
    client_name = Column(String(255))
    gym_name = Column(String(255))
    gym_logo = Column(String(255))
    gym_contact = Column(String(255))
    gym_location = Column(String(255))
    plan_id = Column(Integer, ForeignKey("gym_plans.id"), nullable=False)
    plan_description = Column(String(255))
    fees = Column(Float)
    fees_type = Column(String(50))
    discount = Column(Float)
    discounted_fees = Column(Float)
    due_date = Column(DateTime)
    invoice_number = Column(String(255))
    client_contact = Column(String(45))
    bank_details = Column(String(255))
    ifsc_code = Column(String(255))
    bank_name = Column(String(255))
    upi_id = Column(String(255))
    account_holder_name = Column(String(255))
    invoice_date = Column(String(255))
    payment_method = Column(String(255))
    gst_number = Column(String(55))
    client_email = Column(String(255))
    mail_status = Column(Boolean)  
    created_at = Column(DateTime)
    update_at = Column(DateTime)
    payment_date = Column(DateTime)
    payment_reference_number = Column(String(255), nullable=True)
    gst_percentage = Column(Float, nullable=True, default=18)
    gst_type = Column(String(255), nullable=True)
    branch = Column(String(100),nullable=True)
    total_amount = Column(Float, nullable=True)
 
class GymImportData(Base):
    __tablename__ = "gym_import_data"
 
    import_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete = "CASCADE"), nullable=False)
    client_name = Column(String(45), nullable=False)
    client_contact = Column(String(45), nullable=False)
    client_email = Column(String(255), nullable=True, default=None)
    client_location = Column(String(255), nullable=True, default=None)
    status = Column(String(45))
    gender = Column(String(45), nullable=False)
    sms_status = Column(Boolean, nullable=False, default=False)
    admission_number = Column(String(100), default= None)
    expires_at=Column(Date, nullable=True)
    joined_at=Column(Date, nullable=True)
    import_type=Column(String(45))


class GymDetails(Base):
    __tablename__ = "gym_details"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, unique=True, index=True)
    total_machineries = Column(Integer, nullable=True)
    floor_space = Column(Integer, nullable=True)
    total_trainers = Column(Integer, nullable=True)
    yearly_membership_cost = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class DefaultWorkoutTemplates(Base):
    __tablename__ ='default_workout_templates'
 
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gender = Column(String(20))
    goals = Column(String(20))
    expertise_level=Column(String(100))
    workout_json = Column(JSON)
 
 

class FittbotDietTemplate(Base):
    __tablename__ ='fittbot_diet_template'
 
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    template_name = Column(String(45))
    template_json = Column(JSON)
    gender = Column(String(50))
    goals  = Column(String(45))
    cousine = Column(String(50))
    expertise_level = Column(String(50))
    tip = Column(String(255))
 
 
class WeightJourney(Base):
    __tablename__ ='client_weight_journey'
 
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    start_date=Column(Date)
    end_date = Column(Date)
    start_weight=Column(Float)
    actual_weight=Column(Float)
    target_weight=Column(Float)

class GymAnnouncement(Base):
    __tablename__ = "gym_announcements"

    id          = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id      = Column(Integer, nullable=False)
    title       = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    datetime    = Column(DateTime, nullable=False)
    priority    = Column(String(45),nullable=True)

class GymOffer(Base):
    __tablename__ = "gym_offers"

    id             = Column(Integer, primary_key=True, index=True, autoincrement=True)
    gym_id         = Column(Integer, nullable=False)
    title          = Column(Text, nullable=False)
    subdescription = Column(Text, nullable=True)
    description    = Column(Text, nullable=False)
    validity       = Column(DateTime, nullable=False)
    discount       = Column(Integer, nullable=False)
    category       = Column(String(255), nullable=False)
    tag            = Column(String(255), nullable=True)
    code           = Column(String(100), nullable=False)
    image_url=Column(String(255), nullable=True)


class ClientWeightData(Base):
    __tablename__ = "client_weight_data"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(
        Integer,
        ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False
    )
    weight    = Column(Float, nullable=False)
    status    = Column(Boolean, default=False)  
    date=Column(Date)


class HomePoster(Base):
    __tablename__ = 'home_posters'

    id  = Column(Integer, primary_key=True, index=True)
    description=Column(String(45), nullable=False)
    url = Column(String(255), nullable=False)


class ManualPoster(Base):
    """Manual posters that override conditional frontend posters when show=True"""
    __tablename__ = 'manual_posters'

    id = Column(Integer, primary_key=True, index=True)
    urls = Column(JSON, nullable=False)  # JSON array: [{"url": "...", "description": "..."}, ...]
    show = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AttendanceGym(Base):
    __tablename__ = 'attendance_gym'

    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    gym_id           = Column(
                          Integer,
                          ForeignKey('gyms.gym_id', ondelete='CASCADE', onupdate='CASCADE'),
                          nullable=False
                      )
    date             = Column(Date,    nullable=False)
    attendance_count = Column(Integer, nullable=False)



class OldGymData(Base):
    __tablename__ = "gym_old_data"

    id  = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey('clients.client_id', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)
    gym_client_id = Column(String(25))
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=False)
    profile=Column(String(255))
    location = Column(String(255),nullable=True)
    email = Column(String(100), unique=False, nullable=False)
    contact = Column(String(15), nullable=False)
    lifestyle = Column(Text)
    medical_issues = Column(Text)
    batch_id = Column(Integer, nullable=True)
    training_id = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    goals = Column(Text)
    gender = Column(String(20), nullable=True,default="Other")
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    joined_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob=Column(Date, nullable=True)
    expires_at=Column(Date, nullable=True)
    starts_at=Column(Date, nullable=True)
    admission_number= Column(String(100))


class ClientGym(Base):
    __tablename__ = "gym_client_id"
    id=Column(Integer,primary_key=True,autoincrement=True)
    client_id=Column(Integer, primary_key=True, index=True)
    gym_id=Column(Integer)
    gym_client_id= Column(String(255))
    admission_number= Column(String(100))

class OwnerToken(Base):
    __tablename__ = "support_tokens_owner"
 
    id         = Column(Integer, primary_key=True, autoincrement=True)
    gym_id  = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    token      = Column(String(255), nullable=False)
    subject    = Column(String(255))
    email      = Column(String(255))
    issue      = Column(Text)
    followed_up  = Column(Boolean, nullable=False, default=False)
    resolved  = Column(Boolean, nullable=False, default=False)
    comments = Column(Text)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    resolved_at = Column(DateTime(timezone=True), nullable=True)
 


class FittbotMuscleGroup(Base):
    __tablename__ = "fittbot_muscle_group"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    gender       = Column(Enum("male", "female", "other", name="gender_enum"), nullable=False)
    muscle_group = Column(String(100), nullable=False)
    url          = Column(String(255), nullable=False)



class SmartWatch(Base):
    __tablename__='smart_watch'
    id  = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('clients.client_id', ondelete='CASCADE', onupdate='CASCADE'),unique=True, nullable=True)
    interested = Column(Boolean)

class RewardInterest(Base):
    __tablename__='reward_interest'
    id  = Column(Integer, primary_key=True, index=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('clients.client_id', ondelete='CASCADE', onupdate='CASCADE'),unique=True, nullable=True)
    interested = Column(Boolean)
    next_reminder = Column(DateTime, nullable=True)


class Brochures(Base):
    __tablename__ = "gym_brouchre"
 
    brouchre_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id   = Column(Integer,ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    pic_url  = Column(String(255), nullable=False)


class ClientNextXp(Base):
    __tablename__ = "client_next_xp"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer,
                       ForeignKey("clients.client_id", ondelete="CASCADE"),
                       nullable=False,
                       index=True,
                       unique=True)
    next_xp   = Column(Integer, nullable=False, default=0)
    gift = Column(String(155))



class GymFees(Base):
    __tablename__ = "gym_fees"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    client_id  = Column(Integer, nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date   = Column(Date, nullable=False)



class ClientBirthday(Base):
    __tablename__ = "client_birthdays"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    client_id   = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True)
    client_name = Column(String(100), nullable=False)
    expo_token  = Column(JSON, nullable=False)


class FittbotPlans(Base):
    __tablename__ = "fittbot_plans_legacy"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    plan_name          = Column(String(255), nullable=False)
    duration           = Column(Integer, nullable=False)
    image_url          = Column(String(512), nullable=True)
    price              = Column(Integer, nullable=False)


class AttendanceStreak(Base):
    __tablename__ = "attendance_streak"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    current_streak_days = Column(Integer, default=0)  
    last_attendance_date = Column(Date, nullable=True)
    last_xp_awarded_at = Column(Integer, default=0)  
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class EnquiryEstimates(Base):
    __tablename__ = "enquiry_estimates"
 
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    enquiry_id = Column(Integer, ForeignKey("gym_enquiry.enquiry_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id"), nullable=False)
    client_name = Column(String(255))
    gym_name = Column(String(255))
    gym_logo = Column(String(255))
    gym_contact = Column(String(255))
    gym_location = Column(String(255))
    plan_id = Column(Integer, ForeignKey("gym_plans.id"), nullable=False)
    plan_description = Column(String(255))
    fees = Column(Float)
    admission_fees = Column(Float, nullable=True, default=0)
    fees_type = Column(String(50))
    discount = Column(Float)
    discounted_fees = Column(Float)
    estimate_number = Column(String(255))
    client_contact = Column(String(45))
    bank_details = Column(String(255))
    ifsc_code = Column(String(255))
    bank_name = Column(String(255))
    upi_id = Column(String(255))
    account_holder_name = Column(String(255))
    estimate_date = Column(String(255))
    gst_number = Column(String(55))
    client_email = Column(String(255))
    mail_status = Column(Boolean)  
    created_at = Column(DateTime)
    update_at = Column(DateTime)
    gst_percentage = Column(Float, nullable=True, default=18)
    gst_type = Column(String(255), nullable=True)
    branch = Column(String(100),nullable=True)
    total_amount = Column(Float, nullable=True)


class GymPhoto(Base):
    __tablename__ = "gym_photos"
    
    photo_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    area_type = Column(String(50), nullable=False, index=True)  # entrance, cardio, weight, locker, reception, other
    image_url = Column(String(500), nullable=False)
    file_name = Column(String(255), nullable=True)
    file_size = Column(Integer, nullable=True)  # in bytes
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    
    gym = relationship("Gym", backref="gym_photos")


class GymStudiosPic(Base):
    __tablename__ = "gym_studios_pic"

    photo_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    type = Column(String(55), nullable=False, index=True)  # cover_pic, logo, etc.
    image_url = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    __table_args__ = (
        Index("idx_gym_studios_pic_gym_type", "gym_id", "type"),
    )




class GymBusinessPayment(Base):
    __tablename__ = "gym_business_payment"


    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(String(100), nullable=False, index=True)
    gym_id = Column(String(100), nullable=False, index=True)
    date = Column(Date, nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    status = Column(String(50), nullable=False)
    mode = Column(String(50), nullable=False)
    entitlement_id = Column(String(100), nullable=True, index=True)
    payment_id = Column(String(100), nullable=True, index=True)
    order_id = Column(String(100), nullable=True, index=True)
    membership_id=Column(Integer, nullable=True)
    created_at= Column(DateTime, nullable=True)
    updated_at= Column(DateTime, nullable=True)


class FittbotGymMembership(Base):
    __tablename__ = "fittbot_gym_membership"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(String(100), nullable=False, index=True)
    client_id = Column(String(100), nullable=False, index=True)
    plan_id = Column(Integer, nullable=True, index=True)
    type = Column(String(50), nullable=False, index=True)
    entitlement_id = Column(String(100), nullable=True, index=True)
    amount=Column(Float, nullable=False)
    purchased_at = Column(DateTime(timezone=True), nullable=False, index=True)
    status = Column(String(50), nullable=False, default="upcoming", index=True)
    joined_at = Column(Date, nullable=True, index=True)
    expires_at = Column(Date, nullable=True, index=True)
    pause= Column(String(50), default=False)
    pause_at=Column(Date)
    resume_at=Column(Date)
    old_client=Column(Boolean)


class CharactersCombinationOld(Base):
    __tablename__ = "characters_combination_old"
 
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    characters_id = Column(String(100), nullable=False)
    combination_id = Column(String(100), nullable=False)
    characters_url = Column(String(500), nullable=True)
    gender = Column(String(45), nullable=True)

class CharactersCombination(Base):
    __tablename__ = "characters_combination"
 
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    characters_id = Column(String(100), nullable=False)
    combination_id = Column(String(100), nullable=False)
    characters_url = Column(String(500), nullable=True)
    gender = Column(String(45), nullable=True)


class FittbotCharacters(Base):
    __tablename__ = "fittbot_characters"
 
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    character_id = Column(String(45), nullable=False, unique=True)
    character_url = Column(String(500), nullable=True)
    gender = Column(String(45), nullable=True)
 
 
class ClientWeightSelection(Base):
    __tablename__ = "client_weight_selection"
 
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(String(45), nullable=False)
    current_image_id = Column(String(45), nullable=False)
    target_image_id = Column(String(45), nullable=False)
    combination_id = Column(String(45), nullable=True)

 
class WeightManagementPlan(Base):
    __tablename__ = "weight_management_plan"
 
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    category = Column(String(50), nullable=False)  # 'weight_loss' or 'weight_gain'
    gender = Column(String(20), nullable=False)  # 'male' or 'female'
    weight_min = Column(Integer, nullable=False)
    weight_max = Column(Integer, nullable=False)
    activity_level = Column(String(50), nullable=False)  # sedentary, lightly_active, etc.
    duration_months = Column(Integer, nullable=False)


class ClientCharacter(Base):
    __tablename__ = "client_characters"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    character_id = Column(Integer, nullable=False, index=True)


class IndianFoodMaster(Base):
    """
    Comprehensive Indian food database with diet types, regional cuisines, and health tags.
    Supports personalized meal planning for Indian diet preferences.
    """
    __tablename__ = "indian_food_master"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)

    # Basic food information
    food_name = Column(String(200), nullable=False, index=True)
    food_name_hindi = Column(String(200), nullable=True)
    food_name_regional = Column(String(200), nullable=True)
    category = Column(String(100), nullable=False, index=True)  # e.g., "Breakfast", "Main Course", "Snacks"
    description = Column(Text, nullable=True)

    # Nutritional information (per standard serving)
    quantity = Column(String(100), nullable=False)  # e.g., "1 medium bowl (150g)"
    calories = Column(Float, nullable=False)
    protein = Column(Float, nullable=False)
    carbs = Column(Float, nullable=False)
    fat = Column(Float, nullable=False)
    fiber = Column(Float, nullable=False, default=0)
    sugar = Column(Float, nullable=False, default=0)

    # Micronutrients
    calcium = Column(Float, nullable=True, default=0)
    magnesium = Column(Float, nullable=True, default=0)
    potassium = Column(Float, nullable=True, default=0)
    iodine = Column(Float, nullable=True, default=0)
    iron = Column(Float, nullable=True, default=0)
    vitamin_a = Column(Float, nullable=True, default=0)
    vitamin_c = Column(Float, nullable=True, default=0)
    vitamin_d = Column(Float, nullable=True, default=0)

    # Diet type categorization (multiple can be true)
    is_vegetarian = Column(Boolean, default=False, index=True)
    is_non_vegetarian = Column(Boolean, default=False, index=True)
    is_vegan = Column(Boolean, default=False, index=True)
    is_eggetarian = Column(Boolean, default=False, index=True)
    is_jain = Column(Boolean, default=False, index=True)
    is_paleo = Column(Boolean, default=False, index=True)
    is_ketogenic = Column(Boolean, default=False, index=True)

    # Regional cuisine
    cuisine_type = Column(String(100), nullable=True, index=True)  # "North Indian", "South Indian", "Common"
    state_origin = Column(String(100), nullable=True)  # e.g., "Punjab", "Tamil Nadu", "All India"

    # Meal slot suitability
    suitable_for_early_morning = Column(Boolean, default=False)
    suitable_for_pre_breakfast = Column(Boolean, default=False)
    suitable_for_breakfast = Column(Boolean, default=False)
    suitable_for_mid_morning = Column(Boolean, default=False)
    suitable_for_lunch = Column(Boolean, default=False)
    suitable_for_evening_snack = Column(Boolean, default=False)
    suitable_for_pre_workout = Column(Boolean, default=False)
    suitable_for_post_workout = Column(Boolean, default=False)
    suitable_for_dinner = Column(Boolean, default=False)
    suitable_for_bedtime = Column(Boolean, default=False)

    # Health condition tags
    is_diabetic_friendly = Column(Boolean, default=False, index=True)
    is_high_protein = Column(Boolean, default=False, index=True)
    is_low_calorie = Column(Boolean, default=False, index=True)
    is_weight_loss_friendly = Column(Boolean, default=False, index=True)
    is_muscle_gain_friendly = Column(Boolean, default=False, index=True)
    is_heart_healthy = Column(Boolean, default=False, index=True)
    is_gluten_free = Column(Boolean, default=False, index=True)
    is_lactose_free = Column(Boolean, default=False, index=True)

    # Food type tags
    is_liquid = Column(Boolean, default=False, index=True)  # Beverages/drinks/liquid items

    # Glycemic Index and Load
    glycemic_index = Column(Integer, nullable=True)  # 0-100
    glycemic_load = Column(Float, nullable=True)

    # Additional metadata
    preparation_time_mins = Column(Integer, nullable=True)
    difficulty_level = Column(String(50), nullable=True)  # "Easy", "Medium", "Hard"
    is_seasonal = Column(Boolean, default=False)
    season_availability = Column(String(100), nullable=True)  # e.g., "Summer", "Winter", "All Year"

    # Image and tags
    image_url = Column(Text, nullable=True)
    tags = Column(JSON, nullable=True)  # Additional searchable tags

    # Status flags
    is_active = Column(Boolean, default=True, index=True)
    is_verified = Column(Boolean, default=False)
    popularity_score = Column(Integer, default=0)  # For ranking common foods

    # Timestamps
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    # Indexes for performance
    __table_args__ = (
        Index('idx_diet_cuisine', 'is_vegetarian', 'is_non_vegetarian', 'cuisine_type'),
        Index('idx_health_tags', 'is_diabetic_friendly', 'is_high_protein', 'is_low_calorie'),
        Index('idx_meal_slots', 'suitable_for_breakfast', 'suitable_for_lunch', 'suitable_for_dinner'),
    )

class HomeWorkout(Base):
    __tablename__ = "home_workout"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    home_workout = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class ReferralFittbotCash(Base):
    __tablename__ = "referral_fittbot_cash"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    fittbot_cash = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ReferralFittbotCashLogs(Base):
    __tablename__ = "referral_fittbot_cash_logs"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    fittbot_cash = Column(Integer, nullable=False)
    reason = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ReferralMapping(Base):
    __tablename__ = "referral_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    referrer_id = Column(Integer, nullable=False, index=True)
    referee_id = Column(Integer, nullable=False, index=True)
    referral_date = Column(Date, nullable=False, default=datetime.now().date)
    status= Column(String(45), nullable=True)

class ReferralRedeem(Base):
    __tablename__ = "referral_redeem"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    points_redeemed = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.now())


class ReferralCode(Base):
    __tablename__ = "referral_code"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, unique=True, index=True)
    referral_code = Column(String(50), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.now())


class ReferralGymCode(Base):
    __tablename__ = "referral_gym_code"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    referral_code = Column(String(50), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.now())


class ReferralGymCash(Base):
    __tablename__ = "referral_gym_cash"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    month = Column(Date, nullable=False)
    referral_cash = Column(Integer, nullable=False, default=0)
    status = Column(String(45), nullable=False, default="active")
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ReferralGymCashLogs(Base):
    __tablename__ = "referral_gym_cash_logs"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    referral_cash = Column(Integer, nullable=False)
    reason = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ReferralGymMapping(Base):
    __tablename__ = "referral_gym_mapping"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    referrer_owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    referee_owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    referral_date = Column(Date, nullable=False, default=datetime.now().date)
    status = Column(String(45), nullable=True)


class FittbotRatings(Base):
    __tablename__ = "fittbot_ratings"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    star = Column(Integer, nullable=False)
    feedback = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now())


class FreeTrial(Base):
    __tablename__ = "free_trial"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    status = Column(String(20), nullable=False, default="active")
    created_at = Column(DateTime, default=datetime.now)



class ClientFeedback(Base):
    __tablename__ = "client_feedback"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending")  # pending, canceled, submitted
    next_feedback_date = Column(Date, nullable=True)  # When to ask next if canceled
    feedback_text = Column(Text, nullable=True)  # The actual feedback if submitted
    rating = Column(Integer, nullable=True)  # Optional rating (1-5)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class Royalty(Base):
    __tablename__ = "royalty"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    subscription_id = Column(String(100), nullable=False)
    date = Column(Date, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class RoyaltyStatus(Base):
    __tablename__ = "royalty_status"
    __table_args__ = (
        UniqueConstraint("gym_id", "month", name="uq_royalty_status_gym_month"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    month = Column(String(20), nullable=False, index=True)
    payment_status = Column(String(50), nullable=False, default="not_initiated")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class AppVersion(Base):
    __tablename__ = "app_versions"
    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String(20))
    current_version = Column(String(20))
    min_supported_version = Column(String(20))
    force_update = Column(Boolean, default=False)
    update_url = Column(String(255), nullable=True)
    message = Column(String(255), nullable=True)
    button_label = Column(String(80), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())




class EquipmentWorkout(Base):
    __tablename__ = "equipment_workout"

    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment = Column(JSON, nullable=False)


class GymVerificationDocument(Base):
    __tablename__ = "gym_verification_documents"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    aadhaar_url = Column(String(500), nullable=True)
    aadhaar_back= Column(String(500), nullable=True)
    pan_url = Column(String(500), nullable=True)
    bankbook_url = Column(String(500), nullable=True)
    updated_by = Column(String(255), nullable=True)
    agreement=Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class GymPrefilledAgreement(Base):
    """Stores prefilled agreement PDF links for gyms"""
    __tablename__ = "gym_prefilled_agreement"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True, unique=True)
    s3_link = Column(String(500), nullable=False)
    is_clicked = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class GymAgreementSteps(Base):
    """Tracks multi-step agreement verification: Terms → Selfie → Signature → OTP"""
    __tablename__ = "gym_agreement_steps"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="SET NULL"), nullable=True, index=True)

    # Step 1: Terms acceptance
    terms_accepted = Column(Boolean, default=False)
    terms_accepted_at = Column(DateTime, nullable=True)

    # Step 2: Selfie with timestamp
    selfie_url = Column(String(500), nullable=True)
    selfie_captured_at = Column(DateTime, nullable=True)

    # Step 3: Digital signature
    signature_url = Column(String(500), nullable=True)
    signature_captured_at = Column(DateTime, nullable=True)

    # Step 4: OTP verification
    otp_verified = Column(Boolean, default=False)
    otp_verified_at = Column(DateTime, nullable=True)
    otp_mobile = Column(String(15), nullable=True)

    # Audit fields
    accepted_by_name = Column(String(200), nullable=True)
    accepted_ip = Column(String(50), nullable=True)
    accepted_user_agent = Column(String(500), nullable=True)
    agreement_version = Column(String(50), default="1.0")

    # Completion status
    all_steps_completed = Column(Boolean, default=False)
    completed_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class GymManualData(Base):
    __tablename__ = "gym_manual_data"
    
    id=Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    total_clients= Column(Integer)
    active_clients=Column(Integer)
    inactive_clients=Column(Integer)
    total_enquiries=Column(Integer)
    total_followups=Column(Integer)


class FittbotAssociates(Base):
    __tablename__ = "fittbot_associates"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    mobile_number = Column(String(15), nullable=False, unique=True, index=True)
    gym_ids = Column(JSON, nullable=True)


class GymJoinRequest(Base):
    __tablename__ = "gym_join_requests"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    mobile_number = Column(String(15), nullable=False)
    alternate_mobile_number = Column(String(15), nullable=True)
    dp = Column(String(500), nullable=True)
    status = Column(String(50), nullable=False, default="pending", index=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class GymOnboardingPics(Base):
    __tablename__ = "gym_onboarding_pics"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    machinery_1 = Column(String(255), nullable=True)
    machinery_2 = Column(String(255), nullable=True)
    treadmill_area = Column(String(255), nullable=True)
    cardio_area = Column(String(255), nullable=True)
    dumbell_area = Column(String(255), nullable=True)
    reception_area = Column(String(255), nullable=True)
    uploaded = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    


class BiometricModal(Base):
    __tablename__ = "biometric_modal"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    interest = Column(Boolean, default=False, nullable=False)
    pic_1 = Column(String(255), nullable=True)
    pic_2 = Column(String(255), nullable=True)
    pic_3 = Column(String(255), nullable=True)
    pic_4 = Column(String(255), nullable=True)
    pic_5 = Column(String(255), nullable=True)
    pic_6 = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class ClassSession(Base):

    __tablename__ = "all_sessions"
    __table_args__ = (UniqueConstraint("name", name="uq_sessions_name"), {"schema": SESSION_SCHEMA})

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(150), nullable=False)
    image = Column(String(255), nullable=True)
    description = Column(String(255), nullable=False)
    timing = Column(String(50), nullable=False, default="60 Min Session")
    internal= Column(String(45), nullable=True)


class GymSession(Base):
    """
    Map a gym to its available sessions as a JSON blob.
    """
    __tablename__ = "gym_session"
    __table_args__ = {"schema": SESSION_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer)
    sessions = Column(JSON, nullable=False)


class SessionSetting(Base):

    __tablename__ = "session_settings"
    __table_args__ = (
        UniqueConstraint("gym_id", "session_id", "trainer_id", name="uq_session_settings_gym_session_trainer"),
        {"schema": SESSION_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    session_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.all_sessions.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    trainer_id = Column(Integer, ForeignKey("trainers.trainer_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    is_enabled = Column(Boolean, default=False, nullable=False)
    base_price = Column(Integer, nullable=True)
    discount_percent = Column(Float, default=0.0)
    final_price = Column(Integer, nullable=True)
    capacity = Column(Integer, nullable=True)
    booking_lead_minutes = Column(Integer, nullable=True)
    cancellation_cutoff_minutes = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class SessionSchedule(Base):
    """
    Recurring/one-off schedules for sessions.
    """
    __tablename__ = "session_schedules"
    __table_args__ = (
        Index("ix_session_schedule_gym_session_trainer", "gym_id", "session_id", "trainer_id"),
        Index("ix_session_schedule_weekday", "weekday", "is_active"),
        {"schema": SESSION_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    session_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.all_sessions.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    trainer_id = Column(Integer, ForeignKey("trainers.trainer_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    recurrence = Column(Enum("weekly", "one_off", name="session_recurrence"), default="weekly", nullable=False)
    weekday = Column(Integer, nullable=True)  # 0=Monday .. 6=Sunday for weekly recurrence
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    slot_quota = Column(Integer, nullable=True)  # override capacity per slot
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())


class SessionBooking(Base):
    """
    Bookings across all session types (including personal training).
    """
    __tablename__ = "session_bookings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    session_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.all_sessions.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    trainer_id = Column(Integer, ForeignKey("trainers.trainer_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    schedule_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_schedules.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    booking_date = Column(Date, nullable=False)
    status = Column(Enum("booked", "cancelled", "attended", "no_show", "refunded", name="session_booking_status"), default="booked", nullable=False)
    price_paid = Column(Integer, nullable=True)
    discount_applied = Column(Float, nullable=True)
    checkin_token = Column(String(64), unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    __table_args__ = (
        Index("ix_session_booking_schedule_date", "schedule_id", "booking_date"),
        Index("ix_session_booking_gym_session", "gym_id", "session_id"),
        {"schema": SESSION_SCHEMA},
    )


class SessionPurchase(Base):
    """
    Session payment/purchase envelope (maps to payment orders/order_items).
    """
    __tablename__ = "session_purchases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    razorpay_order_id = Column(String(64), unique=True, nullable=False)
    payment_order_pk = Column(Integer, nullable=True)
    gym_id = Column(Integer, nullable=False)
    client_id = Column(Integer, nullable=False)
    session_id = Column(Integer, nullable=False)
    trainer_id = Column(Integer, nullable=True)
    sessions_count = Column(Integer, nullable=False)
    scheduled_sessions = Column(JSON, nullable=False)
    reward_applied = Column(Boolean, default=False, nullable=False)
    reward_amount = Column(Integer, default=0, nullable=False)
    total_rupees = Column(Integer, nullable=False)
    payable_rupees = Column(Integer, nullable=False)
    price_per_session = Column(Integer, nullable=True)  # Original per-session price (99 for promo, actual otherwise)
    idempotency_key = Column(String(64), nullable=True)
    status = Column(
        Enum("pending", "paid", "failed", "cancelled", "refunded", name="session_purchase_status"),
        default="pending",
        nullable=False,
    )
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    __table_args__ = (
        UniqueConstraint("gym_id", "client_id", "session_id", "trainer_id", "idempotency_key", name="uq_session_purchase_idem"),
        {"schema": SESSION_SCHEMA},
    )


class SessionBookingDay(Base):
    """
    Per-day/slot booking instances tied to a purchase (used for scanning/attendances).
    """
    __tablename__ = "session_booking_days"

    id = Column(Integer, primary_key=True, autoincrement=True)
    purchase_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_purchases.id", ondelete="CASCADE"), nullable=False)
    schedule_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_schedules.id", ondelete="SET NULL"), nullable=True)
    gym_id = Column(Integer, nullable=False)
    client_id = Column(Integer, nullable=False)
    session_id = Column(Integer, nullable=False)
    trainer_id = Column(Integer, nullable=True)
    booking_date = Column(Date, nullable=False)
    start_time = Column(Time, nullable=True)
    end_time = Column(Time, nullable=True)
    status = Column(
        Enum("booked", "cancelled", "attended", "no_show", "refunded", name="session_booking_day_status"),
        default="booked",
        nullable=False,
    )
    checkin_token = Column(String(64), unique=True, nullable=True)
    scanned_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    __table_args__ = (
        Index("ix_session_booking_day_purchase", "purchase_id", "booking_date"),
        {"schema": SESSION_SCHEMA},
    )


class SessionBookingAudit(Base):
    """
    Audit trail for booking status changes and scans.
    """
    __tablename__ = "session_booking_audit"
    __table_args__ = {"schema": SESSION_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    purchase_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_purchases.id", ondelete="CASCADE"), nullable=False)
    booking_day_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_booking_days.id", ondelete="CASCADE"), nullable=True)
    event = Column(String(50), nullable=False)
    actor_role = Column(String(30), nullable=True)
    actor_id = Column(Integer, nullable=True)
    notes = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.now())


class SessionQrCode(Base):
    """
    QR codes issued for session check-ins.
    """
    __tablename__ = "session_qr_codes"
    __table_args__ = {"schema": SESSION_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_day_id = Column(Integer, ForeignKey(f"{SESSION_SCHEMA}.session_booking_days.id", ondelete="CASCADE"), nullable=False)
    qr_code = Column(String(128), unique=True, nullable=False)
    expires_at = Column(DateTime, nullable=True)
    consumed_at = Column(DateTime, nullable=True)
    consumed_by = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now())


class OwnerHomePoster(Base):
    __tablename__ = "owner_home_posters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(45), nullable=True)
    url = Column(String(255), nullable=True)


class ClientModalTracker(Base):
    __tablename__ = "client_modal_tracker"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    last_modal_index = Column(Integer, default=0, nullable=False)  # 0=no_cost_emi, 1=bnpl, 2=session, 3=dailypass
    last_shown_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class OwnerModalTracker(Base):
    __tablename__ = "owner_modal_tracker"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    last_modal_index = Column(Integer, default=0, nullable=False)  # Index in the missing features list
    last_shown_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymStudiosRequest(Base):
    __tablename__ = "gym_studios_request"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    area = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(String(10), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class GymOnboardingEsign(Base):
    """
    Tracks gym onboarding e-sign documents via Leegality.
    Stores document status, URLs, and signed document S3 paths.
    """
    __tablename__ = "gym_onboarding_esign"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)

    # Leegality document identifiers
    document_id = Column(String(100), nullable=True, index=True)
    irn = Column(String(100), nullable=True, unique=True, index=True)  # Internal Reference Number

    # Document details
    gym_name = Column(String(200), nullable=False)
    location = Column(String(255), nullable=True)
    gst_no = Column(String(20), nullable=True)
    pan = Column(String(15), nullable=True)
    address = Column(Text, nullable=True)
    authorised_name = Column(String(200), nullable=False)
    mobile = Column(String(15), nullable=False)
    email = Column(String(100), nullable=False)

    # Status tracking
    status = Column(String(50), default="pending", nullable=False, index=True)  # pending, sent, signed, failed, expired
    signing_url = Column(Text, nullable=True)

    # Signed document storage
    signed_pdf_url = Column(String(500), nullable=True)  # S3 URL after document is signed
    audit_trail_url = Column(String(500), nullable=True)  # S3 URL for audit trail PDF
    signed_at = Column(DateTime, nullable=True)

    # Webhook tracking
    webhook_received_at = Column(DateTime, nullable=True)
    webhook_event_type = Column(String(50), nullable=True)
    webhook_payload = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Indexes for common queries
    __table_args__ = (
        Index("ix_esign_gym_status", "gym_id", "status"),
        Index("ix_esign_created", "created_at"),
    )


class GymAgreement(Base):
    """
    Tracks prefilled gym agreement PDFs generated asynchronously via Celery.
    Stores generation status, S3 paths, and acceptance consent.
    """
    __tablename__ = "gym_agreements"

    agreement_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=True, index=True)

    # Template version for tracking which coords/template was used
    template_version = Column(String(20), default="v1", nullable=False)

    # Status tracking: PENDING -> GENERATING -> READY -> ACCEPTED (or FAILED)
    status = Column(String(20), default="PENDING", nullable=False, index=True)

    # Prefill data stored as JSON for record keeping
    prefill_json = Column(JSON, nullable=True)

    # S3 storage
    s3_key_final = Column(Text, nullable=True)  # Final PDF S3 key
    pdf_sha256 = Column(String(64), nullable=True)  # SHA256 hash for integrity

    # Error tracking
    error_message = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    ready_at = Column(DateTime, nullable=True)  # When PDF generation completed

    # Acceptance/consent fields
    accepted_at = Column(DateTime, nullable=True)
    accepted_by_name = Column(String(200), nullable=True)  # Typed name for consent
    accepted_ip = Column(String(64), nullable=True)  # IP address for audit
    accepted_user_agent = Column(Text, nullable=True)  # User agent for audit
    selfie_s3_key = Column(Text, nullable=True)  # Optional selfie for verification

    # Indexes for common queries
    __table_args__ = (
        Index("ix_gym_agreement_gym_status", "gym_id", "status"),
        Index("ix_gym_agreement_created", "created_at"),
    )


# =============================================================================
# Manual Client Model - For CRM-style offline client management
# These clients have NO connection to Fittbot app
# =============================================================================
class ManualClient(Base):
    __tablename__ = "manual_clients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False, index=True)

    # Personal Info
    name = Column(String(100), nullable=False)
    contact = Column(String(15), nullable=False, index=True)  # Primary identifier
    email = Column(String(100), nullable=True)
    gender = Column(String(20), nullable=True)
    date_of_birth = Column(Date, nullable=True)
    age = Column(Integer, nullable=True)

    # Physical Metrics (optional for manual entry)
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    goal = Column(String(50), nullable=True)  # weight_gain, weight_loss, body_recomposition

    # Membership Info
    admission_number = Column(String(100), nullable=True)  # Owner's custom ID
    batch_id = Column(Integer, nullable=True)
    plan_id = Column(Integer, nullable=True)  # References GymPlans

    # Dates
    joined_at = Column(Date, nullable=True)
    expires_at = Column(Date, nullable=True)

    # Fees
    admission_fee = Column(Float, default=0)
    monthly_fee = Column(Float, default=0)
    total_paid = Column(Float, default=0)
    balance_due = Column(Float, default=0)
    last_payment_date = Column(Date, nullable=True)

    # Status
    status = Column(String(20), default="active")  # active, inactive, expired

    # Notes
    notes = Column(Text, nullable=True)  # Owner can add custom notes

    # Metadata
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Entry tracking
    entry_type = Column(String(20), default="manual")  # Always "manual"

    # Profile Photo
    dp = Column(String(500), nullable=True)  # S3 URL for client photo

    # Indexes for common queries
    __table_args__ = (
        Index("ix_manual_clients_gym_contact", "gym_id", "contact"),
        Index("ix_manual_clients_gym_status", "gym_id", "status"),
    )


class ManualAttendance(Base):
    """Attendance tracking for manual clients - owner punches them in/out"""
    __tablename__ = "manual_attendance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    manual_client_id = Column(Integer, ForeignKey("manual_clients.id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False, index=True)
    in_time = Column(Time, nullable=True)
    out_time = Column(Time, nullable=True)
    in_time_2 = Column(Time, nullable=True)
    out_time_2 = Column(Time, nullable=True)
    in_time_3 = Column(Time, nullable=True)
    out_time_3 = Column(Time, nullable=True)
    punched_by = Column(String(20), default="owner")  # Always owner for manual

    __table_args__ = (
        Index("ix_manual_attendance_client_date", "manual_client_id", "date"),
        Index("ix_manual_attendance_gym_date", "gym_id", "date"),
    )


class ManualFeeHistory(Base):
    """Fee payment history for manual clients"""
    __tablename__ = "manual_fee_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    manual_client_id = Column(Integer, ForeignKey("manual_clients.id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    amount = Column(Float, nullable=False)
    payment_method = Column(String(50), nullable=True)
    payment_reference = Column(String(100), nullable=True)
    payment_date = Column(Date, default=lambda: datetime.now().date())
    type = Column(String(20), nullable=True)  # admission, monthly, penalty
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        Index("ix_manual_fee_history_client", "manual_client_id"),
        Index("ix_manual_fee_history_gym_date", "gym_id", "payment_date"),
    )


class ImportClientAttendance(Base):
    """Attendance tracking for imported clients - owner punches them in/out"""
    __tablename__ = "import_client_attendance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    import_client_id = Column(Integer, ForeignKey("gym_import_data.import_id", ondelete="CASCADE"), nullable=False)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False, index=True)
    in_time = Column(Time, nullable=True)
    out_time = Column(Time, nullable=True)
    in_time_2 = Column(Time, nullable=True)
    out_time_2 = Column(Time, nullable=True)
    in_time_3 = Column(Time, nullable=True)
    out_time_3 = Column(Time, nullable=True)
    punched_by = Column(String(20), default="owner")

    __table_args__ = (
        Index("ix_import_attendance_client_date", "import_client_id", "date"),
        Index("ix_import_attendance_gym_date", "gym_id", "date"),
    )


class AIConsent(Base):
    """AI consent tracking for clients"""
    __tablename__ = "ai_consent"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    consent = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AIReports(Base):
    """AI reports for clients"""
    __tablename__ = "ai_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    content = Column(Text, nullable=True)
    template = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class StepConsent(Base):
    """Step consent tracking for clients"""
    __tablename__ = "step_consent"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    consent = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class DeleteRequest(Base):
    """Delete account requests from clients"""
    __tablename__ = "delete_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    feedback = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class OwnerDeleteRequest(Base):
    """Delete account requests from owners (Fittbot Business)"""
    __tablename__ = "owner_delete_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey("gym_owners.owner_id", ondelete="CASCADE"), nullable=False, index=True)
    feedback = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AppRedirect(Base):
    """App redirect/maintenance modal configuration"""
    __tablename__ = "app_redirect"

    id = Column(Integer, primary_key=True, autoincrement=True)
    app = Column(String(45), nullable=False, index=True)
    type = Column(String(45), nullable=False)  # 'maintenance' or 'redirect'
    message = Column(Text, nullable=True)
    play_store_url = Column(String(255), nullable=True)
    app_store_url = Column(String(255), nullable=True)
    show = Column(Boolean, default=False, nullable=False)


# =============================================================================
# Reward Program Models - Fymble Mega Fitness Rewards Program
# Program Period: Jan 26, 2026 - May 31, 2026
# =============================================================================

class RewardProgramOptIn(Base):
    """
    Tracks which clients have opted into the Fymble Mega Fitness Rewards Program.
    A client must explicitly opt-in to participate.
    """
    __tablename__ = "reward_program_opt_ins"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    opted_in_at = Column(DateTime, default=datetime.now, nullable=False)
    status = Column(String(20), default="active", nullable=False)  # active, withdrawn
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index("ix_reward_opt_in_status", "status"),
    )


class RewardProgramEntry(Base):
    """
    Stores individual reward entries earned by clients.
    Each eligible purchase generates unique Entry IDs based on the purchase type.

    Entry limits per user:
    - Daily Gym Pass: Up to 100 entries
    - Session Booking: Up to 100 entries
    - Fymble Subscription: Up to 8 entries (2 per month)
    - Referral Bonus: Up to 25 entries (1 per 3 referrals)
    """
    __tablename__ = "reward_program_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entry_id = Column(String(36), unique=True, nullable=False, index=True, default=lambda: str(uuid.uuid4()))
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, index=True)
    method = Column(String(50), nullable=False, index=True)  # dailypass, session, subscription, referral
    source_id = Column(String(100), nullable=True)  # purchase_id, payment_id, etc. for traceability
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    status = Column(String(20), default="valid", nullable=False)  # valid, cancelled, winner

    __table_args__ = (
        Index("ix_reward_entry_client_method", "client_id", "method"),
        Index("ix_reward_entry_created", "created_at"),
    )


# =============================================================================
# Feed Interest Modal Tracking
# =============================================================================

class FeedInterest(Base):
    """
    Tracks whether a client has seen the feed interest/referral modal.
    When client opens Feed tab:
    - If no row exists: show modal, create row with feed_interest=0
    - If row exists with feed_interest=0: show modal
    - If row exists with feed_interest=1: don't show modal
    """
    __tablename__ = "feed_interest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    feed_interest = Column(Integer, default=0, nullable=False)  # 0 = show modal, 1 = don't show
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class AppOpen(Base):
    __tablename__ = "app_open"

    id = Column(Integer, primary_key=True, autoincrement=True)
    open_time = Column(DateTime, default=datetime.now, nullable=False)
    device_id = Column(String(255), nullable=False, index=True)
    device_data = Column(JSON, nullable=True)
    platform = Column(String(50), nullable=True)  

class ActiveUser(Base):
    __tablename__ = "active_users"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

