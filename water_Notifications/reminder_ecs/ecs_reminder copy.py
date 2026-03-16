
import os, json, time, logging
from datetime import datetime
import boto3
from sqlalchemy import (
    create_engine, select, update, and_, or_, asc, case,
    Column, Integer, String, Float, Boolean, Time, JSON, DateTime, Index, ForeignKey, Text, Date, Numeric, Enum
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableList
import uuid


REGION       = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME  = "fittbot/secrets"

QUEUE_URL    = os.getenv("REMINDER_QUEUE_URL", "https://sqs.ap-south-2.amazonaws.com/182399696098/reminderqueue")
BATCH_ROWS   = int(os.getenv("BATCH_ROWS", "5000"))


def fetch_secret():
    """Fetch secrets from AWS Secrets Manager"""
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])


# Fetch secrets and get database credentials
secrets = fetch_secret()

# Get database connection details from secrets
DB_USERNAME = secrets.get("DB_USERNAME")
DB_PASSWORD = secrets.get("DB_PASSWORD")
DB_HOST = secrets.get("DB_HOST")
DB_NAME = secrets.get("DB_NAME")

# Construct connection string (WITHOUT port - same as main app settings.py)
conn = f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)

Session = sessionmaker(bind=engine, autoflush=False)
sqs     = boto3.client("sqs", region_name="ap-south-2")


log     = logging.getLogger("due_finder")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s  %(message)s",
)


Base = declarative_base()

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
    reminder_Sent = Column(Boolean, nullable=False, default=False)
    queued = Column(Boolean, default=False, nullable=False)
    sent_at = Column(DateTime)
    reminder_mode = Column(String(45))
    intimation_start_time = Column(Time)
    intimation_end_time = Column(Time)
    water_timing = Column(Float)
    water_amount = Column(Integer)
    gym_count = Column(Integer)
    diet_type = Column(String(45))
    title = Column(String(45))
    others_time = Column(DateTime)

class Client(Base):
    __tablename__ = "clients"

    client_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=False)
    profile = Column(String(255))
    location = Column(String(255), nullable=True)
    email = Column(String(100), unique=True, nullable=False)
    contact = Column(String(15), nullable=False)
    password = Column(String(255), nullable=False)
    lifestyle = Column(Text)
    medical_issues = Column(Text)
    batch_id = Column(Integer, nullable=True)
    training_id = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    goals = Column(Text)
    gender = Column(String(20), nullable=True, default="Other")
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    access = Column(Boolean)
    joined_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob = Column(Date, nullable=True)
    expiry = Column(Enum("joining_date", "start_of_the_month"))
    refresh_token = Column(String(255))
    verification = Column(JSON)
    uuid_client = Column(String(36), unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))
    data_sharing = Column(Boolean)
    pincode = Column(String(10))
    modal_shown = Column(Boolean, default=False)

class LiveCount(Base):
    __tablename__ = "live_count"
    id = Column(Integer, primary_key=True, nullable=False, default=0)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"))
    count = Column(Integer, nullable=False, default=0)

 
def run_once():
    now_dt=datetime.now()
    now = datetime.now().time()
    sess = Session()
    try:

        rows = (
            sess.execute(
                select(Reminder.reminder_id)
                .outerjoin(
                    LiveCount,
                    LiveCount.gym_id == Reminder.gym_id
                )
                .where(
                    Reminder.reminder_Sent.is_(False),
                    Reminder.queued.is_(False),
                    or_(
                        and_(
                            Reminder.reminder_mode.in_(["diet", "water", "gym"]),
                            Reminder.reminder_time <= now,
                        ),
                        and_(
                            Reminder.reminder_mode == "others",
                            Reminder.others_time <= now_dt,
                        ),
                    ),

                    or_(
                        Reminder.intimation_start_time.is_(None),
                        Reminder.intimation_end_time.is_(None),
                        and_(
                            Reminder.intimation_start_time <= now,
                            Reminder.intimation_end_time   >= now,
                        ),
                    ),

                    or_(
                        Reminder.reminder_mode.in_(["diet", "water", "others"]),
                        and_(
                            Reminder.reminder_mode == "gym",
                            LiveCount.count < Reminder.gym_count,
                        ),
                    ),
                )
                .order_by(
                    asc(
                        case(
                            (Reminder.reminder_mode == "others", Reminder.others_time),
                            else_=Reminder.reminder_time,       
                        )
                    )
                )
                .limit(BATCH_ROWS)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )

        if not rows:
            log.info("nothing due")
            sess.commit()
            return

        sess.execute(
            update(Reminder)
            .where(Reminder.reminder_id.in_(rows))
            .values(queued=True)
        )
        sess.commit()

  
        for i in range(0, len(rows), 10):
            sqs.send_message_batch(
                QueueUrl=QUEUE_URL,
                Entries=[
                    {"Id": str(rid), "MessageBody": json.dumps({"rid": rid})}
                    for rid in rows[i:i + 10]
                ],
            )
        log.info("queued %d IDs", len(rows))

    except Exception:
        sess.rollback()
        log.exception("due-finder failed")
   
    finally:
        sess.close()


def main():
    while True:
        run_once()
        time.sleep(30)

if __name__ == "__main__":
    main()
