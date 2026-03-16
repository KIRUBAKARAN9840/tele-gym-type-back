

import os, json, logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo                         
from exponent_server_sdk import PushClient, PushMessage
import boto3
from sqlalchemy import create_engine, select, update, and_, or_ 
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Enum, Text, DateTime, ForeignKey, Date,Time,Boolean,JSON,Numeric
from datetime import datetime
from sqlalchemy.ext.mutable import MutableList
from typing import List
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session
import uuid
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Time, JSON, DateTime, Index,
    ForeignKey
)
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class Reminder(Base):
    __tablename__ = "reminders"

    reminder_id = Column(Integer, primary_key=True, index=True, nullable=False)
    client_id   = Column(Integer, ForeignKey("clients.client_id",
                     ondelete="CASCADE", onupdate="CASCADE"), index=True)
    gym_id      = Column(Integer, ForeignKey("gyms.gym_id",
                     ondelete="CASCADE", onupdate="CASCADE"), index=True)

    reminder_time         = Column(Time)
    details               = Column(String(500), nullable=False)
    vibration_pattern     = Column(JSON)
    reminder_type         = Column(String(45))
    is_recurring          = Column(Boolean, default=False, nullable=False)

    reminder_Sent         = Column(Boolean, default=False, nullable=False)
    queued                = Column(Boolean, default=False, nullable=False)   # NEW
    sent_at               = Column(DateTime)                                 # NEW

    reminder_mode         = Column(String(45))
    intimation_start_time = Column(Time)
    intimation_end_time   = Column(Time)
    water_timing          = Column(Float)
    water_amount          = Column(Integer)
    gym_count             = Column(Integer)
    diet_type             = Column(String(45))
    title                 = Column(String(45))

    __table_args__ = (
        Index("idx_due_scan",
              reminder_Sent, queued, reminder_time),
        Index("idx_mode_time",
              reminder_mode, reminder_time),
    )

class Client(Base):
    __tablename__ = "clients"
 
    client_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=False)
    profile=Column(String(255))
    location = Column(String(255),nullable=True)
    email = Column(String(100), unique=True, nullable=False)
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
    joined_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob=Column(Date, nullable=True)
    expiry=Column(Enum("joining_date", "start_of_the_month"))
    refresh_token=Column(String(255))
    verification=Column(JSON)
    uuid_client = Column(String, unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))



REGION       = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME  = "fittbot/secrets"


def fetch_secret():
    """Fetch secrets from AWS Secrets Manager"""
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])




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
push_client = PushClient()


log = logging.getLogger("lambda_worker")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
    log.addHandler(h)

def _now_ist() -> datetime:
    """Current time in Asia/Kolkata (no DST)."""
    return datetime.now(tz=ZoneInfo("Asia/Kolkata"))

PAGE_BY_MODE = {
    "water": "Water",
    "gym":   "My Gym",
    "diet":  "Diet",
}

DEFAULT_PAGE = "Water"

REMINDER_TITLE = {
    "water": "Water Reminder",
    "gym":   "Gym Crowd Alert",
    "diet":  "Diet Reminder",
}

CHANNEL_BY_MODE = {
    "water": "default",
    "gym":   "workout_channel",
    "diet":  "diet_channel",
}


def lambda_handler(event, _context):
    sess= Session()
    processed = 0

    try:

        for record in event["Records"]:
            rid = json.loads(record["body"])["rid"]
            reminder: Reminder | None = (
                sess.execute(
                    select(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .with_for_update(skip_locked=True)
                )
                .scalar_one_or_none()
            )
            if not reminder or reminder.reminder_Sent:
                continue  

            client: Client = (
                sess.execute(
                    select(Client).where(Client.client_id == reminder.client_id)
                )
                .scalar_one()
            )
            tokens = client.expo_token if isinstance(client.expo_token, list) else [client.expo_token]
            tokens = [t for t in tokens if t]

            if not tokens:
                log.warning("Client %s has no Expo token; skipping", client.client_id)
                continue

            mode     = (reminder.reminder_mode or "").lower()
            page     = PAGE_BY_MODE.get(mode, DEFAULT_PAGE)
            title    = REMINDER_TITLE.get(mode, "Reminder")
            channel  = CHANNEL_BY_MODE.get(mode, "diet_channel")

            messages: List[PushMessage] = [
                PushMessage(
                    to=t,
                    sound="default",
                    priority="high",
                    title=title,
                    body=reminder.details,
                    data={
                        "reminder_id": rid,
                        "vibrationPattern": reminder.vibration_pattern,
                        "page": page,
                    },
                    channel_id=channel,
                    display_in_foreground=True,
                )
                for t in tokens
            ]

            try:
                push_client.publish_multiple(messages)
            except Exception as exc:
                log.error("Expo push failed (rid=%s): %s", rid, exc)
                raise

            delete_after_send = False   

            if mode == "water":

                next_dt = (
                    datetime.combine(_now_ist().date(), reminder.reminder_time, tzinfo=ZoneInfo("Asia/Kolkata"))
                    + timedelta(hours=reminder.water_timing)
                )
                nxt = next_dt.time()

                end_overflow = (
                    reminder.intimation_end_time
                    and nxt > reminder.intimation_end_time
                )

                update_values = {
                    "reminder_time": nxt,
                    "queued": False,
                    "sent_at": _now_ist(),
                }

                if end_overflow or not reminder.is_recurring:
                    update_values["reminder_Sent"] = True
                    delete_after_send = not reminder.is_recurring

                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(**update_values)
                )

            else:
                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(
                        reminder_Sent=True,
                        queued=False,
                        sent_at=_now_ist(),
                    )
                )
                delete_after_send = not reminder.is_recurring


            if delete_after_send:
                sess.execute(delete(Reminder).where(Reminder.reminder_id == rid))

            processed += 1


        sess.commit()
        log.info("Processed %d reminder(s)", processed)
        return {"processed": processed}

    except Exception:
        sess.rollback()
        log.exception("Reminder worker failed")
        raise

    finally:
        sess.close()