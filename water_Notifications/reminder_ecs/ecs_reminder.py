
import os, json, time, logging, sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
import boto3
from sqlalchemy import (
    create_engine, select, update, and_, or_, asc, case,
    Column, Integer, String, Float, Boolean, Time, JSON, DateTime, Index, ForeignKey, Text, Date, Numeric, Enum
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.mutable import MutableList
import uuid



ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"
QUEUE_URL = os.getenv("REMINDER_QUEUE_URL", "https://sqs.ap-south-2.amazonaws.com/182399696098/reminderqueue")
BATCH_ROWS = int(os.getenv("BATCH_ROWS", "5000"))


ENV_FILE = Path(__file__).resolve().parent.parent.parent.parent / ".env"


def load_env_file():
    """Load environment variables from .env file for local development"""
    if not ENV_FILE.exists():
        return {}

    env_vars = {}
    with open(ENV_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            env_vars[key] = value
    return env_vars


def get_db_credentials():
    """Get database credentials based on environment"""

    if ENVIRONMENT in ("local", "development", "dev"):
        # Load from .env file for local
        env_vars = load_env_file()

        db_username = env_vars.get("DB_USERNAME") or os.getenv("DB_USERNAME", "root")
        db_password = env_vars.get("DB_PASSWORD") or os.getenv("DB_PASSWORD", "")
        db_host = env_vars.get("DB_HOST") or os.getenv("DB_HOST", "localhost")
        db_name = env_vars.get("DB_NAME") or os.getenv("DB_NAME", "fittbot_local")

        return {
            "DB_USERNAME": db_username,
            "DB_PASSWORD": db_password,
            "DB_HOST": db_host,
            "DB_NAME": db_name,
        }
    else:
        # Production: fetch from AWS Secrets Manager
        sm = boto3.client("secretsmanager", region_name=REGION)
        val = sm.get_secret_value(SecretId=SECRET_NAME)
        return json.loads(val["SecretString"])


def build_connection_string(creds):
    """Build MySQL connection string with proper URL encoding"""
    username = creds.get("DB_USERNAME")
    password = creds.get("DB_PASSWORD")
    host = creds.get("DB_HOST")
    db_name = creds.get("DB_NAME")

    if password:
        return f"mysql+pymysql://{username}:{quote_plus(password)}@{host}/{db_name}"
    else:
        # No password (local development with root)
        return f"mysql+pymysql://{username}@{host}/{db_name}"


# ─────────────────────────────────────────────────────────────
# INITIALIZE DATABASE & SQS
# ─────────────────────────────────────────────────────────────

log = logging.getLogger("due_finder")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s  %(message)s",
)

log.info(f"Environment: {ENVIRONMENT}")

# Get credentials and build connection
credentials = get_db_credentials()
conn = build_connection_string(credentials)

log.info(f"Connecting to DB: {credentials.get('DB_HOST')}/{credentials.get('DB_NAME')}")

engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)

Session = sessionmaker(bind=engine, autoflush=False)

# SQS client (only used in production, but initialize anyway)
sqs = boto3.client("sqs", region_name=REGION) if ENVIRONMENT not in ("local", "development", "dev") else None



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



def run_once(dry_run=False):

    now_dt = datetime.now()
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
                            Reminder.intimation_start_time <= Reminder.reminder_time,
                            Reminder.intimation_end_time >= Reminder.reminder_time,
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

        log.info("Found %d due reminder(s): %s", len(rows), rows[:10])  # Show first 10

        if dry_run:
            log.info("[DRY RUN] Would queue %d IDs (not updating DB or SQS)", len(rows))
            sess.rollback()
            return

        # Mark as queued in DB
        sess.execute(
            update(Reminder)
            .where(Reminder.reminder_id.in_(rows))
            .values(queued=True)
        )
        sess.commit()

        if ENVIRONMENT in ("local", "development", "dev"):
            log.info("[LOCAL] Marked %d reminders as queued (SQS skipped)", len(rows))
        else:
            for i in range(0, len(rows), 10):
                sqs.send_message_batch(
                    QueueUrl=QUEUE_URL,
                    Entries=[
                        {"Id": str(rid), "MessageBody": json.dumps({"rid": rid})}
                        for rid in rows[i:i + 10]
                    ],
                )
            log.info("queued %d IDs to SQS", len(rows))

    except Exception:
        sess.rollback()
        log.exception("due-finder failed")

    finally:
        sess.close()


def main():
    """Run the reminder finder in a loop (production mode)"""
    while True:
        run_once()
        time.sleep(30)


if __name__ == "__main__":

    if "--once" in sys.argv:
        log.info("Running once (--once flag)")
        run_once()
    elif "--dry-run" in sys.argv:
        
        log.info("Dry run mode (--dry-run flag)")
        run_once(dry_run=True)
    else:
      
        main()


