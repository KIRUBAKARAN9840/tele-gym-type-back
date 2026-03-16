import os, json, logging, boto3
from datetime import datetime, date
from zoneinfo import ZoneInfo
from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError
from sqlalchemy import create_engine, select, or_
import uuid
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy import Column, Integer,BigInteger, String, Float, Enum, Text, DateTime, ForeignKey, Date,Time,Boolean,JSON,Numeric
from sqlalchemy.orm import sessionmaker, Session,declarative_base

Base = declarative_base()



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
    muscle_3  = Column(JSON)





from urllib.parse import quote_plus

# ─── CONFIGURATION ────────────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"

def _get_redis_target():
    """Determine Redis endpoint/connection sizing from env (matches ephemeral_tasks)."""
    # Allow explicit override via environment variable
    if os.getenv("REDIS_HOST"):
        return {"host": os.getenv("REDIS_HOST"), "port": 6379, "max_connections": 100}

    # Environment-based defaults
    if ENVIRONMENT == "production":
        return {
            "host": "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 200,
        }
    elif ENVIRONMENT == "staging":
        return {
            "host": "staging-redis.azdytp.ng.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 150,
        }
    else:  # local, development, dev
        return {"host": "localhost", "port": 6379, "max_connections": 100}


def _redis_connection_kwargs():
    """Shared connection options mirroring ephemeral_tasks defaults."""
    return {
        "decode_responses": True,
        "socket_keepalive": True,
        "socket_keepalive_options": {},
        "retry_on_timeout": True,
        "retry_on_error": [ConnectionError, TimeoutError],
        "health_check_interval": 30,
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
    }


REDIS_TARGET = _get_redis_target()
NUDGE_QUEUE      = os.getenv("NUDGE_QUEUE", "https://sqs.ap-south-2.amazonaws.com/182399696098/GeneralReminderQueue")
PUNCH_QUEUE      = os.getenv("PUNCH_QUEUE", "https://sqs.ap-south-2.amazonaws.com/182399696098/auto-punchout-queue")
MAX_BATCH        = 100
SESSION_TEMPLATE = "session_nudge"
PUNCH_TEMPLATE   = "punchout_intimation"
TZ               = ZoneInfo("Asia/Kolkata")

log = logging.getLogger()
log.setLevel(logging.INFO)

sqs = boto3.client("sqs", region_name=REGION)


def load_env_file():
    """Load environment variables from .env file for local development"""
    from pathlib import Path
    env_file = Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return {}

    env_vars = {}
    with open(env_file, "r") as f:
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
    """Get database credentials based on environment (matches ephemeral_tasks)"""
    # Local/dev: load from .env file
    if ENVIRONMENT in ("local", "development", "dev"):
        env_vars = load_env_file()

        db_username = env_vars.get("DB_USERNAME") or os.getenv("DB_USERNAME", "root")
        db_password = env_vars.get("DB_PASSWORD") or os.getenv("DB_PASSWORD", "")
        db_host = env_vars.get("DB_HOST") or os.getenv("DB_HOST", "localhost")
        db_name = env_vars.get("DB_NAME") or os.getenv("DB_NAME", "fittbot_local")

        log.info(f"[LOCAL] Loading credentials from .env: host={db_host}, db={db_name}")

        return {
            "DB_USERNAME": db_username,
            "DB_PASSWORD": db_password,
            "DB_HOST": db_host,
            "DB_NAME": db_name,
        }

    # Check if credentials already in environment (from ECS task definition)
    if os.getenv("DB_USERNAME") and os.getenv("DB_HOST"):
        return {
            "DB_USERNAME": os.getenv("DB_USERNAME"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "DB_HOST": os.getenv("DB_HOST"),
            "DB_NAME": os.getenv("DB_NAME"),
        }

    # Production: fetch from AWS Secrets Manager
    log.info(f"[PRODUCTION] Fetching credentials from Secrets Manager: {SECRET_NAME}")
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
    return f"mysql+pymysql://{username}@{host}/{db_name}"


creds = get_db_credentials()
conn = build_connection_string(creds)
log.info(f"Environment: {ENVIRONMENT}, DB: {creds.get('DB_HOST')}/{creds.get('DB_NAME')}")

engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False)

redis = Redis(
    host=REDIS_TARGET["host"],
    port=REDIS_TARGET["port"],
    **_redis_connection_kwargs()
)


def _active_slot(att):
    if att.out_time is None:                     return att.in_time, 0
    if att.in_time_2 and att.out_time_2 is None: return att.in_time_2, 1
    if att.in_time_3 and att.out_time_3 is None: return att.in_time_3, 2
    return None, None

def _flag_once(key):       
    return redis.setnx(key, "1") and redis.expire(key, 86400)


def lambda_handler(event, _ctx):
    now = datetime.now(tz=TZ)
    today = now.date()

    sess: Session = SessionLocal()
    # Lists to collect recipients for each template
    session_nudges = []
    punch_nudges   = []
    autos          = []

    try:
        rows = sess.execute(
            select(Attendance, Client.expo_token, Client.name)
            .join(Client, Client.client_id == Attendance.client_id)
            .where(Attendance.date == today)
            .where(
                or_(
                    Attendance.out_time == None,
                    (Attendance.in_time_2 != None) & (Attendance.out_time_2 == None),
                    (Attendance.in_time_3 != None) & (Attendance.out_time_3 == None)
                )
            )
        ).all()

        print("rows",rows)

        for att, token, full_name in rows:
            in_t, slot_idx = _active_slot(att)
            if not in_t or not token:
                continue

            mins = (now - datetime.combine(today, in_t, TZ)).total_seconds() / 60
            rid  = att.record_id
            name = (full_name or "there").split()[0]

            # 1–90 min: session nudge
            if 60 <= mins < 90 and _flag_once(f"60:{rid}"):
                session_nudges += _tok(token, name, att.client_id)

            # 90–120 min: session nudge
            elif 90 <= mins < 120 and _flag_once(f"90:{rid}"):
                session_nudges += _tok(token, name, att.client_id)

            # >= 120 min: punch-out intimation
            elif mins >= 120:
                if redis.exists(f"60:{rid}"):
                    redis.delete(f"60:{rid}")
                if redis.exists(f"90:{rid}"):
                    redis.delete(f"90:{rid}")
                autos.append({"gym_id": att.gym_id, "client_id": att.client_id})
                punch_nudges += _tok(token, name, att.client_id)
                #_close_slot(att, slot_idx, now.time(), sess)

        sess.commit()
    finally:
        sess.close()

    # Enqueue session nudges
    print("punch_nudges",punch_nudges)
    _enqueue(session_nudges, NUDGE_QUEUE, {"template": SESSION_TEMPLATE})
    # Enqueue punch-out nudges with new template
    _enqueue(punch_nudges,   NUDGE_QUEUE, {"template": PUNCH_TEMPLATE})
    # Enqueue auto-punch actions
    _enqueue(autos,          PUNCH_QUEUE, {"action": "auto_punch"})

    log.info("session_nudges=%d  punch_nudges=%d  auto_out=%d", 
             len(session_nudges), len(punch_nudges), len(autos))
    return {"session_nudges": len(session_nudges), "punch_nudges": len(punch_nudges), "auto_out": len(autos)}

def _tok(tok_field, name, client_id):
    if isinstance(tok_field, list):
        return [{"token": t, "name": name, "client_id": client_id} for t in tok_field if t]
    return [{"token": tok_field, "name": name, "client_id": client_id}]

def _enqueue(items, queue_url, base):
    for i in range(0, len(items), MAX_BATCH):
        body = base | {"recipients" if "template" in base else "list": items[i:i+MAX_BATCH]}
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))

def _close_slot(att, idx, out_t, sess):
    if idx == 0:   att.out_time   = out_t
    elif idx == 1: att.out_time_2 = out_t
    else:          att.out_time_3 = out_t
    sess.flush()
