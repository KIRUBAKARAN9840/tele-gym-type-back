import os, json, logging, uuid
from datetime import datetime
from urllib.parse import quote_plus

import boto3
from sqlalchemy import Column, Integer, BigInteger, String, Float, Enum, Text, DateTime, ForeignKey, Date, Time, Boolean, JSON, Numeric
from sqlalchemy import create_engine, select, and_, or_, func, cast, exists
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.ext.mutable import MutableList


# ─── CONFIGURATION ────────────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"
SQS_URL = os.getenv("SQS_URL", "https://sqs.ap-south-2.amazonaws.com/182399696098/GeneralReminderQueue")
BATCH_SIZE = 100
PREMIUM_SUBSCRIPTION_STATUSES = ("active", "renewed")

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


# ────────────────────────── DB INITIALISATION ──────────────────────────────
creds = get_db_credentials()
conn = build_connection_string(creds)
log.info(f"Environment: {ENVIRONMENT}, DB: {creds.get('DB_HOST')}/{creds.get('DB_NAME')}")

engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False)

Base = declarative_base()


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
    uuid_client = Column(String, unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = {"schema": "payments"}

    id = Column(String(100), primary_key=True)
    customer_id = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)
    active_from = Column(DateTime(timezone=True), nullable=True)
    active_until = Column(DateTime(timezone=True), nullable=True)


class ClientBirthday(Base):
    __tablename__ = "client_birthdays"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"), nullable=False, unique=True)
    client_name = Column(String(100), nullable=False)
    expo_token = Column(JSON, nullable=False)



def lambda_handler(event, _ctx):
    tpl_key = event.get("template")
    if not tpl_key:
        raise ValueError("Event must include 'template'")

    sess: Session = SessionLocal()
    try:

        if tpl_key == "birthday":
            rows = sess.execute(
                select(ClientBirthday.client_name,
                       ClientBirthday.expo_token)
            ).all()
        else:
            premium_filter = or_(
                Subscription.status.in_(PREMIUM_SUBSCRIPTION_STATUSES),
                and_(
                    Subscription.status == "canceled",
                    Subscription.active_until.isnot(None),
                    Subscription.active_until >= func.now(),
                ),
            )
            # Premium access: active/renewed subscription or canceled but still within the paid window.
            premium_exists = exists().where(
                Subscription.customer_id == cast(Client.client_id, String(100)),
                premium_filter,
            )
            rows = sess.execute(
                select(Client.name, Client.expo_token)
                .where(premium_exists)
            ).all()

        recipients = []
        for full_name, tok in rows:
            if not tok:
                continue
            first = (full_name or "there").split()[0]
            if isinstance(tok, list):
                recipients += [{"token": t, "name": first} for t in tok if t]
            else:
                recipients.append({"token": tok, "name": first})

        log.info("DB mode – %d recipients for template '%s'", len(recipients), tpl_key)

    finally:
        sess.close()

    chunks = 0
    for i in range(0, len(recipients), BATCH_SIZE):
        batch = recipients[i : i + BATCH_SIZE]
        sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=json.dumps({
                "template":   tpl_key,
                "recipients": batch
            }),
        )
        chunks += 1

    log.info("Enqueued %d SQS message(s) for template '%s'", chunks, tpl_key)
    return {"recipients": len(recipients), "chunks": chunks}
