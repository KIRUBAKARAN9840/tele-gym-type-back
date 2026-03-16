
import json, os, sys, logging, uuid
from datetime     import date, datetime
from typing       import List
from urllib.parse import quote_plus

import boto3
import pymysql
from sqlalchemy    import (
    create_engine, Column, Integer, String, Float, Boolean, Date,
    DateTime, Enum, JSON, ForeignKey, func, BigInteger
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session


# ─── CONFIGURATION ────────────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"
SQS_URL = os.getenv("SQS_URL", "https://sqs.ap-south-2.amazonaws.com/182399696098/reminderqueue")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

log = logging.getLogger("birthday-job")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s  %(message)s",
)

# ────────────────────────── AWS CLIENTS ────────────────────────────────────
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
dsn = build_connection_string(creds)
log.info(f"Environment: {ENVIRONMENT}, DB: {creds.get('DB_HOST')}/{creds.get('DB_NAME')}")

engine       = create_engine(dsn, pool_pre_ping=True, pool_size=4, max_overflow=0, pool_recycle=300)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base         = declarative_base()

# ────────────────────────── MODELS ─────────────────────────────────────────
class Client(Base):
    __tablename__ = "clients"

    client_id   = Column(Integer, primary_key=True, index=True)
    gym_id      = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE"))
    name        = Column(String(100), nullable=False)
    email       = Column(String(100), unique=True, nullable=False)
    contact     = Column(String(15), nullable=False)
    age         = Column(Integer)
    status      = Column(Enum("active", "inactive"), default="active")
    dob         = Column(Date)
    expo_token  = Column(JSON)                        # list OR single string
    created_at  = Column(DateTime, default=datetime.now())
    updated_at  = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class BirthdayNotification(Base):
    __tablename__ = "birthday_notification"

    id         = Column(BigInteger, primary_key=True, autoincrement=True)
    client_id  = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE"))
    expo_token = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)

# ────────────────────────── CORE FUNCTION ─────────────────────────────────
def run_birthday_job( db: Session) -> dict:
    today = date.today()

    birthday_clients: List[Client] = (
        db.query(Client)
        .filter(
            Client.dob.isnot(None),
            func.month(Client.dob) == today.month,
            func.day(Client.dob)   == today.day,
        )
        .all()
    )

    if not birthday_clients:
        log.info("Gym %s: no birthdays today")
        return {"updated": 0, "notified": 0, "chunks": 0}

    notifications: List[BirthdayNotification] = []
    recipients    = []

    for c in birthday_clients:
        # age bump
        c.age = (c.age + 1) if c.age else today.year - c.dob.year

        tokens = c.expo_token or []
        if isinstance(tokens, str):
            tokens = [tokens]

        if c.status == "active":
            first = (c.name or "there").split()[0]
            for tok in tokens:
                if not tok:
                    continue
                notifications.append(BirthdayNotification(client_id=c.client_id, expo_token=tok))
                recipients.append({"token": tok, "name": first, "client_id": c.client_id})

    if notifications:
        db.bulk_save_objects(notifications)
    db.commit()

    # Send to SQS
    chunks = 0
    for i in range(0, len(recipients), BATCH_SIZE):
        batch = recipients[i : i + BATCH_SIZE]
        sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=json.dumps({"template": "Birthday", "recipients": batch}),
        )
        chunks += 1

    log.info(
        "Gym %s: %d clients age-updated, %d notifications, %d SQS chunk(s)",
         len(birthday_clients), len(recipients), chunks
    )
    return {
        "updated":   len(birthday_clients),
        "notified":  len(recipients),
        "chunks":    chunks,
        "client_ids": [c.client_id for c in birthday_clients],
    }

# ────────────────────────── CLI ENTRYPOINT ────────────────────────────────
if __name__ == "__main__":

    with SessionLocal() as session:
        run_birthday_job(session)
