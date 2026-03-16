import os, json, time, logging, requests, boto3
from datetime import datetime
from urllib.parse import quote_plus
from botocore.exceptions import ClientError, EndpointConnectionError
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, ForeignKey,
    Text, Boolean, func
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


REGION       = "ap-south-2"
SECRET_NAME  = "fittbot/secrets"

BROADCAST_URL = "https://app.fittbot.com/websocket_feed/internal/new_post"
CACHE_INVALIDATE_URL = "https://app.fittbot.com/websocket_feed/internal/invalidate_cache"
BROADCAST_KEY = "lambda_header_feed_not_out"                                       


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ───────── ORM  ───────────────────────────────────────────
Base = declarative_base()

class Post(Base):
    __tablename__ = "posts"
    post_id    = Column(Integer, primary_key=True, index=True)
    gym_id     = Column(Integer, nullable=False, index=True)
    client_id  = Column(Integer, nullable=True, index=True)
    content    = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_pinned  = Column(Boolean, default=False)
    status     = Column(String(45))
    media      = relationship(
        "PostMedia",
        back_populates="post",
        cascade="all, delete-orphan",
    )

class PostMedia(Base):
    __tablename__ = "post_media"
    media_id   = Column(Integer, primary_key=True)
    post_id    = Column(Integer, ForeignKey("posts.post_id", ondelete="CASCADE"))
    file_name  = Column(String, nullable=False)
    file_type  = Column(String, nullable=False)
    file_path  = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status     = Column(String(45))
    post       = relationship("Post", back_populates="media")

# ───────── Configuration ─────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()

# ───────── Cached globals ─────────────────────────────────
_engine = _SessionLocal = None

# ───────── Helpers ───────────────────────────────────────
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

        logger.info(f"[LOCAL] Loading credentials from .env: host={db_host}, db={db_name}")

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
    logger.info(f"[PRODUCTION] Fetching credentials from Secrets Manager: {SECRET_NAME}")
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


def init_db():
    global _engine, _SessionLocal
    if _engine:
        return
    creds = get_db_credentials()
    conn = build_connection_string(creds)
    logger.info(f"Environment: {ENVIRONMENT}, DB: {creds.get('DB_HOST')}/{creds.get('DB_NAME')}")
    _engine = create_engine(
        conn,
        pool_pre_ping=True,
        pool_size=4,
        max_overflow=0,
        pool_recycle=300,
        connect_args={"connect_timeout": 5},
        echo=False,
    )
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


_bcast_sess = requests.Session()
_bcast_sess.mount(
    "https://",
    requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=3),
)

def invalidate_cache(gym_id: int, post_id: int) -> None:
    """
    Invalidate Redis cache via HTTP endpoint.
    Deletes gym posts cache and post media cache.
    """
    payload = {"gym_id": gym_id, "post_id": post_id}

    try:
        resp = _bcast_sess.post(
            CACHE_INVALIDATE_URL,
            headers={"x-api-key": BROADCAST_KEY},
            json=payload,
            timeout=3,
        )
        logger.info("✔ Cache invalidated %s → HTTP %s • body=%s",
                   payload, resp.status_code, resp.text[:120])
    except requests.exceptions.RequestException as exc:
        logger.error("✖ Cache invalidation failed [%s] %s", type(exc).__name__, exc)

def broadcast(gym_id: int, post_id: int) -> None:


    payload = {"gym_id": gym_id, "post_id": post_id}

    try:
        resp = _bcast_sess.post(
            BROADCAST_URL,                      
            headers={"x-api-key": BROADCAST_KEY},
            json=payload,
            timeout=2,     
        )
        logger.info("📣 broadcast %s → HTTP %s • body=%s…",
                    payload, resp.status_code, resp.text[:120])

    except requests.exceptions.RequestException as exc:
        logger.error("✖ broadcast failed [%s] %s", type(exc).__name__, exc)




from sqlalchemy import func   

def handle_object(bucket: str, key: str, region: str, session):
    media = (
        session.query(PostMedia)
        .filter(PostMedia.file_name == key)
        .with_for_update()
        .one_or_none()
    )
    if not media:
        logger.warning("⚠ no PostMedia row for %s", key)
        return

    if media.status != "completed":
        media.file_path = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        media.status    = "completed"
        logger.info("✔ marked completed %s", key)

    post = (
        session.query(Post)
        .filter(Post.post_id == media.post_id)
        .with_for_update()
        .one()
    )

    remaining = (
        session.query(func.count(PostMedia.media_id))
        .filter(
            PostMedia.post_id == post.post_id,
            PostMedia.status != "completed"
        )
        .scalar()
    )
    print("remianing is",remaining)

    should_broadcast = False
    if remaining == 1 and post.status != "completed":
        post.status = "completed"
        should_broadcast = True        # we’ll broadcast after we COMMIT

    session.commit()                  # 🔓 release locks

    # ✅ CACHE INVALIDATION - Delete Redis cache after DB has S3 URLs
    if should_broadcast:
        # Invalidate cache via HTTP endpoint (avoids VPC networking issues)
        invalidate_cache(post.gym_id, post.post_id)

        # Broadcast to websocket clients
        print("going to broadcast")
        broadcast(post.gym_id, post.post_id)

    else:
        print("inside else - not all media completed yet")


def lambda_handler(event, context):
    logger.info("Lambda start – %d SQS records", len(event.get("Records", [])))
    init_db()

    if not event.get("Records"):
        return {"statusCode": 200, "body": json.dumps({"msg": "empty batch"})}

    for record in event["Records"]:
        body = json.loads(record["body"])
        for s3 in body.get("Records", []):
            bucket = s3["s3"]["bucket"]["name"]
            key    = s3["s3"]["object"]["key"]
            region = s3["awsRegion"]
            logger.info("▶ %s/%s", bucket, key)

            with _SessionLocal() as session:
                try:
                    handle_object(bucket, key, region,session)
                except OperationalError as exc:
                    session.rollback()
                    logger.error("✖ DB error for %s – %s", key, exc)
                    raise

    return {"statusCode": 200, "body": json.dumps({"msg": "done"})}



