import os, json, time, logging, requests, boto3
from datetime import datetime
from botocore.exceptions import ClientError, EndpointConnectionError
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, ForeignKey,
    Text, Boolean, func
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


REGION       = "ap-south-2"
SECRET_NAME  = "fittbot/mysqldb"
RDS_HOST     = "devfittbotdb.c5ayks2cmx9u.ap-south-2.rds.amazonaws.com"
DB_NAME      = "fittbot"

BROADCAST_URL = "https://app.fittbot.com/websocket_feed/internal/new_post"   
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

# ───────── Cached globals ─────────────────────────────────
_engine = _SessionLocal = None

# ───────── Helpers ───────────────────────────────────────
def fetch_secret():
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])

def init_db():
    global _engine, _SessionLocal
    if _engine:
        return
    creds = fetch_secret()
    conn  = (
        f"mysql+pymysql://{creds['username']}:{creds['password']}@"
        f"{RDS_HOST}:3306/{DB_NAME}"
    )
    _engine = create_engine(
        conn,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
        echo=False,
    )
    _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


_bcast_sess = requests.Session()
_bcast_sess.mount(
    "https://",
    requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=16, max_retries=3),
)

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

    if should_broadcast:
        print("going to broadcast")
        broadcast(post.gym_id, post.post_id)

    else:
        print(" inside else going to broadcast")



# ───────── Lambda entry ──────────────────────────────────
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



