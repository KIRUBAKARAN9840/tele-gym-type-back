

import asyncio
import json
import logging
import os
import random
import sys
from pathlib import Path
from urllib.parse import quote_plus

import boto3
import firebase_admin
from redis.asyncio import Redis as AsyncRedis, ConnectionPool as AsyncConnectionPool
from redis.exceptions import ConnectionError, TimeoutError
from firebase_admin import credentials, messaging
from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy.orm import sessionmaker, declarative_base

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"
FCM_BATCH_LIMIT = 500

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

log = logging.getLogger("rich_notif_task")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s  %(message)s",
)

# ─────────────────────────────────────────────────────────────
# MINIMAL ORM MODEL (mirrors the main app's Client)
# ─────────────────────────────────────────────────────────────

Base = declarative_base()


class Client(Base):
    __tablename__ = "clients"
    client_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    device_token = Column(JSON)


# ─────────────────────────────────────────────────────────────
# DB CREDENTIALS (same pattern as water_Notifications/reminder_ecs)
# ─────────────────────────────────────────────────────────────

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
        return f"mysql+pymysql://{username}@{host}/{db_name}"


# ─────────────────────────────────────────────────────────────
# FIREBASE
# ─────────────────────────────────────────────────────────────

def init_firebase():
    """Initialise Firebase Admin SDK."""
    if firebase_admin._apps:
        return
    sa_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "firebase",
        "fittbot-c72eb-firebase-adminsdk-fbsvc-bfc6a7f7e9.json",
    )
    sa_path = os.path.normpath(sa_path)
    if not os.path.exists(sa_path):
        log.error("Firebase service account JSON not found at %s", sa_path)
        sys.exit(1)
    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)
    log.info("Firebase initialised (project: %s)", cred.project_id)


# ─────────────────────────────────────────────────────────────
# TEMPLATES
# ─────────────────────────────────────────────────────────────

def load_templates(category: str) -> dict:
    """Load templates JSON and return the chosen category block."""
    tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notification_templates.json")
    with open(tpl_path, "r") as f:
        data = json.load(f)
    if category not in data:
        log.error("Category '%s' not found in templates. Available: %s", category, list(data.keys()))
        sys.exit(1)
    return data[category]


# ─────────────────────────────────────────────────────────────
# REDIS — cycle tracking (mirrors app/utils/redis_config.py)
# ─────────────────────────────────────────────────────────────

# Global connection pool — reused across calls (same pattern as main app)

_redis_pool: None | AsyncConnectionPool = None
_redis_client: None | AsyncRedis = None


def _get_redis_target() -> dict:
    """Determine Redis endpoint/connection sizing from env."""
    environment = os.getenv("ENVIRONMENT", "production").lower()

    if environment == "production":
        target = {
            "host": "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 200,
        }
        log.info("[redis-config] ENV=production target=%s", target)
        return target

    if environment == "staging":
        target = {
            "host": "staging-redis.azdytp.ng.0001.aps2.cache.amazonaws.com",
            "port": 6379,
            "max_connections": 150,
        }
        log.info("[redis-config] ENV=staging target=%s", target)
        return target

    target = {"host": "localhost", "port": 6379, "max_connections": 100}
    log.info("[redis-config] ENV=%s default target=%s", environment, target)
    return target


def _get_async_connection_kwargs() -> dict:
    """Socket tuning for asyncio redis pools."""
    return dict(
        decode_responses=True,
        socket_keepalive=True,
        socket_keepalive_options={},
        retry_on_timeout=True,
        retry_on_error=[ConnectionError, TimeoutError],
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


def create_redis_pool() -> AsyncConnectionPool:
    """Create Redis connection pool for enterprise connection management."""
    target = _get_redis_target()
    connection_kwargs = _get_async_connection_kwargs()

    if "url" in target:
        return AsyncConnectionPool.from_url(
            target["url"],
            max_connections=target["max_connections"],
            **connection_kwargs,
        )

    return AsyncConnectionPool(
        host=target["host"],
        port=target["port"],
        max_connections=target["max_connections"],
        **connection_kwargs,
    )


async def get_redis() -> AsyncRedis:
    """Get Redis client with enterprise connection pooling."""
    global _redis_pool, _redis_client

    if _redis_client is None:
        if _redis_pool is None:
            _redis_pool = create_redis_pool()

        _redis_client = AsyncRedis(connection_pool=_redis_pool)

        # Test connection
        try:
            await _redis_client.ping()
        except Exception as e:
            log.warning("Redis connection failed: %s", e)
            # Reset and retry once
            _redis_client = None
            _redis_pool = None
            if _redis_pool is None:
                _redis_pool = create_redis_pool()
            _redis_client = AsyncRedis(connection_pool=_redis_pool)

    return _redis_client


async def close_redis():
    """Close Redis connections gracefully."""
    global _redis_pool, _redis_client

    if _redis_client:
        await _redis_client.close()
        _redis_client = None

    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None


async def pick_from_cycle(rds: AsyncRedis, category: str, pool_name: str, pool_size: int) -> int:
    """
    Return the next unused random index from a pool.
    Once every index has been used the cycle resets automatically.
    """
    key = f"notif:cycle:{category}:{pool_name}:used"
    used = await rds.smembers(key)
    used_indices = {int(x) for x in used}
    all_indices = set(range(pool_size))
    remaining = all_indices - used_indices

    if not remaining:
        await rds.delete(key)
        remaining = all_indices
        log.info("Cycle reset for %s:%s (all %d items used)", category, pool_name, pool_size)

    chosen = random.choice(sorted(remaining))
    await rds.sadd(key, chosen)
    log.info("Picked %s:%s index %d  (%d/%d used after this pick)",
             category, pool_name, chosen,
             pool_size - len(remaining) + 1, pool_size)
    return chosen


# ─────────────────────────────────────────────────────────────
# FCM SEND
# ─────────────────────────────────────────────────────────────

def send_fcm_batch(tokens, title, body, image_url):
    """Send a multicast FCM message. Returns (success, failure, invalid_tokens)."""
    notification = messaging.Notification(title=title, body=body, image=image_url)

    android_config = messaging.AndroidConfig(
        priority="high",
        notification=messaging.AndroidNotification(
            channel_id="rich_notifications",
            image=image_url,
            sound="default",
        ),
    )

    apns_config = messaging.APNSConfig(
        payload=messaging.APNSPayload(
            aps=messaging.Aps(
                alert=messaging.ApsAlert(title=title, body=body),
                mutable_content=True,
                sound="default",
            ),
        ),
        fcm_options=messaging.APNSFCMOptions(image=image_url) if image_url else None,
    )

    msg = messaging.MulticastMessage(
        tokens=tokens,
        notification=notification,
        android=android_config,
        apns=apns_config,
        data={"type": "rich_notification"},
    )

    resp = messaging.send_each_for_multicast(msg)

    invalid = []
    for i, sr in enumerate(resp.responses):
        if sr.exception:
            err = str(sr.exception).upper()
            if "UNREGISTERED" in err or "NOT_FOUND" in err:
                invalid.append(tokens[i])

    return resp.success_count, resp.failure_count, invalid


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

async def main():
    category = os.environ.get("NOTIF_CATEGORY", "dailypass")
    log.info("=== Rich Notification Task  |  category: %s  |  env: %s ===", category, ENVIRONMENT)

    # 1. Load template + image (Redis-backed cycle — no repeats until full rotation)
    cat_data = load_templates(category)

    try:
        rds = await get_redis()
        template_idx = await pick_from_cycle(rds, category, "templates", len(cat_data["templates"]))
        image_idx = await pick_from_cycle(rds, category, "images", len(cat_data["images"]))
        template = cat_data["templates"][template_idx]
        image_url = cat_data["images"][image_idx]
    except Exception as exc:
        log.warning("Redis unavailable (%s) — falling back to random.choice", exc)
        template = random.choice(cat_data["templates"])
        image_url = random.choice(cat_data["images"])

    log.info("Template  → %s", template["title"])
    log.info("Image     → %s", image_url)

    # 2. Firebase
    init_firebase()

    # 3. DB — same credential flow as water reminder task
    creds = get_db_credentials()
    conn = build_connection_string(creds)
    log.info("Connecting to DB: %s/%s", creds.get("DB_HOST"), creds.get("DB_NAME"))

    engine = create_engine(
        conn, pool_pre_ping=True, pool_size=2,
        max_overflow=1, pool_recycle=300,
    )
    Session = sessionmaker(bind=engine)
    db = Session()

    try:
        test_ids = os.environ.get("TEST_CLIENT_IDS")
        if test_ids:
            ids = [int(x.strip()) for x in test_ids.split(",")]
            log.info("TEST MODE — sending only to client IDs: %s", ids)
            clients = db.query(Client).filter(
                Client.client_id.in_(ids),
                Client.device_token.isnot(None),
            ).all()
        else:
            clients = db.query(Client).filter(Client.device_token.isnot(None)).all()
        log.info("Clients with device tokens: %d", len(clients))

        total_sent = 0
        total_failed = 0
        total_invalid = []

        for client in clients:
            tokens = client.device_token
            if not tokens:
                continue
            if not isinstance(tokens, list):
                tokens = [tokens]
            tokens = [t for t in tokens if t]
            if not tokens:
                continue

            # Personalise
            client_name = client.name or "there"
            title = template["title"].replace("{{name}}", client_name)
            body = template["body"].replace("{{name}}", client_name)

            # Send in batches of 500
            for i in range(0, len(tokens), FCM_BATCH_LIMIT):
                batch = tokens[i : i + FCM_BATCH_LIMIT]
                sent, failed, invalid = send_fcm_batch(batch, title, body, image_url)
                total_sent += sent
                total_failed += failed

                # Clean up invalid tokens from DB
                if invalid:
                    total_invalid.extend(invalid)
                    current = client.device_token if isinstance(client.device_token, list) else [client.device_token]
                    updated = [t for t in current if t and t not in invalid]
                    client.device_token = updated if updated else None

        # Commit token cleanups
        if total_invalid:
            db.commit()
            log.info("Cleaned %d invalid device tokens", len(total_invalid))

        log.info("=== DONE  |  sent: %d  |  failed: %d  |  clients: %d ===", total_sent, total_failed, len(clients))

    except Exception:
        db.rollback()
        log.exception("Fatal error during notification send")
        sys.exit(1)
    finally:
        db.close()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
