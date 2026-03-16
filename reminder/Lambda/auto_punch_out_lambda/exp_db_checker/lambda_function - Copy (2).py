import os
import json
import logging
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo
from redis import Redis, RedisError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ——— CONFIG —————————————————————————————————————————————
TZ = ZoneInfo("Asia/Kolkata")

REDIS_HOST    = os.environ.get("REDIS_HOST", "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com")
SECRET_NAME   = os.environ.get("SECRET_NAME", "fittbot/mysqldb")
REGION        = os.environ.get("AWS_REGION", "ap-south-2")

# ——— LOGGING SETUP ——————————————————————————————————————
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
logger.handlers = [handler]

# ——— AWS CLIENTS ————————————————————————————————————————
sm_client = boto3.client("secretsmanager", region_name=REGION)

# ——— REDIS CLIENT ——————————————————————————————————————
redis_client = Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def lambda_handler(event, context):
    now = datetime.now(tz=TZ)
    logger.info("=== Connectivity check invoked at %s ===", now.isoformat())

    result = {
        "secrets_manager": {"status": None, "error": None},
        "redis":           {"status": None, "error": None},
        "mysql":           {"status": None, "error": None},
    }

    # 1️⃣ Fetch secrets
    try:
        logger.debug("Fetching secret '%s' from Secrets Manager…", SECRET_NAME)
        sec = sm_client.get_secret_value(SecretId=SECRET_NAME)
        creds = json.loads(sec["SecretString"])
        DB_USER = creds["username"]
        DB_PASS = creds["password"]
        DB_HOST = creds.get("host", creds.get("host", "")) or os.environ.get("RDS_HOST")
        DB_NAME = creds.get("dbname", creds.get("database", "")) or os.environ.get("DB_NAME")
        logger.info("✅ Secrets fetched successfully")
        result["secrets_manager"]["status"] = "OK"
    except Exception as e:
        logger.error("❌ Secrets Manager fetch failed: %s", e, exc_info=True)
        result["secrets_manager"]["status"] = "ERROR"
        result["secrets_manager"]["error"] = str(e)

    # 2️⃣ Test Redis
    try:
        logger.debug("Pinging Redis at %s…", REDIS_HOST)
        if redis_client.ping():
            logger.info("✅ Redis ping successful")
            result["redis"]["status"] = "OK"
        else:
            raise RedisError("PING returned False")
    except Exception as e:
        logger.error("❌ Redis ping failed: %s", e, exc_info=True)
        result["redis"]["status"] = "ERROR"
        result["redis"]["error"] = str(e)

    # 3️⃣ Test MySQL (only if secrets came back OK)
    if result["secrets_manager"]["status"] == "OK":
        try:
            conn_str = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}"
            logger.debug("Creating engine to MySQL: %s…", DB_HOST)
            engine = create_engine(conn_str, pool_pre_ping=True, pool_size=2, max_overflow=0)
            with engine.connect() as conn:
                logger.debug("Executing SELECT 1…")
                val = conn.execute(text("SELECT 1")).scalar()
                if val == 1:
                    logger.info("✅ MySQL test query returned 1")
                    result["mysql"]["status"] = "OK"
                else:
                    raise SQLAlchemyError(f"Unexpected result: {val}")
        except Exception as e:
            logger.error("❌ MySQL connectivity failed: %s", e, exc_info=True)
            result["mysql"]["status"] = "ERROR"
            result["mysql"]["error"] = str(e)
    else:
        logger.warning("Skipping MySQL test because Secrets Manager fetch failed")

    logger.info("=== Result: %s ===", json.dumps(result))
    return {
        "statusCode": 200,
        "body": json.dumps({
            "timestamp": now.isoformat(),
            "connectivity": result
        })
    }
