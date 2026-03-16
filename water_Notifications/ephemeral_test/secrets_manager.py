import json
import logging
import os
from pathlib import Path
from typing import Dict, Any
from urllib.parse import quote_plus

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION (same as ecs_reminder.py)
# ─────────────────────────────────────────────────────────────

ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"

ENV_FILE = Path(__file__).resolve().parent / ".env"

_secrets_client = None


def _get_secrets_client(region_name: str):
    """Create or reuse a Secrets Manager client."""
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client(service_name="secretsmanager", region_name=region_name)
    return _secrets_client


def load_env_file() -> Dict[str, str]:
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


def get_db_credentials() -> Dict[str, Any]:
    """Get database credentials based on environment (same logic as ecs_reminder.py)"""

    if ENVIRONMENT in ("local", "development", "dev"):
        # Load from .env file for local
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
    else:
        # Production: fetch from AWS Secrets Manager
        logger.info(f"[PRODUCTION] Fetching credentials from Secrets Manager: {SECRET_NAME}")
        client = _get_secrets_client(REGION)
        val = client.get_secret_value(SecretId=SECRET_NAME)
        return json.loads(val["SecretString"])


def build_connection_string(creds: Dict[str, Any]) -> str:
    """Build MySQL connection string with proper URL encoding (same as ecs_reminder.py)"""
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
# LEGACY FUNCTION (kept for backward compatibility)
# ─────────────────────────────────────────────────────────────

def _fetch_secret(secret_name: str, region_name: str) -> Dict[str, Any]:
    """Fetch a single secret payload."""
    client = _get_secrets_client(region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString")
    if secret_string:
        try:
            return json.loads(secret_string)
        except json.JSONDecodeError:
            return {"SECRET_VALUE": secret_string}
    secret_binary = response.get("SecretBinary")
    if secret_binary:
        return {"SECRET_VALUE": secret_binary.decode("utf-8")}
    return {}


def load_secrets_into_env():
    """Load DB credentials into environment variables.

    Uses get_db_credentials() which handles both local (.env) and
    production (AWS Secrets Manager) environments.
    """
    # Skip if already loaded
    if os.getenv("DB_USERNAME") and os.getenv("DB_HOST") and os.getenv("DB_NAME"):
        logger.info("[secrets] DB credentials already in environment, skipping load")
        return

    try:
        creds = get_db_credentials()
        for key, value in creds.items():
            if value is not None and not os.getenv(key):
                os.environ[key] = str(value)
        logger.info("[secrets] Loaded DB credentials into environment")
    except (ClientError, BotoCoreError) as exc:
        logger.error("Failed to load secrets: %s", exc)
        raise
