import json
import logging
import os
from typing import Dict, Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

_secrets_client = None


def _get_secrets_client(region_name: str):
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client(service_name="secretsmanager", region_name=region_name)
    return _secrets_client


def _fetch_secret(secret_name: str, region_name: str) -> Dict[str, Any]:
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
    secret_names = os.getenv("AWS_SECRETS_MANAGER_SECRETS")
    if not secret_names:
        return

    if os.getenv("AWS_SECRETS_MANAGER_SKIP", "0").lower() in ("1", "true", "yes"):
        return

    region = os.getenv("AWS_REGION", "ap-south-2")
    environment = os.getenv("ENVIRONMENT")
    if environment and environment.lower() not in ("production", "staging"):
        if os.getenv("AWS_SECRETS_MANAGER_FORCE_LOAD", "0").lower() not in ("1", "true", "yes"):
            return

    names = [name.strip() for name in secret_names.split(",") if name.strip()]
    if not names:
        return

    for name in names:
        try:
            payload = _fetch_secret(name, region)
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Unable to load secret %s: %s", name, exc)
            continue

        for key, value in payload.items():
            if value is None:
                continue
            if os.getenv(key):
                continue
            os.environ[key] = str(value)
