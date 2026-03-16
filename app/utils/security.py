

from datetime import datetime, timedelta
from jose import jwt
from passlib.context import CryptContext
import os
import boto3
from botocore.exceptions import ClientError
import json
from dotenv import load_dotenv

load_dotenv()


def get_secret(secret_name, region_name="ap-south-2"):
    """Fetch secrets from AWS Secrets Manager."""
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        raise Exception(f"Unable to retrieve secret: {str(e)}")


ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

if ENVIRONMENT == "production":
    secret_name = "fittbot/sessiontoken"
    secrets = get_secret(secret_name)
else:
    secrets = {
        "SECRET_KEY": os.getenv("SECRET_KEY", "local-secret-key"),
        "ALGORITHM": os.getenv("ALGORITHM", "HS256"),
        "ACCESS_TOKEN_EXPIRE_MINUTES": os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 15),
        "REFRESH_TOKEN_EXPIRE_DAYS": os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 30)
    }


SECRET_KEY = secrets.get("SECRET_KEY")
ALGORITHM = secrets.get("ALGORITHM")


ACCESS_TOKEN_EXPIRE_MINUTES = int(secrets.get("ACCESS_TOKEN_EXPIRE_MINUTES", 15))
REFRESH_TOKEN_EXPIRE_DAYS = int(secrets.get("REFRESH_TOKEN_EXPIRE_DAYS", 30))


# ACCESS_TOKEN_EXPIRE_MINUTES=10000
# REFRESH_TOKEN_EXPIRE_DAYS=30






pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

refresh_tokens_store = {}


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    refresh_tokens_store[data["sub"]] = {"token": token, "exp": expire.timestamp()}
    return token
