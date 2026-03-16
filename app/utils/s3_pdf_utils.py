"""
S3 utilities for PDF agreement generation.
Handles template download, PDF upload, and presigned URL generation.
"""
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config.settings import settings


# S3 client with retry configuration
_s3 = boto3.client(
    "s3",
    region_name=settings.aws_region,
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
    config=Config(retries={"max_attempts": 5, "mode": "standard"})
)


def s3_download_bytes(key: str, bucket: str = None) -> bytes:
    """
    Download a file from S3 and return its contents as bytes.

    Args:
        key: S3 object key
        bucket: S3 bucket name (defaults to pdf_s3_bucket from settings)

    Returns:
        File contents as bytes

    Raises:
        ClientError: If download fails
    """
    bucket = bucket or settings.pdf_s3_bucket
    try:
        obj = _s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        raise ClientError(
            {"Error": {"Code": error_code, "Message": f"Failed to download {key} from {bucket}"}},
            "GetObject"
        )


def s3_upload_bytes(
    key: str,
    data: bytes,
    content_type: str = "application/pdf",
    bucket: str = None
) -> str:
    """
    Upload bytes to S3 with server-side encryption.

    Args:
        key: S3 object key
        data: File contents as bytes
        content_type: MIME type for the file
        bucket: S3 bucket name (defaults to pdf_s3_bucket from settings)

    Returns:
        S3 key of uploaded object

    Raises:
        ClientError: If upload fails
    """
    bucket = bucket or settings.pdf_s3_bucket
    try:
        _s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            ServerSideEncryption="AES256",
        )
        return key
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        raise ClientError(
            {"Error": {"Code": error_code, "Message": f"Failed to upload {key} to {bucket}"}},
            "PutObject"
        )


def s3_presign_get_url(key: str, expires: int = None, bucket: str = None) -> str:
    """
    Generate a presigned URL for downloading a file from S3.

    Args:
        key: S3 object key
        expires: URL expiration time in seconds (defaults to settings)
        bucket: S3 bucket name (defaults to pdf_s3_bucket from settings)

    Returns:
        Presigned URL string
    """
    bucket = bucket or settings.pdf_s3_bucket
    expires = expires or settings.pdf_presign_expires_seconds

    return _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


def s3_check_object_exists(key: str, bucket: str = None) -> bool:
    """
    Check if an object exists in S3.

    Args:
        key: S3 object key
        bucket: S3 bucket name (defaults to pdf_s3_bucket from settings)

    Returns:
        True if object exists, False otherwise
    """
    bucket = bucket or settings.pdf_s3_bucket
    try:
        _s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def s3_delete_object(key: str, bucket: str = None) -> bool:
    """
    Delete an object from S3.

    Args:
        key: S3 object key
        bucket: S3 bucket name (defaults to pdf_s3_bucket from settings)

    Returns:
        True if deletion successful
    """
    bucket = bucket or settings.pdf_s3_bucket
    try:
        _s3.delete_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
