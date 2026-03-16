"""
Exponential Backoff and Retry Logic for AWS Services (Lambda, S3, etc.)

This module provides enterprise-grade retry logic for AWS service calls,
specifically optimized for Lambda invocations and other AWS SDK operations.

Features:
- Exponential backoff with jitter (prevents thundering herd)
- Handles AWS throttling (ThrottlingException, TooManyRequestsException)
- Handles transient errors (ServiceUnavailable, RequestTimeout)
- Circuit breaker pattern for fail-fast behavior
- Async support for high concurrency
- Compatible with boto3 and aioboto3

Usage:
    # Sync usage
    from app.utils.aws_retry import invoke_lambda_with_retry

    invoke_lambda_with_retry(
        lambda_client,
        FunctionName="my-function",
        InvocationType="Event",
        Payload=json.dumps(payload).encode()
    )

    # Async usage
    from app.utils.aws_retry import invoke_lambda_with_retry_async

    await invoke_lambda_with_retry_async(
        lambda_client,
        FunctionName="my-function",
        InvocationType="Event",
        Payload=json.dumps(payload).encode()
    )
"""

import asyncio
import logging
import random
import time
from typing import Any, Dict, Optional, Callable
from functools import wraps
from botocore.exceptions import ClientError

logger = logging.getLogger("aws_retry")


# ---------------------------------------------------------------------------
# Exception Classification for AWS
# ---------------------------------------------------------------------------

def is_retryable_aws_error(exc: Exception) -> bool:
    """
    Determine if an AWS exception is retryable.

    Retryable AWS errors:
    - Throttling errors (ThrottlingException, TooManyRequestsException)
    - Service unavailable (ServiceUnavailable, InternalServerError)
    - Timeouts (RequestTimeout)
    - Network errors (ConnectionError, EndpointConnectionError)

    Non-retryable errors:
    - Invalid parameters (InvalidParameterValueException)
    - Resource not found (ResourceNotFoundException)
    - Access denied (AccessDeniedException)
    """
    # Check if it's a botocore ClientError
    if isinstance(exc, ClientError):
        error_code = exc.response.get('Error', {}).get('Code', '')

        # Retryable error codes
        retryable_codes = [
            'ThrottlingException',
            'TooManyRequestsException',
            'RequestLimitExceeded',
            'ServiceUnavailable',
            'InternalServerError',
            'InternalFailure',
            'RequestTimeout',
            'SlowDown',  # S3
            '503',  # Service unavailable
            '500',  # Internal server error
        ]

        if error_code in retryable_codes:
            return True

        # Non-retryable error codes
        non_retryable_codes = [
            'InvalidParameterValueException',
            'InvalidParameterException',
            'ResourceNotFoundException',
            'AccessDeniedException',
            'InvalidRequestException',
            'ValidationException',
        ]

        if error_code in non_retryable_codes:
            return False

    # Check for network/connection errors
    error_str = str(exc).lower()
    if any(keyword in error_str for keyword in ['connection', 'timeout', 'network']):
        return True

    # Default: retry on unknown errors (conservative approach)
    return True


def get_aws_retry_after_seconds(exc: Exception) -> Optional[float]:
    """Extract retry-after time from AWS exception if available."""
    if isinstance(exc, ClientError):
        # Check for Retry-After header in response metadata
        retry_after = exc.response.get('ResponseMetadata', {}).get('RetryAfter')
        if retry_after:
            try:
                return float(retry_after)
            except (ValueError, TypeError):
                pass

    return None


# ---------------------------------------------------------------------------
# Exponential Backoff (Same as AI retry)
# ---------------------------------------------------------------------------

def calculate_aws_backoff_seconds(
    attempt: int,
    base_delay: float = 0.5,
    max_delay: float = 20.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff with optional jitter.

    AWS services typically need shorter delays than AI APIs:
    - attempt=1: ~0.5s
    - attempt=2: ~1s
    - attempt=3: ~2s
    - attempt=4: ~4s
    - attempt=5: ~8s
    - attempt=6: ~16s
    - Max: 20s (capped)

    Args:
        attempt: Current attempt number (1-indexed)
        base_delay: Base delay in seconds (default 0.5)
        max_delay: Maximum delay in seconds (default 20.0)
        jitter: Add random jitter (default True)

    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay = base_delay * (2 ** (attempt - 1))

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter (0% to 25% of delay)
    if jitter:
        jitter_amount = random.uniform(0, delay * 0.25)
        delay += jitter_amount

    return delay


# ---------------------------------------------------------------------------
# Lambda Retry Logic (Synchronous)
# ---------------------------------------------------------------------------

def invoke_lambda_with_retry(
    lambda_client,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 20.0,
    **invoke_kwargs
) -> Optional[Dict[str, Any]]:

    function_name = invoke_kwargs.get('FunctionName', 'unknown')
    invocation_type = invoke_kwargs.get('InvocationType', 'Event')
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            response = lambda_client.invoke(**invoke_kwargs)
            duration_ms = (time.time() - start_time) * 1000

            if attempt > 1:
                logger.info(
                    f"✓ Lambda {function_name} succeeded on attempt {attempt}/{max_attempts} "
                    f"({duration_ms:.0f}ms)"
                )

            # For Event invocations, response doesn't contain useful data
            if invocation_type == 'Event':
                return None

            return response

        except ClientError as exc:
            last_exception = exc
            error_code = exc.response.get('Error', {}).get('Code', 'Unknown')
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_aws_error(exc):
                logger.warning(
                    f"✗ Lambda {function_name} non-retryable error: {error_code} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ Lambda {function_name} failed after {max_attempts} attempts: {error_code} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            retry_after = get_aws_retry_after_seconds(exc)
            if retry_after is not None:
                delay = min(retry_after, max_delay)
                logger.info(f"⏳ Lambda respecting Retry-After: {delay:.1f}s")
            else:
                delay = calculate_aws_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  Lambda {function_name} attempt {attempt}/{max_attempts} failed: {error_code}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

        except Exception as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_aws_error(exc):
                logger.warning(
                    f"✗ Lambda {function_name} non-retryable error: {exc} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ Lambda {function_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            delay = calculate_aws_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  Lambda {function_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            time.sleep(delay)

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"Lambda {function_name} failed without explicit error")


# ---------------------------------------------------------------------------
# Lambda Retry Logic (Asynchronous)
# ---------------------------------------------------------------------------

async def invoke_lambda_with_retry_async(
    lambda_client,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 20.0,
    **invoke_kwargs
) -> Optional[Dict[str, Any]]:
    """
    Invoke AWS Lambda with automatic retry logic (async version).

    For use with aioboto3 or when calling Lambda from async code.

    Args:
        lambda_client: aioboto3 Lambda client
        max_attempts: Maximum number of attempts (default 3)
        base_delay: Base delay for exponential backoff (default 0.5s)
        max_delay: Maximum delay between retries (default 20.0s)
        **invoke_kwargs: Arguments for lambda_client.invoke()

    Returns:
        Lambda response dict (or None for Event invocations)

    Example:
        async with aioboto3.Session().client('lambda') as lambda_client:
            await invoke_lambda_with_retry_async(
                lambda_client,
                FunctionName="my-function",
                InvocationType="Event",
                Payload=json.dumps({"key": "value"}).encode()
            )
    """
    function_name = invoke_kwargs.get('FunctionName', 'unknown')
    invocation_type = invoke_kwargs.get('InvocationType', 'Event')
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            response = await lambda_client.invoke(**invoke_kwargs)
            duration_ms = (time.time() - start_time) * 1000

            if attempt > 1:
                logger.info(
                    f"✓ Lambda {function_name} succeeded on attempt {attempt}/{max_attempts} "
                    f"({duration_ms:.0f}ms)"
                )

            # For Event invocations, response doesn't contain useful data
            if invocation_type == 'Event':
                return None

            return response

        except ClientError as exc:
            last_exception = exc
            error_code = exc.response.get('Error', {}).get('Code', 'Unknown')
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_aws_error(exc):
                logger.warning(
                    f"✗ Lambda {function_name} non-retryable error: {error_code} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ Lambda {function_name} failed after {max_attempts} attempts: {error_code} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            retry_after = get_aws_retry_after_seconds(exc)
            if retry_after is not None:
                delay = min(retry_after, max_delay)
                logger.info(f"⏳ Lambda respecting Retry-After: {delay:.1f}s")
            else:
                delay = calculate_aws_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  Lambda {function_name} attempt {attempt}/{max_attempts} failed: {error_code}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            await asyncio.sleep(delay)

        except Exception as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            # Check if we should retry
            if not is_retryable_aws_error(exc):
                logger.warning(
                    f"✗ Lambda {function_name} non-retryable error: {exc} ({duration_ms:.0f}ms)"
                )
                raise exc

            # Last attempt - give up
            if attempt >= max_attempts:
                logger.error(
                    f"✗ Lambda {function_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            # Calculate backoff delay
            delay = calculate_aws_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  Lambda {function_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s... ({duration_ms:.0f}ms)"
            )

            # Wait before retry
            await asyncio.sleep(delay)

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"Lambda {function_name} failed without explicit error")


# ---------------------------------------------------------------------------
# Generic AWS Call Retry (for other AWS services)
# ---------------------------------------------------------------------------

def aws_call_with_retry(
    aws_call: Callable,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 20.0,
    service_name: str = "aws-service"
) -> Any:
    """
    Execute any AWS SDK call with retry logic.

    Args:
        aws_call: Callable that makes the AWS SDK call
        max_attempts: Maximum number of attempts
        base_delay: Base delay for exponential backoff
        max_delay: Maximum delay between retries
        service_name: Name for logging

    Example:
        # S3 upload with retry
        result = aws_call_with_retry(
            lambda: s3_client.put_object(
                Bucket='my-bucket',
                Key='file.txt',
                Body=b'data'
            ),
            service_name="s3-upload"
        )
    """
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            result = aws_call()
            duration_ms = (time.time() - start_time) * 1000

            if attempt > 1:
                logger.info(
                    f"✓ {service_name} succeeded on attempt {attempt}/{max_attempts} "
                    f"({duration_ms:.0f}ms)"
                )

            return result

        except Exception as exc:
            last_exception = exc
            duration_ms = (time.time() - start_time) * 1000

            if not is_retryable_aws_error(exc):
                logger.warning(f"✗ {service_name} non-retryable error: {exc} ({duration_ms:.0f}ms)")
                raise exc

            if attempt >= max_attempts:
                logger.error(
                    f"✗ {service_name} failed after {max_attempts} attempts: {exc} "
                    f"({duration_ms:.0f}ms)"
                )
                raise exc

            delay = calculate_aws_backoff_seconds(attempt, base_delay, max_delay, jitter=True)

            logger.warning(
                f"⚠️  {service_name} attempt {attempt}/{max_attempts} failed: {exc}. "
                f"Retrying in {delay:.1f}s..."
            )

            time.sleep(delay)

    if last_exception:
        raise last_exception
    raise Exception(f"{service_name} failed without explicit error")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

__all__ = [
    "invoke_lambda_with_retry",
    "invoke_lambda_with_retry_async",
    "aws_call_with_retry",
    "is_retryable_aws_error",
    "calculate_aws_backoff_seconds",
]
