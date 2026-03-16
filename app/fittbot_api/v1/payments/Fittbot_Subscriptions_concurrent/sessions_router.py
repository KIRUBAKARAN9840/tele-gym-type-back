import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple, List

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.utils.redis_config import get_redis
from app.utils.request_auth import resolve_authenticated_user_id

from .dependencies import (
    get_sessions_command_dispatcher,
    get_sessions_command_store,
)
from .schemas import (
    SessionCheckoutRequest,
    SessionVerifyRequest,
    SessionCheckoutAccepted,
    SessionVerifyAccepted,
    CommandStatusResponse,
)
from .services.session_dispatcher import SessionCommandDispatcher
from .stores.command_store import CommandStore

logger = logging.getLogger("payments.sessions.v2")

router = APIRouter(prefix="/sessions_payment", tags=["Session Payments v2"])


# ============================================================================
# Structured Error Response Models
# ============================================================================
class ErrorDetail(BaseModel):
    error_code: str
    message: str
    field: Optional[str] = None
    request_id: Optional[str] = None
    retry_after: Optional[int] = None
    violation_level: Optional[int] = None
    violations_until_block: Optional[int] = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


# ============================================================================
# Error Codes
# ============================================================================
class SessionErrorCodes:
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SESSIONS_COUNT_MISMATCH = "SESSIONS_COUNT_MISMATCH"
    EMPTY_SCHEDULED_SESSIONS = "EMPTY_SCHEDULED_SESSIONS"
    COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"
    UNAUTHORIZED = "UNAUTHORIZED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    WEBHOOK_PARSE_ERROR = "WEBHOOK_PARSE_ERROR"
    DISPATCHER_ERROR = "DISPATCHER_ERROR"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    RATE_LIMIT_BLOCKED = "RATE_LIMIT_BLOCKED"


# ============================================================================
# Rate Limiting Configuration
# ============================================================================
@dataclass
class RateLimitConfig:
    """Rate limit configuration for an endpoint."""
    requests: int          # Max requests allowed
    window_seconds: int    # Time window in seconds
    key_prefix: str        # Redis key prefix for this limit

    @property
    def key(self) -> str:
        return f"ratelimit:sessions:{self.key_prefix}"


class RateLimits:
    """Enterprise rate limits per endpoint type."""
    # Payment initiation - strict limits (expensive operation)
    CHECKOUT = RateLimitConfig(requests=10, window_seconds=60, key_prefix="checkout")

    # Payment verification - moderate limits
    VERIFY = RateLimitConfig(requests=20, window_seconds=60, key_prefix="verify")

    # Status polling - higher limits (clients poll frequently)
    STATUS = RateLimitConfig(requests=60, window_seconds=60, key_prefix="status")

    # Webhook - per-IP limits (Razorpay IPs)
    WEBHOOK = RateLimitConfig(requests=100, window_seconds=60, key_prefix="webhook")

    # Global per-user limit across all endpoints
    GLOBAL_USER = RateLimitConfig(requests=100, window_seconds=60, key_prefix="global")


# ============================================================================
# Escalating Penalties Configuration (Stripe-like)
# ============================================================================
class PenaltyLevel(Enum):
    """Escalating penalty levels."""
    WARNING = 1      # First violation: 60s wait
    ELEVATED = 2     # Second violation: 120s wait
    SEVERE = 3       # Third violation: 300s (5 min) wait
    BLOCKED = 4      # Fourth+ violation: 1 hour block


@dataclass
class PenaltyConfig:
    """Configuration for escalating penalties."""
    level: PenaltyLevel
    retry_after_seconds: int
    violation_threshold: int  # Number of violations to reach this level
    message: str


class EscalatingPenalties:
    """
    Escalating penalty system:
    - 1st violation: 60 seconds wait
    - 2nd violation: 120 seconds wait
    - 3rd violation: 300 seconds (5 min) wait
    - 4th+ violation: 3600 seconds (1 hour) block
    """
    LEVELS: List[PenaltyConfig] = [
        PenaltyConfig(
            level=PenaltyLevel.WARNING,
            retry_after_seconds=60,
            violation_threshold=1,
            message="Rate limit exceeded. Please wait before retrying.",
        ),
        PenaltyConfig(
            level=PenaltyLevel.ELEVATED,
            retry_after_seconds=120,
            violation_threshold=2,
            message="Repeated rate limit violations. Extended wait required.",
        ),
        PenaltyConfig(
            level=PenaltyLevel.SEVERE,
            retry_after_seconds=300,
            violation_threshold=3,
            message="Multiple rate limit violations detected. 5 minute cooldown.",
        ),
        PenaltyConfig(
            level=PenaltyLevel.BLOCKED,
            retry_after_seconds=3600,
            violation_threshold=4,
            message="Too many violations. You are temporarily blocked for 1 hour.",
        ),
    ]

    # Violations decay after this many seconds of good behavior
    VIOLATION_DECAY_SECONDS = 3600  # 1 hour - violations reset after 1 hour of no violations

    # Redis key prefix for violation tracking
    VIOLATION_KEY_PREFIX = "ratelimit:violations:sessions"

    @classmethod
    def get_penalty_for_violations(cls, violation_count: int) -> PenaltyConfig:
        """Get the appropriate penalty based on violation count."""
        for penalty in reversed(cls.LEVELS):
            if violation_count >= penalty.violation_threshold:
                return penalty
        return cls.LEVELS[0]


# ============================================================================
# Enterprise Rate Limiter (Sliding Window + Escalating Penalties)
# ============================================================================
@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    limit: int
    remaining: int
    reset_at: int  # Unix timestamp
    retry_after: Optional[int] = None
    violation_count: int = 0
    penalty_level: Optional[PenaltyLevel] = None
    is_blocked: bool = False


class SlidingWindowRateLimiter:
    """
    Enterprise-grade Redis-based sliding window rate limiter with escalating penalties.

    Features:
    - Sliding window (more accurate than fixed window)
    - Atomic operations (no race conditions)
    - Escalating penalties (60s → 120s → 300s → 1hr block)
    - Violation decay (resets after good behavior)
    - Graceful degradation (allows traffic if Redis is down)
    - Per-user and per-IP support
    - Rate limit headers for client feedback
    """

    # Lua script for sliding window rate limiting
    RATE_LIMIT_SCRIPT = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    local window_start = now - window

    -- Remove expired entries
    redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

    -- Count current requests in window
    local current = redis.call('ZCARD', key)

    if current < limit then
        -- Add new request
        redis.call('ZADD', key, now, now .. ':' .. math.random())
        redis.call('EXPIRE', key, window + 1)
        return {1, limit - current - 1, now + window, 0}
    else
        -- Get oldest entry to calculate retry time
        local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local retry_after = 0
        if oldest and #oldest >= 2 then
            retry_after = math.ceil(tonumber(oldest[2]) + window - now)
        end
        return {0, 0, now + window, retry_after}
    end
    """

    # Lua script for violation tracking with escalating penalties
    VIOLATION_SCRIPT = """
    local violation_key = KEYS[1]
    local block_key = KEYS[2]
    local now = tonumber(ARGV[1])
    local decay_seconds = tonumber(ARGV[2])
    local block_duration = tonumber(ARGV[3])

    -- Check if currently blocked
    local block_until = redis.call('GET', block_key)
    if block_until and tonumber(block_until) > now then
        local remaining_block = tonumber(block_until) - now
        return {-1, remaining_block, tonumber(block_until)}  -- -1 = blocked
    end

    -- Increment violation count
    local violations = redis.call('INCR', violation_key)

    -- Set expiry for violation decay (resets after good behavior)
    redis.call('EXPIRE', violation_key, decay_seconds)

    -- If violations >= 4, set a block
    if violations >= 4 then
        local block_until_ts = now + block_duration
        redis.call('SET', block_key, block_until_ts)
        redis.call('EXPIRE', block_key, block_duration + 60)
        return {violations, block_duration, block_until_ts}
    end

    return {violations, 0, 0}
    """

    # Lua script to check if blocked (without incrementing)
    CHECK_BLOCK_SCRIPT = """
    local block_key = KEYS[1]
    local now = tonumber(ARGV[1])

    local block_until = redis.call('GET', block_key)
    if block_until and tonumber(block_until) > now then
        local remaining = tonumber(block_until) - now
        return {1, remaining, tonumber(block_until)}  -- 1 = blocked
    end
    return {0, 0, 0}  -- 0 = not blocked
    """

    def __init__(self):
        self._script_sha: Optional[str] = None

    async def check(
        self,
        identifier: str,
        config: RateLimitConfig,
    ) -> RateLimitResult:
        """
        Check if request is allowed under rate limit with escalating penalties.
        """
        try:
            redis = await get_redis()
            now = int(time.time())

            # First check if user is currently blocked
            block_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:block:{identifier}"
            block_result = await redis.eval(
                self.CHECK_BLOCK_SCRIPT,
                1,
                block_key,
                now,
            )

            if block_result[0] == 1:  # Currently blocked
                remaining_block = int(block_result[1])
                block_until = int(block_result[2])

                # Get current violation count for response
                violation_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:{identifier}"
                violation_count = await redis.get(violation_key)
                violation_count = int(violation_count) if violation_count else 4

                return RateLimitResult(
                    allowed=False,
                    limit=config.requests,
                    remaining=0,
                    reset_at=block_until,
                    retry_after=remaining_block,
                    violation_count=violation_count,
                    penalty_level=PenaltyLevel.BLOCKED,
                    is_blocked=True,
                )

            # Check sliding window rate limit
            rate_key = f"{config.key}:{identifier}"
            result = await redis.eval(
                self.RATE_LIMIT_SCRIPT,
                1,
                rate_key,
                now,
                config.window_seconds,
                config.requests,
            )

            allowed = bool(result[0])
            remaining = int(result[1])
            reset_at = int(result[2])
            base_retry_after = int(result[3]) if result[3] else 0

            if allowed:
                return RateLimitResult(
                    allowed=True,
                    limit=config.requests,
                    remaining=remaining,
                    reset_at=reset_at,
                    retry_after=None,
                    violation_count=0,
                    penalty_level=None,
                    is_blocked=False,
                )

            # Rate limit exceeded - record violation and get penalty
            violation_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:{identifier}"
            violation_result = await redis.eval(
                self.VIOLATION_SCRIPT,
                2,
                violation_key,
                block_key,
                now,
                EscalatingPenalties.VIOLATION_DECAY_SECONDS,
                EscalatingPenalties.LEVELS[-1].retry_after_seconds,  # 1 hour block
            )

            violation_count = int(violation_result[0])

            # Check if just got blocked
            if violation_count == -1 or violation_count >= 4:
                remaining_block = int(violation_result[1])
                return RateLimitResult(
                    allowed=False,
                    limit=config.requests,
                    remaining=0,
                    reset_at=int(violation_result[2]) if violation_result[2] else now + 3600,
                    retry_after=remaining_block if remaining_block > 0 else 3600,
                    violation_count=4 if violation_count == -1 else violation_count,
                    penalty_level=PenaltyLevel.BLOCKED,
                    is_blocked=True,
                )

            # Get escalating penalty based on violation count
            penalty = EscalatingPenalties.get_penalty_for_violations(violation_count)

            return RateLimitResult(
                allowed=False,
                limit=config.requests,
                remaining=0,
                reset_at=now + penalty.retry_after_seconds,
                retry_after=penalty.retry_after_seconds,
                violation_count=violation_count,
                penalty_level=penalty.level,
                is_blocked=False,
            )

        except Exception as e:
            # Graceful degradation: allow traffic if Redis is unavailable
            logger.warning(
                "RATE_LIMIT_REDIS_ERROR",
                extra={
                    "error": str(e),
                    "identifier": identifier,
                    "config": config.key_prefix,
                },
            )
            return RateLimitResult(
                allowed=True,
                limit=config.requests,
                remaining=config.requests,
                reset_at=int(time.time()) + config.window_seconds,
            )

    async def check_multiple(
        self,
        identifier: str,
        configs: List[RateLimitConfig],
    ) -> Tuple[bool, RateLimitResult]:
        """
        Check multiple rate limits (e.g., per-endpoint + global).
        Returns the most restrictive result.
        """
        most_restrictive: Optional[RateLimitResult] = None

        for config in configs:
            result = await self.check(identifier, config)
            if not result.allowed:
                return False, result
            if most_restrictive is None or result.remaining < most_restrictive.remaining:
                most_restrictive = result

        return True, most_restrictive

    async def get_violation_count(self, identifier: str) -> int:
        """Get current violation count for an identifier."""
        try:
            redis = await get_redis()
            violation_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:{identifier}"
            count = await redis.get(violation_key)
            return int(count) if count else 0
        except Exception:
            return 0

    async def reset_violations(self, identifier: str) -> bool:
        """Manually reset violations for an identifier (admin use)."""
        try:
            redis = await get_redis()
            violation_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:{identifier}"
            block_key = f"{EscalatingPenalties.VIOLATION_KEY_PREFIX}:block:{identifier}"
            await redis.delete(violation_key, block_key)
            return True
        except Exception:
            return False


# Global rate limiter instance
rate_limiter = SlidingWindowRateLimiter()


# ============================================================================
# Helper Functions
# ============================================================================
def get_request_id(request: Request) -> str:
    """Extract or generate a request ID for tracing."""
    return (
        request.headers.get("X-Request-ID")
        or request.headers.get("X-Correlation-ID")
        or str(uuid.uuid4())
    )


def get_client_ip(request: Request) -> str:
    """Extract client IP address, handling proxies."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()

    if request.client:
        return request.client.host

    return "unknown"


def log_request_start(
    operation: str,
    request_id: str,
    **extra_fields,
) -> float:
    """Log request start and return start time for duration calculation."""
    logger.info(
        f"SESSION_{operation}_START",
        extra={"request_id": request_id, "operation": operation, **extra_fields},
    )
    return time.perf_counter()


def log_request_success(
    operation: str,
    request_id: str,
    start_time: float,
    **extra_fields,
) -> None:
    """Log successful request completion with duration."""
    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        f"SESSION_{operation}_SUCCESS",
        extra={
            "request_id": request_id,
            "operation": operation,
            "duration_ms": round(duration_ms, 2),
            **extra_fields,
        },
    )


def log_request_error(
    operation: str,
    request_id: str,
    error_code: str,
    error_message: str,
    start_time: Optional[float] = None,
    **extra_fields,
) -> None:
    """Log request error with context."""
    extra = {
        "request_id": request_id,
        "operation": operation,
        "error_code": error_code,
        "error_message": error_message,
        **extra_fields,
    }
    if start_time:
        extra["duration_ms"] = round((time.perf_counter() - start_time) * 1000, 2)
    logger.error(f"SESSION_{operation}_ERROR", extra=extra)


def log_rate_limit_exceeded(
    operation: str,
    request_id: str,
    identifier: str,
    result: RateLimitResult,
    **extra_fields,
) -> None:
    """Log rate limit exceeded event with escalation details."""
    level = "BLOCKED" if result.is_blocked else (
        result.penalty_level.name if result.penalty_level else "WARNING"
    )

    logger.warning(
        f"SESSION_{operation}_RATE_LIMITED",
        extra={
            "request_id": request_id,
            "operation": operation,
            "identifier": identifier,
            "violation_count": result.violation_count,
            "penalty_level": level,
            "retry_after": result.retry_after,
            "is_blocked": result.is_blocked,
            **extra_fields,
        },
    )


def raise_structured_error(
    status_code: int,
    error_code: str,
    message: str,
    field: Optional[str] = None,
    request_id: Optional[str] = None,
    retry_after: Optional[int] = None,
) -> None:
    """Raise HTTPException with structured error response."""
    raise HTTPException(
        status_code=status_code,
        detail=ErrorDetail(
            error_code=error_code,
            message=message,
            field=field,
            request_id=request_id,
            retry_after=retry_after,
        ).dict(),
    )


def create_rate_limit_response(
    result: RateLimitResult,
    request_id: str,
) -> JSONResponse:
    """Create a 429 response with rate limit headers and escalation info."""

    # Determine error code and message based on penalty level
    if result.is_blocked:
        error_code = SessionErrorCodes.RATE_LIMIT_BLOCKED
        message = EscalatingPenalties.LEVELS[-1].message
    else:
        error_code = SessionErrorCodes.RATE_LIMIT_EXCEEDED
        penalty = EscalatingPenalties.get_penalty_for_violations(result.violation_count)
        message = penalty.message

    # Calculate violations until block
    violations_until_block = max(0, 4 - result.violation_count)

    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "error_code": error_code,
                "message": message,
                "request_id": request_id,
                "retry_after": result.retry_after or 60,
                "violation_count": result.violation_count,
                "violation_level": result.penalty_level.value if result.penalty_level else 1,
                "violations_until_block": violations_until_block,
                "is_blocked": result.is_blocked,
            }
        },
        headers={
            "X-RateLimit-Limit": str(result.limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(result.reset_at),
            "Retry-After": str(result.retry_after or 60),
            "X-RateLimit-Violation-Count": str(result.violation_count),
            "X-RateLimit-Blocked": "true" if result.is_blocked else "false",
        },
    )


# ============================================================================
# Rate Limit Dependency
# ============================================================================
async def check_rate_limit_user(
    request: Request,
    endpoint_config: RateLimitConfig,
    operation: str,
) -> Optional[JSONResponse]:
    """
    Check rate limits for authenticated endpoints with escalating penalties.
    Returns None if allowed, or a 429 response if rate limited.
    """
    request_id = get_request_id(request)

    try:
        # Get user identifier (prefer user_id, fallback to IP)
        try:
            user_id = resolve_authenticated_user_id(request)
            identifier = f"user:{user_id}"
        except Exception:
            identifier = f"ip:{get_client_ip(request)}"

        # Check both endpoint-specific and global limits
        allowed, result = await rate_limiter.check_multiple(
            identifier,
            [endpoint_config, RateLimits.GLOBAL_USER],
        )

        if not allowed:
            log_rate_limit_exceeded(
                operation,
                request_id,
                identifier,
                result,
            )
            return create_rate_limit_response(result, request_id)

        return None

    except Exception as e:
        logger.error(
            "RATE_LIMIT_CHECK_ERROR",
            extra={"error": str(e), "request_id": request_id},
        )
        return None


async def check_rate_limit_ip(
    request: Request,
    endpoint_config: RateLimitConfig,
    operation: str,
) -> Optional[JSONResponse]:
    """
    Check rate limits for unauthenticated endpoints (webhooks) with escalating penalties.
    Uses IP-based limiting.
    """
    request_id = get_request_id(request)

    try:
        ip = get_client_ip(request)
        identifier = f"ip:{ip}"

        result = await rate_limiter.check(identifier, endpoint_config)

        if not result.allowed:
            log_rate_limit_exceeded(
                operation,
                request_id,
                identifier,
                result,
                client_ip=ip,
            )
            return create_rate_limit_response(result, request_id)

        return None

    except Exception as e:
        logger.error(
            "RATE_LIMIT_CHECK_ERROR",
            extra={"error": str(e), "request_id": request_id},
        )
        return None


# ============================================================================
# Endpoints
# ============================================================================
@router.post(
    "/checkout",
    response_model=SessionCheckoutAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"model": ErrorResponse, "description": "Validation error"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def enqueue_session_checkout(
    request: Request,
    body: SessionCheckoutRequest,
    dispatcher: SessionCommandDispatcher = Depends(get_sessions_command_dispatcher),
):

    rate_limit_response = await check_rate_limit_user(
        request, RateLimits.CHECKOUT, "CHECKOUT_ENQUEUE"
    )
    if rate_limit_response:
        return rate_limit_response

    request_id = get_request_id(request)
    start_time = log_request_start(
        "CHECKOUT_ENQUEUE",
        request_id,
        gym_id=body.gym_id,
        client_id=body.client_id,
        session_id=body.session_id,
        sessions_count=body.sessions_count,
    )

    try:
        idem = body.idempotency_key or request.headers.get("Idempotency-Key")
        if idem:
            body = body.copy(update={"idempotency_key": idem})

        # Validate based on session_type
        from .schemas import SessionType

        if body.session_type == SessionType.custom:
            # Custom: validate custom_slot has entries
            if not body.custom_slot:
                log_request_error(
                    "CHECKOUT_ENQUEUE",
                    request_id,
                    SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    "custom_slot cannot be empty when session_type is 'custom'",
                    start_time,
                )
                raise_structured_error(
                    status_code=400,
                    error_code=SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    message="custom_slot cannot be empty when session_type is 'custom'",
                    field="custom_slot",
                    request_id=request_id,
                )

            # Check custom_slot keys count matches sessions_count
            custom_slot_count = len(body.custom_slot.keys())
            if custom_slot_count != body.sessions_count:
                log_request_error(
                    "CHECKOUT_ENQUEUE",
                    request_id,
                    SessionErrorCodes.SESSIONS_COUNT_MISMATCH,
                    f"Expected {body.sessions_count} sessions, got {custom_slot_count} custom_slot keys",
                    start_time,
                    expected=body.sessions_count,
                    actual=custom_slot_count,
                )
                raise_structured_error(
                    status_code=400,
                    error_code=SessionErrorCodes.SESSIONS_COUNT_MISMATCH,
                    message=f"custom_slot keys count ({custom_slot_count}) does not match sessions_count ({body.sessions_count})",
                    field="custom_slot",
                    request_id=request_id,
                )
        else:
            # same_time (default): validate scheduled_dates
            if not body.scheduled_dates:
                log_request_error(
                    "CHECKOUT_ENQUEUE",
                    request_id,
                    SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    "scheduled_dates cannot be empty when session_type is 'same_time'",
                    start_time,
                )
                raise_structured_error(
                    status_code=400,
                    error_code=SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    message="scheduled_dates cannot be empty when session_type is 'same_time'",
                    field="scheduled_dates",
                    request_id=request_id,
                )

            if not body.default_slot:
                log_request_error(
                    "CHECKOUT_ENQUEUE",
                    request_id,
                    SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    "default_slot is required when session_type is 'same_time'",
                    start_time,
                )
                raise_structured_error(
                    status_code=400,
                    error_code=SessionErrorCodes.EMPTY_SCHEDULED_SESSIONS,
                    message="default_slot is required when session_type is 'same_time'",
                    field="default_slot",
                    request_id=request_id,
                )

            # Check scheduled_dates count matches sessions_count
            if len(body.scheduled_dates) != body.sessions_count:
                log_request_error(
                    "CHECKOUT_ENQUEUE",
                    request_id,
                    SessionErrorCodes.SESSIONS_COUNT_MISMATCH,
                    f"Expected {body.sessions_count} sessions, got {len(body.scheduled_dates)} scheduled_dates",
                    start_time,
                    expected=body.sessions_count,
                    actual=len(body.scheduled_dates),
                )
                raise_structured_error(
                    status_code=400,
                    error_code=SessionErrorCodes.SESSIONS_COUNT_MISMATCH,
                    message=f"scheduled_dates length ({len(body.scheduled_dates)}) does not match sessions_count ({body.sessions_count})",
                    field="scheduled_dates",
                    request_id=request_id,
                )

        client_id = resolve_authenticated_user_id(request, str(body.client_id))
        status_record = await dispatcher.enqueue_checkout(body, owner_id=client_id)
        status_url = request.url_for(
            "get_session_command_status", command_id=status_record.request_id
        )

        # Track checkout initiation (non-blocking)
        from app.services.activity_tracker import track_event
        await track_event(
            int(client_id), "checkout_initiated",
            gym_id=body.gym_id,
            product_type="session",
            product_details={"sessions_count": getattr(body, "sessions_count", 1), "session_id": getattr(body, "session_id", None)},
            source="payment_session",
            command_id=status_record.request_id,
        )

        log_request_success(
            "CHECKOUT_ENQUEUE",
            request_id,
            start_time,
            command_id=status_record.request_id,
            client_id=client_id,
        )

        return SessionCheckoutAccepted(
            request_id=status_record.request_id,
            status=status_record.status,
            status_url=str(status_url),
        )

    except HTTPException:
        raise
    except Exception as e:
        log_request_error(
            "CHECKOUT_ENQUEUE",
            request_id,
            SessionErrorCodes.DISPATCHER_ERROR,
            str(e),
            start_time,
            exception_type=type(e).__name__,
        )
        raise_structured_error(
            status_code=500,
            error_code=SessionErrorCodes.INTERNAL_ERROR,
            message="Failed to enqueue checkout request",
            request_id=request_id,
        )


@router.post(
    "/verify",
    response_model=SessionVerifyAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"model": ErrorResponse, "description": "Validation error"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def enqueue_session_verify(
    request: Request,
    body: SessionVerifyRequest,
    dispatcher: SessionCommandDispatcher = Depends(get_sessions_command_dispatcher),
):

    rate_limit_response = await check_rate_limit_user(
        request, RateLimits.VERIFY, "VERIFY_ENQUEUE"
    )
    if rate_limit_response:
        return rate_limit_response

    request_id = get_request_id(request)
    # Get client_id from JWT token (like dailypass), not from body
    client_id = resolve_authenticated_user_id(request)
    start_time = log_request_start(
        "VERIFY_ENQUEUE",
        request_id,
        client_id=client_id,
        razorpay_order_id=body.razorpay_order_id,
    )

    try:
        status_record = await dispatcher.enqueue_verify(body, owner_id=client_id)
        status_url = request.url_for(
            "get_session_command_status", command_id=status_record.request_id
        )

        # Track payment verification (non-blocking)
        from app.services.activity_tracker import track_event
        await track_event(
            int(client_id), "checkout_completed",
            product_type="session",
            source="payment_session",
            command_id=status_record.request_id,
        )

        log_request_success(
            "VERIFY_ENQUEUE",
            request_id,
            start_time,
            command_id=status_record.request_id,
            client_id=client_id,
            razorpay_order_id=body.razorpay_order_id,
        )

        return SessionVerifyAccepted(
            request_id=status_record.request_id,
            status=status_record.status,
            status_url=str(status_url),
        )

    except HTTPException:
        raise
    except Exception as e:
        log_request_error(
            "VERIFY_ENQUEUE",
            request_id,
            SessionErrorCodes.DISPATCHER_ERROR,
            str(e),
            start_time,
            exception_type=type(e).__name__,
        )
        raise_structured_error(
            status_code=500,
            error_code=SessionErrorCodes.INTERNAL_ERROR,
            message="Failed to enqueue verify request",
            request_id=request_id,
        )


@router.get(
    "/commands/{command_id}",
    response_model=CommandStatusResponse,
    name="get_session_command_status",
    responses={
        404: {"model": ErrorResponse, "description": "Command not found"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
    },
)
async def get_session_command_status(
    command_id: str,
    request: Request,
    store: CommandStore = Depends(get_sessions_command_store),
):

    rate_limit_response = await check_rate_limit_user(
        request, RateLimits.STATUS, "STATUS_CHECK"
    )
    if rate_limit_response:
        return rate_limit_response

    request_id = get_request_id(request)
    start_time = log_request_start(
        "STATUS_CHECK",
        request_id,
        command_id=command_id,
    )

    try:
        client_id = resolve_authenticated_user_id(request)
        record = await store.get(command_id, owner_id=client_id)

        if not record:
            log_request_error(
                "STATUS_CHECK",
                request_id,
                SessionErrorCodes.COMMAND_NOT_FOUND,
                f"Command {command_id} not found",
                start_time,
                command_id=command_id,
            )
            raise_structured_error(
                status_code=404,
                error_code=SessionErrorCodes.COMMAND_NOT_FOUND,
                message=f"Command '{command_id}' not found or access denied",
                request_id=request_id,
            )

        log_request_success(
            "STATUS_CHECK",
            request_id,
            start_time,
            command_id=command_id,
            command_status=record.status,
        )

        return record.to_response()

    except HTTPException:
        raise
    except Exception as e:
        log_request_error(
            "STATUS_CHECK",
            request_id,
            SessionErrorCodes.INTERNAL_ERROR,
            str(e),
            start_time,
            exception_type=type(e).__name__,
        )
        raise_structured_error(
            status_code=500,
            error_code=SessionErrorCodes.INTERNAL_ERROR,
            message="Failed to retrieve command status",
            request_id=request_id,
        )


@router.post(
    "/webhook",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid webhook payload"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def enqueue_session_webhook(
    request: Request,
    dispatcher: SessionCommandDispatcher = Depends(get_sessions_command_dispatcher),
):

    rate_limit_response = await check_rate_limit_ip(
        request, RateLimits.WEBHOOK, "WEBHOOK_ENQUEUE"
    )
    if rate_limit_response:
        return rate_limit_response

    request_id = get_request_id(request)
    webhook_id = request.headers.get("X-Razorpay-Webhook-ID", "unknown")
    start_time = log_request_start(
        "WEBHOOK_ENQUEUE",
        request_id,
        webhook_id=webhook_id,
        has_signature=bool(request.headers.get("X-Razorpay-Signature")),
        client_ip=get_client_ip(request),
    )

    try:
        raw_body = await request.body()
        if not raw_body:
            log_request_error(
                "WEBHOOK_ENQUEUE",
                request_id,
                SessionErrorCodes.WEBHOOK_PARSE_ERROR,
                "Empty webhook body",
                start_time,
            )
            raise_structured_error(
                status_code=400,
                error_code=SessionErrorCodes.WEBHOOK_PARSE_ERROR,
                message="Webhook body cannot be empty",
                request_id=request_id,
            )

        signature = request.headers.get("X-Razorpay-Signature", "")
        if not signature:
            logger.warning(
                "SESSION_WEBHOOK_MISSING_SIGNATURE",
                extra={"request_id": request_id, "webhook_id": webhook_id},
            )

        status_record = await dispatcher.enqueue_webhook(
            signature=signature, raw_body=raw_body.decode()
        )

        log_request_success(
            "WEBHOOK_ENQUEUE",
            request_id,
            start_time,
            command_id=status_record.request_id,
            webhook_id=webhook_id,
        )

        return {
            "request_id": status_record.request_id,
            "status": status_record.status,
        }

    except HTTPException:
        raise
    except UnicodeDecodeError:
        log_request_error(
            "WEBHOOK_ENQUEUE",
            request_id,
            SessionErrorCodes.WEBHOOK_PARSE_ERROR,
            "Invalid encoding in webhook body",
            start_time,
        )
        raise_structured_error(
            status_code=400,
            error_code=SessionErrorCodes.WEBHOOK_PARSE_ERROR,
            message="Invalid encoding in webhook body",
            request_id=request_id,
        )
    except Exception as e:
        log_request_error(
            "WEBHOOK_ENQUEUE",
            request_id,
            SessionErrorCodes.DISPATCHER_ERROR,
            str(e),
            start_time,
            exception_type=type(e).__name__,
        )
        raise_structured_error(
            status_code=500,
            error_code=SessionErrorCodes.INTERNAL_ERROR,
            message="Failed to enqueue webhook",
            request_id=request_id,
        )



