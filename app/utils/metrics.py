"""
Comprehensive Metrics Module for Fittbot API

Supports Prometheus multiprocess mode (required for uvicorn --workers > 1).
Set PROMETHEUS_MULTIPROC_DIR env var to enable.
"""

import os
import time
import functools
import asyncio
from typing import Optional, Callable, Any
from contextlib import contextmanager, asynccontextmanager

from prometheus_client import (
    Histogram,
    Counter,
    Gauge,
    Info,
    Summary,
    CollectorRegistry,
    REGISTRY,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# Detect multiprocess mode (set by Dockerfile / entrypoint)
_MULTIPROC = bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR"))


# =============================================================================
# HTTP REQUEST METRICS
# =============================================================================

# Detailed HTTP latency with more granular buckets for percentile analysis
HTTP_REQUEST_LATENCY = Histogram(
    "http_request_latency_seconds",
    "HTTP request latency in seconds with detailed buckets",
    ["method", "endpoint", "status_code"],
    buckets=[
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3,
        0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0
    ],
)

HTTP_REQUEST_TOTAL = Counter(
    "http_requests_total_v2",
    "Total HTTP requests with detailed labels",
    ["method", "endpoint", "status_code", "user_type"],  # user_type: anonymous, authenticated, admin
)

HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method", "endpoint"],
    multiprocess_mode="livesum",
)

HTTP_REQUEST_SIZE = Histogram(
    "http_request_size_bytes",
    "HTTP request body size in bytes",
    ["method", "endpoint"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
)

HTTP_RESPONSE_SIZE = Histogram(
    "http_response_size_bytes",
    "HTTP response body size in bytes",
    ["method", "endpoint", "status_code"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
)

# Slow request tracking
SLOW_REQUESTS = Counter(
    "http_slow_requests_total",
    "Requests exceeding threshold (1s default)",
    ["method", "endpoint", "threshold_ms"],
)


# =============================================================================
# DATABASE METRICS
# =============================================================================

DB_QUERY_LATENCY = Histogram(
    "db_query_duration_seconds",
    "Database query latency",
    ["query_type", "table", "operation"],  # operation: select, insert, update, delete
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

DB_QUERY_TOTAL = Counter(
    "db_queries_total",
    "Total database queries",
    ["query_type", "table", "operation", "success"],
)

DB_CONNECTION_POOL = Gauge(
    "db_connection_pool_size",
    "Database connection pool metrics",
    ["pool_name", "metric"],  # metric: size, checked_in, checked_out, overflow
    multiprocess_mode="liveall",
)

DB_SLOW_QUERIES = Counter(
    "db_slow_queries_total",
    "Queries exceeding slow threshold (100ms default)",
    ["table", "operation"],
)


# =============================================================================
# EXTERNAL API METRICS (Razorpay, OpenAI, RevenueCat, etc.)
# =============================================================================

EXTERNAL_API_LATENCY = Histogram(
    "external_api_duration_seconds",
    "External API call latency",
    ["service", "endpoint", "method", "status"],
    buckets=[0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 3.0, 5.0, 10.0, 30.0],
)

EXTERNAL_API_TOTAL = Counter(
    "external_api_calls_total",
    "Total external API calls",
    ["service", "endpoint", "method", "status", "success"],
)

EXTERNAL_API_ERRORS = Counter(
    "external_api_errors_total",
    "External API errors by type",
    ["service", "endpoint", "error_type"],  # error_type: timeout, connection, http_error, parse_error
)

EXTERNAL_API_CIRCUIT_BREAKER = Gauge(
    "external_api_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half_open)",
    ["service"],
    multiprocess_mode="liveall",
)


# =============================================================================
# BUSINESS METRICS
# =============================================================================

# Payment metrics
PAYMENT_ATTEMPTS = Counter(
    "payment_attempts_total",
    "Total payment attempts",
    ["provider", "payment_type", "status"],  # provider: razorpay, revenuecat, dailypass
)

PAYMENT_AMOUNT = Counter(
    "payment_amount_total_inr",
    "Total payment amount in INR (paise)",
    ["provider", "payment_type", "status"],
)

PAYMENT_LATENCY = Histogram(
    "payment_processing_seconds",
    "Payment processing time",
    ["provider", "payment_type"],
    buckets=[0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 30.0],
)

# Authentication metrics
AUTH_ATTEMPTS = Counter(
    "auth_attempts_total",
    "Authentication attempts",
    ["method", "status"],  # method: otp, token_refresh, login
)

AUTH_FAILURES = Counter(
    "auth_failures_total",
    "Authentication failures by reason",
    ["method", "reason"],  # reason: invalid_otp, expired_token, invalid_credentials
)

# User activity metrics
ACTIVE_USERS = Gauge(
    "active_users",
    "Currently active users",
    ["user_type"],  # user_type: client, owner, admin, marketing
    multiprocess_mode="livesum",
)

USER_REGISTRATIONS = Counter(
    "user_registrations_total",
    "New user registrations",
    ["user_type", "referral_source"],
)

# AI/ML metrics
AI_REQUESTS = Counter(
    "ai_requests_total",
    "AI service requests",
    ["service", "model", "status"],  # service: food_scanner, chatbot, workout_generator
)

AI_LATENCY = Histogram(
    "ai_request_duration_seconds",
    "AI request processing time",
    ["service", "model"],
    buckets=[0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 30.0, 60.0],
)

AI_TOKENS_USED = Counter(
    "ai_tokens_used_total",
    "Total AI tokens consumed",
    ["service", "model", "token_type"],  # token_type: input, output
)


# =============================================================================
# INFRASTRUCTURE METRICS
# =============================================================================

# Process metrics
# NOTE: process_cpu_seconds_total is automatically provided by Prometheus's built-in ProcessCollector

PROCESS_MEMORY_BYTES = Gauge(
    "process_memory_bytes",
    "Process memory usage",
    ["type"],  # type: rss, vms, shared
    multiprocess_mode="liveall",
)

PROCESS_OPEN_FDS = Gauge(
    "process_open_file_descriptors",
    "Number of open file descriptors",
    multiprocess_mode="livesum",
)

# Redis metrics
REDIS_OPERATIONS = Counter(
    "redis_operations_total",
    "Redis operations",
    ["operation", "status"],  # operation: get, set, hget, hset, lpush, etc.
)

REDIS_LATENCY = Histogram(
    "redis_operation_duration_seconds",
    "Redis operation latency",
    ["operation", "command_type"],  # command_type: read, write, list, hash, set, pub/sub
    buckets=[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)

REDIS_CONNECTIONS = Gauge(
    "redis_connections_active",
    "Active Redis connections",
    multiprocess_mode="livesum",
)

REDIS_POOL_STATS = Gauge(
    "redis_connection_pool",
    "Redis connection pool statistics",
    ["metric"],  # metric: max, in_use, available, created
    multiprocess_mode="liveall",
)

REDIS_CACHE_HITS = Counter(
    "redis_cache_hits_total",
    "Redis cache hits",
    ["cache_type"],  # cache_type: session, user_data, eligibility, geo, etc.
)

REDIS_CACHE_MISSES = Counter(
    "redis_cache_misses_total",
    "Redis cache misses",
    ["cache_type"],
)

REDIS_MEMORY_BYTES = Gauge(
    "redis_memory_used_bytes",
    "Redis memory usage in bytes",
    multiprocess_mode="livemax",
)

REDIS_KEYS_COUNT = Gauge(
    "redis_keys_total",
    "Total keys in Redis",
    ["db"],
    multiprocess_mode="livemax",
)

REDIS_ERRORS = Counter(
    "redis_errors_total",
    "Redis errors by type",
    ["error_type"],  # error_type: connection, timeout, command_error
)

REDIS_SLOW_COMMANDS = Counter(
    "redis_slow_commands_total",
    "Redis commands exceeding 10ms",
    ["operation"],
)


# =============================================================================
# CELERY TASK METRICS
# =============================================================================

CELERY_TASK_LATENCY = Histogram(
    "celery_task_duration_seconds",
    "Celery task execution time",
    ["task_name", "queue", "status"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

CELERY_TASK_TOTAL = Counter(
    "celery_tasks_total",
    "Total Celery tasks",
    ["task_name", "queue", "status"],
)

CELERY_QUEUE_LENGTH = Gauge(
    "celery_queue_length",
    "Number of tasks in queue",
    ["queue"],
    multiprocess_mode="livemax",
)

CELERY_WORKERS_ACTIVE = Gauge(
    "celery_workers_active",
    "Number of active Celery workers",
    ["queue"],
    multiprocess_mode="livemax",
)

CELERY_TASKS_ACTIVE = Gauge(
    "celery_tasks_active",
    "Number of tasks currently being processed",
    ["queue"],
    multiprocess_mode="livemax",
)

CELERY_TASKS_RESERVED = Gauge(
    "celery_tasks_reserved",
    "Number of tasks reserved (prefetched) by workers",
    ["queue"],
    multiprocess_mode="livemax",
)


# =============================================================================
# WEBSOCKET METRICS
# =============================================================================

WEBSOCKET_CONNECTIONS = Gauge(
    "websocket_connections_active",
    "Active WebSocket connections",
    ["hub_type"],  # hub_type: feed, live, chat, sessions
    multiprocess_mode="livesum",
)

WEBSOCKET_MESSAGES = Counter(
    "websocket_messages_total",
    "WebSocket messages",
    ["hub_type", "direction"],  # direction: inbound, outbound
)


# =============================================================================
# APPLICATION INFO
# =============================================================================

if not _MULTIPROC:
    APP_INFO = Info(
        "app",
        "Application information",
    )
else:
    APP_INFO = None


# =============================================================================
# HELPER FUNCTIONS AND DECORATORS
# =============================================================================

def normalize_endpoint(path: str, max_segments: int = 4) -> str:
    """
    Normalize endpoint path to prevent cardinality explosion.
    Replaces numeric IDs with placeholders.

    /client/123/workout/456 -> /client/{id}/workout/{id}
    /pay/dailypass_v2/commands/37:dp_cmd_1770022335_5915fc9d -> /pay/dailypass_v2/commands/{cmd_id}
    """
    import re

    if not path:
        return "/"

    segments = path.strip("/").split("/")
    normalized = []

    # Pattern for command IDs like "37:gym_cmd_1770022419_740e90af" or "37:dp_cmd_xxx"
    cmd_id_pattern = re.compile(r'^\d+:(gym_cmd|dp_cmd|sess_cmd|sub_cmd)_\d+_[a-f0-9]+$')
    # Pattern for generic IDs with mixed alphanumeric and special chars (underscores, dashes)
    generic_id_pattern = re.compile(r'^[a-zA-Z0-9_-]{15,}$')

    for segment in segments[:max_segments]:
        # Replace numeric IDs
        if segment.isdigit():
            normalized.append("{id}")
        # Replace UUIDs
        elif len(segment) == 36 and segment.count("-") == 4:
            normalized.append("{uuid}")
        # Replace command IDs (e.g., 37:gym_cmd_1770022419_740e90af)
        elif cmd_id_pattern.match(segment):
            normalized.append("{cmd_id}")
        # Replace long alphanumeric strings (likely IDs)
        elif len(segment) > 20 and segment.isalnum():
            normalized.append("{id}")
        # Replace generic long IDs with underscores/dashes
        elif len(segment) > 15 and generic_id_pattern.match(segment):
            normalized.append("{id}")
        else:
            normalized.append(segment)

    return "/" + "/".join(normalized)


def get_user_type(request) -> str:
    """Determine user type from request for metrics labeling."""
    if hasattr(request.state, "role"):
        role = request.state.role
        if role in ("admin", "super_admin"):
            return "admin"
        elif role in ("owner", "trainer"):
            return "owner"
        elif role == "marketing":
            return "marketing"
        elif role == "telecaller":
            return "telecaller"
        return "authenticated"
    return "anonymous"


@contextmanager
def track_db_query(table: str, operation: str):
    """
    Context manager to track database query metrics.

    Usage:
        with track_db_query("users", "select"):
            result = await db.execute(query)
    """
    start = time.perf_counter()
    success = "true"
    try:
        yield
    except Exception:
        success = "false"
        raise
    finally:
        duration = time.perf_counter() - start
        DB_QUERY_LATENCY.labels(
            query_type="sql",
            table=table,
            operation=operation
        ).observe(duration)
        DB_QUERY_TOTAL.labels(
            query_type="sql",
            table=table,
            operation=operation,
            success=success
        ).inc()

        # Track slow queries (>100ms)
        if duration > 0.1:
            DB_SLOW_QUERIES.labels(table=table, operation=operation).inc()


@asynccontextmanager
async def track_external_api(service: str, endpoint: str, method: str = "POST"):
    """
    Async context manager to track external API calls.

    Usage:
        async with track_external_api("razorpay", "/orders", "POST"):
            response = await client.post(...)
    """
    start = time.perf_counter()
    status = "success"
    status_code = "200"

    try:
        yield
    except asyncio.TimeoutError:
        status = "timeout"
        status_code = "timeout"
        EXTERNAL_API_ERRORS.labels(
            service=service,
            endpoint=endpoint,
            error_type="timeout"
        ).inc()
        raise
    except ConnectionError:
        status = "connection_error"
        status_code = "connection_error"
        EXTERNAL_API_ERRORS.labels(
            service=service,
            endpoint=endpoint,
            error_type="connection"
        ).inc()
        raise
    except Exception as e:
        status = "error"
        status_code = "error"
        error_type = type(e).__name__
        EXTERNAL_API_ERRORS.labels(
            service=service,
            endpoint=endpoint,
            error_type=error_type
        ).inc()
        raise
    finally:
        duration = time.perf_counter() - start
        EXTERNAL_API_LATENCY.labels(
            service=service,
            endpoint=endpoint,
            method=method,
            status=status_code
        ).observe(duration)
        EXTERNAL_API_TOTAL.labels(
            service=service,
            endpoint=endpoint,
            method=method,
            status=status_code,
            success="true" if status == "success" else "false"
        ).inc()


def track_payment(provider: str, payment_type: str):
    """
    Decorator to track payment operations.

    Usage:
        @track_payment("razorpay", "subscription")
        async def create_subscription(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status = "success"
            amount = 0

            try:
                result = await func(*args, **kwargs)
                # Try to extract amount from result
                if isinstance(result, dict):
                    amount = result.get("amount", 0)
                return result
            except Exception:
                status = "failed"
                raise
            finally:
                duration = time.perf_counter() - start
                PAYMENT_ATTEMPTS.labels(
                    provider=provider,
                    payment_type=payment_type,
                    status=status
                ).inc()
                PAYMENT_LATENCY.labels(
                    provider=provider,
                    payment_type=payment_type
                ).observe(duration)
                if amount > 0:
                    PAYMENT_AMOUNT.labels(
                        provider=provider,
                        payment_type=payment_type,
                        status=status
                    ).inc(amount)

        return wrapper
    return decorator


def track_ai_request(service: str, model: str):
    """
    Decorator to track AI/ML service requests.

    Usage:
        @track_ai_request("food_scanner", "gemini-pro")
        async def analyze_food_image(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            status = "success"

            try:
                result = await func(*args, **kwargs)
                return result
            except Exception:
                status = "failed"
                raise
            finally:
                duration = time.perf_counter() - start
                AI_REQUESTS.labels(
                    service=service,
                    model=model,
                    status=status
                ).inc()
                AI_LATENCY.labels(
                    service=service,
                    model=model
                ).observe(duration)

        return wrapper
    return decorator


def record_ai_tokens(service: str, model: str, input_tokens: int, output_tokens: int):
    """Record AI token usage for cost tracking."""
    AI_TOKENS_USED.labels(
        service=service,
        model=model,
        token_type="input"
    ).inc(input_tokens)
    AI_TOKENS_USED.labels(
        service=service,
        model=model,
        token_type="output"
    ).inc(output_tokens)


def track_auth(method: str, status: str, reason: Optional[str] = None):
    """Record authentication attempt."""
    AUTH_ATTEMPTS.labels(method=method, status=status).inc()
    if status == "failed" and reason:
        AUTH_FAILURES.labels(method=method, reason=reason).inc()


def update_circuit_breaker_state(service: str, state: str):
    """Update circuit breaker state metric."""
    state_map = {"closed": 0, "open": 1, "half_open": 2}
    EXTERNAL_API_CIRCUIT_BREAKER.labels(service=service).set(state_map.get(state, 0))


# =============================================================================
# METRICS COLLECTION UTILITIES
# =============================================================================

def collect_process_metrics():
    """Collect process-level metrics (call periodically)."""
    try:
        import psutil
        process = psutil.Process()

        # Memory
        mem_info = process.memory_info()
        PROCESS_MEMORY_BYTES.labels(type="rss").set(mem_info.rss)
        PROCESS_MEMORY_BYTES.labels(type="vms").set(mem_info.vms)

        # CPU
        cpu_times = process.cpu_times()
        # Note: These are cumulative, so we set them directly

        # Open file descriptors
        try:
            PROCESS_OPEN_FDS.set(process.num_fds())
        except Exception:
            pass  # Not available on all platforms

    except ImportError:
        pass  # psutil not installed
    except Exception:
        pass


def collect_db_pool_metrics(engine):
    """Collect database connection pool metrics."""
    try:
        pool = engine.pool
        PROCESS_OPEN_FDS.set(pool.size())
        DB_CONNECTION_POOL.labels(pool_name="main", metric="size").set(pool.size())
        DB_CONNECTION_POOL.labels(pool_name="main", metric="checked_in").set(pool.checkedin())
        DB_CONNECTION_POOL.labels(pool_name="main", metric="checked_out").set(pool.checkedout())
        DB_CONNECTION_POOL.labels(pool_name="main", metric="overflow").set(pool.overflow())
    except Exception:
        pass


async def collect_redis_metrics(redis_client, redis_pool=None):
    """Collect comprehensive Redis metrics."""
    try:
        # Get Redis INFO stats
        info = await redis_client.info()

        # Connection metrics
        REDIS_CONNECTIONS.set(info.get("connected_clients", 0))

        # Memory metrics
        REDIS_MEMORY_BYTES.set(info.get("used_memory", 0))

        # Keys per database
        for key, value in info.items():
            if key.startswith("db") and isinstance(value, dict):
                REDIS_KEYS_COUNT.labels(db=key).set(value.get("keys", 0))

        # Connection pool stats (if available)
        if redis_pool:
            REDIS_POOL_STATS.labels(metric="max").set(
                getattr(redis_pool, "max_connections", 0)
            )
            REDIS_POOL_STATS.labels(metric="in_use").set(
                len(getattr(redis_pool, "_in_use_connections", []))
            )
            REDIS_POOL_STATS.labels(metric="available").set(
                len(getattr(redis_pool, "_available_connections", []))
            )

    except Exception:
        pass


def get_redis_command_type(operation: str) -> str:
    """Categorize Redis operation by command type."""
    operation = operation.lower()

    read_ops = {"get", "mget", "exists", "ttl", "pttl", "type", "keys", "scan"}
    write_ops = {"set", "setex", "setnx", "mset", "del", "expire", "incr", "decr", "incrby"}
    hash_ops = {"hget", "hset", "hmget", "hmset", "hgetall", "hdel", "hexists", "hkeys", "hvals", "hincrby"}
    list_ops = {"lpush", "rpush", "lpop", "rpop", "llen", "lrange", "lindex", "lset"}
    set_ops = {"sadd", "srem", "smembers", "sismember", "scard", "sunion", "sinter"}
    sorted_set_ops = {"zadd", "zrem", "zrange", "zrevrange", "zscore", "zcard", "zincrby"}
    geo_ops = {"geoadd", "geodist", "georadius", "geosearch", "geopos"}
    pubsub_ops = {"publish", "subscribe", "unsubscribe", "psubscribe"}

    if operation in read_ops:
        return "read"
    elif operation in write_ops:
        return "write"
    elif operation in hash_ops:
        return "hash"
    elif operation in list_ops:
        return "list"
    elif operation in set_ops:
        return "set"
    elif operation in sorted_set_ops:
        return "sorted_set"
    elif operation in geo_ops:
        return "geo"
    elif operation in pubsub_ops:
        return "pubsub"
    else:
        return "other"


@asynccontextmanager
async def track_redis_operation(operation: str):
    """
    Async context manager to track Redis operations.

    Usage:
        async with track_redis_operation("get"):
            value = await redis.get("key")
    """
    start = time.perf_counter()
    status = "success"
    command_type = get_redis_command_type(operation)

    try:
        yield
    except asyncio.TimeoutError:
        status = "timeout"
        REDIS_ERRORS.labels(error_type="timeout").inc()
        raise
    except ConnectionError:
        status = "connection_error"
        REDIS_ERRORS.labels(error_type="connection").inc()
        raise
    except Exception as e:
        status = "error"
        REDIS_ERRORS.labels(error_type="command_error").inc()
        raise
    finally:
        duration = time.perf_counter() - start
        REDIS_OPERATIONS.labels(operation=operation, status=status).inc()
        REDIS_LATENCY.labels(operation=operation, command_type=command_type).observe(duration)

        # Track slow commands (>10ms)
        if duration > 0.01:
            REDIS_SLOW_COMMANDS.labels(operation=operation).inc()


def track_cache_hit(cache_type: str):
    """Record a cache hit."""
    REDIS_CACHE_HITS.labels(cache_type=cache_type).inc()


def track_cache_miss(cache_type: str):
    """Record a cache miss."""
    REDIS_CACHE_MISSES.labels(cache_type=cache_type).inc()


async def collect_celery_queue_metrics(redis_client):
    """Collect Celery queue length metrics."""
    try:
        queues = ["celery", "ai", "payments"]
        for queue in queues:
            length = await redis_client.llen(queue)
            CELERY_QUEUE_LENGTH.labels(queue=queue).set(length)
    except Exception:
        pass


def collect_celery_worker_metrics():
    """
    Collect Celery worker metrics using Celery's inspect API.
    Works with ECS Fargate autoscaling - no worker name parsing needed.

    Collects:
    - Number of active workers per queue (by checking active_queues)
    - Active tasks count per queue (currently being processed)
    - Reserved tasks count per queue (prefetched by workers)
    """
    try:
        from app.celery_app import celery_app

        # Get celery inspect object with short timeout
        inspect = celery_app.control.inspect(timeout=1.0)

        # Initialize counters
        queue_worker_count = {"celery": 0, "ai": 0, "payments": 0}
        queue_active_tasks = {"celery": 0, "ai": 0, "payments": 0}
        queue_reserved_tasks = {"celery": 0, "ai": 0, "payments": 0}

        # Get active queues to count workers per queue
        active_queues_data = inspect.active_queues()
        if active_queues_data:
            for worker_name, queues_list in active_queues_data.items():
                # queues_list is like [{'name': 'ai', 'routing_key': 'ai', ...}]
                for queue_info in queues_list:
                    queue_name = queue_info.get('name', '')
                    if queue_name in queue_worker_count:
                        queue_worker_count[queue_name] += 1

        # Get active tasks (currently being processed)
        active_tasks_data = inspect.active()
        if active_tasks_data:
            for worker_name, tasks_list in active_tasks_data.items():
                # tasks_list is a list of task info dicts
                for task in tasks_list:
                    # Get the queue from task delivery_info
                    delivery_info = task.get('delivery_info', {})
                    queue_name = delivery_info.get('routing_key', '')
                    if queue_name in queue_active_tasks:
                        queue_active_tasks[queue_name] += 1

        # Get reserved tasks (prefetched by workers but not yet executing)
        reserved_tasks_data = inspect.reserved()
        if reserved_tasks_data:
            for worker_name, tasks_list in reserved_tasks_data.items():
                for task in tasks_list:
                    delivery_info = task.get('delivery_info', {})
                    queue_name = delivery_info.get('routing_key', '')
                    if queue_name in queue_reserved_tasks:
                        queue_reserved_tasks[queue_name] += 1

        # Set all the metrics
        for queue in ["celery", "ai", "payments"]:
            CELERY_WORKERS_ACTIVE.labels(queue=queue).set(queue_worker_count[queue])
            CELERY_TASKS_ACTIVE.labels(queue=queue).set(queue_active_tasks[queue])
            CELERY_TASKS_RESERVED.labels(queue=queue).set(queue_reserved_tasks[queue])

    except Exception as e:
        # Silently fail - worker inspection might timeout or be unavailable in some environments
        pass


def set_app_info(version: str, environment: str, commit_sha: str = "unknown"):
    """Set application info metric."""
    if APP_INFO is not None:
        APP_INFO.info({
            "version": version,
            "environment": environment,
            "commit_sha": commit_sha,
        })


# =============================================================================
# METRICS EXPORT
# =============================================================================

def get_metrics() -> bytes:
    """Generate Prometheus metrics output.
    In multiprocess mode, aggregates metrics from all uvicorn workers.
    """
    if _MULTIPROC:
        from prometheus_client import multiprocess
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return generate_latest(registry)
    return generate_latest(REGISTRY)


def get_metrics_content_type() -> str:
    """Get the content type for Prometheus metrics."""
    return CONTENT_TYPE_LATEST
