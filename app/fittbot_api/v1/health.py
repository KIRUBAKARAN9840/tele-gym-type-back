"""
Database health monitoring and connection pool metrics
Refactored to remove logger usage and use FittbotHTTPException-only error handling.
"""
from __future__ import annotations

import time
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models.database import get_db, engine
from app.config.settings import settings
from app.utils.logging_utils import FittbotHTTPException  # using only the exception wrapper
from app.utils.redis_config import get_redis

router = APIRouter(prefix="/health", tags=["Health & Monitoring"])


CELERY_QUEUES = ["ai", "payments", "celery"]


@router.get("/")
@router.options("/")
async def root_health():
    """Basic health check for the health router"""
    return {"status": "ok", "version": "1.0.0", "environment": settings.environment}


def _cfg(name: str, default):
    """Safely read settings.* with a default fallback."""
    return getattr(settings, name, default)


def _safe_pool_metric(pool, attr: str, default: int = 0) -> int:
    """Call a pool metric if present (and callable), else return default."""
    if hasattr(pool, attr):
        val = getattr(pool, attr)
        try:
            return int(val() if callable(val) else val)
        except Exception:
            return default
    return default


@router.get("/database")
@router.options("/database")
async def database_health(db: Session = Depends(get_db)):
    try:
        pool = engine.pool

        # Pool configuration (with safe fallbacks)
        base_pool_size = int(_cfg("db_pool_size", 5))
        max_overflow = int(_cfg("db_max_overflow", 10))
        total_capacity = max(1, base_pool_size + max_overflow)  # avoid divide-by-zero

        # Pool metrics (guard against driver differences)
        pool_size = _safe_pool_metric(pool, "size")
        checked_in = _safe_pool_metric(pool, "checkedin")
        checked_out = _safe_pool_metric(pool, "checkedout")
        overflow = _safe_pool_metric(pool, "overflow")
        invalid_connections = _safe_pool_metric(pool, "invalid", 0)  # may not exist on vanilla pools

        total_connections = pool_size + overflow
        pool_utilization_percent = round(((checked_out + overflow) / total_capacity) * 100, 2)

        # DB connectivity + response time
        t0 = time.monotonic()
        db.execute(text("SELECT 1"))
        db_response_time = (time.monotonic() - t0) * 1000.0  # ms

        health_status = "healthy"
        warnings: list[str] = []

        if pool_utilization_percent > 80:
            warnings.append("High pool utilization - consider increasing pool size")
            if pool_utilization_percent > 95:
                health_status = "warning"

        if invalid_connections > 0:
            warnings.append(f"{invalid_connections} invalid connections detected")

        if db_response_time > 1000:
            warnings.append(f"Slow database response time: {db_response_time:.2f}ms")
            health_status = "warning"

        result = {
            "status": health_status,
            "timestamp": datetime.now().isoformat(),
            "database": {"responsive": True, "response_time_ms": round(db_response_time, 2)},
            "connection_pool": {
                "pool_size": pool_size,
                "checked_in": checked_in,
                "checked_out": checked_out,
                "overflow": overflow,
                "invalid_connections": invalid_connections,
                "total_connections": total_connections,
                "max_connections": total_capacity,
                "pool_utilization_percent": pool_utilization_percent,
            },
            "configuration": {
                "pool_size": base_pool_size,
                "max_overflow": max_overflow,
                "pool_pre_ping": _cfg("db_pool_pre_ping", True),
                "pool_recycle": _cfg("db_pool_recycle", 1800),
                "pool_timeout": _cfg("db_pool_timeout", 30),
                "environment": _cfg("environment", "unknown"),
            },
            "warnings": warnings,
            "recommendations": _get_recommendations(
                {
                    "pool_utilization_percent": pool_utilization_percent,
                    "invalid_connections": invalid_connections,
                    "overflow": overflow,
                    "pool_size": pool_size,
                },
                db_response_time,
            ),
        }
        return result

    except FittbotHTTPException:
        # pass through already-structured errors
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to compute database health",
            error_code="DB_HEALTH_ERROR",
            log_data={"error": repr(e)},
        )


@router.get("/database/connections")
@router.options("/database/connections")
async def database_connections():
    try:
        pool = engine.pool

        base_pool_size = int(_cfg("db_pool_size", 5))
        max_overflow = int(_cfg("db_max_overflow", 10))
        total_capacity = max(1, base_pool_size + max_overflow)

        size = _safe_pool_metric(pool, "size")
        checked_out = _safe_pool_metric(pool, "checkedout")
        checked_in = _safe_pool_metric(pool, "checkedin")
        overflow = _safe_pool_metric(pool, "overflow")
        invalid_connections = _safe_pool_metric(pool, "invalid", 0)

        result = {
            "timestamp": datetime.now().isoformat(),
            "connections": {
                "total_available": size,
                "currently_checked_out": checked_out,
                "currently_checked_in": checked_in,
                "overflow_connections": overflow,
                "invalid_connections": invalid_connections,
                "total_capacity": total_capacity,
            },
            "utilization": {
                "active_percentage": round((checked_out / total_capacity) * 100, 2),
                "overflow_in_use": overflow > 0,
                "approaching_limit": checked_out > (base_pool_size * 0.8),
            },
            "configuration": {
                "base_pool_size": base_pool_size,
                "max_overflow": max_overflow,
                "recycle_time_seconds": _cfg("db_pool_recycle", 1800),
                "timeout_seconds": _cfg("db_pool_timeout", 30),
                "pre_ping_enabled": _cfg("db_pool_pre_ping", True),
            },
        }
        return result

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to get database connection statistics",
            error_code="DB_CONNECTION_STATS_ERROR",
            log_data={"error": repr(e)},
        )


@router.get("/database/performance")
@router.options("/database/performance")
async def database_performance(db: Session = Depends(get_db)):
    try:
        performance_metrics = {}

        # Test 1: Simple SELECT
        t0 = time.monotonic()
        db.execute(text("SELECT 1"))
        performance_metrics["simple_query_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

        # Test 2: Current timestamp (portable)
        t0 = time.monotonic()
        db.execute(text("SELECT CURRENT_TIMESTAMP"))
        performance_metrics["timestamp_query_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

        # Test 3: "connection id" (DB-specific; try MySQL then Postgres)
        conn_ms = None
        t0 = time.monotonic()
        try:
            db.execute(text("SELECT CONNECTION_ID()"))  # MySQL/MariaDB
            conn_ms = round((time.monotonic() - t0) * 1000.0, 2)
        except Exception:
            t0 = time.monotonic()
            try:
                db.execute(text("SELECT pg_backend_pid()"))  # PostgreSQL
                conn_ms = round((time.monotonic() - t0) * 1000.0, 2)
            except Exception:
                conn_ms = None
        if conn_ms is not None:
            performance_metrics["connection_validation_ms"] = conn_ms

        # Assessment
        times = list(performance_metrics.values())
        avg_response = sum(times) / max(1, len(times))
        performance_status = "excellent" if avg_response < 10 else "good" if avg_response < 50 else "slow"

        return {
            "status": performance_status,
            "timestamp": datetime.now().isoformat(),
            "metrics": performance_metrics,
            "average_response_time_ms": round(avg_response, 2),
            "assessment": {
                "status": performance_status,
                "message": _get_performance_message(avg_response),
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Database performance test failed",
            error_code="DB_PERFORMANCE_TEST_ERROR",
            log_data={"error": repr(e)},
        )


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def _get_recommendations(pool_metrics: dict, response_time: float) -> list[str]:
    """Generate recommendations based on pool metrics."""
    recommendations: list[str] = []

    util = pool_metrics.get("pool_utilization_percent", 0)
    invalid = pool_metrics.get("invalid_connections", 0)
    overflow = pool_metrics.get("overflow", 0)
    pool_size = pool_metrics.get("pool_size", 0)

    if util > 90:
        recommendations.append("Consider increasing DB_POOL_SIZE and DB_MAX_OVERFLOW")

    if invalid > 2:
        recommendations.append("Check network connectivity - many invalid connections detected")

    if response_time > 500:
        recommendations.append("Database queries are slow - consider query optimization")

    if pool_size and overflow > (pool_size * 0.5):
        recommendations.append("Frequently using overflow connections - increase base pool size")

    if not recommendations:
        recommendations.append("Database connection pool is performing well")

    return recommendations


def _get_performance_message(avg_response: float) -> str:
    """Get performance assessment message."""
    if avg_response < 10:
        return "Database performance is excellent"
    if avg_response < 50:
        return "Database performance is good"
    if avg_response < 200:
        return "Database performance is acceptable but could be improved"
    return "Database performance is slow - investigation needed"


@router.get("/celery/queues")
@router.options("/celery/queues")
async def celery_queue_health():
    """
    Monitor Celery queue depths for auto-scaling decisions.

    Scaling thresholds:
    - queue_depth > 2x workers → consider scaling up
    - queue_depth > 4x workers → scale up immediately
    - queue_depth < 0.5x workers for 5min → scale down
    """
    try:
        redis = await get_redis()

        queues = {}
        total_depth = 0

        for queue_name in CELERY_QUEUES:
            depth = await redis.llen(queue_name)
            queues[queue_name] = {
                "depth": depth,
                "status": _get_queue_status(queue_name, depth),
            }
            total_depth += depth

        # Overall assessment
        ai_depth = queues.get("ai", {}).get("depth", 0)
        payments_depth = queues.get("payments", {}).get("depth", 0)

        scaling_recommendations = []
        if ai_depth > 24:  # 2x workers (12)
            scaling_recommendations.append("AI queue backing up - consider adding Fargate task")
        if payments_depth > 12:  # 1x workers (12) - more critical
            scaling_recommendations.append("Payments queue backing up - scale immediately")

        return {
            "status": "healthy" if total_depth < 36 else "warning" if total_depth < 72 else "critical",
            "timestamp": datetime.now().isoformat(),
            "queues": queues,
            "total_queued_tasks": total_depth,
            "scaling": {
                "ai_workers": 12,  # 3 tasks × 4 workers
                "payments_workers": 12,  # 3 tasks × 4 workers
                "recommendations": scaling_recommendations,
            },
        }

    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to get Celery queue metrics",
            error_code="CELERY_QUEUE_ERROR",
            log_data={"error": repr(e)},
        )


def _get_queue_status(queue_name: str, depth: int) -> str:
    """Determine queue health status based on depth and queue type."""
    if queue_name == "payments":
        # Payments are more critical - lower thresholds
        if depth == 0:
            return "idle"
        if depth <= 6:
            return "healthy"
        if depth <= 12:
            return "busy"
        return "overloaded"
    else:
        # AI queue can tolerate more backlog
        if depth == 0:
            return "idle"
        if depth <= 12:
            return "healthy"
        if depth <= 24:
            return "busy"
        return "overloaded"
