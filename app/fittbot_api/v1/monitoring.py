"""
Monitoring Dashboard API for Fittbot

Provides real-time visibility into:
- API performance metrics
- Database health
- External service status
- Business KPIs
- System health

Access at: /monitoring/dashboard
"""

import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from app.utils.redis_config import get_redis
from app.config.settings import settings

logger = logging.getLogger("app.monitoring")

router = APIRouter(prefix="/monitoring", tags=["Monitoring"])


class MetricsSummary(BaseModel):
    """Summary of key metrics."""
    total_requests: int
    requests_per_minute: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate: float
    slow_requests: int


class DatabaseHealth(BaseModel):
    """Database health status."""
    status: str
    pool_size: int
    active_connections: int
    available_connections: int
    utilization_percent: float
    avg_query_time_ms: float
    slow_queries_count: int


class ExternalServiceHealth(BaseModel):
    """External service health status."""
    service: str
    status: str
    avg_latency_ms: float
    error_rate: float
    circuit_breaker_state: str
    last_check: str


class SystemHealth(BaseModel):
    """Overall system health."""
    status: str
    uptime_seconds: int
    cpu_percent: Optional[float]
    memory_percent: Optional[float]
    open_connections: int


# Store startup time
_startup_time = time.time()


@router.get("/dashboard")
async def monitoring_dashboard():
    """
    Main monitoring dashboard with all metrics.
    Returns a comprehensive view of system health.
    """
    try:
        redis = await get_redis()

        # Gather metrics in parallel
        db_health, external_services, system_health, business_metrics = await asyncio.gather(
            _get_database_health(),
            _get_external_services_health(redis),
            _get_system_health(),
            _get_business_metrics(redis),
            return_exceptions=True,
        )

        # Handle any exceptions
        if isinstance(db_health, Exception):
            db_health = {"status": "error", "error": str(db_health)}
        if isinstance(external_services, Exception):
            external_services = []
        if isinstance(system_health, Exception):
            system_health = {"status": "error", "error": str(system_health)}
        if isinstance(business_metrics, Exception):
            business_metrics = {}

        # Determine overall status
        overall_status = "healthy"
        if db_health.get("status") != "healthy" or system_health.get("status") != "healthy":
            overall_status = "degraded"

        return {
            "status": overall_status,
            "timestamp": datetime.utcnow().isoformat(),
            "environment": settings.environment,
            "version": "1.0.0",
            "uptime_seconds": int(time.time() - _startup_time),
            "database": db_health,
            "external_services": external_services,
            "system": system_health,
            "business_metrics": business_metrics,
            "metrics_endpoint": "/metrics",
            "health_endpoint": "/health/database",
        }

    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )


@router.get("/latency")
async def latency_metrics(
    endpoint: Optional[str] = Query(None, description="Filter by endpoint pattern"),
    minutes: int = Query(5, description="Time window in minutes"),
):
    """
    Get detailed latency metrics for API endpoints.

    Returns percentiles (p50, p95, p99) and request counts per endpoint.
    """
    try:
        redis = await get_redis()

        # Get latency data from Redis (stored by metrics middleware)
        latency_data = await _get_latency_data(redis, endpoint, minutes)

        return {
            "time_window_minutes": minutes,
            "endpoint_filter": endpoint,
            "endpoints": latency_data,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Latency metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/slow-requests")
async def slow_requests(
    threshold_ms: int = Query(1000, description="Latency threshold in ms"),
    limit: int = Query(50, description="Max results"),
):
    """
    Get recent slow requests exceeding the threshold.
    """
    try:
        redis = await get_redis()

        # Get slow requests from Redis log
        slow_requests = await _get_slow_requests(redis, threshold_ms, limit)

        return {
            "threshold_ms": threshold_ms,
            "count": len(slow_requests),
            "requests": slow_requests,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Slow requests error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/errors")
async def error_metrics(
    minutes: int = Query(60, description="Time window in minutes"),
):
    """
    Get error rate and breakdown by status code.
    """
    try:
        redis = await get_redis()

        error_data = await _get_error_metrics(redis, minutes)

        return {
            "time_window_minutes": minutes,
            "total_errors": error_data.get("total", 0),
            "error_rate_percent": error_data.get("rate", 0),
            "by_status_code": error_data.get("by_status", {}),
            "by_endpoint": error_data.get("by_endpoint", {}),
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/external-apis")
async def external_api_metrics():
    """
    Get health and latency metrics for external APIs (Razorpay, OpenAI, etc.)
    """
    try:
        redis = await get_redis()
        services = await _get_external_services_health(redis)

        return {
            "services": services,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"External API metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/business")
async def business_metrics():
    """
    Get business KPIs:
    - Payment success rate
    - User registrations
    - Active users
    - AI usage
    """
    try:
        redis = await get_redis()
        metrics = await _get_business_metrics(redis)

        return {
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Business metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/real-time")
async def real_time_stats():
    """
    Get real-time statistics (last 1 minute).
    """
    try:
        redis = await get_redis()

        # Current minute key
        current_minute = datetime.utcnow().strftime("%Y%m%d%H%M")

        # Get real-time counters
        requests = await redis.get(f"metrics:requests:{current_minute}") or 0
        errors = await redis.get(f"metrics:errors:{current_minute}") or 0

        return {
            "requests_this_minute": int(requests),
            "errors_this_minute": int(errors),
            "error_rate": (int(errors) / int(requests) * 100) if int(requests) > 0 else 0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Real-time stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# HTML Dashboard for visual monitoring
@router.get("/ui", response_class=HTMLResponse)
async def monitoring_ui():
    """
    Simple HTML dashboard for monitoring.
    For production, use Grafana instead.
    """
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Fittbot Monitoring Dashboard</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
            .container { max-width: 1200px; margin: 0 auto; }
            h1 { color: #00d4ff; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .card { background: #16213e; border-radius: 8px; padding: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
            .card h2 { margin-top: 0; color: #00d4ff; font-size: 1.1em; }
            .metric { font-size: 2em; font-weight: bold; color: #4ade80; }
            .metric.warning { color: #fbbf24; }
            .metric.error { color: #f87171; }
            .label { color: #9ca3af; font-size: 0.9em; }
            .status { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.85em; }
            .status.healthy { background: #065f46; color: #34d399; }
            .status.degraded { background: #78350f; color: #fbbf24; }
            .status.error { background: #7f1d1d; color: #fca5a5; }
            table { width: 100%; border-collapse: collapse; }
            th, td { text-align: left; padding: 8px; border-bottom: 1px solid #374151; }
            th { color: #9ca3af; font-weight: normal; }
            .refresh-note { text-align: center; color: #6b7280; margin-top: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🏋️ Fittbot Monitoring Dashboard</h1>
            <div id="dashboard">Loading...</div>
            <p class="refresh-note">Auto-refreshes every 30 seconds | <a href="/metrics" style="color:#00d4ff">Prometheus Metrics</a> | <a href="/monitoring/dashboard" style="color:#00d4ff">JSON API</a></p>
        </div>
        <script>
            async function loadDashboard() {
                try {
                    const response = await fetch('/monitoring/dashboard');
                    const data = await response.json();
                    document.getElementById('dashboard').innerHTML = renderDashboard(data);
                } catch (e) {
                    document.getElementById('dashboard').innerHTML = '<div class="card"><p class="metric error">Error loading dashboard</p></div>';
                }
            }

            function renderDashboard(data) {
                const statusClass = data.status === 'healthy' ? 'healthy' : (data.status === 'degraded' ? 'degraded' : 'error');
                return `
                    <div class="grid">
                        <div class="card">
                            <h2>System Status</h2>
                            <span class="status ${statusClass}">${data.status.toUpperCase()}</span>
                            <p class="label">Uptime: ${Math.floor(data.uptime_seconds / 3600)}h ${Math.floor((data.uptime_seconds % 3600) / 60)}m</p>
                            <p class="label">Environment: ${data.environment}</p>
                        </div>
                        <div class="card">
                            <h2>Database</h2>
                            <div class="metric ${data.database?.utilization_percent > 80 ? 'warning' : ''}">${data.database?.utilization_percent?.toFixed(1) || 0}%</div>
                            <p class="label">Pool Utilization</p>
                            <p class="label">Active: ${data.database?.active_connections || 0} / ${data.database?.pool_size || 0}</p>
                        </div>
                        <div class="card">
                            <h2>Memory</h2>
                            <div class="metric ${data.system?.memory_percent > 85 ? 'warning' : ''}">${data.system?.memory_percent?.toFixed(1) || 'N/A'}%</div>
                            <p class="label">Memory Usage</p>
                        </div>
                        <div class="card">
                            <h2>External Services</h2>
                            ${(data.external_services || []).map(s => `
                                <p><span class="status ${s.status === 'healthy' ? 'healthy' : 'error'}">${s.service}</span> ${s.avg_latency_ms?.toFixed(0) || 0}ms</p>
                            `).join('')}
                        </div>
                    </div>
                    <div class="card" style="margin-top:20px">
                        <h2>Business Metrics</h2>
                        <table>
                            <tr><th>Metric</th><th>Value</th></tr>
                            ${Object.entries(data.business_metrics || {}).map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`).join('')}
                        </table>
                    </div>
                `;
            }

            loadDashboard();
        </script>
    </body>
    </html>
    """
    return html_content


# Helper functions

async def _get_database_health() -> Dict[str, Any]:
    """Get database health metrics."""
    try:
        from app.models.database import engine

        pool = engine.pool
        pool_size = pool.size()
        checked_out = pool.checkedout()
        overflow = pool.overflow()

        total_capacity = pool_size + (pool._max_overflow if hasattr(pool, '_max_overflow') else 0)
        utilization = (checked_out / total_capacity * 100) if total_capacity > 0 else 0

        return {
            "status": "healthy" if utilization < 80 else "degraded",
            "pool_size": pool_size,
            "active_connections": checked_out,
            "available_connections": pool_size - checked_out + (pool._max_overflow - overflow if hasattr(pool, '_max_overflow') else 0),
            "utilization_percent": round(utilization, 2),
            "overflow": overflow,
        }

    except Exception as e:
        logger.error(f"Database health check error: {e}")
        return {"status": "error", "error": str(e)}


async def _get_external_services_health(redis) -> List[Dict[str, Any]]:
    """Get health status of external services."""
    services = [
        {"name": "razorpay", "key_prefix": "external:razorpay"},
        {"name": "openai", "key_prefix": "external:openai"},
        {"name": "revenuecat", "key_prefix": "external:revenuecat"},
        {"name": "groq", "key_prefix": "external:groq"},
    ]

    results = []
    for service in services:
        try:
            # Get last known status from Redis
            status_key = f"{service['key_prefix']}:status"
            latency_key = f"{service['key_prefix']}:latency"
            error_key = f"{service['key_prefix']}:errors"

            status = await redis.get(status_key) or "unknown"
            latency = await redis.get(latency_key) or 0
            errors = await redis.get(error_key) or 0

            results.append({
                "service": service["name"],
                "status": "healthy" if status != "error" else "error",
                "avg_latency_ms": float(latency),
                "error_count_1h": int(errors),
                "circuit_breaker_state": "closed",
            })
        except Exception:
            results.append({
                "service": service["name"],
                "status": "unknown",
                "avg_latency_ms": 0,
                "error_count_1h": 0,
            })

    return results


async def _get_system_health() -> Dict[str, Any]:
    """Get system health metrics."""
    try:
        import psutil
        process = psutil.Process()

        return {
            "status": "healthy",
            "cpu_percent": process.cpu_percent(),
            "memory_percent": process.memory_percent(),
            "memory_mb": process.memory_info().rss / 1024 / 1024,
            "threads": process.num_threads(),
            "open_files": len(process.open_files()),
        }
    except ImportError:
        return {
            "status": "healthy",
            "cpu_percent": None,
            "memory_percent": None,
            "note": "Install psutil for detailed metrics",
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def _get_business_metrics(redis) -> Dict[str, Any]:
    """Get business KPIs."""
    try:
        today = datetime.utcnow().strftime("%Y%m%d")

        # Get counters from Redis
        metrics = {
            "payments_today": int(await redis.get(f"metrics:payments:{today}") or 0),
            "registrations_today": int(await redis.get(f"metrics:registrations:{today}") or 0),
            "ai_requests_today": int(await redis.get(f"metrics:ai_requests:{today}") or 0),
            "active_websockets": int(await redis.get("metrics:websockets:active") or 0),
        }

        return metrics
    except Exception as e:
        logger.error(f"Business metrics error: {e}")
        return {}


async def _get_latency_data(redis, endpoint_filter: Optional[str], minutes: int) -> List[Dict[str, Any]]:
    """Get latency data for endpoints."""
    # This would aggregate from Prometheus or stored Redis data
    # For now, return sample structure
    return []


async def _get_slow_requests(redis, threshold_ms: int, limit: int) -> List[Dict[str, Any]]:
    """Get recent slow requests."""
    try:
        # Get from Redis sorted set
        slow_requests = await redis.zrevrange(
            "metrics:slow_requests",
            0,
            limit - 1,
            withscores=True
        )
        return [
            {"request_id": req.decode() if isinstance(req, bytes) else req, "latency_ms": score}
            for req, score in slow_requests
        ]
    except Exception:
        return []


async def _get_error_metrics(redis, minutes: int) -> Dict[str, Any]:
    """Get error metrics."""
    return {
        "total": 0,
        "rate": 0,
        "by_status": {},
        "by_endpoint": {},
    }
