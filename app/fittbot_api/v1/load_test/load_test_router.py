"""
Load Testing API Routes

Production Access:
    Add header: X-Load-Test-Key: <your-secret-key>
    Set env var: LOAD_TEST_SECRET_KEY=<your-secret-key>

Usage in Production:
    curl -X POST https://api.fittbot.com/api/v1/load-test/burst \
        -H "X-Load-Test-Key: your-secret-key" \
        -H "Content-Type: application/json" \
        -d '{"ai_tasks": 50}'
"""

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional
import os

from app.tasks.load_test_tasks import (
    simulate_ai_task,
    simulate_payment_task,
    simulate_heavy_ai_task,
    simulate_light_ai_task,
)
from app.utils.redis_config import get_redis

router = APIRouter(prefix="/load-test", tags=["Load Testing"])

# Secret key for production access (set in AWS Secrets Manager)
LOAD_TEST_SECRET_KEY = os.getenv("LOAD_TEST_SECRET_KEY", "fittbot-load-test-secret-2024")


def _check_environment(x_load_test_key: Optional[str] = None):
    """
    Check if load testing is allowed.

    Rules:
    - Local/Staging: Always allowed
    - Production: Only with valid X-Load-Test-Key header
    """
    env = os.getenv("ENVIRONMENT", "local").lower()

    if env == "production":
        if x_load_test_key and x_load_test_key == LOAD_TEST_SECRET_KEY:
            return  # Authorized
        raise HTTPException(
            status_code=403,
            detail="Load testing requires X-Load-Test-Key header in production"
        )


class SingleTaskRequest(BaseModel):
    task_id: Optional[int] = Field(default=None, description="Task identifier")
    duration: float = Field(default=2.0, ge=0.1, le=30.0, description="Task duration in seconds")


class BurstRequest(BaseModel):
    ai_tasks: int = Field(default=50, ge=1, le=500, description="Number of AI tasks")
    payment_tasks: int = Field(default=20, ge=0, le=200, description="Number of payment tasks")
    task_duration: float = Field(default=2.0, ge=0.5, le=10.0, description="Duration per task")


class LoadTestResponse(BaseModel):
    status: str
    tasks_queued: int
    queue: str
    message: str


@router.post("/ai", response_model=LoadTestResponse)
async def queue_ai_task(
    request: SingleTaskRequest,
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Queue a single mock AI task.

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    task_id = request.task_id or 1
    result = simulate_ai_task.delay(task_id=task_id, duration=request.duration)

    return LoadTestResponse(
        status="queued",
        tasks_queued=1,
        queue="ai",
        message=f"Task {result.id} queued to AI queue"
    )


@router.post("/payment", response_model=LoadTestResponse)
async def queue_payment_task(
    request: SingleTaskRequest,
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Queue a single mock payment task.

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    task_id = request.task_id or 1
    result = simulate_payment_task.delay(task_id=task_id, duration=request.duration)

    return LoadTestResponse(
        status="queued",
        tasks_queued=1,
        queue="payments",
        message=f"Task {result.id} queued to payments queue"
    )


@router.post("/burst", response_model=LoadTestResponse)
async def queue_burst(
    request: BurstRequest,
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Queue a burst of tasks to test auto-scaling.

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    total_queued = 0

    for i in range(request.ai_tasks):
        simulate_ai_task.delay(task_id=i, duration=request.task_duration)
        total_queued += 1

    for i in range(request.payment_tasks):
        simulate_payment_task.delay(task_id=i, duration=request.task_duration)
        total_queued += 1

    return LoadTestResponse(
        status="queued",
        tasks_queued=total_queued,
        queue="ai+payments",
        message=f"Burst complete: {request.ai_tasks} AI + {request.payment_tasks} payment tasks queued"
    )


@router.post("/scaling-test")
async def trigger_scaling_test(
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Queue enough tasks to trigger auto-scaling (60 AI + 30 payment tasks).

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    ai_count = 60
    payment_count = 30

    for i in range(ai_count):
        simulate_ai_task.delay(task_id=i, duration=3.0, variance=0.5)

    for i in range(payment_count):
        simulate_payment_task.delay(task_id=i, duration=2.5, variance=0.5)

    return {
        "status": "scaling_test_started",
        "ai_tasks": ai_count,
        "payment_tasks": payment_count,
        "expected_behavior": {
            "ai_queue": "Should trigger scale-out when depth > 24",
            "payments_queue": "Should trigger scale-out when depth > 12"
        },
        "message": "Monitor /health/celery/queues and CloudWatch for scaling events"
    }


@router.get("/status")
async def get_load_test_status(
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Get current queue depths and scaling status.

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    redis = await get_redis()

    ai_depth = await redis.llen("ai") or 0
    payments_depth = await redis.llen("payments") or 0
    celery_depth = await redis.llen("celery") or 0

    return {
        "queues": {
            "ai": {
                "depth": ai_depth,
                "scale_out_threshold": 24,
                "would_scale": ai_depth > 24
            },
            "payments": {
                "depth": payments_depth,
                "scale_out_threshold": 12,
                "would_scale": payments_depth > 12
            },
            "celery": {
                "depth": celery_depth
            }
        },
        "total_queued": ai_depth + payments_depth + celery_depth,
        "recommendations": _get_recommendations(ai_depth, payments_depth)
    }


def _get_recommendations(ai_depth: int, payments_depth: int) -> list:
    recs = []

    if ai_depth == 0 and payments_depth == 0:
        recs.append("Queues are empty - send tasks using /load-test/burst")

    if ai_depth > 24:
        recs.append(f"AI queue at {ai_depth} - auto-scaling should trigger")
    elif ai_depth > 12:
        recs.append(f"AI queue at {ai_depth} - approaching scale-out threshold (24)")

    if payments_depth > 12:
        recs.append(f"Payments queue at {payments_depth} - auto-scaling should trigger")
    elif payments_depth > 6:
        recs.append(f"Payments queue at {payments_depth} - approaching scale-out threshold (12)")

    return recs


@router.delete("/clear")
async def clear_queues(
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access")
):
    """
    Clear all test tasks from queues.

    WARNING: This will delete ALL pending tasks!

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    redis = await get_redis()

    ai_count = await redis.llen("ai") or 0
    payments_count = await redis.llen("payments") or 0
    celery_count = await redis.llen("celery") or 0

    await redis.delete("ai")
    await redis.delete("payments")
    await redis.delete("celery")

    return {
        "status": "cleared",
        "tasks_removed": {
            "ai": ai_count,
            "payments": payments_count,
            "celery": celery_count,
            "total": ai_count + payments_count + celery_count
        },
        "warning": "All pending tasks have been removed"
    }


@router.get("/failed-tasks")
async def get_failed_tasks(
    x_load_test_key: Optional[str] = Header(None, description="Secret key for production access"),
    limit: int = 50
):
    """
    Get recently failed Celery tasks from Redis.

    **Production:** Requires `X-Load-Test-Key` header.
    """
    _check_environment(x_load_test_key)

    redis = await get_redis()

    # Celery stores failed task results with celery-task-meta- prefix
    failed_tasks = []

    # Scan for task results
    cursor = 0
    checked = 0
    while checked < 1000:  # Limit scan
        cursor, keys = await redis.scan(cursor, match="celery-task-meta-*", count=100)
        for key in keys:
            try:
                task_data = await redis.get(key)
                if task_data:
                    import json
                    data = json.loads(task_data)
                    if data.get("status") == "FAILURE":
                        failed_tasks.append({
                            "task_id": key.replace("celery-task-meta-", ""),
                            "status": data.get("status"),
                            "error": str(data.get("result", ""))[:200],
                            "traceback": str(data.get("traceback", ""))[:500] if data.get("traceback") else None,
                        })
                        if len(failed_tasks) >= limit:
                            break
            except Exception:
                pass
        checked += len(keys)
        if cursor == 0 or len(failed_tasks) >= limit:
            break

    # Also get queue stats
    ai_depth = await redis.llen("ai") or 0
    payments_depth = await redis.llen("payments") or 0

    return {
        "failed_count": len(failed_tasks),
        "failed_tasks": failed_tasks,
        "current_queues": {
            "ai": ai_depth,
            "payments": payments_depth
        },
        "note": "Failed tasks are stored for 1 hour (result_expires=3600)"
    }
