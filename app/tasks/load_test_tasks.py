"""
Load Testing Tasks for Celery

These tasks simulate real workloads WITHOUT calling external APIs.
Use these to test auto-scaling behavior safely.

Usage:
    # From Python
    from app.tasks.load_test_tasks import simulate_ai_task, simulate_payment_task

    # Send 100 AI tasks
    for i in range(100):
        simulate_ai_task.delay(task_id=i, duration=2.0)

    # Send 50 payment tasks
    for i in range(50):
        simulate_payment_task.delay(task_id=i, duration=1.5)
"""

import time
import random
from celery import shared_task

from app.celery_app import celery_app


@celery_app.task(
    name="load_test.simulate_ai_task",
    bind=True,
    queue="ai",
)
def simulate_ai_task(self, task_id: int, duration: float = 2.0, variance: float = 0.5):

    # Add random variance to simulate real-world variability
    actual_duration = duration + random.uniform(-variance, variance)
    actual_duration = max(0.1, actual_duration)  # Minimum 100ms

    print(f"[LoadTest] AI Task {task_id} starting, will take {actual_duration:.2f}s")

    # Simulate I/O wait (like waiting for API response)
    time.sleep(actual_duration)

    # Simulate some CPU work (like parsing JSON response)
    result = sum(i * i for i in range(10000))

    print(f"[LoadTest] AI Task {task_id} completed")

    return {
        "task_id": task_id,
        "duration": actual_duration,
        "status": "success",
        "simulated_tokens": random.randint(100, 500),
    }


@celery_app.task(
    name="load_test.simulate_payment_task",
    bind=True,
    queue="payments",
)
def simulate_payment_task(self, task_id: int, duration: float = 1.5, variance: float = 0.3):
    """
    Simulates a payment task (like Razorpay checkout/verify) with configurable duration.

    Args:
        task_id: Unique identifier for logging
        duration: Base duration in seconds (default 1.5s like real payment calls)
        variance: Random variance +/- seconds

    Returns:
        dict: Simulated result
    """
    actual_duration = duration + random.uniform(-variance, variance)
    actual_duration = max(0.1, actual_duration)

    print(f"[LoadTest] Payment Task {task_id} starting, will take {actual_duration:.2f}s")

    # Simulate payment API call
    time.sleep(actual_duration)

    # Simulate DB write
    time.sleep(0.05)

    print(f"[LoadTest] Payment Task {task_id} completed")

    return {
        "task_id": task_id,
        "duration": actual_duration,
        "status": "success",
        "payment_id": f"pay_test_{task_id}_{random.randint(1000, 9999)}",
    }


@celery_app.task(
    name="load_test.simulate_heavy_ai_task",
    bind=True,
    queue="ai",
)
def simulate_heavy_ai_task(self, task_id: int):
    """
    Simulates a heavy AI task like workout template generation (5-10s).
    """
    duration = random.uniform(5.0, 10.0)

    print(f"[LoadTest] Heavy AI Task {task_id} starting, will take {duration:.2f}s")
    time.sleep(duration)
    print(f"[LoadTest] Heavy AI Task {task_id} completed")

    return {"task_id": task_id, "duration": duration, "type": "heavy"}


@celery_app.task(
    name="load_test.simulate_light_ai_task",
    bind=True,
    queue="ai",
)
def simulate_light_ai_task(self, task_id: int):
    """
    Simulates a light AI task like intent classification (0.5-1.5s).
    """
    duration = random.uniform(0.5, 1.5)

    print(f"[LoadTest] Light AI Task {task_id} starting, will take {duration:.2f}s")
    time.sleep(duration)
    print(f"[LoadTest] Light AI Task {task_id} completed")

    return {"task_id": task_id, "duration": duration, "type": "light"}


# ─────────────────────────────────────────────────────────────
# Load Test Runner Functions
# ─────────────────────────────────────────────────────────────

def run_ai_load_test(num_tasks: int = 100, burst: bool = False):
    """
    Send multiple AI tasks for load testing.

    Args:
        num_tasks: Number of tasks to send
        burst: If True, send all at once. If False, spread over time.
    """
    print(f"[LoadTest] Sending {num_tasks} AI tasks (burst={burst})")

    for i in range(num_tasks):
        simulate_ai_task.delay(task_id=i)

        if not burst and i % 10 == 0:
            time.sleep(0.1)  # Small delay to spread load

    print(f"[LoadTest] All {num_tasks} AI tasks queued")


def run_payment_load_test(num_tasks: int = 50, burst: bool = False):
    """
    Send multiple payment tasks for load testing.
    """
    print(f"[LoadTest] Sending {num_tasks} payment tasks (burst={burst})")

    for i in range(num_tasks):
        simulate_payment_task.delay(task_id=i)

        if not burst and i % 10 == 0:
            time.sleep(0.1)

    print(f"[LoadTest] All {num_tasks} payment tasks queued")


def run_mixed_load_test(ai_tasks: int = 80, payment_tasks: int = 20):
    """
    Send a mix of AI and payment tasks (typical production ratio).
    """
    print(f"[LoadTest] Sending mixed load: {ai_tasks} AI + {payment_tasks} payment")

    # Mix the tasks
    total = ai_tasks + payment_tasks
    ai_sent = 0
    payment_sent = 0

    for i in range(total):
        # Maintain ratio throughout the test
        if ai_sent < ai_tasks and (payment_sent >= payment_tasks or random.random() < 0.8):
            simulate_ai_task.delay(task_id=ai_sent)
            ai_sent += 1
        else:
            simulate_payment_task.delay(task_id=payment_sent)
            payment_sent += 1

    print(f"[LoadTest] Mixed load complete: {ai_sent} AI + {payment_sent} payment queued")


def run_scaling_trigger_test():
    """
    Send enough tasks to trigger auto-scaling (> 24 for AI, > 12 for payments).
    """
    print("[LoadTest] Running scaling trigger test...")

    # Send 50 AI tasks (should trigger scale out at 24)
    for i in range(50):
        simulate_ai_task.delay(task_id=i, duration=3.0)  # Slower tasks to build up queue

    # Send 20 payment tasks (should trigger scale out at 12)
    for i in range(20):
        simulate_payment_task.delay(task_id=i, duration=2.0)

    print("[LoadTest] Scaling trigger test queued - monitor CloudWatch for scaling events")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.tasks.load_test_tasks <test_type> [num_tasks]")
        print("  test_type: ai, payment, mixed, scaling")
        print("  num_tasks: number of tasks (default varies by test)")
        sys.exit(1)

    test_type = sys.argv[1]
    num_tasks = int(sys.argv[2]) if len(sys.argv) > 2 else None

    if test_type == "ai":
        run_ai_load_test(num_tasks or 100)
    elif test_type == "payment":
        run_payment_load_test(num_tasks or 50)
    elif test_type == "mixed":
        run_mixed_load_test()
    elif test_type == "scaling":
        run_scaling_trigger_test()
    else:
        print(f"Unknown test type: {test_type}")
