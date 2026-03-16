# Quick load test for Razorpay order creation (or any JSON POST).
# Sends requests at a target QPS and reports latency and error counts.
import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Any

import httpx


def percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    if f == c:
        return data[f]
    d0 = data[f] * (c - k)
    d1 = data[c] * (k - f)
    return d0 + d1


async def post_once(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    payload: Any,
    semaphore: asyncio.Semaphore,
    results: Dict[str, List[float]],
):
    results["launched"] += 1
    async with semaphore:
        start = time.perf_counter()
        headers = args.headers.copy()
        if args.idempotency:
            headers["X-Idempotency-Key"] = str(uuid.uuid4())

        try:
            resp = await client.post(
                args.base_url.rstrip("/") + args.path,
                json=payload,
                timeout=args.request_timeout,
                headers=headers,
            )
        except Exception as exc:
            if args.debug and len(results["errors"]) < args.debug_max_errors:
                print(f"[error] POST exception: {exc}", file=sys.stderr, flush=True)
            results["post_failures"].append(time.perf_counter() - start)
            return

        latency = time.perf_counter() - start
        results["post_latencies"].append(latency)

        if resp.status_code >= 400:
            if args.debug and len(results["errors"]) < args.debug_max_errors:
                body = ""
                try:
                    body = resp.text[:200]
                except Exception:
                    body = "<unreadable>"
                results["errors"].append(f"HTTP {resp.status_code}: {body}")
                print(f"[error] POST status {resp.status_code}: {body}", file=sys.stderr, flush=True)
            results["post_failures"].append(latency)
            return

        results["completed"] += 1


async def run_load(args: argparse.Namespace):
    # Prepare payload
    if args.body_file:
        payload = json.loads(Path(args.body_file).read_text())
    else:
        payload = json.loads(args.body)

    headers: Dict[str, str] = {}
    if args.token:
        headers["Authorization"] = f"{args.auth_scheme} {args.token}"
    if args.extra_header:
        for kv in args.extra_header:
            if ":" in kv:
                k, v = kv.split(":", 1)
                headers[k.strip()] = v.strip()

    limits = httpx.Limits(max_keepalive_connections=args.connections, max_connections=args.connections)
    timeout = httpx.Timeout(args.request_timeout, connect=args.connect_timeout)
    results: Dict[str, List[float]] = {
        "post_latencies": [],
        "post_failures": [],
        "errors": [],
        "launched": 0,
        "completed": 0,
    }

    semaphore = asyncio.Semaphore(args.max_inflight)

    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=True) as client:
        tasks = []
        loop = asyncio.get_event_loop()
        next_fire = loop.time()
        end_time = next_fire + args.duration
        interval = 1.0 / args.qps

        async def progress_reporter():
            while True:
                await asyncio.sleep(args.progress_interval)
                total = int(args.qps * args.duration)
                print(
                    f"[progress] launched={results['launched']} "
                    f"completed={results['completed']} "
                    f"post_failures={len(results['post_failures'])} "
                    f"of ~{total}",
                    file=sys.stderr,
                    flush=True,
                )

        reporter_task = asyncio.create_task(progress_reporter()) if args.progress_interval > 0 else None

        while next_fire < end_time:
            tasks.append(asyncio.create_task(post_once(client, args, payload, semaphore, results)))
            next_fire += interval
            sleep_for = max(0, next_fire - loop.time())
            if sleep_for:
                await asyncio.sleep(sleep_for)

        await asyncio.gather(*tasks, return_exceptions=True)

        if reporter_task:
            reporter_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reporter_task

    return results


def summarize(title: str, data: List[float]) -> str:
    if not data:
        return f"{title}: none"
    ordered = sorted(data)
    return (
        f"{title}: count={len(data)}, "
        f"p50={percentile(ordered, 50):.3f}s, "
        f"p90={percentile(ordered, 90):.3f}s, "
        f"p95={percentile(ordered, 95):.3f}s, "
        f"p99={percentile(ordered, 99):.3f}s, "
        f"max={max(ordered):.3f}s"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Blast Razorpay order creation and measure latency.")
    parser.add_argument("--base-url", required=True, help="API base URL (e.g., https://staging.fittbot.com)")
    parser.add_argument("--path", required=True, help="Endpoint path for order creation (e.g., /razorpay/orders)")
    parser.add_argument("--token", help="Access token for Authorization header")
    parser.add_argument("--auth-scheme", default="Bearer", help="Auth scheme prefix (default: Bearer)")
    parser.add_argument("--body", default="{}", help="JSON body as string")
    parser.add_argument("--body-file", help="Path to JSON body file (overrides --body)")
    parser.add_argument("--extra-header", action="append", help="Extra header k:v (can be repeated)")
    parser.add_argument("--idempotency", action="store_true", help="Add X-Idempotency-Key per request")
    parser.add_argument("--qps", type=float, default=20.0, help="Requests per second to fire")
    parser.add_argument("--duration", type=float, default=5.0, help="How long to run the test in seconds")
    parser.add_argument("--max-inflight", type=int, default=200, help="Max in-flight requests to bound memory")
    parser.add_argument("--connections", type=int, default=200, help="HTTP connection pool size")
    parser.add_argument("--request-timeout", type=float, default=30.0, help="Per-request timeout (seconds)")
    parser.add_argument("--connect-timeout", type=float, default=5.0, help="Connect timeout (seconds)")
    parser.add_argument("--progress-interval", type=float, default=5.0, help="Seconds between progress prints (0=off)")
    parser.add_argument("--debug", action="store_true", help="Print sample errors/status codes")
    parser.add_argument("--debug-max-errors", type=int, default=5, help="Max error samples to print when --debug")
    return parser.parse_args()


def main():
    args = parse_args()
    # Attach headers to args for convenience
    args.headers = {}
    if args.token:
        args.headers["Authorization"] = f"{args.auth_scheme} {args.token}"
    if args.extra_header:
        for kv in args.extra_header:
            if ":" in kv:
                k, v = kv.split(":", 1)
                args.headers[k.strip()] = v.strip()

    results = asyncio.run(run_load(args))

    print("\n=== Razorpay Order Load Test ===")
    print(f"Base URL       : {args.base_url.rstrip('/')}")
    print(f"Path           : {args.path}")
    print(f"QPS x Duration : {args.qps} x {args.duration}s (~{int(args.qps * args.duration)} requests)")
    if args.token:
        print(f"Auth scheme    : {args.auth_scheme}")
    if args.idempotency:
        print("Idempotency    : X-Idempotency-Key per request")

    print(summarize("POST latency", results["post_latencies"]))
    print(summarize("POST failures", results["post_failures"]))


if __name__ == "__main__":
    import contextlib  # Needed for reporter_task suppression
    main()
