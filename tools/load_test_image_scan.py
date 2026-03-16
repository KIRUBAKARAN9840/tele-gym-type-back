# Quick load test for the image scanner endpoints.
# Fires requests at a target QPS and records post + completion latencies.
import argparse
import asyncio
import json
import statistics
import time
import sys
import contextlib
from pathlib import Path
from typing import Dict, List, Optional

import httpx


def percentile(data: List[float], pct: float) -> float:
    """Lightweight percentile calculator."""
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
    image_bytes: bytes,
    semaphore: asyncio.Semaphore,
    results: Dict[str, List[float]],
):
    """Send one request and optionally poll for completion."""
    results["launched"] += 1
    async with semaphore:
        start = time.perf_counter()
        files = {"files": ("image.jpg", image_bytes, "image/jpeg")}
        data: Dict[str, str] = {}
        if args.client_id is not None:
            data["client_id"] = str(args.client_id)
        if args.food_scan is not None:
            data["food_scan"] = "true" if args.food_scan else "false"

        try:
            resp = await client.post(
                args.base_url.rstrip("/") + args.path,
                files=files,
                data=data,
                timeout=args.request_timeout,
            )
        except Exception as exc:
            if args.debug and len(results["errors"]) < args.debug_max_errors:
                print(f"[error] POST exception: {exc}", file=sys.stderr, flush=True)
            results["post_failures"].append(time.perf_counter() - start)
            return

        post_latency = time.perf_counter() - start

        # Collect post latency regardless of status; track failures separately.
        results["post_latencies"].append(post_latency)
        if resp.status_code >= 400:
            if args.debug and len(results["errors"]) < args.debug_max_errors:
                body = ""
                try:
                    body = resp.text[:200]
                except Exception:
                    body = "<unreadable>"
                results["errors"].append(f"HTTP {resp.status_code}: {body}")
                print(f"[error] POST status {resp.status_code}: {body}", file=sys.stderr, flush=True)
            results["post_failures"].append(post_latency)
            return

    # Sync endpoint: we're done.
    if not args.async_mode:
        results["complete_latencies"].append(post_latency)
        results["completed"] += 1
        return

    # Async endpoint: poll until completion or timeout.
    try:
        payload = resp.json()
    except Exception:
        results["post_failures"].append(post_latency)
        return

    job_id = payload.get("job_id")
    poll_url = payload.get("poll_url")
    if not job_id or not poll_url:
        results["post_failures"].append(post_latency)
        return
    # If backend returns a bare poll URL without the /food_scanner prefix, add it.
    if args.poll_prefix and poll_url.startswith("/analyze/status"):
        poll_url = args.poll_prefix.rstrip("/") + poll_url
        if args.debug and len(results["errors"]) < args.debug_max_errors:
            print(f"[debug] adjusted poll_url to {poll_url}", file=sys.stderr, flush=True)

    poll_endpoint = poll_url if poll_url.startswith("/") else f"/{poll_url}"
    # 0 means no deadline
    deadline = time.perf_counter() + args.poll_timeout if args.poll_timeout > 0 else None
    while deadline is None or time.perf_counter() < deadline:
        poll_resp = await client.get(
            args.base_url.rstrip("/") + poll_endpoint,
            timeout=args.request_timeout,
        )
        if poll_resp.status_code >= 400:
            await asyncio.sleep(args.poll_interval)
            continue

        try:
            poll_payload = poll_resp.json()
        except Exception:
            await asyncio.sleep(args.poll_interval)
            continue

        state = poll_payload.get("state")
        if state == "completed":
            results["complete_latencies"].append(time.perf_counter() - start)
            results["completed"] += 1
            return
        if state == "failed":
            results["complete_failures"].append(time.perf_counter() - start)
            return

        await asyncio.sleep(args.poll_interval)

    results["complete_failures"].append(time.perf_counter() - start)


async def run_load(args: argparse.Namespace):
    image_bytes = Path(args.image).read_bytes()
    headers = {}
    if args.token:
        headers["Authorization"] = f"{args.auth_scheme} {args.token}"

    limits = httpx.Limits(max_keepalive_connections=args.connections, max_connections=args.connections)
    timeout = httpx.Timeout(args.request_timeout, connect=args.connect_timeout)
    results: Dict[str, List[float]] = {
        "post_latencies": [],
        "post_failures": [],
        "complete_latencies": [],
        "complete_failures": [],
        "launched": 0,
        "completed": 0,
        "errors": [],
    }

    semaphore = asyncio.Semaphore(args.max_inflight)

    async with httpx.AsyncClient(headers=headers, limits=limits, timeout=timeout, http2=True) as client:
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
                    f"complete_failures={len(results['complete_failures'])} "
                    f"of ~{total}",
                    file=sys.stderr,
                    flush=True,
                )

        reporter_task = asyncio.create_task(progress_reporter()) if args.progress_interval > 0 else None

        while next_fire < end_time:
            tasks.append(
                asyncio.create_task(post_once(client, args, image_bytes, semaphore, results))
            )
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
    parser = argparse.ArgumentParser(description="Blast image scans and measure latency.")
    parser.add_argument("--base-url", required=True, help="API base URL (e.g., https://api.yourapp.com)")
    parser.add_argument("--path", default="/food_scanner/analyze_async", help="Endpoint path to hit")
    parser.add_argument("--token", help="Access token (Authorization header value without scheme)")
    parser.add_argument("--auth-scheme", default="Bearer", help="Auth scheme prefix (default: Bearer)")
    parser.add_argument("--image", required=True, help="Path to image file to upload")
    parser.add_argument("--qps", type=float, default=100.0, help="Requests per second to fire")
    parser.add_argument("--duration", type=float, default=10.0, help="How long to run the test in seconds")
    parser.add_argument("--max-inflight", type=int, default=500, help="Max in-flight requests to bound memory")
    parser.add_argument("--connections", type=int, default=200, help="HTTP connection pool size")
    parser.add_argument("--request-timeout", type=float, default=30.0, help="Per-request timeout (seconds)")
    parser.add_argument("--connect-timeout", type=float, default=5.0, help="Connect timeout (seconds)")
    parser.add_argument("--poll-timeout", type=float, default=0.0, help="Max time to wait for completion (seconds, 0 = no limit)")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="How often to poll async job status")
    parser.add_argument("--poll-prefix", default="", help="Prefix to prepend to poll_url when backend returns bare status paths")
    parser.add_argument("--client-id", type=int, help="Optional client_id form field")
    parser.add_argument("--food-scan", action="store_true", help="Set food_scan=true form field")
    parser.add_argument("--sync-mode", dest="async_mode", action="store_false", help="Use sync endpoint (no polling)")
    parser.add_argument("--progress-interval", type=float, default=5.0, help="Seconds between progress prints (0=off)")
    parser.add_argument("--debug", action="store_true", help="Print sample errors/status codes")
    parser.add_argument("--debug-max-errors", type=int, default=5, help="Max error samples to print when --debug")
    return parser.parse_args()


def main():
    args = parse_args()
    results = asyncio.run(run_load(args))

    print("\n=== Image Scan Load Test ===")
    print(f"Base URL       : {args.base_url.rstrip('/')}")
    print(f"Path           : {args.path}")
    print(f"Async mode     : {args.async_mode}")
    print(f"QPS x Duration : {args.qps} x {args.duration}s (~{int(args.qps * args.duration)} requests)")
    if args.token:
        print(f"Auth scheme    : {args.auth_scheme}")

    print(summarize("POST latency", results["post_latencies"]))
    print(summarize("POST failures", results["post_failures"]))
    print(summarize("Completion latency", results["complete_latencies"]))
    print(summarize("Completion failures", results["complete_failures"]))


if __name__ == "__main__":
    main()
