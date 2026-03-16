#!/usr/bin/env python3
"""
Quick single endpoint load test
Tests: POST /client/new_registration/login
"""

import asyncio
import aiohttp
import time
import statistics
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"
ENDPOINT = "/client/new_registration/login"
METHOD = "POST"
BODY = {"mobile_number": "8667458723"}  # Test number from code

CONCURRENT_REQUESTS = 10
TOTAL_REQUESTS = 50
TIMEOUT_SECONDS = 30


async def make_request(session: aiohttp.ClientSession, request_num: int) -> tuple:
    """Make a single request, return (latency_ms, status_code, error)"""
    url = f"{BASE_URL}{ENDPOINT}"
    headers = {"Content-Type": "application/json"}

    start = time.perf_counter()
    try:
        async with session.post(
            url,
            json=BODY,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        ) as resp:
            latency_ms = (time.perf_counter() - start) * 1000
            body = await resp.text()
            return latency_ms, resp.status, None
    except asyncio.TimeoutError:
        return (time.perf_counter() - start) * 1000, 0, "Timeout"
    except Exception as e:
        return (time.perf_counter() - start) * 1000, 0, str(e)[:50]


def percentile(data: list, p: int) -> float:
    """Calculate percentile"""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f]) if c != f else sorted_data[f]


async def run_load_test():
    print("=" * 60)
    print("SINGLE ENDPOINT LOAD TEST")
    print("=" * 60)
    print(f"Endpoint: {METHOD} {ENDPOINT}")
    print(f"Concurrent: {CONCURRENT_REQUESTS} | Total: {TOTAL_REQUESTS}")
    print(f"Body: {BODY}")
    print("=" * 60)

    latencies = []
    status_codes = {}
    errors = []

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.perf_counter()

        # Run in batches
        for batch_start in range(0, TOTAL_REQUESTS, CONCURRENT_REQUESTS):
            batch_size = min(CONCURRENT_REQUESTS, TOTAL_REQUESTS - batch_start)
            tasks = [make_request(session, i) for i in range(batch_size)]
            results = await asyncio.gather(*tasks)

            for lat, status, err in results:
                latencies.append(lat)
                status_codes[status] = status_codes.get(status, 0) + 1
                if err:
                    errors.append(err)

            # Progress
            done = batch_start + batch_size
            print(f"Progress: {done}/{TOTAL_REQUESTS} ({done/TOTAL_REQUESTS*100:.0f}%)", end='\r')

        total_time = time.perf_counter() - start_time

    # Calculate stats
    sorted_lat = sorted(latencies)
    p50 = percentile(sorted_lat, 50)
    p95 = percentile(sorted_lat, 95)
    p99 = percentile(sorted_lat, 99)

    print("\n")
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nLatency Statistics:")
    print(f"  Min:  {min(latencies):.2f} ms")
    print(f"  Max:  {max(latencies):.2f} ms")
    print(f"  Avg:  {statistics.mean(latencies):.2f} ms")
    print(f"  p50:  {p50:.2f} ms")
    print(f"  p95:  {p95:.2f} ms")
    print(f"  p99:  {p99:.2f} ms")

    print(f"\nThroughput:")
    print(f"  Total Requests: {len(latencies)}")
    print(f"  Total Time: {total_time:.2f}s")
    print(f"  RPS: {len(latencies)/total_time:.2f}")

    print(f"\nStatus Codes:")
    for code, count in sorted(status_codes.items()):
        pct = count/len(latencies)*100
        print(f"  {code}: {count} ({pct:.1f}%)")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in set(errors):
            print(f"  - {err}")

    # Performance rating
    print(f"\nPerformance Rating:")
    if p99 < 50:
        print("  [EXCELLENT] p99 < 50ms")
    elif p99 < 100:
        print("  [GOOD] p99 < 100ms")
    elif p99 < 300:
        print("  [ACCEPTABLE] p99 < 300ms")
    elif p99 < 1000:
        print("  [SLOW] p99 < 1000ms")
    else:
        print("  [VERY SLOW] p99 > 1000ms")

    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_load_test())
