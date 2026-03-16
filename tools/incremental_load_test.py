#!/usr/bin/env python3
"""
Incremental Load Testing Script for Fittbot API
- Tests endpoints one at a time with real database data
- Saves results incrementally (can resume if stopped)
- Shows real-time progress
- Generates comprehensive report with p50, p95, p99 latencies
"""

import asyncio
import aiohttp
import json
import time
import statistics
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import pymysql
from jose import jwt

# Configuration
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
RESULTS_FILE = Path(__file__).parent / "load_test_results.json"
REPORT_FILE = Path(__file__).parent / "load_test_report.md"
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "10"))
REQUESTS_PER_ENDPOINT = int(os.getenv("REQUESTS_PER_ENDPOINT", "50"))
TIMEOUT_SECONDS = 30

# JWT Configuration (from .env)
SECRET_KEY = os.getenv("SECRET_KEY", "local-secret-key")
ALGORITHM = "HS256"


@dataclass
class EndpointResult:
    endpoint: str
    method: str
    total_requests: int
    successful_requests: int
    failed_requests: int
    error_rate: float
    latencies_ms: List[float]
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    avg_ms: float
    requests_per_second: float
    errors: List[str]
    status_codes: Dict[int, int]
    tested_at: str


@dataclass
class TestProgress:
    total_endpoints: int
    completed_endpoints: int
    current_endpoint: str
    status: str  # 'running', 'completed', 'stopped'


class DatabaseHelper:
    """Helper to fetch real test data from the database"""

    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=os.getenv("DB_HOST", "localhost"),
                user=os.getenv("DB_USERNAME", "root"),
                password=os.getenv("DB_PASSWORD", ""),
                database=os.getenv("DB_NAME", "fittbot_local"),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("[DB] Connected to database successfully")
            return True
        except Exception as e:
            print(f"[DB] Failed to connect: {e}")
            return False

    def close(self):
        if self.connection:
            self.connection.close()

    def get_test_client_id(self) -> Optional[int]:
        """Get a real client ID for testing"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT id FROM clients LIMIT 1")
                result = cursor.fetchone()
                return result['id'] if result else 1
        except:
            return 1

    def get_test_owner_id(self) -> Optional[int]:
        """Get a real owner ID for testing"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT id FROM owners LIMIT 1")
                result = cursor.fetchone()
                return result['id'] if result else 1
        except:
            return 1

    def get_test_gym_id(self) -> Optional[int]:
        """Get a real gym ID for testing"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT id FROM gyms LIMIT 1")
                result = cursor.fetchone()
                return result['id'] if result else 1
        except:
            return 1

    def get_test_data(self) -> Dict[str, Any]:
        """Get all test data needed for API calls"""
        return {
            "client_id": self.get_test_client_id(),
            "owner_id": self.get_test_owner_id(),
            "gym_id": self.get_test_gym_id(),
            "user_id": self.get_test_client_id(),  # alias
        }


class TokenGenerator:
    """Generate valid JWT tokens for testing"""

    @staticmethod
    def create_client_token(client_id: int) -> str:
        """Create a valid client JWT token"""
        payload = {
            "sub": str(client_id),
            "role": "client",
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    @staticmethod
    def create_owner_token(owner_id: int) -> str:
        """Create a valid owner JWT token"""
        payload = {
            "sub": str(owner_id),
            "role": "owner",
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    @staticmethod
    def create_admin_token() -> str:
        """Create a valid admin JWT token"""
        payload = {
            "sub": "admin_1",
            "role": "admin",
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


class EndpointDiscovery:
    """Discover all API endpoints from OpenAPI spec"""

    @staticmethod
    async def get_endpoints_from_openapi(base_url: str) -> List[Dict]:
        """Fetch endpoints from OpenAPI spec"""
        endpoints = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base_url}/openapi.json", timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        spec = await resp.json()
                        paths = spec.get("paths", {})
                        for path, methods in paths.items():
                            for method, details in methods.items():
                                if method.upper() in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
                                    endpoints.append({
                                        "path": path,
                                        "method": method.upper(),
                                        "summary": details.get("summary", ""),
                                        "tags": details.get("tags", []),
                                        "parameters": details.get("parameters", []),
                                        "requestBody": details.get("requestBody", None)
                                    })
                        print(f"[DISCOVERY] Found {len(endpoints)} endpoints from OpenAPI spec")
        except Exception as e:
            print(f"[DISCOVERY] Failed to fetch OpenAPI spec: {e}")
        return endpoints

    @staticmethod
    def get_fallback_endpoints() -> List[Dict]:
        """Fallback list of common endpoints to test"""
        return [
            {"path": "/health", "method": "GET"},
            {"path": "/health/ready", "method": "GET"},
            {"path": "/", "method": "GET"},
            {"path": "/api/v1/client/home", "method": "GET"},
            {"path": "/api/v1/client/profile", "method": "GET"},
            {"path": "/api/v1/owner/home", "method": "GET"},
        ]


class LoadTester:
    """Main load testing class with incremental results"""

    def __init__(self, base_url: str, db: DatabaseHelper, test_data: Dict):
        self.base_url = base_url
        self.db = db
        self.test_data = test_data
        self.results: Dict[str, EndpointResult] = {}
        self.progress = TestProgress(0, 0, "", "running")
        self.client_token = TokenGenerator.create_client_token(test_data.get("client_id", 1))
        self.owner_token = TokenGenerator.create_owner_token(test_data.get("owner_id", 1))
        self.admin_token = TokenGenerator.create_admin_token()

        # Load existing results if resuming
        self._load_existing_results()

    def _load_existing_results(self):
        """Load existing results to support resume functionality"""
        if RESULTS_FILE.exists():
            try:
                with open(RESULTS_FILE, 'r') as f:
                    data = json.load(f)
                    # Convert back to EndpointResult objects
                    for key, value in data.get("results", {}).items():
                        self.results[key] = EndpointResult(**value)
                    print(f"[RESUME] Loaded {len(self.results)} existing results")
            except Exception as e:
                print(f"[RESUME] Could not load existing results: {e}")

    def _save_results(self):
        """Save results incrementally to file"""
        try:
            data = {
                "results": {k: asdict(v) for k, v in self.results.items()},
                "progress": asdict(self.progress),
                "last_updated": datetime.now().isoformat()
            }
            with open(RESULTS_FILE, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            print(f"[SAVE] Error saving results: {e}")

    def _get_token_for_endpoint(self, path: str) -> str:
        """Determine which token to use based on endpoint path"""
        if "/admin/" in path or path.startswith("/admin"):
            return self.admin_token
        elif "/owner/" in path or path.startswith("/owner"):
            return self.owner_token
        return self.client_token

    def _substitute_path_params(self, path: str) -> str:
        """Replace path parameters with real values"""
        replacements = {
            "{client_id}": str(self.test_data.get("client_id", 1)),
            "{owner_id}": str(self.test_data.get("owner_id", 1)),
            "{gym_id}": str(self.test_data.get("gym_id", 1)),
            "{user_id}": str(self.test_data.get("user_id", 1)),
            "{id}": "1",
        }
        for key, value in replacements.items():
            path = path.replace(key, value)
        return path

    async def _make_request(self, session: aiohttp.ClientSession, method: str, path: str) -> tuple:
        """Make a single request and return (latency_ms, status_code, error)"""
        url = f"{self.base_url}{path}"
        token = self._get_token_for_endpoint(path)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        start_time = time.perf_counter()
        try:
            async with session.request(
                method,
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            ) as resp:
                latency_ms = (time.perf_counter() - start_time) * 1000
                await resp.read()  # Consume response
                return latency_ms, resp.status, None
        except asyncio.TimeoutError:
            latency_ms = (time.perf_counter() - start_time) * 1000
            return latency_ms, 0, "Timeout"
        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000
            return latency_ms, 0, str(e)[:100]

    async def test_endpoint(self, endpoint: Dict) -> EndpointResult:
        """Test a single endpoint with multiple concurrent requests"""
        path = self._substitute_path_params(endpoint["path"])
        method = endpoint["method"]
        endpoint_key = f"{method}:{path}"

        # Skip if already tested (resume support)
        if endpoint_key in self.results:
            print(f"  [SKIP] Already tested: {endpoint_key}")
            return self.results[endpoint_key]

        print(f"\n  [TEST] {method} {path}")
        print(f"         Requests: {REQUESTS_PER_ENDPOINT} | Concurrency: {CONCURRENT_REQUESTS}")

        latencies = []
        errors = []
        status_codes = {}

        connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Run requests in batches
            start_time = time.perf_counter()

            for batch_start in range(0, REQUESTS_PER_ENDPOINT, CONCURRENT_REQUESTS):
                batch_size = min(CONCURRENT_REQUESTS, REQUESTS_PER_ENDPOINT - batch_start)
                tasks = [
                    self._make_request(session, method, path)
                    for _ in range(batch_size)
                ]
                results = await asyncio.gather(*tasks)

                for latency_ms, status, error in results:
                    latencies.append(latency_ms)
                    status_codes[status] = status_codes.get(status, 0) + 1
                    if error:
                        errors.append(error)

                # Progress indicator
                completed = batch_start + batch_size
                pct = (completed / REQUESTS_PER_ENDPOINT) * 100
                print(f"         Progress: {completed}/{REQUESTS_PER_ENDPOINT} ({pct:.0f}%)", end='\r')

            total_time = time.perf_counter() - start_time

        # Calculate statistics
        sorted_latencies = sorted(latencies)
        successful = sum(1 for s in status_codes.keys() if 200 <= s < 400)
        failed = len(latencies) - successful

        result = EndpointResult(
            endpoint=endpoint_key,
            method=method,
            total_requests=len(latencies),
            successful_requests=status_codes.get(200, 0) + status_codes.get(201, 0) + status_codes.get(204, 0),
            failed_requests=failed,
            error_rate=round((failed / len(latencies)) * 100, 2) if latencies else 0,
            latencies_ms=sorted_latencies,
            p50_ms=round(self._percentile(sorted_latencies, 50), 2),
            p95_ms=round(self._percentile(sorted_latencies, 95), 2),
            p99_ms=round(self._percentile(sorted_latencies, 99), 2),
            min_ms=round(min(latencies), 2) if latencies else 0,
            max_ms=round(max(latencies), 2) if latencies else 0,
            avg_ms=round(statistics.mean(latencies), 2) if latencies else 0,
            requests_per_second=round(len(latencies) / total_time, 2) if total_time > 0 else 0,
            errors=list(set(errors))[:5],  # Keep unique errors, max 5
            status_codes=status_codes,
            tested_at=datetime.now().isoformat()
        )

        # Print summary
        print(f"\n         p50: {result.p50_ms}ms | p95: {result.p95_ms}ms | p99: {result.p99_ms}ms")
        print(f"         RPS: {result.requests_per_second} | Errors: {result.error_rate}%")

        # Save immediately
        self.results[endpoint_key] = result
        self._save_results()

        return result

    @staticmethod
    def _percentile(data: List[float], percentile: int) -> float:
        """Calculate percentile value"""
        if not data:
            return 0.0
        k = (len(data) - 1) * percentile / 100
        f = int(k)
        c = f + 1 if f + 1 < len(data) else f
        return data[f] + (k - f) * (data[c] - data[f]) if c != f else data[f]

    async def run_all_tests(self, endpoints: List[Dict]):
        """Run tests for all endpoints"""
        self.progress.total_endpoints = len(endpoints)
        self.progress.status = "running"

        print(f"\n{'='*60}")
        print(f"LOAD TESTING {len(endpoints)} ENDPOINTS")
        print(f"Base URL: {self.base_url}")
        print(f"Requests per endpoint: {REQUESTS_PER_ENDPOINT}")
        print(f"Concurrent requests: {CONCURRENT_REQUESTS}")
        print(f"{'='*60}")

        for i, endpoint in enumerate(endpoints, 1):
            self.progress.completed_endpoints = i
            self.progress.current_endpoint = f"{endpoint['method']}:{endpoint['path']}"

            print(f"\n[{i}/{len(endpoints)}] Testing endpoint...")

            try:
                await self.test_endpoint(endpoint)
            except Exception as e:
                print(f"  [ERROR] Failed to test endpoint: {e}")

            # Small delay between endpoints to avoid overwhelming the server
            await asyncio.sleep(0.5)

        self.progress.status = "completed"
        self._save_results()
        print(f"\n{'='*60}")
        print("LOAD TESTING COMPLETED")
        print(f"{'='*60}")

    def generate_report(self) -> str:
        """Generate a comprehensive markdown report"""
        if not self.results:
            return "No results to report."

        # Sort by p99 latency (slowest first)
        sorted_results = sorted(
            self.results.values(),
            key=lambda x: x.p99_ms,
            reverse=True
        )

        # Calculate overall statistics
        all_p50 = [r.p50_ms for r in sorted_results]
        all_p95 = [r.p95_ms for r in sorted_results]
        all_p99 = [r.p99_ms for r in sorted_results]
        all_rps = [r.requests_per_second for r in sorted_results]
        all_errors = [r.error_rate for r in sorted_results]

        report = f"""# Load Test Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Configuration
- **Base URL:** {self.base_url}
- **Requests per endpoint:** {REQUESTS_PER_ENDPOINT}
- **Concurrent requests:** {CONCURRENT_REQUESTS}
- **Total endpoints tested:** {len(sorted_results)}

## Overall Summary

| Metric | Value |
|--------|-------|
| Total Endpoints | {len(sorted_results)} |
| Avg p50 Latency | {statistics.mean(all_p50):.2f} ms |
| Avg p95 Latency | {statistics.mean(all_p95):.2f} ms |
| Avg p99 Latency | {statistics.mean(all_p99):.2f} ms |
| Avg RPS | {statistics.mean(all_rps):.2f} |
| Avg Error Rate | {statistics.mean(all_errors):.2f}% |

## Slowest Endpoints (by p99 latency)

| Rank | Endpoint | Method | p50 (ms) | p95 (ms) | p99 (ms) | RPS | Error % |
|------|----------|--------|----------|----------|----------|-----|---------|
"""
        # Top 20 slowest
        for i, r in enumerate(sorted_results[:20], 1):
            endpoint_short = r.endpoint[:50] + "..." if len(r.endpoint) > 50 else r.endpoint
            report += f"| {i} | {endpoint_short} | {r.method} | {r.p50_ms} | {r.p95_ms} | {r.p99_ms} | {r.requests_per_second} | {r.error_rate} |\n"

        # Fastest endpoints
        fastest = sorted(sorted_results, key=lambda x: x.p50_ms)[:10]
        report += f"""

## Fastest Endpoints (by p50 latency)

| Rank | Endpoint | Method | p50 (ms) | p95 (ms) | p99 (ms) | RPS |
|------|----------|--------|----------|----------|----------|-----|
"""
        for i, r in enumerate(fastest, 1):
            endpoint_short = r.endpoint[:50] + "..." if len(r.endpoint) > 50 else r.endpoint
            report += f"| {i} | {endpoint_short} | {r.method} | {r.p50_ms} | {r.p95_ms} | {r.p99_ms} | {r.requests_per_second} |\n"

        # High error rate endpoints
        high_error = [r for r in sorted_results if r.error_rate > 0]
        if high_error:
            high_error = sorted(high_error, key=lambda x: x.error_rate, reverse=True)[:10]
            report += f"""

## Endpoints with Errors

| Endpoint | Method | Error Rate | Status Codes | Errors |
|----------|--------|------------|--------------|--------|
"""
            for r in high_error:
                endpoint_short = r.endpoint[:40] + "..." if len(r.endpoint) > 40 else r.endpoint
                status_str = ", ".join([f"{k}:{v}" for k, v in r.status_codes.items()])
                errors_str = ", ".join(r.errors[:2]) if r.errors else "None"
                report += f"| {endpoint_short} | {r.method} | {r.error_rate}% | {status_str} | {errors_str[:30]} |\n"

        # Performance buckets
        report += f"""

## Performance Distribution

| Latency Bucket (p99) | Count | Percentage |
|----------------------|-------|------------|
"""
        buckets = {
            "< 50ms (Excellent)": 0,
            "50-100ms (Good)": 0,
            "100-300ms (Acceptable)": 0,
            "300-1000ms (Slow)": 0,
            "> 1000ms (Very Slow)": 0,
        }
        for r in sorted_results:
            if r.p99_ms < 50:
                buckets["< 50ms (Excellent)"] += 1
            elif r.p99_ms < 100:
                buckets["50-100ms (Good)"] += 1
            elif r.p99_ms < 300:
                buckets["100-300ms (Acceptable)"] += 1
            elif r.p99_ms < 1000:
                buckets["300-1000ms (Slow)"] += 1
            else:
                buckets["> 1000ms (Very Slow)"] += 1

        for bucket, count in buckets.items():
            pct = (count / len(sorted_results)) * 100 if sorted_results else 0
            report += f"| {bucket} | {count} | {pct:.1f}% |\n"

        # Full results table
        report += f"""

## All Endpoints (sorted by p99 latency)

| Endpoint | Method | p50 | p95 | p99 | Min | Max | Avg | RPS | Err% |
|----------|--------|-----|-----|-----|-----|-----|-----|-----|------|
"""
        for r in sorted_results:
            endpoint_short = r.endpoint.split(":")[-1][:35]
            report += f"| {endpoint_short} | {r.method} | {r.p50_ms} | {r.p95_ms} | {r.p99_ms} | {r.min_ms} | {r.max_ms} | {r.avg_ms} | {r.requests_per_second} | {r.error_rate} |\n"

        report += f"""

---
*Report generated by Fittbot Load Tester*
"""

        # Save report
        with open(REPORT_FILE, 'w') as f:
            f.write(report)
        print(f"\n[REPORT] Saved to: {REPORT_FILE}")

        return report


async def main():
    """Main entry point"""
    print("\n" + "="*60)
    print("FITTBOT API LOAD TESTER")
    print("="*60)

    # Initialize database
    db = DatabaseHelper()
    if not db.connect():
        print("[ERROR] Cannot connect to database. Using fallback test data.")
        test_data = {"client_id": 1, "owner_id": 1, "gym_id": 1, "user_id": 1}
    else:
        test_data = db.get_test_data()
        print(f"[DB] Test data: {test_data}")

    # Discover endpoints
    print("\n[DISCOVERY] Fetching API endpoints...")
    endpoints = await EndpointDiscovery.get_endpoints_from_openapi(BASE_URL)

    if not endpoints:
        print("[DISCOVERY] Using fallback endpoints")
        endpoints = EndpointDiscovery.get_fallback_endpoints()

    # Filter to only GET endpoints for initial safety (can be changed)
    # Comment out the next line to test all methods
    # endpoints = [e for e in endpoints if e["method"] == "GET"]

    print(f"[DISCOVERY] Will test {len(endpoints)} endpoints")

    # Initialize tester
    tester = LoadTester(BASE_URL, db, test_data)

    # Run tests
    try:
        await tester.run_all_tests(endpoints)
    except KeyboardInterrupt:
        print("\n\n[STOPPED] Test interrupted by user")
        tester.progress.status = "stopped"
        tester._save_results()
    finally:
        db.close()

    # Generate report
    print("\n[REPORT] Generating report...")
    report = tester.generate_report()

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Results saved to: {RESULTS_FILE}")
    print(f"Report saved to: {REPORT_FILE}")
    print(f"Endpoints tested: {len(tester.results)}")

    if tester.results:
        all_p99 = [r.p99_ms for r in tester.results.values()]
        print(f"Average p99 latency: {statistics.mean(all_p99):.2f}ms")
        print(f"Max p99 latency: {max(all_p99):.2f}ms")


if __name__ == "__main__":
    asyncio.run(main())
