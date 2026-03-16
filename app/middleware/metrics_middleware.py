"""
Enhanced Metrics Middleware for Fittbot API
Provides detailed request/response metrics with minimal overhead.
"""

import time
import logging
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.utils.metrics import (
    HTTP_REQUEST_LATENCY,
    HTTP_REQUEST_TOTAL,
    HTTP_REQUESTS_IN_PROGRESS,
    HTTP_REQUEST_SIZE,
    HTTP_RESPONSE_SIZE,
    SLOW_REQUESTS,
    normalize_endpoint,
    get_user_type,
)


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive metrics middleware that tracks:
    - Request latency with detailed buckets
    - Request/response sizes
    - In-progress requests
    - Slow request detection
    - User type segmentation
    """

    def __init__(
        self,
        app: ASGIApp,
        slow_request_threshold_ms: int = 1000,
        exclude_paths: set = None,
    ) -> None:
        super().__init__(app)
        self.slow_request_threshold_ms = slow_request_threshold_ms
        self.exclude_paths = exclude_paths or {"/health", "/health/ready", "/metrics", "/"}
        self.logger = logging.getLogger("app.metrics")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip metrics for excluded paths
        path = request.url.path
        if path in self.exclude_paths:
            return await call_next(request)

        # Normalize endpoint to prevent cardinality explosion
        endpoint = normalize_endpoint(path)
        method = request.method

        # Track in-progress requests
        HTTP_REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).inc()

        # Track request size
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                HTTP_REQUEST_SIZE.labels(method=method, endpoint=endpoint).observe(int(content_length))
            except ValueError:
                pass

        start_time = time.perf_counter()
        response = None
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as e:
            status_code = 500
            raise
        finally:
            # Calculate duration
            duration = time.perf_counter() - start_time
            duration_ms = int(duration * 1000)

            # Decrement in-progress
            HTTP_REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()

            # Determine user type
            user_type = get_user_type(request)

            # Record latency
            HTTP_REQUEST_LATENCY.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code)
            ).observe(duration)

            # Record request count
            HTTP_REQUEST_TOTAL.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code),
                user_type=user_type
            ).inc()

            # Track response size if available
            if response is not None:
                response_content_length = response.headers.get("content-length")
                if response_content_length:
                    try:
                        HTTP_RESPONSE_SIZE.labels(
                            method=method,
                            endpoint=endpoint,
                            status_code=str(status_code)
                        ).observe(int(response_content_length))
                    except ValueError:
                        pass

            # Track slow requests
            if duration_ms >= self.slow_request_threshold_ms:
                SLOW_REQUESTS.labels(
                    method=method,
                    endpoint=endpoint,
                    threshold_ms=str(self.slow_request_threshold_ms)
                ).inc()

                # Log slow request details
                self.logger.warning(
                    "Slow request detected",
                    extra={
                        "path": path,
                        "endpoint": endpoint,
                        "method": method,
                        "status_code": status_code,
                        "duration_ms": duration_ms,
                        "threshold_ms": self.slow_request_threshold_ms,
                        "user_type": user_type,
                    }
                )


class DatabaseMetricsMiddleware:
    """
    SQLAlchemy event listener for database query metrics.
    Attach to engine to automatically track all queries.
    """

    def __init__(self, engine):
        from sqlalchemy import event
        from app.utils.metrics import DB_QUERY_LATENCY, DB_QUERY_TOTAL, DB_SLOW_QUERIES

        self.engine = engine
        self.logger = logging.getLogger("app.db_metrics")

        # Before cursor execute
        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault("query_start_time", []).append(time.perf_counter())

        # After cursor execute
        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            start_time = conn.info["query_start_time"].pop(-1)
            duration = time.perf_counter() - start_time

            # Parse operation type from statement
            operation = self._get_operation(statement)
            table = self._get_table(statement)

            DB_QUERY_LATENCY.labels(
                query_type="sql",
                table=table,
                operation=operation
            ).observe(duration)

            DB_QUERY_TOTAL.labels(
                query_type="sql",
                table=table,
                operation=operation,
                success="true"
            ).inc()

            # Track slow queries (>100ms)
            if duration > 0.1:
                DB_SLOW_QUERIES.labels(table=table, operation=operation).inc()
                self.logger.warning(
                    "Slow database query",
                    extra={
                        "duration_ms": int(duration * 1000),
                        "operation": operation,
                        "table": table,
                        "statement_preview": statement[:200] if statement else "",
                    }
                )

        # Handle errors
        @event.listens_for(engine, "handle_error")
        def handle_error(exception_context):
            statement = exception_context.statement
            operation = self._get_operation(statement)
            table = self._get_table(statement)

            DB_QUERY_TOTAL.labels(
                query_type="sql",
                table=table,
                operation=operation,
                success="false"
            ).inc()

    def _get_operation(self, statement: str) -> str:
        """Extract operation type from SQL statement."""
        if not statement:
            return "unknown"
        statement = statement.strip().upper()
        if statement.startswith("SELECT"):
            return "select"
        elif statement.startswith("INSERT"):
            return "insert"
        elif statement.startswith("UPDATE"):
            return "update"
        elif statement.startswith("DELETE"):
            return "delete"
        elif statement.startswith("CREATE"):
            return "create"
        elif statement.startswith("ALTER"):
            return "alter"
        elif statement.startswith("DROP"):
            return "drop"
        return "other"

    def _get_table(self, statement: str) -> str:
        """Extract table name from SQL statement."""
        if not statement:
            return "unknown"

        import re
        statement = statement.strip()

        # Common patterns
        patterns = [
            r"FROM\s+[`\"]?(\w+)[`\"]?",  # SELECT ... FROM table
            r"INTO\s+[`\"]?(\w+)[`\"]?",  # INSERT INTO table
            r"UPDATE\s+[`\"]?(\w+)[`\"]?",  # UPDATE table
            r"DELETE\s+FROM\s+[`\"]?(\w+)[`\"]?",  # DELETE FROM table
            r"JOIN\s+[`\"]?(\w+)[`\"]?",  # JOIN table
        ]

        for pattern in patterns:
            match = re.search(pattern, statement, re.IGNORECASE)
            if match:
                return match.group(1).lower()

        return "unknown"
