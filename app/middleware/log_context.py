import logging
import time
import uuid
from typing import Optional, Tuple

from fastapi import Request
from jose import jwt
from starlette.middleware.base import BaseHTTPMiddleware

from app.middleware.rate_limit_middleware import get_real_client_ip
from app.utils.logging_config import (
    clear_log_context,
    set_log_context,
)
from app.utils.security import ALGORITHM, SECRET_KEY


class LogContextMiddleware(BaseHTTPMiddleware):
    """Adds request_id/user_id/role context to logs and emits slow-request warnings."""

    def __init__(self, app, slow_request_ms: int = 1000) -> None:
        super().__init__(app)
        self.logger = logging.getLogger("app.request")
        self.slow_request_ms = slow_request_ms

    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        request_id = self._get_request_id(request)
        trace_id = request_id
        client_ip = get_real_client_ip(request)
        user_agent = request.headers.get("user-agent", "")
        user_id, role = self._get_user_and_role(request)

        set_log_context(
            request_id=request_id,
            trace_id=trace_id,
            user_id=user_id or "",
            role=role or "",
            path=request.url.path,
            method=request.method,
            client_ip=client_ip,
            user_agent=user_agent,
        )

        response = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            latency_ms = int((time.perf_counter() - start) * 1000)
            set_log_context(status=status_code, latency_ms=latency_ms)

            if response is not None:
                response.headers["X-Request-ID"] = request_id
                response.headers["X-Trace-ID"] = trace_id

            if self._should_log_request(request.url.path):
                log_method = self.logger.warning if latency_ms >= self.slow_request_ms else self.logger.info
                log_method(
                    "request completed",
                    extra={
                        "request_id": request_id,
                        "user_id": user_id,
                        "role": role,
                        "path": request.url.path,
                        "method": request.method,
                        "status": status_code,
                        "latency_ms": latency_ms,
                        "client_ip": client_ip,
                    },
                )

            clear_log_context()

    def _get_request_id(self, request: Request) -> str:
        return (
            request.headers.get("x-request-id")
            or request.headers.get("x-correlation-id")
            or str(uuid.uuid4())
        )

    def _get_user_and_role(self, request: Request) -> Tuple[Optional[str], Optional[str]]:
        auth_header = request.headers.get("authorization")
        if not auth_header or not auth_header.lower().startswith("bearer "):
            return None, None
        token = auth_header.split(" ", 1)[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            uid = payload.get("sub")
            role = payload.get("role") or payload.get("roles")
            return (str(uid) if uid is not None else None, str(role) if role is not None else None)
        except Exception:
            return None, None

    def _should_log_request(self, path: str) -> bool:
        noisy_paths = {"/health", "/health/ready", "/metrics", "/"}
        return path not in noisy_paths
