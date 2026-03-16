from typing import Any, Dict, Optional
from datetime import datetime, timezone
from fastapi import HTTPException
from starlette.requests import Request
from .logging_setup import jlog
import time

# Optional enums—nice for consistency
class SecuritySeverity:
    LOW = "low"
    MEDIUM = "medium" 
    HIGH = "high"

class EventType:
    API = "api_access"
    BUSINESS = "business_event"
    SECURITY = "security_event"

class FittbotHTTPException(HTTPException):

    def __init__(
        self,
        status_code: int,
        detail: str,
        error_code: Optional[str] = None,
        log_level: str = "error",  # "warning" for expected/user errors
        log_data: Optional[Dict[str, Any]] = None,
        security_event: bool = False,
    ):
        super().__init__(status_code=status_code, detail=detail)
        ts = datetime.now(timezone.utc).isoformat()
        self.error_code = error_code or f"HTTP_{status_code}"

        # one concise JSON log line
        jlog(
            log_level,
            {
                "type": "error",
                "error_code": self.error_code,
                "detail": detail,
                "status_code": status_code,
                "security_event": security_event,
                "context": log_data or {},
                "timestamp": ts,
            },
        )
        self.timestamp = ts


class _AuthLogger:
    """
    Lightweight structured logger facade.
    Keep hot-path noise low: only log warn/error in prod by LOG_LEVEL.
    """
    def set_request_context(self, request_or_body: Any) -> str:
        # you can attach a request-id here if you use one
        return ""

    def _log(self, level: str, **payload):
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        jlog(level, payload)

    # Debug/Info (disabled in prod unless LOG_LEVEL lowered)
    def debug(self, msg: str, **kv): self._log("debug", type="debug", msg=msg, **kv)
    def info(self, msg: str, **kv):  self._log("info",  type="info",  msg=msg, **kv)

    # Warnings/Errors
    def warning(self, msg: str, **kv): self._log("warning", type="warn", msg=msg, **kv)
    def error(self, msg: str, **kv):   self._log("error",   type="error", msg=msg, **kv)

    # Domain helpers
    def security_event(self, name: str, severity: str = SecuritySeverity.MEDIUM, **kv):
        self._log("warning", type=EventType.SECURITY, event=name, severity=severity, **kv)

    def business_event(self, name: str, **kv):
        self._log("info", type=EventType.BUSINESS, event=name, **kv)

    def api_access(self, method: str, endpoint: str, response_time: float, **kv):
        self._log("info", type=EventType.API, method=method, endpoint=endpoint,
                  response_time_ms=int(response_time * 1000), **kv)

auth_logger = _AuthLogger()

def log_exceptions(func):
    # Optional decorator (not required if you already use try/except)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as e:
            raise FittbotHTTPException(
                status_code=500,
                detail="Unexpected server error",
                error_code="UNEXPECTED",
                log_data={"exc": repr(e), "fn": func.__name__},
            )
    return wrapper