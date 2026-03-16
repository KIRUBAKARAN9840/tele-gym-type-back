import contextvars
import json
import logging
import os
from datetime import datetime, timezone
import pytz
from logging.config import dictConfig

# Context vars filled per request by middleware
request_id_var = contextvars.ContextVar("request_id", default="")
user_id_var = contextvars.ContextVar("user_id", default="")
role_var = contextvars.ContextVar("role", default="")
path_var = contextvars.ContextVar("path", default="")
method_var = contextvars.ContextVar("method", default="")
status_var = contextvars.ContextVar("status", default="")
latency_ms_var = contextvars.ContextVar("latency_ms", default="")
client_ip_var = contextvars.ContextVar("client_ip", default="")
trace_id_var = contextvars.ContextVar("trace_id", default="")
user_agent_var = contextvars.ContextVar("user_agent", default="")


def set_log_context(**kwargs) -> None:
    """Set context variables for the current request."""
    for key, var in {
        "request_id": request_id_var,
        "user_id": user_id_var,
        "role": role_var,
        "path": path_var,
        "method": method_var,
        "status": status_var,
        "latency_ms": latency_ms_var,
        "client_ip": client_ip_var,
        "trace_id": trace_id_var,
        "user_agent": user_agent_var,
    }.items():
        if key in kwargs and kwargs[key] is not None:
            var.set(kwargs[key])


def clear_log_context() -> None:
    """Reset request context after the response is sent."""
    for var in (
        request_id_var,
        user_id_var,
        role_var,
        path_var,
        method_var,
        status_var,
        latency_ms_var,
        client_ip_var,
        trace_id_var,
        user_agent_var,
    ):
        var.set("")


class RequestContextFilter(logging.Filter):
    """Inject request-scoped fields into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("")
        record.user_id = user_id_var.get("")
        record.role = role_var.get("")
        record.path = path_var.get("")
        record.method = method_var.get("")
        record.status = status_var.get("")
        record.latency_ms = latency_ms_var.get("")
        record.client_ip = client_ip_var.get("")
        record.trace_id = trace_id_var.get("")
        record.user_agent = user_agent_var.get("")
        return True


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter suitable for log shipping."""

    # Fields that are always present in the base JSON output.
    _BASE_FIELDS = frozenset({
        "ts_ist", "level", "logger", "msg",
        "request_id", "user_id", "role", "path", "method",
        "status", "latency_ms", "client_ip", "trace_id", "user_agent",
        "stack",
    })

    # Standard LogRecord attributes that should NOT leak into extra.
    _LOGRECORD_ATTRS = frozenset({
        "name", "msg", "args", "created", "relativeCreated", "exc_info",
        "exc_text", "stack_info", "lineno", "funcName", "filename",
        "module", "pathname", "levelname", "levelno", "msecs",
        "process", "processName", "thread", "threadName", "taskName",
        "message",
        # Our context-filter attrs (already in base):
        "request_id", "user_id", "role", "path", "method",
        "status", "latency_ms", "client_ip", "trace_id", "user_agent",
    })

    def format(self, record: logging.LogRecord) -> str:
        ist = pytz.timezone("Asia/Kolkata")
        ist_ts = datetime.now(ist).isoformat()
        base = {
            "ts_ist": ist_ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Only include request-context fields when they have values
        for attr in ("request_id", "user_id", "role", "path", "method",
                     "status", "latency_ms", "client_ip", "trace_id", "user_agent"):
            val = getattr(record, attr, "")
            if val:
                base[attr] = val

        if record.exc_info:
            base["stack"] = self.formatException(record.exc_info)

        # Merge extra fields passed via logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in self._LOGRECORD_ATTRS or key in self._BASE_FIELDS:
                continue
            # Skip None values to keep logs clean
            if value is None:
                continue
            base[key] = value

        return json.dumps(base, default=str, ensure_ascii=True)


def build_logging_config() -> dict:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"request_context": {"()": RequestContextFilter}},
        "formatters": {
            "json": {
                "()": JsonFormatter,
            }
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "filters": ["request_context"],
                "formatter": "json",
            },
            # Swallow noisy framework loggers like uvicorn.access so we only emit app request logs.
            "null": {
                "class": "logging.NullHandler",
            }
        },
        "loggers": {
            "": {"handlers": ["stdout"], "level": level},
            # Keep uvicorn errors so stack traces are visible; drop access logs.
            "uvicorn.error": {"handlers": ["stdout"], "level": level, "propagate": False},
            "uvicorn.access": {"handlers": ["null"], "level": "CRITICAL", "propagate": False},
            "httpx": {"handlers": ["null"], "level": "CRITICAL", "propagate": False},
        },
    }


def setup_logging() -> None:
    """Apply the logging configuration."""
    dictConfig(build_logging_config())
