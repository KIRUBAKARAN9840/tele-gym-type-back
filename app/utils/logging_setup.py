import logging
import sys
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING").upper()

def _json_formatter(record: logging.LogRecord) -> str:
    # keep it tiny & fast: one-line JSON strings (already composed upstream)
    return "%(message)s"

def setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_json_formatter(None)))
    logger = logging.getLogger("fittbot")
    logger.setLevel(LOG_LEVEL)
    logger.handlers = [handler]
    logger.propagate = False
    return logger

logger = setup_logging()

def jlog(level: str, message: dict):
    # message must already be a dict of primitives/JSON-safe values
    import json
    line = json.dumps(message, default=str, separators=(",", ":"))
    getattr(logger, level.lower(), logger.error)(line)