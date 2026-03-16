import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

try:
    from settings import settings  # Preferred local settings module for the task runner
except ModuleNotFoundError:
    # Fallback: pull values directly from environment variables
    class _FallbackSettings:
        db_username = os.getenv("DB_USERNAME", "root")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST", "localhost:3306")
        db_name = os.getenv("DB_NAME", "latest")

        db_pool_size = int(os.getenv("DB_POOL_SIZE", 5))
        db_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", 10))
        db_pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", 30))
        db_pool_recycle = int(os.getenv("DB_POOL_RECYCLE", 3600))
        db_pool_pre_ping = os.getenv("DB_POOL_PRE_PING", "true").lower() == "true"
        db_pool_echo = os.getenv("DB_POOL_ECHO", "false").lower() == "true"

        @property
        def database_url(self) -> str:
            if self.db_password:
                return (
                    f"mysql+pymysql://{self.db_username}:{self.db_password}@"
                    f"{self.db_host}/{self.db_name}"
                )
            return f"mysql+pymysql://{self.db_username}@{self.db_host}/{self.db_name}"

    settings = _FallbackSettings()

logger = logging.getLogger(__name__)

_engine = None
_SessionLocal = None


def create_database_engine():
    """Create database engine using centralized settings configuration"""
    db_url = settings.database_url
    print(f"[DATABASE-DEBUG] Creating engine with URL: {db_url}")
    if hasattr(settings, "db_name"):
        print(f"[DATABASE-DEBUG] DB_NAME from settings: {settings.db_name}")
    if hasattr(settings, "db_host"):
        print(f"[DATABASE-DEBUG] DB_HOST from settings: {settings.db_host}")
    if hasattr(settings, "db_username"):
        print(f"[DATABASE-DEBUG] DB_USERNAME from settings: {settings.db_username}")

    engine = create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=getattr(settings, "db_pool_size", 5),
        max_overflow=getattr(settings, "db_max_overflow", 10),
        pool_timeout=getattr(settings, "db_pool_timeout", 30),
        pool_recycle=getattr(settings, "db_pool_recycle", 3600),
        pool_pre_ping=getattr(settings, "db_pool_pre_ping", True),
        pool_reset_on_return="rollback",
        isolation_level="READ COMMITTED",
        connect_args={"connect_timeout": 5, "charset": "utf8mb4"},
        echo=getattr(settings, "db_pool_echo", False),
        future=True,
    )

    print(f"[DATABASE-DEBUG] Engine created successfully with URL: {engine.url}")

    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )
    return engine, SessionLocal


def _ensure_engine():
    """Ensure engine is created (lazy initialization)"""
    global _engine, _SessionLocal
    if _engine is None:
        print("[DATABASE-DEBUG] Lazy initializing engine...")
        _engine, _SessionLocal = create_database_engine()
    return _engine, _SessionLocal


# Declarative base shared across models
Base = declarative_base()


def get_db():
    """Dependency-style session generator"""
    _, SessionLocal = _ensure_engine()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def __getattr__(name):
    """Backwards compatibility for modules importing engine/SessionLocal"""
    if name == "engine":
        engine, _ = _ensure_engine()
        return engine
    if name == "SessionLocal":
        _, SessionLocal = _ensure_engine()
        return SessionLocal
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

