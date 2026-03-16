import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from app.config.settings import settings

logger = logging.getLogger(__name__)


def create_database_engine():
    """Create database engine using centralized settings configuration"""

    db_url = settings.database_url

    engine = create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        pool_pre_ping=settings.db_pool_pre_ping,
        pool_reset_on_return="rollback",
        isolation_level="READ COMMITTED",
        connect_args={
            "connect_timeout": 5,
            "charset": "utf8mb4",
        },
        echo=settings.db_pool_echo,
        future=True,
    )

    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False
    )
    return engine, SessionLocal

# Global variables for lazy initialization
_engine = None
_SessionLocal = None

def _ensure_engine():
    global _engine, _SessionLocal
    if _engine is None:
        _engine, _SessionLocal = create_database_engine()
    return _engine, _SessionLocal

# Create declarative base
Base = declarative_base()

def get_db_sync():
    """Get database session for sync context (Celery tasks)"""
    _, SessionLocal = _ensure_engine()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()




def get_db():
    """Get database session with lazy engine initialization"""
    _, SessionLocal = _ensure_engine()  # Ensure engine is created
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_engine():
    """Get database engine for metrics collection."""
    engine, _ = _ensure_engine()
    return engine

# For backwards compatibility - create engine/SessionLocal on first access
def __getattr__(name):
    if name == "engine":
        engine, _ = _ensure_engine()
        return engine
    elif name == "SessionLocal":
        _, SessionLocal = _ensure_engine()
        return SessionLocal
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
