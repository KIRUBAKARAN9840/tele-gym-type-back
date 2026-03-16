"""Payment database configuration - Uses main app database"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy.orm import Session

from app.models.database import get_db


class PaymentDatabase:
    """Payment database manager - uses main app database"""

    def __init__(self):
        # No need for separate database setup, use main app database
        pass

    def create_tables(self):
        """Create all payment tables - handled by main app"""
        # Tables are created through main app's alembic migrations
        pass

    def drop_tables(self):
        """Drop all payment tables (use with caution!)"""
        # Not implemented - use alembic for schema management
        pass

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager that yields a main DB session and ensures cleanup."""
        session = next(get_db())
        try:
            yield session
        finally:
            session.close()

    def close(self):
        """Close database connections - handled by main app"""
        pass


# Global database instance
_payment_db: PaymentDatabase = None


def get_payment_db() -> PaymentDatabase:
    """Get payment database singleton"""
    global _payment_db
    if _payment_db is None:
        _payment_db = PaymentDatabase()
    return _payment_db


def get_db_session() -> Generator[Session, None, None]:
    """Dependency for getting database session - uses main app database"""
    with get_payment_db().get_session() as session:
        yield session
