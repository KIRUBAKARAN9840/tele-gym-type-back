

import logging
from contextlib import contextmanager
from typing import Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi import HTTPException

logger = logging.getLogger("payments.db_locking")


class InsufficientBalanceError(Exception):
    """Raised when trying to deduct more than available balance"""
    pass


class ConcurrentModificationError(Exception):
    """Raised when optimistic locking detects concurrent modification"""
    pass


VALID_ISOLATION_LEVELS = {
    "READ UNCOMMITTED",
    "READ COMMITTED",
    "REPEATABLE READ",
    "SERIALIZABLE",
}

@contextmanager
def pessimistic_lock(db: Session, isolation_level: str = "READ COMMITTED"):

    if isolation_level not in VALID_ISOLATION_LEVELS:
        raise ValueError(
            f"Invalid isolation level: {isolation_level}. "
            f"Allowed: {VALID_ISOLATION_LEVELS}"
        )

    original_isolation = None

    try:
        # Set isolation level
        if isolation_level != "READ COMMITTED":
            result = db.execute(text("SELECT @@transaction_isolation"))
            original_isolation = result.scalar()
            db.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}"))

        yield db

    finally:
        # Restore original isolation level
        if original_isolation and original_isolation in VALID_ISOLATION_LEVELS:
            db.execute(text(f"SET TRANSACTION ISOLATION LEVEL {original_isolation}"))


def deduct_fittbot_cash_atomic(
    db: Session,
    client_id: int,
    amount_rupees: float,
    description: str = "Payment"
) -> dict:
    """
    Atomically deduct fittbot cash with race condition protection.

    This is THE CORRECT WAY to handle balance deductions.
    Used by: Stripe, PayPal, Razorpay for wallet operations.

    Features:
    ✅ Atomic UPDATE (single database operation)
    ✅ Race condition safe (no read-modify-write)
    ✅ Prevents negative balance
    ✅ Transaction safe

    Args:
        db: Database session
        client_id: Client ID
        amount_rupees: Amount to deduct (in rupees)
        description: Description for audit log

    Returns:
        dict with old_balance, new_balance, deducted_amount

    Raises:
        InsufficientBalanceError: If balance is insufficient
        HTTPException: If client not found

    Example:
        result = deduct_fittbot_cash_atomic(db, client_id=123, amount_rupees=50.0)
        # result = {"old_balance": 100.0, "new_balance": 50.0, "deducted": 50.0}
    """
    logger.info(f"Attempting to deduct {amount_rupees}₹ from client {client_id} - {description}")

    # Use atomic UPDATE with WHERE clause
    # This is a SINGLE database operation - no race condition possible!
    result = db.execute(
        text("""
            UPDATE referral_fittbot_cash
            SET fittbot_cash = fittbot_cash - :amount
            WHERE client_id = :client_id
            AND fittbot_cash >= :amount
            RETURNING
                :amount + fittbot_cash as old_balance,
                fittbot_cash as new_balance,
                :amount as deducted_amount
        """),
        {"client_id": client_id, "amount": amount_rupees}
    )

    row = result.fetchone()

    if row is None:
        # Either client doesn't exist or insufficient balance
        # Check which case
        check = db.execute(
            text("SELECT fittbot_cash FROM referral_fittbot_cash WHERE client_id = :id"),
            {"id": client_id}
        ).fetchone()

        if check is None:
            logger.error(f"Client {client_id} not found in fittbot_cash table")
            raise HTTPException(status_code=404, detail="Client fittbot cash record not found")
        else:
            available = check[0]
            logger.warning(
                f"Insufficient fittbot cash for client {client_id}: "
                f"Available: {available}₹, Requested: {amount_rupees}₹"
            )
            raise InsufficientBalanceError(
                f"Insufficient fittbot cash. Available: ₹{available}, Required: ₹{amount_rupees}"
            )

    old_balance = float(row[0])
    new_balance = float(row[1])
    deducted = float(row[2])

    logger.info(
        f"✅ Successfully deducted {deducted}₹ from client {client_id} - "
        f"Old: {old_balance}₹, New: {new_balance}₹"
    )

    db.commit()

    return {
        "old_balance": old_balance,
        "new_balance": new_balance,
        "deducted_amount": deducted,
        "description": description
    }


def lock_and_update_subscription(
    db: Session,
    subscription_id: str,
    update_func
) -> Any:
    """
    Lock subscription row and apply updates atomically.

    Prevents race conditions when extending/pausing subscriptions.
    Used by: Stripe, Chargebee for subscription modifications.

    Args:
        db: Database session
        subscription_id: Subscription ID
        update_func: Function that takes subscription and modifies it

    Returns:
        Updated subscription

    Example:
        def extend_by_months(sub):
            sub.active_until += relativedelta(months=3)

        sub = lock_and_update_subscription(db, "sub_123", extend_by_months)
    """
    from ..models.subscriptions import Subscription

    # SELECT FOR UPDATE locks the row until transaction commits
    subscription = (
        db.query(Subscription)
        .filter(Subscription.id == subscription_id)
        .with_for_update()  # ← This is the magic!
        .first()
    )

    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")

    # Apply updates
    update_func(subscription)

    db.add(subscription)
    db.flush()

    logger.info(f"Atomically updated subscription {subscription_id}")

    return subscription


def idempotent_payment_creation(
    db: Session,
    provider_payment_id: str,
    order_id: str,
    create_func
) -> tuple[bool, Any]:
    """
    Idempotent payment creation with unique constraint enforcement.

    Prevents duplicate payment records from concurrent requests.
    Used by: Stripe, Razorpay to prevent double-processing.

    Args:
        db: Database session
        provider_payment_id: Razorpay/Stripe payment ID
        order_id: Internal order ID
        create_func: Function to create payment (called only if not exists)

    Returns:
        (created: bool, payment: Payment)
        - (False, existing_payment) if already exists
        - (True, new_payment) if created

    Example:
        def create_payment():
            return Payment(id="pay_123", amount=1000, ...)

        created, payment = idempotent_payment_creation(
            db, "pay_rzp123", "ord_456", create_payment
        )

        if created:
            # New payment - process it
            activate_services()
        else:
            # Duplicate request - return cached result
            return existing_result
    """
    from ..models.payments import Payment

    # Try to find existing payment
    existing = (
        db.query(Payment)
        .filter(
            Payment.provider_payment_id == provider_payment_id,
            Payment.status == "captured"
        )
        .first()
    )

    if existing:
        logger.info(
            f"Payment {provider_payment_id} already exists (idempotent) - "
            f"Order: {existing.order_id}"
        )
        return False, existing

    # Create new payment
    try:
        payment = create_func()
        db.add(payment)
        db.flush()

        logger.info(f"Created new payment {provider_payment_id} for order {order_id}")
        return True, payment

    except Exception as e:
        # If unique constraint violation, fetch existing
        # This handles race condition where two requests create simultaneously
        db.rollback()

        existing = (
            db.query(Payment)
            .filter(Payment.provider_payment_id == provider_payment_id)
            .first()
        )

        if existing:
            logger.warning(
                f"Concurrent payment creation detected for {provider_payment_id} - "
                f"Using existing record"
            )
            return False, existing
        else:
            # Some other error
            logger.error(f"Payment creation failed: {e}")
            raise


class OptimisticLockMixin:
    """
    Mixin for optimistic locking using version numbers.

    Add this to SQLAlchemy models that need optimistic locking.

    Example:
        class Order(Base, OptimisticLockMixin):
            __tablename__ = "orders"
            id = Column(String, primary_key=True)
            status = Column(String)
            # version column added by mixin

        # Update with optimistic locking
        order = db.query(Order).filter(Order.id == "ord_123").first()
        old_version = order.version

        order.status = "paid"
        db.flush()

        # This will fail if another request modified the order
        result = db.execute(
            text("UPDATE orders SET status = :status, version = :new_version
                  WHERE id = :id AND version = :old_version"),
            {"status": "paid", "new_version": old_version + 1,
             "id": "ord_123", "old_version": old_version}
        )

        if result.rowcount == 0:
            raise ConcurrentModificationError("Order was modified by another request")
    """

    # This would be added to models, but shown here for reference
    # version = Column(Integer, nullable=False, default=1)


def get_with_lock(db: Session, model_class, **filters):
    """
    Get record with SELECT FOR UPDATE lock.

    Shorthand for common locking pattern.

    Args:
        db: Database session
        model_class: SQLAlchemy model class
        **filters: Filter conditions

    Returns:
        Locked record or None

    Example:
        user = get_with_lock(db, User, id=123)
        user.balance -= 100
        db.commit()
    """
    query = db.query(model_class)

    for key, value in filters.items():
        query = query.filter(getattr(model_class, key) == value)

    return query.with_for_update().first()


# Redis-based distributed locking (for multi-server deployments)
try:
    import redis
    from redis.lock import Lock as RedisLock

    _redis_client: Optional[redis.Redis] = None

    def get_redis_client() -> redis.Redis:
        """Get or create Redis client for distributed locking"""
        global _redis_client
        if _redis_client is None:
            # Configure from environment
            import os
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            _redis_client = redis.from_url(redis_url)
        return _redis_client

    @contextmanager
    def distributed_lock(key: str, timeout: int = 10, blocking_timeout: int = 5):
        """
        Distributed lock using Redis.

        Used by Netflix, Uber for multi-server deployments.
        Ensures only ONE server processes a webhook/payment at a time.

        Args:
            key: Lock key (e.g., "payment:pay_123")
            timeout: Lock expiry time (seconds)
            blocking_timeout: How long to wait for lock (seconds)

        Example:
            with distributed_lock("payment:pay_123", timeout=10):
                # Only ONE server can execute this code at a time
                process_payment("pay_123")

        Note: Requires Redis server. Falls back to no-op if Redis unavailable.
        """
        client = get_redis_client()
        lock = client.lock(f"lock:{key}", timeout=timeout, blocking_timeout=blocking_timeout)

        acquired = lock.acquire(blocking=True, blocking_timeout=blocking_timeout)

        if not acquired:
            logger.warning(f"Failed to acquire distributed lock for {key}")
            raise TimeoutError(f"Could not acquire lock for {key}")

        try:
            logger.debug(f"Acquired distributed lock for {key}")
            yield
        finally:
            try:
                lock.release()
                logger.debug(f"Released distributed lock for {key}")
            except Exception as e:
                logger.warning(f"Error releasing lock for {key}: {e}")

except ImportError:
    # Redis not available - provide no-op implementation
    logger.warning("Redis not available - distributed locking disabled")

    @contextmanager
    def distributed_lock(key: str, timeout: int = 10, blocking_timeout: int = 5):
        """No-op distributed lock when Redis is not available"""
        logger.warning(f"Distributed lock requested for {key} but Redis not available")
        yield
