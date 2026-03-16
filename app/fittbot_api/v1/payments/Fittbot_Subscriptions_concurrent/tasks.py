import logging
import time

from app.celery_app import celery_app
from app.utils.celery_asyncio import run_in_worker_loop
from app.utils.redis_config import get_redis_sync

from ..config.database import get_payment_db
from ..metrics import (
    payment_task_metrics,
    record_payment_operation,
    PAYMENT_OPERATIONS_IN_PROGRESS,
    PAYMENT_TASK_RETRY_COUNT,
)
from .config import get_high_concurrency_config
from .services.dailypass_processor import DailyPassProcessor
from .services.gym_membership_processor import GymMembershipProcessor
from .services.revenuecat_processor import RevenueCatProcessor
from .services.subscription_processor import SubscriptionProcessor
from .services.webhook_processor import WebhookProcessor
from .services.session_processor import SessionProcessor
from .stores.command_store import CommandStore

logger = logging.getLogger("payments.razorpay.v2.tasks")


# =============================================================================
# RAZORPAY SUBSCRIPTION WORKERS
# =============================================================================

@payment_task_metrics("razorpay", "checkout", "subscription")
async def _checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(redis, config)
    processor = SubscriptionProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "subscription")
async def _verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(redis, config)
    processor = SubscriptionProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_verify(command_id, store)


@payment_task_metrics("razorpay", "webhook", "subscription")
async def _webhook_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(redis, config)
    processor = WebhookProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process(command_id, store)


@celery_app.task(name="payments.razorpay.process_checkout", max_retries=5)
def process_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("Checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="subscription"
        ).inc()
        raise


@celery_app.task(name="payments.razorpay.process_webhook", max_retries=5)
def process_webhook_task(command_id: str):
    try:
        run_in_worker_loop(_webhook_worker(command_id))
    except Exception as exc:
        logger.exception("Webhook task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="webhook", payment_type="subscription"
        ).inc()
        raise


@celery_app.task(name="payments.razorpay.process_verify", max_retries=5)
def process_verify_task(command_id: str):
    try:
        run_in_worker_loop(_verify_worker(command_id))
    except Exception as exc:
        logger.exception("Verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="subscription"
        ).inc()
        raise


# =============================================================================
# REVENUECAT WORKERS
# =============================================================================

@payment_task_metrics("revenuecat", "order", "subscription")
async def _revenuecat_order_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.revenuecat_redis_prefix,
        command_id_prefix="rc_cmd",
    )
    processor = RevenueCatProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_order(command_id, store)


@payment_task_metrics("revenuecat", "verify", "subscription")
async def _revenuecat_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.revenuecat_redis_prefix,
        command_id_prefix="rc_cmd",
    )
    processor = RevenueCatProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_verify(command_id, store)


@payment_task_metrics("revenuecat", "webhook", "subscription")
async def _revenuecat_webhook_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.revenuecat_redis_prefix,
        command_id_prefix="rc_cmd",
    )
    processor = RevenueCatProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_webhook(command_id, store)


@celery_app.task(name="payments.revenuecat.process_order", max_retries=5)
def process_revenuecat_order_task(command_id: str):
    try:
        run_in_worker_loop(_revenuecat_order_worker(command_id))
    except Exception as exc:
        logger.exception("RevenueCat order task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="revenuecat", operation="order", payment_type="subscription"
        ).inc()
        raise


@celery_app.task(name="payments.revenuecat.process_verify", max_retries=5)
def process_revenuecat_verify_task(command_id: str):
    try:
        run_in_worker_loop(_revenuecat_verify_worker(command_id))
    except Exception as exc:
        logger.exception("RevenueCat verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="revenuecat", operation="verify", payment_type="subscription"
        ).inc()
        raise


@celery_app.task(name="payments.revenuecat.process_webhook", max_retries=5)
def process_revenuecat_webhook_task(command_id: str):
    try:
        run_in_worker_loop(_revenuecat_webhook_worker(command_id))
    except Exception as exc:
        logger.exception("RevenueCat webhook task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="revenuecat", operation="webhook", payment_type="subscription"
        ).inc()
        raise


# =============================================================================
# DAILYPASS WORKERS
# =============================================================================

@payment_task_metrics("razorpay", "checkout", "daily_pass")
async def _dailypass_checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "daily_pass")
async def _dailypass_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_verify(command_id, store)


@celery_app.task(name="payments.dailypass.process_checkout", max_retries=5)
def process_dailypass_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="daily_pass"
        ).inc()
        raise


@celery_app.task(name="payments.dailypass.process_verify", max_retries=5)
def process_dailypass_verify_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_verify_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="daily_pass"
        ).inc()
        raise


@payment_task_metrics("razorpay", "checkout", "daily_pass_upgrade")
async def _dailypass_upgrade_checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_upgrade_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "daily_pass_upgrade")
async def _dailypass_upgrade_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_upgrade_verify(command_id, store)


@celery_app.task(name="payments.dailypass.process_upgrade_checkout", max_retries=5)
def process_dailypass_upgrade_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_upgrade_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass upgrade checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="daily_pass_upgrade"
        ).inc()
        raise


@celery_app.task(name="payments.dailypass.process_upgrade_verify", max_retries=5)
def process_dailypass_upgrade_verify_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_upgrade_verify_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass upgrade verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="daily_pass_upgrade"
        ).inc()
        raise


@payment_task_metrics("razorpay", "checkout", "daily_pass_topup")
async def _dailypass_edit_topup_checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_edit_topup_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "daily_pass_topup")
async def _dailypass_edit_topup_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.dailypass_redis_prefix,
        command_id_prefix="dp_cmd",
    )
    processor = DailyPassProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_edit_topup_verify(command_id, store)


@celery_app.task(name="payments.dailypass.process_edit_topup_checkout", max_retries=5)
def process_dailypass_edit_topup_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_edit_topup_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass edit top-up checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="daily_pass_topup"
        ).inc()
        raise


@celery_app.task(name="payments.dailypass.process_edit_topup_verify", max_retries=5)
def process_dailypass_edit_topup_verify_task(command_id: str):
    try:
        run_in_worker_loop(_dailypass_edit_topup_verify_worker(command_id))
    except Exception as exc:
        logger.exception("DailyPass edit top-up verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="daily_pass_topup"
        ).inc()
        raise


# =============================================================================
# GYM MEMBERSHIP WORKERS
# =============================================================================

@payment_task_metrics("razorpay", "checkout", "gym_membership")
async def _gym_membership_checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.gym_membership_redis_prefix,
        command_id_prefix="gym_cmd",
    )
    processor = GymMembershipProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "gym_membership")
async def _gym_membership_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.gym_membership_redis_prefix,
        command_id_prefix="gym_cmd",
    )
    processor = GymMembershipProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_verify(command_id, store)


@payment_task_metrics("razorpay", "webhook", "gym_membership")
async def _gym_membership_webhook_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.gym_membership_redis_prefix,
        command_id_prefix="gym_cmd",
    )
    processor = GymMembershipProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_webhook(command_id, store)


@celery_app.task(name="payments.gym_membership.process_checkout", max_retries=5)
def process_gym_membership_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_gym_membership_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("Gym membership checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="gym_membership"
        ).inc()
        raise


@celery_app.task(name="payments.gym_membership.process_verify", max_retries=5)
def process_gym_membership_verify_task(command_id: str):
    try:
        run_in_worker_loop(_gym_membership_verify_worker(command_id))
    except Exception as exc:
        logger.exception("Gym membership verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="gym_membership"
        ).inc()
        raise


@celery_app.task(name="payments.gym_membership.process_webhook", max_retries=5)
def process_gym_membership_webhook_task(command_id: str):
    try:
        run_in_worker_loop(_gym_membership_webhook_worker(command_id))
    except Exception as exc:
        logger.exception("Gym membership webhook task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="webhook", payment_type="gym_membership"
        ).inc()
        raise


# =============================================================================
# SESSION BOOKING WORKERS
# =============================================================================

@payment_task_metrics("razorpay", "checkout", "session_booking")
async def _session_checkout_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.sessions_redis_prefix,
        command_id_prefix="sess_cmd",
    )
    processor = SessionProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_checkout(command_id, store)


@payment_task_metrics("razorpay", "verify", "session_booking")
async def _session_verify_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.sessions_redis_prefix,
        command_id_prefix="sess_cmd",
    )
    processor = SessionProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_verify(command_id, store)


@payment_task_metrics("razorpay", "webhook", "session_booking")
async def _session_webhook_worker(command_id: str):
    config = get_high_concurrency_config()
    redis = get_redis_sync()
    store = CommandStore(
        redis,
        config,
        redis_prefix=config.sessions_redis_prefix,
        command_id_prefix="sess_cmd",
    )
    processor = SessionProcessor(config=config, payment_db=get_payment_db(), redis=redis)
    await processor.process_webhook(command_id, store)


@celery_app.task(name="payments.sessions.process_checkout", max_retries=5)
def process_sessions_checkout_task(command_id: str):
    try:
        run_in_worker_loop(_session_checkout_worker(command_id))
    except Exception as exc:
        logger.exception("Sessions checkout task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="checkout", payment_type="session_booking"
        ).inc()
        raise


@celery_app.task(name="payments.sessions.process_verify", max_retries=5)
def process_sessions_verify_task(command_id: str):
    try:
        run_in_worker_loop(_session_verify_worker(command_id))
    except Exception as exc:
        logger.exception("Sessions verify task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="verify", payment_type="session_booking"
        ).inc()
        raise


@celery_app.task(name="payments.sessions.process_webhook", max_retries=5)
def process_sessions_webhook_task(command_id: str):
    try:
        run_in_worker_loop(_session_webhook_worker(command_id))
    except Exception as exc:
        logger.exception("Sessions webhook task failed: %s", exc)
        PAYMENT_TASK_RETRY_COUNT.labels(
            provider="razorpay", operation="webhook", payment_type="session_booking"
        ).inc()
        raise
