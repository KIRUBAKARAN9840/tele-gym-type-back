"""
Automated Payment Recovery Jobs
Set these up to run automatically via cron or task scheduler
"""

import asyncio
import logging
from datetime import datetime

from ..services.webhook_recovery_service import scheduled_webhook_recovery
from ..services.payment_monitoring import PaymentMonitoringService
from ..config.database import get_db_session

logger = logging.getLogger("payment_jobs")

# Job 1: Run every 15 minutes
async def webhook_recovery_job():
    """
    Cron: */15 * * * * (every 15 minutes)
    Recovery missed/failed webhooks
    """
    try:
        logger.info("🔄 Starting webhook recovery job")
        await scheduled_webhook_recovery()
        logger.info("✅ Webhook recovery job completed")
    except Exception as e:
        logger.error(f"❌ Webhook recovery job failed: {e}")

# Job 2: Run every 5 minutes  
async def payment_monitoring_job():
    """
    Cron: */5 * * * * (every 5 minutes)
    Monitor payment system health and send alerts
    """
    try:
        logger.info("👀 Starting payment monitoring job")
        
        with get_db_session() as db:
            monitoring = PaymentMonitoringService()
            alerts = await monitoring.run_monitoring_checks(db)
            
            if alerts:
                logger.warning(f"⚠️ Found {len(alerts)} alerts")
            else:
                logger.info("✅ All payment systems healthy")
                
    except Exception as e:
        logger.error(f"❌ Payment monitoring job failed: {e}")

# Job 3: Run every hour
async def cleanup_old_pending_orders():
    """
    Cron: 0 * * * * (every hour)
    Clean up very old pending orders (24+ hours)
    """
    try:
        logger.info("🧹 Starting cleanup job")
        
        with get_db_session() as db:
            from ..models.orders import Order
            from ..webhooks.revenuecat_handler import now_ist
            from datetime import timedelta
            
            # Mark orders older than 24 hours as failed
            cutoff = now_ist() - timedelta(hours=24)
            
            old_orders = db.query(Order).filter(
                Order.status == "pending",
                Order.created_at < cutoff
            ).all()
            
            for order in old_orders:
                order.status = "failed"
                order.updated_at = now_ist()
                logger.info(f"Marked old order {order.id} as failed")
            
            db.commit()
            logger.info(f"✅ Cleaned up {len(old_orders)} old pending orders")
            
    except Exception as e:
        logger.error(f"❌ Cleanup job failed: {e}")

# Job 4: Run daily
async def daily_payment_report():
    """
    Cron: 0 9 * * * (every day at 9 AM)
    Generate daily payment system report
    """
    try:
        logger.info("📊 Generating daily payment report")
        
        with get_db_session() as db:
            from sqlalchemy import func
            from ..models.orders import Order
            from ..models.payments import Payment
            
            yesterday = datetime.now().date() - timedelta(days=1)
            
            # Daily stats
            daily_orders = db.query(func.count(Order.id)).filter(
                func.date(Order.created_at) == yesterday
            ).scalar()
            
            daily_revenue = db.query(func.sum(Payment.amount_minor)).filter(
                func.date(Payment.captured_at) == yesterday,
                Payment.status == "captured"
            ).scalar() or 0
            
            failed_orders = db.query(func.count(Order.id)).filter(
                func.date(Order.created_at) == yesterday,
                Order.status == "failed"
            ).scalar()
            
            success_rate = ((daily_orders - failed_orders) / daily_orders * 100) if daily_orders > 0 else 0
            
            report = f"""
📊 Daily Payment Report - {yesterday}

💰 Revenue: ₹{daily_revenue/100:,.2f}
📦 Orders: {daily_orders}
❌ Failed: {failed_orders}
✅ Success Rate: {success_rate:.1f}%

📈 System Health: {"✅ Good" if success_rate > 95 else "⚠️ Needs attention"}
            """
            
            # Send report via email/Slack
            logger.info(report)
            
    except Exception as e:
        logger.error(f"❌ Daily report job failed: {e}")


# Main runner for all jobs
if __name__ == "__main__":
    # For testing - run all jobs once
    asyncio.run(webhook_recovery_job())
    asyncio.run(payment_monitoring_job())
    asyncio.run(cleanup_old_pending_orders())
    asyncio.run(daily_payment_report())