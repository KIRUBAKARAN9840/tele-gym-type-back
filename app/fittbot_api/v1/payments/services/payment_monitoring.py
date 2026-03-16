"""
Payment Monitoring & Alert System
Proactive monitoring to catch payment issues before customers complain
"""

import logging
from datetime import datetime, timedelta
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from ..models.orders import Order
from ..models.payments import Payment
from ..webhooks.revenuecat_handler import now_ist

logger = logging.getLogger("payment_monitoring")

class PaymentMonitoringService:
    """Proactive payment monitoring and alerting"""
    
    async def run_monitoring_checks(self, db: Session):
        """Run all monitoring checks - call this every 5-10 minutes"""
        
        alerts = []
        
        # Check 1: Stuck pending orders
        stuck_orders = await self.check_stuck_pending_orders(db)
        if stuck_orders:
            alerts.append({
                "type": "stuck_orders",
                "severity": "high",
                "count": len(stuck_orders),
                "message": f"{len(stuck_orders)} orders stuck in pending status",
                "orders": [o.id for o in stuck_orders]
            })
        
        # Check 2: High failure rate
        failure_rate = await self.check_payment_failure_rate(db)
        if failure_rate > 0.1:  # > 10% failure rate
            alerts.append({
                "type": "high_failure_rate", 
                "severity": "critical",
                "rate": failure_rate,
                "message": f"Payment failure rate is {failure_rate:.1%} (last hour)"
            })
        
        # Check 3: Webhook delays
        webhook_delays = await self.check_webhook_delays(db)
        if webhook_delays:
            alerts.append({
                "type": "webhook_delays",
                "severity": "medium", 
                "message": f"Webhooks are delayed by average {webhook_delays:.1f} minutes"
            })
        
        # Check 4: Missing webhooks
        missing_webhooks = await self.check_missing_webhooks(db)
        if missing_webhooks:
            alerts.append({
                "type": "missing_webhooks",
                "severity": "high",
                "count": len(missing_webhooks),
                "message": f"{len(missing_webhooks)} potential missing webhooks detected"
            })
        
        # Send alerts if any found
        if alerts:
            await self.send_alerts(alerts)
        
        return alerts
    
    async def check_stuck_pending_orders(self, db: Session) -> List[Order]:
        """Find orders stuck in pending for too long"""
        
        cutoff = now_ist() - timedelta(minutes=15)
        
        stuck_orders = db.query(Order).filter(
            Order.status == "pending",
            Order.provider == "google_play", 
            Order.created_at < cutoff
        ).limit(50).all()  # Limit to avoid spam
        
        return stuck_orders
    
    async def check_payment_failure_rate(self, db: Session) -> float:
        """Calculate payment failure rate in last hour"""
        
        one_hour_ago = now_ist() - timedelta(hours=1)
        
        total_orders = db.query(func.count(Order.id)).filter(
            Order.created_at >= one_hour_ago,
            Order.provider == "google_play"
        ).scalar()
        
        failed_orders = db.query(func.count(Order.id)).filter(
            Order.created_at >= one_hour_ago,
            Order.provider == "google_play",
            Order.status.in_(["failed", "cancelled"])
        ).scalar()
        
        if total_orders == 0:
            return 0.0
            
        return failed_orders / total_orders
    
    async def check_webhook_delays(self, db: Session) -> float:
        """Check average delay between order creation and webhook processing"""
        
        recent_orders = db.query(Order).filter(
            Order.created_at >= now_ist() - timedelta(hours=1),
            Order.status == "paid"
        ).all()
        
        if not recent_orders:
            return 0.0
        
        delays = []
        for order in recent_orders:
            if order.updated_at and order.created_at:
                delay_seconds = (order.updated_at - order.created_at).total_seconds()
                delays.append(delay_seconds / 60)  # Convert to minutes
        
        if not delays:
            return 0.0
            
        return sum(delays) / len(delays)  # Average delay in minutes
    
    async def check_missing_webhooks(self, db: Session) -> List[Order]:
        """Detect potential missing webhooks by comparing with expected patterns"""
        
        # Orders that are 1+ hours old and still pending (likely missing webhook)
        cutoff = now_ist() - timedelta(hours=1)
        
        potential_missing = db.query(Order).filter(
            Order.status == "pending",
            Order.created_at < cutoff,
            Order.provider == "google_play"
        ).all()
        
        return potential_missing
    
    async def send_alerts(self, alerts: List[dict]):
        """Send alerts via multiple channels"""
        
        # Format alert message
        alert_message = "🚨 PAYMENT SYSTEM ALERTS:\n\n"
        
        for alert in alerts:
            severity_emoji = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "💥"}
            emoji = severity_emoji.get(alert["severity"], "⚪")
            
            alert_message += f"{emoji} {alert['message']}\n"
        
        alert_message += f"\nTime: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}"
        
        # Send via multiple channels
        await self.send_slack_alert(alert_message)
        await self.send_email_alert(alert_message, alerts)
        await self.log_alerts(alerts)
    
    async def send_slack_alert(self, message: str):
        """Send alert to Slack channel"""
        # Implement Slack webhook integration
        try:
            import aiohttp
            webhook_url = "your-slack-webhook-url"
            
            payload = {
                "text": message,
                "channel": "#payment-alerts",
                "username": "Payment Monitor",
                "icon_emoji": ":warning:"
            }
            
            async with aiohttp.ClientSession() as session:
                await session.post(webhook_url, json=payload)
                
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    async def send_email_alert(self, message: str, alerts: List[dict]):
        """Send email alert to operations team"""
        # Implement email sending
        try:
            # Use your email service (SendGrid, SES, etc.)
            pass
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
    
    async def log_alerts(self, alerts: List[dict]):
        """Log alerts for tracking"""
        for alert in alerts:
            logger.warning(f"ALERT: {alert}")


# Health check endpoint
from fastapi import APIRouter

monitoring_router = APIRouter(prefix="/monitoring", tags=["Payment Monitoring"])

@monitoring_router.get("/payment-health")
async def payment_system_health_check(db: Session = Depends(get_db_session)):
    """Health check endpoint for payment system"""
    
    monitoring = PaymentMonitoringService()
    
    # Quick health checks
    recent_orders = db.query(func.count(Order.id)).filter(
        Order.created_at >= now_ist() - timedelta(minutes=30)
    ).scalar()
    
    stuck_orders = db.query(func.count(Order.id)).filter(
        Order.status == "pending",
        Order.created_at < now_ist() - timedelta(minutes=15)
    ).scalar()
    
    health_status = "healthy"
    if stuck_orders > 5:
        health_status = "degraded"
    if stuck_orders > 20:
        health_status = "critical"
    
    return {
        "status": health_status,
        "recent_orders_30min": recent_orders,
        "stuck_orders": stuck_orders,
        "last_check": now_ist().isoformat(),
        "webhook_endpoint_status": "active"  # You can ping RevenueCat here
    }