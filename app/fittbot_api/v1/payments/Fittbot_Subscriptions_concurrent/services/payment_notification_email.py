"""
Fire-and-forget payment notification emails via AWS SES.

Sends an HTML email to the team whenever a payment is captured.
Uses a daemon thread + sync boto3 so the email survives even after
the Celery task / asyncio loop finishes.
"""

import logging
import threading
from datetime import datetime
from typing import Optional

import boto3

logger = logging.getLogger("payments.notification_email")

SOURCE_EMAIL = "support@fittbot.com"
PAYMENT_NOTIFY_TO = [
    "martinraju53@gmail.com",
    "naveenkulandasamy@gmail.com",
]


def _send_payment_email(
    *,
    provider: str,
    payment_type: str,
    client_id: Optional[str] = None,
    command_id: Optional[str] = None,
    razorpay_payment_id: Optional[str] = None,
    razorpay_subscription_id: Optional[str] = None,
    plan_sku: Optional[str] = None,
    amount: Optional[str] = None,
    extra_fields: Optional[dict] = None,
) -> None:
    """Send payment captured notification email via SES (runs in a background thread)."""
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = [
            ("Timestamp", now),
            ("Provider", provider),
            ("Payment Type", payment_type),
            ("Client ID", client_id or "N/A"),
            ("Command ID", command_id or "N/A"),
        ]
        if razorpay_payment_id:
            rows.append(("Razorpay Payment ID", razorpay_payment_id))
        if razorpay_subscription_id:
            rows.append(("Razorpay Subscription ID", razorpay_subscription_id))
        if plan_sku:
            rows.append(("Plan SKU", plan_sku))
        if amount:
            rows.append(("Amount", amount))
        if extra_fields:
            for k, v in extra_fields.items():
                rows.append((k, str(v)))

        table_rows = "".join(
            f"<tr><td style='padding:6px 12px;border:1px solid #ddd;background:#f8f9fa;'>"
            f"<b>{label}</b></td><td style='padding:6px 12px;border:1px solid #ddd;'>{value}</td></tr>"
            for label, value in rows
        )

        html_body = f"""
        <div style="font-family:Arial,sans-serif;max-width:620px;margin:0 auto;">
            <h2 style="color:#27ae60;">Payment Captured</h2>
            <table style="border-collapse:collapse;width:100%;margin:12px 0;">
                {table_rows}
            </table>
            <p style="font-size:13px;color:#666;">This is an automated notification from the Fymble payment system.</p>
        </div>
        """

        subject = f"Payment Captured | {payment_type} | {provider} | {now}"

        ses = boto3.client("ses", region_name="ap-south-1")
        ses.send_email(
            Source=SOURCE_EMAIL,
            Destination={"ToAddresses": PAYMENT_NOTIFY_TO},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Charset": "UTF-8", "Data": html_body},
                },
            },
        )
        logger.info(
            "Payment notification email sent",
            extra={"provider": provider, "payment_type": payment_type, "client_id": client_id},
        )
    except Exception:
        logger.exception("Failed to send payment notification email")


def fire_payment_notification_email(**kwargs) -> None:
    """Spawn a daemon thread to send the email. Truly fire-and-forget, works in Celery/asyncio/sync."""
    t = threading.Thread(target=_send_payment_email, kwargs=kwargs, daemon=True)
    t.start()
