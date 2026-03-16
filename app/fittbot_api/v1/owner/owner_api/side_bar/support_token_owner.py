# app/routers/support_token_owner.py

import logging
import secrets
from datetime import datetime
from typing import Optional

import aioboto3
from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import Gym, OwnerToken
from app.models.adminmodels import SupportTicketAssignment
from app.utils.logging_utils import FittbotHTTPException

logger = logging.getLogger(__name__)

SOURCE_EMAIL = "support@fittbot.com"
SUPPORT_TO = ["gurunr.fymble@gmail.com"]
SUPPORT_CC = [
    "nishad@fymble.app",
    "shama@fymble.app",
    "martin@fymble.app",
    "naveen@fymble.app",
]

router = APIRouter(prefix="/support_token_owner", tags=["Owner Tokens"])


async def send_owner_support_ticket_email(token: str, gym_name: str, subject: str, email: str, issue: str):
    session = aioboto3.Session()
    async with session.client("ses", region_name="ap-south-1") as ses:
        await ses.send_email(
            Source=SOURCE_EMAIL,
            Destination={
                "ToAddresses": SUPPORT_TO,
                "CcAddresses": SUPPORT_CC,
            },
            Message={
                "Subject": {"Data": f"New Fymble Business Support Ticket: {token}", "Charset": "UTF-8"},
                "Body": {
                    "Html": {
                        "Charset": "UTF-8",
                        "Data": f"""
                        <h2>New Owner Support Ticket Created</h2>
                        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
                            <tr><td><b>Token</b></td><td>{token}</td></tr>
                            <tr><td><b>Gym Name</b></td><td>{gym_name}</td></tr>
                            <tr><td><b>Subject</b></td><td>{subject or 'N/A'}</td></tr>
                            <tr><td><b>Email</b></td><td>{email or 'N/A'}</td></tr>
                            <tr><td><b>Issue</b></td><td>{issue or 'N/A'}</td></tr>
                            <tr><td><b>Created At</b></td><td>{datetime.today().strftime('%Y-%m-%d %H:%M')}</td></tr>
                        </table>
                        <br><p>Please follow up on this ticket.</p>
                        <p>- Fymble Support System</p>
                        """,
                    }
                },
            },
        )
    logger.info(f"Owner support ticket email sent for token {token}")


async def send_owner_acknowledgment_email(token: str, gym_name: str, subject: str, email: str, issue: str):
    if not email:
        logger.warning(f"No owner email provided for token {token}, skipping acknowledgment email")
        return
    session = aioboto3.Session()
    async with session.client("ses", region_name="ap-south-1") as ses:
        await ses.send_email(
            Source=SOURCE_EMAIL,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": f"Support Ticket Received: {token}", "Charset": "UTF-8"},
                "Body": {
                    "Html": {
                        "Charset": "UTF-8",
                        "Data": f"""
                        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
                            <h2 style="color:#2c3e50;">Hi {gym_name},</h2>
                            <p>Thank you for reaching out to <b>Fymble Business Support</b>. We have received your support request and our team is looking into it.</p>
                            <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;width:100%;margin:16px 0;">
                                <tr><td style="background:#f8f9fa;"><b>Ticket ID</b></td><td>{token}</td></tr>
                                <tr><td style="background:#f8f9fa;"><b>Subject</b></td><td>{subject or 'N/A'}</td></tr>
                                <tr><td style="background:#f8f9fa;"><b>Issue</b></td><td>{issue or 'N/A'}</td></tr>
                                <tr><td style="background:#f8f9fa;"><b>Date</b></td><td>{datetime.today().strftime('%Y-%m-%d %H:%M')}</td></tr>
                            </table>
                            <p>Our support team will get back to you shortly.</p>
                            <br>
                            <p>Warm regards,<br><b>Fymble Business Support Team</b></p>
                            <hr style="border:none;border-top:1px solid #ddd;margin:20px 0;">
                            <p style="font-size:12px;color:#999;">This is an automated message. Please do not reply to this email. For further assistance, raise a new support ticket from the app.</p>
                        </div>
                        """,
                    }
                },
            },
        )
    logger.info(f"Owner acknowledgment email sent to {email} for token {token}")


class TokenCreate(BaseModel):
    gym_id: int
    subject: Optional[str] = None
    email: Optional[EmailStr] = None
    issue: Optional[str] = None


@router.post("/generate")
async def generate_owner_support_token(
    payload: TokenCreate, db: AsyncSession = Depends(get_async_db)
):

    try:
        gym_result = await db.execute(
            select(Gym).where(Gym.gym_id == payload.gym_id)
        )
        gym = gym_result.scalars().first()
        if not gym:
            raise FittbotHTTPException(
                status_code=404,
                detail="Gym not found",
                error_code="GYM_NOT_FOUND",
                log_data={"gym_id": payload.gym_id},
            )

        new_row = OwnerToken(
            gym_id=payload.gym_id,
            subject=payload.subject,
            email=payload.email,
            issue=payload.issue,
            followed_up=False,
            created_at=datetime.utcnow(),
            token="",
            comments="We are processing your request",
        )

        db.add(new_row)
        await db.flush()

        id_str = str(new_row.id)
        rand_part = str(secrets.randbelow(10 ** (10 - len(id_str)))).zfill(10 - len(id_str))
        new_row.token = f"FYMBE{id_str}{rand_part}"
        await db.commit()
        await db.refresh(new_row)

        db.add(SupportTicketAssignment(
            ticket_id=new_row.id,
            ticket_source="Fittbot Business",
            admin_id=6,
            assigned_at=datetime.now(),
        ))
        await db.commit()

        try:
            await send_owner_support_ticket_email(
                token=new_row.token,
                gym_name=gym.name or str(payload.gym_id),
                subject=payload.subject,
                email=payload.email,
                issue=payload.issue,
            )
        except Exception as mail_err:
            logger.error(f"Failed to send owner support ticket email: {repr(mail_err)}")

        try:
            await send_owner_acknowledgment_email(
                token=new_row.token,
                gym_name=gym.name or str(payload.gym_id),
                subject=payload.subject,
                email=payload.email,
                issue=payload.issue,
            )
        except Exception as mail_err:
            logger.error(f"Failed to send owner acknowledgment email: {repr(mail_err)}")

        return {
            "status": 200,
            "message": "Support token generated",
            "data": new_row.token,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to generate support token",
            error_code="SUPPORT_TOKEN_CREATE_ERROR",
            log_data={"error": repr(e), "gym_id": payload.gym_id},
        )
