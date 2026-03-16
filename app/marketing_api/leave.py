from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models.database import get_db
from app.models.marketingmodels import Leave, Executives, Managers
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import time
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(tags=["Leave"], prefix="/marketing/leave")

# Pydantic models for request/response
class LeaveApplicationRequest(BaseModel):
    employee_id: str
    name: str
    role: str
    reason: str
    message: Optional[str] = None
    leave_from: Optional[str] = None
    leave_to: Optional[str] = None

class LeaveApplicationResponse(BaseModel):
    id: int
    employee_id: str
    name: str
    role: str
    reason: str
    message: Optional[str]
    status: str
    date_applied: datetime
    created_at: datetime
    leave_from: Optional[str] = None
    leave_to: Optional[str] = None

    @classmethod
    def from_orm(cls, obj):
        """Create response from ORM model with proper date formatting"""
        return cls(
            id=obj.id,
            employee_id=obj.employee_id,
            name=obj.name,
            role=obj.role,
            reason=obj.reason,
            message=obj.message,
            status=obj.status,
            date_applied=obj.date_applied,
            created_at=obj.created_at,
            leave_from=obj.leave_from.isoformat() if obj.leave_from else None,
            leave_to=obj.leave_to.isoformat() if obj.leave_to else None,
        )

    class Config:
        from_attributes = True

def send_leave_email(leave_data: LeaveApplicationResponse):
    
    try:
        print(f"📧 Email function called with data:")
        print(f"   Name: {leave_data.name}")
        print(f"   Role: {leave_data.role}")
        print(f"   Reason: {leave_data.reason}")
        print(f"   Leave From: {leave_data.leave_from}")
        print(f"   Leave To: {leave_data.leave_to}")
        print(f"   Message: {leave_data.message}")

        sender_email = os.getenv("SMTP_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")
        smtp_server = os.getenv("SMTP_SERVER", "smtp.office365.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))

        if not sender_email or not sender_password:
            print("SMTP credentials not found in environment variables")
            print(f"SMTP_EMAIL: {sender_email}")
            print(f"SMTP_PASSWORD: {'configured' if sender_password else 'missing'}")
            return False

        print(f"Attempting to send email using {smtp_server}:{smtp_port} with sender {sender_email}")

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = "shama@fittbot.com"
        msg['Subject'] = f"Leave Application - {leave_data.name} ({leave_data.role})"

        # Email body
        body = f"""
        <html>
        <body>
            <h2>New Leave Application</h2>
            <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <td><strong>Name:</strong></td>
                    <td>{leave_data.name}</td>
                </tr>
                <tr>
                    <td><strong>Role:</strong></td>
                    <td>{leave_data.role}</td>
                </tr>
                <tr>
                    <td><strong>Leave Reason:</strong></td>
                    <td>{leave_data.reason}</td>
                </tr>
                {f'<tr><td><strong>Leave Period:</strong></td><td>{leave_data.leave_from}{f" to {leave_data.leave_to}" if leave_data.leave_to else ""}</td></tr>' if leave_data.leave_from else ''}
                <tr>
                    <td><strong>Message:</strong></td>
                    <td>{leave_data.message or 'No additional message provided'}</td>
                </tr>
                <tr>
                    <td><strong>Date Applied:</strong></td>
                    <td>{leave_data.date_applied.strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
            </table>
            <br>
            <p>Please review this leave application and take appropriate action.</p>
            <p>Best regards,<br>Team Fittbot</p>
        </body>
        </html>
        """

        msg.attach(MIMEText(body, 'html'))

        # Send email using Office365 SMTP with retry logic
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(1, max_retries + 1):
            try:
                print(f"🔄 Attempt {attempt}/{max_retries} - Connecting to SMTP server...")

                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
                server.login(sender_email, sender_password)

                text = msg.as_string()
                server.sendmail(sender_email, "shama@fittbot.com", text)
                server.quit()

                print(f"✅ Leave application email sent successfully for {leave_data.name}")
                return True

            except smtplib.SMTPException as smtp_error:
                print(f"⚠️  SMTP error on attempt {attempt}/{max_retries}: {str(smtp_error)}")
                if attempt < max_retries:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"❌ All {max_retries} attempts failed for SMTP connection")
                    raise

            except Exception as conn_error:
                print(f"⚠️  Connection error on attempt {attempt}/{max_retries}: {str(conn_error)}")
                if attempt < max_retries:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"❌ All {max_retries} attempts failed")
                    raise

    except Exception as e:
        print(f"❌ Failed to send leave application email: {str(e)}")
        print(f"Error details: {type(e).__name__}: {e}")
        return False

@router.post("/apply", response_model=LeaveApplicationResponse)
async def apply_leave(request: LeaveApplicationRequest, user_id: int, role: str, db: Session = Depends(get_db)):

    try:
        # Parse date strings if provided
        leave_from_date = None
        leave_to_date = None

        if request.leave_from:
            try:
                leave_from_date = datetime.strptime(request.leave_from, '%Y-%m-%d').date()
            except ValueError:
                leave_from_date = None

        if request.leave_to:
            try:
                leave_to_date = datetime.strptime(request.leave_to, '%Y-%m-%d').date()
            except ValueError:
                leave_to_date = None

        # Create new leave application
        leave = Leave(
            employee_id=request.employee_id,
            name=request.name,
            role=request.role,
            reason=request.reason,
            message=request.message,
            leave_from=leave_from_date,
            leave_to=leave_to_date,
            status="Pending",
            date_applied=datetime.now()
        )

        db.add(leave)
        db.commit()
        db.refresh(leave)

        print(f"Leave application created: ID={leave.id}, Name={leave.name}, Role={leave.role}")

        # Create response with proper date formatting
        response_data = LeaveApplicationResponse.from_orm(leave)

        print(f"Response data: Name={response_data.name}, leave_from={response_data.leave_from}, leave_to={response_data.leave_to}")

        # Send email notification
        send_leave_email(response_data)

        return response_data

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit leave application: {str(e)}"
        )

@router.get("/applications/{employee_id}", response_model=list[LeaveApplicationResponse])
async def get_employee_leave_applications(
    employee_id: str,
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    limit: int = 50,
    offset: int = 0
):
    """
    Get leave applications for an employee
    """
    try:
        query = db.query(Leave).filter(Leave.employee_id == employee_id)
        leave_applications = query.order_by(desc(Leave.created_at)).offset(offset).limit(limit).all()

        return leave_applications

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get leave applications: {str(e)}"
        )

@router.get("/all", response_model=list[LeaveApplicationResponse])
async def get_all_leave_applications(
    user_id: int,
    role: str,
    db: Session = Depends(get_db),
    limit: int = 100,
    offset: int = 0,
    status_filter: Optional[str] = None
):
    """
    Get all leave applications (for managers/admins)
    """
    try:
        query = db.query(Leave)

        if status_filter and status_filter in ["Pending", "Approved", "Rejected"]:
            query = query.filter(Leave.status == status_filter)

        leave_applications = query.order_by(desc(Leave.created_at)).offset(offset).limit(limit).all()

        return leave_applications

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get leave applications: {str(e)}"
        )

@router.put("/{leave_id}/status")
async def update_leave_status(
    leave_id: int,
    status: str,
    user_id: int,
    role: str,
    db: Session = Depends(get_db)
):
    """
    Update leave application status (for managers/admins)
    """
    try:
        if status not in ["Pending", "Approved", "Rejected"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid status. Must be one of: Pending, Approved, Rejected"
            )

        leave = db.query(Leave).filter(Leave.id == leave_id).first()
        if not leave:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Leave application not found"
            )

        leave.status = status
        leave.updated_at = datetime.now()
        db.commit()
        db.refresh(leave)

        return {"message": f"Leave application status updated to {status}", "leave_id": leave_id}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update leave status: {str(e)}"
        )