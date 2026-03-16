from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models.database import get_db
from app.models.telecaller_models import LeaveApplication, Manager, Telecaller
from app.telecaller.dependencies import get_current_manager
from pydantic import BaseModel
from typing import Optional, Union
from datetime import datetime, date
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import time
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(tags=["telecaller-leave"], prefix="/leave")

# Pydantic models for request/response
class LeaveApplicationRequest(BaseModel):
    mobile_number: str
    name: str
    role: str
    reason: str
    message: str
    leave_from: str
    leave_to: Optional[str] = None

class LeaveApplicationResponse(BaseModel):
    id: int
    mobile_number: str
    name: str
    role: str
    reason: str
    message: Optional[str]
    status: str
    date_applied: datetime
    created_at: datetime
    leave_from: Optional[str] = None
    leave_to: Optional[str] = None
    manager_id: Optional[int] = None
    telecaller_id: Optional[int] = None

    @classmethod
    def from_orm(cls, obj):
        """Create response from ORM model with proper date formatting"""
        return cls(
            id=obj.id,
            mobile_number=obj.mobile_number,
            name=obj.name,
            role=obj.role,
            reason=obj.reason,
            message=obj.message,
            status=obj.status,
            date_applied=obj.date_applied,
            created_at=obj.created_at,
            leave_from=obj.leave_from.isoformat() if obj.leave_from else None,
            leave_to=obj.leave_to.isoformat() if obj.leave_to else None,
            manager_id=obj.manager_id,
            telecaller_id=obj.telecaller_id,
        )

    class Config:
        from_attributes = True


def send_leave_email(leave_data: LeaveApplicationResponse):
   
    try:
        sender_email = os.getenv("SMTP_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")
        smtp_server = os.getenv("SMTP_SERVER", "smtp.office365.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))

        if not sender_email or not sender_password:
            return False

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = "kirubakaran9540@gmail.com"
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
            <p>Best regards,<br>Team Fymble</p>
        </body>
        </html>
        """

        msg.attach(MIMEText(body, 'html'))

        # Send email using Office365 SMTP with retry logic
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(1, max_retries + 1):
            try:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.starttls()
                server.login(sender_email, sender_password)

                text = msg.as_string()
                server.sendmail(sender_email, "kirubakaran9540@gmail.com", text)
                server.quit()

                return True

            except smtplib.SMTPException as smtp_error:
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise

            except Exception as conn_error:
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise

    except Exception as e:
        return False


async def get_current_user(
    request: Request,
    db: Session = Depends(get_db)
) -> Union[Manager, Telecaller]:
    """
    Get current user (Manager or Telecaller) from JWT token
    """
    from jose import jwt, JWTError
    from app.utils.security import SECRET_KEY, ALGORITHM
    from sqlalchemy import select

    # Get token from cookie
    access_token = request.cookies.get("access_token")
    if not access_token:
        # Fallback to Authorization header if no cookie
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            access_token = auth_header.split(" ")[1]
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        mobile_number: str = payload.get("sub")
        role: str = payload.get("role")
        user_id: int = payload.get("id")
        user_type: str = payload.get("type")

        if user_type != "telecaller":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        if role == "manager":
            # Get manager from database
            stmt = select(Manager).where(Manager.id == user_id)
            result = db.execute(stmt)
            user = result.scalar_one_or_none()
        elif role == "telecaller":
            # Get telecaller from database
            stmt = select(Telecaller).where(Telecaller.id == user_id)
            result = db.execute(stmt)
            user = result.scalar_one_or_none()
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

        return user

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )


@router.post("/apply")
async def apply_leave(
    body_request: LeaveApplicationRequest,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Apply for leave - works for both Manager and Telecaller
    - Manager applies: manager_id is set, telecaller_id is NULL
    - Telecaller applies: both manager_id and telecaller_id are set
    """
    try:
        # Get current user
        current_user = await get_current_user(request, db)

        # Parse date strings if provided
        leave_from_date = None
        leave_to_date = None

        if body_request.leave_from:
            try:
                leave_from_date = datetime.strptime(body_request.leave_from, '%Y-%m-%d').date()
            except ValueError:
                leave_from_date = None

        if body_request.leave_to:
            try:
                leave_to_date = datetime.strptime(body_request.leave_to, '%Y-%m-%d').date()
            except ValueError:
                leave_to_date = None

        # Determine manager_id and telecaller_id based on user type
        manager_id = None
        telecaller_id = None

        if isinstance(current_user, Manager):
            # Manager applying for leave
            manager_id = current_user.id
            # telecaller_id remains None
        elif isinstance(current_user, Telecaller):
            # Telecaller applying for leave
            manager_id = current_user.manager_id
            telecaller_id = current_user.id

        # Create new leave application
        leave = LeaveApplication(
            manager_id=manager_id,
            telecaller_id=telecaller_id,
            mobile_number=body_request.mobile_number,
            name=body_request.name,
            role=body_request.role,
            reason=body_request.reason,
            message=body_request.message,
            leave_from=leave_from_date,
            leave_to=leave_to_date,
            status="Pending",
            date_applied=datetime.now()
        )

        db.add(leave)
        db.commit()
        db.refresh(leave)

        # Create response with proper date formatting
        response_data = LeaveApplicationResponse.from_orm(leave)

        # Send email notification
        send_leave_email(response_data)

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit leave application: {str(e)}"
        )


# @router.get("/my-applications")
# async def get_my_leave_applications(
#     request: Request,
#     db: Session = Depends(get_db),
#     limit: int = 50,
#     offset: int = 0
# ):
#     """
#     Get leave applications for the current user (Manager or Telecaller)
#     """
#     try:
#         # Get current user
#         current_user = await get_current_user(request, db)

#         # Query based on user type
#         if isinstance(current_user, Manager):
#             # Manager's leave applications
#             query = db.query(LeaveApplication).filter(
#                 LeaveApplication.manager_id == current_user.id,
#                 LeaveApplication.telecaller_id.is_(None)  # Only manager's own applications
#             )
#         elif isinstance(current_user, Telecaller):
#             # Telecaller's leave applications
#             query = db.query(LeaveApplication).filter(
#                 LeaveApplication.telecaller_id == current_user.id
#             )

#         leave_applications = query.order_by(desc(LeaveApplication.created_at)).offset(offset).limit(limit).all()

#         return [
#             LeaveApplicationResponse.from_orm(leave)
#             for leave in leave_applications
#         ]

#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to get leave applications: {str(e)}"
#         )


# @router.get("/all")
# async def get_all_leave_applications(
#     db: Session = Depends(get_db),
#     current_manager: Manager = Depends(get_current_manager),
#     limit: int = 100,
#     offset: int = 0,
#     status_filter: Optional[str] = None
# ):
#     """
#     Get all leave applications (for managers only)
#     """
#     try:
#         query = db.query(LeaveApplication)

#         if status_filter and status_filter in ["Pending", "Approved", "Rejected"]:
#             query = query.filter(LeaveApplication.status == status_filter)

#         leave_applications = query.order_by(desc(LeaveApplication.created_at)).offset(offset).limit(limit).all()

#         return [
#             LeaveApplicationResponse.from_orm(leave)
#             for leave in leave_applications
#         ]

#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to get leave applications: {str(e)}"
#         )


# @router.put("/{leave_id}/status")
# async def update_leave_status(
#     leave_id: int,
#     status: str,
#     db: Session = Depends(get_db),
#     current_manager: Manager = Depends(get_current_manager)
# ):
#     """
#     Update leave application status (for managers only)
#     """
#     try:
#         if status not in ["Pending", "Approved", "Rejected"]:
#             raise HTTPException(
#                 status_code=status.HTTP_400_BAD_REQUEST,
#                 detail="Invalid status. Must be one of: Pending, Approved, Rejected"
#             )

#         leave = db.query(LeaveApplication).filter(LeaveApplication.id == leave_id).first()
#         if not leave:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="Leave application not found"
#             )

#         leave.status = status
#         leave.updated_at = datetime.now()
#         db.commit()
#         db.refresh(leave)

#         return {"message": f"Leave application status updated to {status}", "leave_id": leave_id}

#     except HTTPException:
#         raise
#     except Exception as e:
#         db.rollback()
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to update leave status: {str(e)}"
#         )
