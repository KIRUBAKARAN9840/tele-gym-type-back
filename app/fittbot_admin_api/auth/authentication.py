from fastapi import APIRouter, Depends, HTTPException, FastAPI, Response, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from app.models.adminmodels import Admins, Employees
from app.models.database import get_db
from typing import Optional
from starlette.requests import Request
from starlette.responses import JSONResponse
from jose.exceptions import ExpiredSignatureError
from jose import jwt, JWTError
import logging
from app.utils.security import (
    verify_password, create_access_token, create_refresh_token,
    SECRET_KEY, ALGORITHM, get_password_hash
)
from app.utils.otp import generate_otp, send_verification_sms
from app.config.settings import settings
from datetime import datetime, timedelta

app = FastAPI()
 
router = APIRouter(prefix="/api/admin/auth", tags=["AdminAuthentication"])
logger = logging.getLogger("auth_middleware")
logger.setLevel(logging.DEBUG)






class LoginRequest(BaseModel):
    mobile_number: str
    password: str


class SendOTPRequest(BaseModel):
    mobile_number: str


class VerifyOTPRequest(BaseModel):
    mobile_number: str
    otp: str


class ChangePasswordRequest(BaseModel):
    mobile_number: str
    new_password: str


@router.post("/login")
async def login(request: LoginRequest, db: Session = Depends(get_db)):
    try:
        mobile_number = request.mobile_number
        password = request.password

        admin = db.query(Admins).filter(Admins.contact_number == mobile_number).first()

        if not admin:
            raise HTTPException(status_code=400, detail="You are not Authorised")

        # Determine user type and get user object
        user = admin
        user_type = "admin" if admin.role=="admin" else "support"
        user_id = admin.admin_id
        stored_password = admin.password
        role = admin.role

        # Verify password
        if not stored_password:
            raise HTTPException(status_code=400, detail="Password not set for this account")

        if not verify_password(password, stored_password):
            raise HTTPException(status_code=401, detail="Invalid password")

        # Create tokens
        access_token = create_access_token({"sub": str(admin.admin_id), "role": role, "user_type": "admin"})
        refresh_token = create_refresh_token({"sub": str(admin.admin_id), "user_type": "admin"})

        # Save refresh token to database
        user.refresh_token = refresh_token
        db.commit()

        response_data = {
            "status": 200,
            "message": "Login successful",
            "data": {
                "user_id": admin.admin_id,
                "role": role,
                "name": admin.name,
                "user_type": "admin"
            },
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }

        # Set HTTP-only cookies (web-only flow)
        json_response = JSONResponse(content=response_data)
        json_response.set_cookie(
            key="access_token",
            value=access_token,
            max_age=3600,  # 1 hour
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        json_response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            max_age=604800,  # 7 days
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        return json_response

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')


@router.post("/send_otp")
async def send_otp(request: SendOTPRequest, db: Session = Depends(get_db)):
    """Send OTP for password reset"""
    try:
        mobile_number = request.mobile_number

        # Check if admin exists
        admin = db.query(Admins).filter(Admins.contact_number == mobile_number).first()
        if not admin:
            raise HTTPException(status_code=404, detail="Admin not found with this mobile number")

        # Generate OTP
        otp = generate_otp()

        # Store OTP in admin record with expiration (5 minutes)
        admin.otp = otp
        admin.expires_at = datetime.now() + timedelta(minutes=5)
        db.commit()

        # Send OTP via SMS
        sms_sent = send_verification_sms(mobile_number, otp)

        if not sms_sent:
            raise HTTPException(status_code=500, detail="Failed to send OTP. Please try again.")

        return {
            "status": 200,
            "message": "OTP sent successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/verify_otp")
async def verify_otp(request: VerifyOTPRequest, db: Session = Depends(get_db)):
    """Verify OTP"""
    try:
        mobile_number = request.mobile_number
        otp = request.otp

        # Get admin
        admin = db.query(Admins).filter(Admins.contact_number == mobile_number).first()
        if not admin:
            raise HTTPException(status_code=404, detail="Admin not found")

        # Check if OTP exists
        if not admin.otp:
            raise HTTPException(status_code=400, detail="OTP not found. Please request a new OTP.")

        # Check if OTP has expired
        if not admin.expires_at or datetime.now() > admin.expires_at:
            admin.otp = None
            admin.expires_at = None
            db.commit()
            raise HTTPException(status_code=400, detail="OTP has expired. Please request a new OTP.")

        # Verify OTP
        if otp != admin.otp:
            raise HTTPException(status_code=400, detail="Invalid OTP")

        return {
            "status": 200,
            "message": "OTP verified successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/change_password")
async def change_password(request: ChangePasswordRequest, db: Session = Depends(get_db)):
    """Change password after OTP verification"""
    try:
        mobile_number = request.mobile_number
        new_password = request.new_password

        # Validate password length
        if len(new_password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters long")

        # Get admin
        admin = db.query(Admins).filter(Admins.contact_number == mobile_number).first()
        if not admin:
            raise HTTPException(status_code=404, detail="Admin not found")

        # Hash and update the new password
        hashed_password = get_password_hash(new_password)
        admin.password = hashed_password
        admin.otp = None
        admin.expires_at = None

        db.commit()

        return {
            "status": 200,
            "message": "Password changed successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/token-status")
async def check_token_status(request:Request):
    auth_header = request.headers.get("Authorization")
 
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
   
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header format")
   
    token = parts[1]
   
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])  
        admin_id =payload.get('sub')
 
        if admin_id is None:
            raise HTTPException(status_code=401, detail="Token missing subject (admin_id)")
 
        return{
            "status":200, "message":"valid token"
        }
    except ExpiredSignatureError:
        return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    


@router.get("/verify")
async def verify_token(
    request: Request,
    device: Optional[str] = Query(None),
    role: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Verify authentication token for admin portal.
    For admin access, device should be 'web' (cookies used).
    Employees can use mobile or web.
    """
    normalized_device = (device or "").strip().lower()
    requested_role = (role or "").strip().lower()

    # Admin access is web-only
    if requested_role == "admin" and normalized_device and normalized_device != "web":
        raise HTTPException(
            status_code=403,
            detail="Admin access is only available via web"
        )

    # Check for access token in cookies (web) or header (mobile)
    access_token = request.cookies.get("access_token")
    token_source = "cookie" if access_token else None

    if not access_token:
        # Check Authorization header for mobile/employee access
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(
                status_code=401,
                detail="No authentication method found"
            )

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise HTTPException(
                status_code=401,
                detail="Invalid authorization header format"
            )

        access_token = parts[1]
        token_source = "header"

    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        token_role = payload.get("role")
        user_type = payload.get("user_type")

        # Verify role matches if specified
        if requested_role:
            if requested_role == "admin" and user_type != "admin":
                raise HTTPException(
                    status_code=403,
                    detail="Admin access required"
                )
            elif requested_role != "admin" and token_role != requested_role:
                raise HTTPException(
                    status_code=403,
                    detail=f"Role mismatch: expected {requested_role}"
                )

        # Get user details based on user_type
        user_data = None
        if user_type == "admin":
            admin = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
            if admin:
                user_data = {
                    "user_id": admin.admin_id,
                    "name": admin.name,
                    "role": "admin",
                    "user_type": "admin"
                }
        elif user_type == "employee":
            employee = db.query(Employees).filter(Employees.id == int(user_id)).first()
            if employee:
                user_data = {
                    "user_id": employee.id,
                    "name": employee.name,
                    "role": employee.role,
                    "user_type": "employee"
                }

        response = {
            "status": 200,
            "message": "valid token"
        }

        if user_data:
            response["data"] = user_data

        return response

    except ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Session expired, Please Login again"
        )
    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Invalid token"
        )
    

class refreshtoken(BaseModel):
    id: int
    user_type: str  

@router.post("/refresh")
async def refresh(request:refreshtoken,db: Session = Depends(get_db)):
 
    try:
        id = request.id
        user_type = request.user_type
 
        if user_type == "admin":
            user = db.query(Admins).filter(Admins.admin_id == id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Admin not found")
        elif user_type == "employee":
            user = db.query(Employees).filter(Employees.id == id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Employee not found")
        else:
            raise HTTPException(status_code=400, detail="Invalid user type")

        refresh_token = user.refresh_token

        if not refresh_token:
            raise HTTPException(status_code=401, detail="Refresh token not recognized or expired")
 
        try:
            payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
 
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Refresh token expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
       
       
        access_token = create_access_token({"sub": str(request.id), "role": user.role, "user_type": user_type})
        refresh_token = create_refresh_token({"sub": str(request.id), "user_type": user_type})
 
        user.refresh_token = refresh_token
        db.commit()
       
        # ✅ FIXED: Set cookies in response (was missing before!)
        json_response = JSONResponse(content={
            "status":200,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        })

        # Set access_token cookie
        json_response.set_cookie(
            key="access_token",
            value=access_token,
            max_age=3600,  # 1 hour
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        # Set refresh_token cookie
        json_response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            max_age=604800,  # 7 days
            httponly=True,
            secure=settings.cookie_secure,
            domain=settings.cookie_domain_value,
            samesite=settings.cookie_samesite_value,
        )

        return json_response
 
    except HTTPException as http_exc:
        raise http_exc
   
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")
    



@router.post("/logout")
async def logout(response: Response, request: Request, db: Session = Depends(get_db)):
    """Clear httpOnly cookies and logout"""
    try:
        access_token = request.cookies.get("access_token")
        
        if access_token:
            try:
                payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
                user_id = payload.get("sub")
                user_type = payload.get("user_type")
                
                if user_id:
                    if user_type == "admin":
                        user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
                    elif user_type == "employee":
                        user = db.query(Employees).filter(Employees.id == int(user_id)).first()
                    else:
                        user = None
                    
                    if user:
                        user.refresh_token = None
                        db.commit()
            except (JWTError, ValueError):
                pass
        
        
        
        return {
            "status": 200,
            "message": "Logged out successfully"
        }
        
    except Exception as e:
        return {
            "status": 200,
            "message": "Logged out successfully"
        }


@router.post("/refresh-cookie")
async def refresh_cookie(request: Request, response: Response, db: Session = Depends(get_db)):
    """Refresh access token using httpOnly refresh cookie"""
    try:
        refresh_token = request.cookies.get("refresh_token")

        if not refresh_token:
            raise HTTPException(status_code=401, detail="No refresh token found")

        try:
            payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = payload.get("sub")
            user_type = payload.get("user_type")

            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid refresh token")

            if user_type == "admin":
                user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
                if not user or user.refresh_token != refresh_token:
                    raise HTTPException(status_code=401, detail="Refresh token not recognized")
            elif user_type == "employee":
                user = db.query(Employees).filter(Employees.id == int(user_id)).first()
                if not user or user.refresh_token != refresh_token:
                    raise HTTPException(status_code=401, detail="Refresh token not recognized")
            else:
                raise HTTPException(status_code=400, detail="Invalid user type")

            new_access_token = create_access_token({"sub": str(user_id), "role": user.role, "user_type": user_type})

            json_response = JSONResponse(content={"status": 200, "message": "Token refreshed successfully"})

            # ⚠️ CRITICAL: Set the new access token in cookie!
            json_response.set_cookie(
                key="access_token",
                value=new_access_token,
                max_age=3600,  # 1 hour
                httponly=True,
                secure=settings.cookie_secure,
                domain=settings.cookie_domain_value,
                samesite=settings.cookie_samesite_value,
            )

            return json_response

        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Refresh token expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@router.get("/profile")
async def get_profile(request: Request, db: Session = Depends(get_db)):
    """Get current user profile data using httpOnly access_token cookie"""
    try:
        access_token = request.cookies.get("access_token")
        
        if not access_token:
            raise HTTPException(status_code=401, detail="No access token found")
        
        try:
            payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = payload.get("sub")
            user_type = payload.get("user_type")
            
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid access token")
            
            # Get user from database based on user type
            if user_type == "admin":
                user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
                if not user:
                    raise HTTPException(status_code=404, detail="Admin not found")
                
                return {
                    "status": 200,
                    "message": "Profile fetched successfully",
                    "data": {
                        "user_id": user.admin_id,
                        "name": user.name,
                        "email": user.email,
                        "role": user.role,
                        "contact_number": user.contact_number,
                        "user_type": "admin",
                        "created_at": user.created_at.isoformat() if user.created_at else None
                    }
                }
            
            elif user_type == "employee":
                user = db.query(Employees).filter(Employees.id == int(user_id)).first()
                if not user:
                    raise HTTPException(status_code=404, detail="Employee not found")
                
                return {
                    "status": 200,
                    "message": "Profile fetched successfully",
                    "data": {
                        "user_id": user.id,
                        "name": user.name,
                        "email": user.email,
                        "role": user.role,
                        "contact": user.contact,
                        "department": user.department,
                        "designation": user.designation,
                        "employee_id": user.employee_id,
                        "user_type": "employee",
                        "manager_role":user.manager_role,
                        "created_at": user.created_at.isoformat() if user.created_at else None
                    }
                }
            else:
                raise HTTPException(status_code=400, detail="Invalid user type")
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Access token expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid access token")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


async def get_current_user_from_cookie(request: Request, db: Session = Depends(get_db)):
    """Helper function to get current user (admin or employee) from httpOnly cookie"""
    access_token = request.cookies.get("access_token")
    
    if not access_token:
        raise HTTPException(status_code=401, detail="No access token found")
    
    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        user_type = payload.get("user_type")
        
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    
        if user_type == "admin":
            user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
            if not user:
                raise HTTPException(status_code=404, detail="Admin not found")
        elif user_type == "employee":
            user = db.query(Employees).filter(Employees.id == int(user_id)).first()
            if not user:
                raise HTTPException(status_code=404, detail="Employee not found")
        else:
            raise HTTPException(status_code=400, detail="Invalid user type")
            
        return user, user_type
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Access token expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_admin_from_cookie(request: Request, db: Session = Depends(get_db)):
    user, user_type = await get_current_user_from_cookie(request, db)
    if user_type != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


async def get_current_employee_for_support(request: Request, db: Session = Depends(get_db)):
    try:
        access_token = request.cookies.get("access_token")
        
        if not access_token:
            raise HTTPException(status_code=401, detail="No access token found")
        
        try:
            payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id = payload.get("sub")
            user_type = payload.get("user_type")
            
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid access token")
            
            if user_type == "admin":
                user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
                if not user:
                    raise HTTPException(status_code=404, detail="Admin not found")
                
                # Admins are always managers in support system
                return {
                    "id": user.admin_id,
                    "name": user.name,
                    "email": user.email,
                    "role": user.role,
                    "department": "Admin",
                    "designation": "Administrator",
                    "manager_role": True,  # Admins always have manager access
                    "user_type": "admin",
                    "contact": user.contact_number
                }
            
            elif user_type == "employee":
                user = db.query(Employees).filter(Employees.id == int(user_id)).first()
                if not user:
                    raise HTTPException(status_code=404, detail="Employee not found")
                
                # Check if employee has access
                if not user.access or user.status != "active":
                    raise HTTPException(status_code=403, detail="Employee access denied")
                
                return {
                    "id": user.id,
                    "name": user.name,
                    "email": user.email,
                    "role": user.role,
                    "department": user.department or "General",
                    "designation": user.designation or "Employee",
                    "manager_role": user.manager_role or False,
                    "user_type": "employee",
                    "contact": user.contact,
                    "employee_id": user.employee_id
                }
            else:
                raise HTTPException(status_code=400, detail="Invalid user type")
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Access token expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid access token")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Authentication error: {str(e)}")

async def require_manager_role_for_support(request: Request, db: Session = Depends(get_db)):
    current_employee = await get_current_employee_for_support(request, db)
    
    if not current_employee.get("manager_role", False):
        raise HTTPException(
            status_code=403, 
            detail="Manager access required for this operation"
        )
    
    return current_employee

async def require_support_access(request: Request, db: Session = Depends(get_db)):
    current_employee = await get_current_employee_for_support(request, db)
    
    # Admins and managers always have access
    if current_employee.get("manager_role", False):
        return current_employee
    
    # Check if employee is in support department or has support role
    department = current_employee.get("department", "").lower()
    role = current_employee.get("role", "").lower()
    
    if "support" not in department and "support" not in role:
        raise HTTPException(
            status_code=403, 
            detail="Support team access required"
        )
    
    return current_employee

async def check_ticket_access_permission(
    ticket_id: int, 
    ticket_source: str, 
    current_employee: dict,
    db: Session
):
    if current_employee.get("manager_role", False):
        return True
    
    # Non-managers can only access tickets assigned to them
    from app.models.adminmodels import TicketAssignment  # Import your TicketAssignment model
    
    assignment = db.query(TicketAssignment).filter(
        and_(
            TicketAssignment.ticket_id == ticket_id,
            TicketAssignment.ticket_source == ticket_source,
            TicketAssignment.employee_id == current_employee.get("id"),
            TicketAssignment.status == "active"
        )
    ).first()
    
    return assignment is not None

# Additional helper function for getting user info in a format compatible with your existing system
async def get_current_user_info(request: Request, db: Session = Depends(get_db)):
    """
    Get current user info in a standardized format for both admin and employee
    """
    try:
        access_token = request.cookies.get("access_token")
        
        if not access_token:
            raise HTTPException(status_code=401, detail="No access token found")
        
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        user_type = payload.get("user_type")
        
        if user_type == "admin":
            user = db.query(Admins).filter(Admins.admin_id == int(user_id)).first()
            if not user:
                raise HTTPException(status_code=404, detail="Admin not found")
            
            return {
                "user_id": user.admin_id,
                "name": user.name,
                "email": user.email,
                "role": user.role,
                "user_type": "admin",
                "is_manager": True
            }
        
        elif user_type == "employee":
            user = db.query(Employees).filter(Employees.id == int(user_id)).first()
            if not user:
                raise HTTPException(status_code=404, detail="Employee not found")
            
            return {
                "user_id": user.id,
                "name": user.name,
                "email": user.email,
                "role": user.role,
                "user_type": "employee",
                "is_manager": user.manager_role or False,
                "department": user.department,
                "designation": user.designation
            }
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Access token expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid access token")

# Add this route to check current user's permissions
@router.get("/permissions")
async def get_user_permissions(request: Request, db: Session = Depends(get_db)):
    """Get current user's permissions for the support system"""
    try:
        current_employee = await get_current_employee_for_support(request, db)
        
        permissions = {
            "can_view_all_tickets": current_employee.get("manager_role", False),
            "can_assign_tickets": current_employee.get("manager_role", False),
            "can_manage_employees": current_employee.get("manager_role", False),
            "can_view_statistics": True,
            "support_access": True,
            "user_info": {
                "id": current_employee.get("id"),
                "name": current_employee.get("name"),
                "role": current_employee.get("role"),
                "department": current_employee.get("department"),
                "is_manager": current_employee.get("manager_role", False)
            }
        }
        
        return {
            "status": 200,
            "message": "Permissions fetched successfully",
            "data": permissions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching permissions: {str(e)}")

# Add this route to get support team members (for managers only)
@router.get("/support-team")
async def get_support_team(request: Request, db: Session = Depends(get_db)):
    """Get list of support team members (managers only)"""
    try:
        current_employee = await require_manager_role_for_support(request, db)
        
        # Get all active employees in support department or with support roles
        support_employees = db.query(Employees).filter(
            and_(
                Employees.status == "active",
                Employees.access == True,
                or_(
                    Employees.department.ilike("%support%"),
                    Employees.role.ilike("%support%")
                )
            )
        ).all()
        
        # Also include admins as they can handle support tickets
        admins = db.query(Admins).all()
        
        team_members = []
        
        # Add employees
        for emp in support_employees:
            team_members.append({
                "id": emp.id,
                "name": emp.name,
                "email": emp.email,
                "department": emp.department or "Support",
                "designation": emp.designation or "Support Staff",
                "user_type": "employee",
                "manager_role": emp.manager_role or False,
                "avatar": get_employee_avatar(emp.name)
            })
        
        # Add admins
        for admin in admins:
            team_members.append({
                "id": admin.admin_id,
                "name": admin.name,
                "email": admin.email,
                "department": "Administration",
                "designation": "Administrator",
                "user_type": "admin",
                "manager_role": True,
                "avatar": get_employee_avatar(admin.name)
            })
        
        return {
            "status": 200,
            "message": "Support team fetched successfully",
            "data": team_members
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching support team: {str(e)}")

def get_employee_avatar(name: str) -> str:
    """Generate avatar initials from name"""
    if not name:
        return "NA"
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}{parts[1][0]}".upper()
    return name[:2].upper()
