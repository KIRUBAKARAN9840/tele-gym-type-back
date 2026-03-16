from fastapi import APIRouter, Depends, HTTPException, FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.utils.redis_config import get_redis
from app.models.marketingmodels import Executives, Managers
from app.models.database import get_db
from typing import Optional
from dotenv import load_dotenv
import random
from redis.asyncio import Redis
from starlette.requests import Request
from starlette.responses import JSONResponse
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException
from jose import jwt, JWTError
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import random
import os
from email.message import EmailMessage
from app.utils.security import (
    verify_password, create_access_token, create_refresh_token,
    refresh_tokens_store, get_password_hash, SECRET_KEY, ALGORITHM
)
from app.utils.otp import generate_otp, async_send_verification_sms

load_dotenv()
app = FastAPI()

router = APIRouter(prefix="/marketing/auth", tags=["marketingAuthentication"])

logger = logging.getLogger("auth_middleware")


def send_verification_email(user_email, user_name, otp_code, validity_minutes, support_email):
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    msg = EmailMessage()
    subject = "Your OTP Code for Email Verification"
    body = f"""
    <html>
    <body>
        <p>Hello {user_name},</p>
 
        <p>Please use the following One-Time Password (OTP) to proceed with your email verification:</p>
 
        <h2 style="color: #007bff;">OTP Code: <b>{otp_code}</b></h2>
 
        <p>This code is valid for <b>{validity_minutes} minutes</b>.</p>
 
        <p>If you have any questions or need further assistance, please feel free to contact our support team at
        <a href="mailto:{support_email}">{support_email}</a>.</p>
 
        <p>Thank you!</p>
 
        <p>Best Regards,<br>
        <b>Fittbot Team</b></p>
    </body>
    </html>
    """
 
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
 
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT"))
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, user_email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

@router.get("/subscription-status")
async def check_subscrition_status(request:Request):
    auth_header = request.headers.get("Authorization")
    logger.debug("auth header is %s", auth_header)
 
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
   
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header format")
   
    token = parts[1]
    logger.debug("token is %s", token)
   
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])  
        user_id =payload.get('sub')
 
        if user_id is None:
            raise HTTPException(status_code=401, detail="Token missing subject (user_id)")
 
        return{
            "status":200, "message":"valid token"
        }
    except ExpiredSignatureError:
        return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    


@router.get("/verify")
async def verify_token(request:Request):
    auth_header = request.headers.get("Authorization")
    logger.debug("auth header is %s", auth_header)

    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header format")
    
    token = parts[1]
    logger.debug("token is %s", token)
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])        
        return{
            "status":200, "message":"valid token"
        }
    except ExpiredSignatureError:
        return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    

class refreshtoken(BaseModel):
    id:int
 
 
@router.post("/refresh")
async def refresh(request:refreshtoken,db: Session = Depends(get_db)):

    try:
        id=request.id
        print(f"🔄 [MARKETING REFRESH] Starting refresh for user_id: {id}")

        refresh_t=db.query(Executives).filter(Executives.id==id).first()

        if not refresh_t:
            print(f"❌ [MARKETING REFRESH] Executive not found with ID: {id}")
            raise HTTPException(status_code=404, detail="Executive not found")

        refresh_token=refresh_t.refresh_token
        print(f"✅ [MARKETING REFRESH] Executive found: {refresh_t.name}, Role: {refresh_t.role}")

        if not refresh_token:
            print(f"❌ [MARKETING REFRESH] No refresh token stored for user_id: {id}")
            raise HTTPException(status_code=401, detail="Refresh token not recognized or expired")

        print(f"🔍 [MARKETING REFRESH] Validating stored refresh token...")
        try:
            payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
            print(f"✅ [MARKETING REFRESH] Refresh token is valid, payload: {payload}")

        except jwt.ExpiredSignatureError:
            print(f"❌ [MARKETING REFRESH] Refresh token EXPIRED for user_id: {id}")
            raise HTTPException(status_code=401, detail="Refresh token expired")
        except JWTError as e:
            print(f"❌ [MARKETING REFRESH] Invalid refresh token for user_id: {id}, error: {str(e)}")
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        print(f"🔑 [MARKETING REFRESH] Generating new tokens for user_id: {id}, role: {refresh_t.role}")
        access_token = create_access_token({"sub": str(request.id), "role": refresh_t.role})
        refresh_token = create_refresh_token({"sub": str(request.id)})

        refresh_t.refresh_token=refresh_token
        db.commit()
        print(f"✅ [MARKETING REFRESH] SUCCESS! New tokens generated and saved for user_id: {id}")
        print(f"   📊 New access_token (first 20 chars): {access_token[:20]}...")

        return {"status":200,"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}

    except HTTPException as http_exc:
        print(f"❌ [MARKETING REFRESH] HTTPException: {http_exc.detail}")
        raise http_exc

    except Exception as e:
        print(f"❌ [MARKETING REFRESH] Unexpected error for user_id: {id if 'id' in locals() else 'unknown'}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")
    

class OTPRequest(BaseModel):
    data: str
 
@router.post("/resend-otp")
async def send_otp( request:OTPRequest, db: Session = Depends(get_db), redis: Redis = Depends(get_redis)):
    try:
        if await redis.exists(f"otp:{request.data}"):
            await redis.delete(f"otp:{request.data}")
        otp= generate_otp()
        print(otp)
        await redis.set(f"otp:{request.data}", otp, ex=300)  
 
        if "@" in request.data:
            owner = db.query(Executives).filter(Executives.email == request.data).first()
            if send_verification_email(request.data, owner.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
 
        else:
            if await async_send_verification_sms(request.data, otp):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
 
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")


class LoginRequest(BaseModel):
    mobile_number: str
 
@router.post("/login")
async def login(request: LoginRequest, db: Session = Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        mobile_number = request.mobile_number
  
        executives = db.query(Executives).filter(Executives.contact == mobile_number).first()
        manager = db.query(Managers).filter(Managers.contact == mobile_number).first()
        if not executives and not manager:
            raise HTTPException(status_code=400, detail="Mobile Number is not registered")
        if executives:
            mobile_otp= generate_otp()
           
            # if mobile_number=="9743555216":
            #     mobile_otp="123456"
            await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
                    
            if await async_send_verification_sms(mobile_number, mobile_otp):
                return {"status": 200,"message": "Otp Send successful"}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
            
        elif manager:
            mobile_otp= generate_otp()
            print("#####mobile_otp is for managers",mobile_otp)
            
            await redis.set(f"otp:{mobile_number}", mobile_otp, ex=300)
            if await async_send_verification_sms(mobile_number, mobile_otp):
                return {"status": 200,"message": "Otp Send successful"}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
            
    except HTTPException:
        raise

    except Exception as e:
      
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')
    


class verificationRequest(BaseModel):
    data:str
    otp:int


@router.post('/otp-verification')
async def otp_verification(request:verificationRequest, db:Session=Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        data=request.data
        otp=request.otp

        stored_otp = await redis.get(f"otp:{data}")  
        if stored_otp and stored_otp == str(otp):  
            await redis.delete(f"otp:{data}")
            user = db.query(Executives).filter(Executives.contact == data).first()
            if not user:
                user = db.query(Managers).filter(Managers.contact == data).first()
                
            access_token = create_access_token({"sub": str(user.id), "role": user.role})
            refresh_token = create_refresh_token({"sub": str(user.id)})
            user.refresh_token=refresh_token
            db.commit()

            return {
                "status": 200,
                "message": "Otp Send successful",
                "data": {
                    "user_id": user.id,
                    "role": user.role,
                    "name": user.name,
                    "access_token": access_token,
                    "refresh_token": refresh_token
                }
            }
       
        else:
            raise HTTPException(status_code=400, detail=f"Incorrect otp entered")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")
    

def send_sms(phone_number,otp):
    api_key = os.getenv("SMS_API_KEY")
    sender_id = os.getenv("SMS_SENDER_ID", "NFCFIT")
    entity_id = os.getenv("SMS_ENTITY_ID", "1701174022473316577")
    template_id = os.getenv("SMS_TEMPLATE_ID", "1707174038099354450")
    encoded_message = f"Your OTP for Reset Password is {otp}. Please Do not share this code with anyone.-NFCFIT"
 
 
    url = (
        f"http://pwtpl.com/sms/V1/send-sms-api.php?"
        f"apikey={api_key}&senderid={sender_id}&templateid={template_id}"
        f"&entityid={entity_id}&number={phone_number}&message={encoded_message}&format=json"
    )
 
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_response = response.json()
            print("SMS API Response:", json_response)
            if json_response.get('status') == 'OK':
                print("SMS sent successfully!")
                return True
            else:
                print("Failed to send SMS. Please check the API credentials and parameters.")
                return False
        else:
            print("HTTP error occurred:", response.status_code)
    except Exception as e:
        print("An error occurred:", e)


def send_otp_email(user_email, user_name, otp_code, validity_minutes, support_email):
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    msg = EmailMessage()
    subject = "Your OTP Code for Password Reset"
    body = f"""
    <html>
    <body>
        <p>Hello {user_name},</p>
 
        <p>We received a request to reset your password. Please use the following One-Time Password (OTP) to proceed:</p>
 
        <h3 style="color: #000000;">OTP Code: <b>{otp_code}</b></h3>
 
        <p>This code is valid for <b>{validity_minutes} minutes</b>. If you did not request a password reset, please ignore this email or contact our support team immediately.</p>
 
        <p>Thank you,</p>
 
        <p>If you need help, contact our support team: <a href="mailto:{support_email}">{support_email}</a></p>
    </body>
    </html>
    """
 
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
 
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT"))
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, user_email, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False



class forgotRequest(BaseModel):
    data: str
    type: Optional[str] = None 
 
 
@router.post("/send-otp")
async def send_otp( request:forgotRequest, db: Session = Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        if await redis.exists(f"otp:{request.data}"):
            await redis.delete(f"otp:{request.data}")
        data=request.data
        type=request.type
        if type=="email":
            user =  db.query(Executives).filter(Executives.email == data).first()
       
        if type=="mobile":
            user =  db.query(Executives).filter(Executives.contact == data).first()
 
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        otp= generate_otp()

        await redis.set(f"otp:{data}", otp, ex=300)  
 
        if type=="email":
            if send_otp_email(request.data, user.name, otp, validity_minutes = 5, support_email="support@fittbot.com"):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
        if type=="mobile":
            if send_sms(data, otp):
                return {"success": True, "message": "OTP sent successfully", "status": 200}
            else:
                raise HTTPException(status_code=500, detail="Failed to send OTP")
 
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")


class VerifyRequest(BaseModel):
    data: str
    otp: str

@router.post("/verify-otp")
async def verify_otp(request:VerifyRequest, redis:Redis=Depends(get_redis)):
    try:
        data=request.data
        otp=request.otp
        stored_otp = await redis.get(f"otp:{data}")  
        if stored_otp and stored_otp == str(otp):  
            await redis.delete(f"otp:{data}")  
            return {"success": True, "message": "OTP verified successfully", "status": 200}
        else:
            raise HTTPException(status_code=400, detail=f"Unable to Verify otp")
 
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred,.{str(e)}")
 

class ExpoTokenPayload(BaseModel):
    user_id: int
    expo_token: str
    
@router.post("/update_expo_token")
def update_expo_token(payload: ExpoTokenPayload, db: Session = Depends(get_db)):

    try:
        user = db.query(Executives).filter(Executives.id == payload.user_id).first()
        if not user:
            raise HTTPException(status_code=400, detail="user not found")

        current_tokens = user.expo_token if user.expo_token else []

        if not isinstance(current_tokens, list):
            current_tokens = [current_tokens]


        if payload.expo_token in current_tokens:
            return {"status":200,"message": "Expo token already exists"}

        current_tokens.append(payload.expo_token)
        
        user.expo_token = current_tokens

        db.commit()
        user = db.query(Executives).filter(Executives.id == payload.user_id).first()
        return {"status":200,"message": "Expo token added successfully"}
    
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')
