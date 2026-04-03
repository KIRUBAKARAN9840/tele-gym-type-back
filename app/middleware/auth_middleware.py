from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from jose import jwt, JWTError
from app.utils.security import SECRET_KEY, ALGORITHM

import logging

logger = logging.getLogger("auth_middleware")


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        
        
        if request.scope["type"] == "websocket":
            return await call_next(request)

        if request.url.path.startswith("/websocket_feed/ws"):
            return await call_next(request)

        # Skip auth for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)

        public_paths = ["/app_open/track","/owner/new_registration/register","/owner/new_registration/login","/owner/new_registration/otp_verification",
            # Monitoring & Health (must be public for Prometheus/ECS health checks)
            "/metrics","/monitoring","/health","/docs","/redoc","/openapi.json",
            # Webhooks & Auth
            "/whatsapp/send","/whatsapp/dlr","/whatsapp/dlr-debug","/whatsapp/dlr-reports","/auth/login","/auth/refresh","/razorpay/payments","/razorpay/webhook","/razorpay_payments/webhooks/razorpay","/auth/send-otp","/auth/verify-otp","/auth/change-password","/auth/update_verification_status","/auth/send_verification_otp","/auth/new_gym_owner_registration","/auth/resend-otp","/client/combined_summary","/owner/create_post",
            "/auth/register-user","/attendance/out_punch","/websocket_feed/internal/new_post","/websocket_feed/internal/invalidate_cache","/auth/otp-verification", "/auth/verify-client-otp", "/auth/complete-registeration", "/auth/verify", "/auth/subscription-status","/feed/create_presigned_url","/auth/check-mobile-availability","/gym_photos/registration-presigned-urls","/gym_photos/registration-confirm","/auth/validate-gym-referral-code","/auth/validate-referral-code","/auth/check-trainer","/auth/set-trainer-password",
            "/chatbot","/analysis","/workout_template","/food_log","/food_template","/workout_log","/food_scanner","/owner/create_post","/owner/get_post","/webhooks/razorpay","/revenuecat/webhooks","/razorpay_payments/webhooks/razorpay","/marketing/auth/login","/marketing/auth/otp-verification","/marketing/auth/refresh","/marketing/auth/resend-otp","/razorpay_payments_v2/webhook","/revenuecat_v2/webhooks","/owner_registration/register","/owner_registration/check_already_registered","/owner_registration/verify-otp","/owner_registration/resend-otp","/owner_registration/document-steps","/owner_registration/update-gym-data","/owner_registration/account-details","/owner_registration/document-upload-url","/owner_registration/document-confirm","/owner_registration/get-documents",
            "/admin/gym-agreement","/redirect/check","/whatsapp/test/template","/whatsapp/test/raw",
            "/api/v1/load-test","/client/new_registration","/notifications/send-rich"]
        
        public_exact_paths = {"/"}


        admin_auth_paths=["/api/admin/auth/login", "/api/admin/auth/2fa/verify", "/api/admin/auth/otp-verification", "/api/admin/auth/resend-otp",
            "/api/admin/auth/logout", "/api/admin/auth/refresh-cookie", "/api/admin/auth/verify",
            "/api/admin/auth/profile", "/api/admin/auth/test-cookies","/api/admin/auth/send_otp","/api/admin/auth/change_password","/api/admin/auth/verify_otp",
            "/api/admin/auth/permissions", "/api/admin/auth/support-team", "/api/admin/auth/token-status",
            "/api/admin/auth/totp/setup", "/api/admin/auth/totp/verify", "/api/admin/auth/totp/enable",
            "/api/admin/auth/totp/disable", "/api/admin/auth/totp/status"]

        telecaller_auth_paths = [
            "/telecaller/manager/send-otp",
            "/telecaller/manager/verify-otp",
            "/telecaller/manager/logout",
            "/telecaller/manager/session-status",
            "/telecaller/telecaller/send-otp",
            "/telecaller/telecaller/verify-otp",
            "/telecaller/telecaller/logout",
            "/telecaller/telecaller/session-status",
        ]

        current_path = request.url.path
        if current_path in public_exact_paths or any(current_path.startswith(path) for path in public_paths) or current_path in admin_auth_paths or current_path in telecaller_auth_paths:            
            return await call_next(request)

        token = None
        auth_method = None

        auth_header = request.headers.get("Authorization")

        if auth_header:
            parts = auth_header.split()
            if len(parts) != 2 or parts[0].lower() != "bearer":
                logger.warning(
                    "[auth-middleware] Invalid auth header format",
                    extra={
                        "path": current_path,
                        "method": request.method,
                        "client_ip": request.client.host if request.client else "unknown",
                        "user_agent": request.headers.get("user-agent", "unknown"),
                        "origin": request.headers.get("origin", "unknown"),
                    }
                )
                return JSONResponse(status_code=401, content={"detail": "Invalid authorization header format"})
            token = parts[1]
            auth_method = "header"
        else:
            access_token = request.cookies.get("access_token")
            if access_token:
                token = access_token
                auth_method = "cookie"
            else:
                logger.warning(
                    "[auth-middleware] Missing authentication token",
                    extra={
                        "path": current_path,
                        "method": request.method,
                        "client_ip": request.client.host if request.client else "unknown",
                        "user_agent": request.headers.get("user-agent", "unknown"),
                        "origin": request.headers.get("origin", "unknown"),
                    }
                )
                return JSONResponse(status_code=401, content={"detail": "Missing authentication token"})


        if not token:
            return JSONResponse(status_code=401, content={"detail": "Missing authentication token"})
        
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            request.state.user = payload.get("sub")
            request.state.role = payload.get("role", "client")  # Store role for IDOR checks
            request.state.auth_method = auth_method
        

        except jwt.ExpiredSignatureError:
            if auth_method == "cookie":
                refresh_token = request.cookies.get("refresh_token")
                if refresh_token:
                    try:
                        # Verify refresh token
                        refresh_payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
                        admin_id = refresh_payload.get("sub")
                        if admin_id:
                            # Auto-refresh will be handled by the refresh-cookie endpoint
                            logger.info(
                                "[auth-middleware] Token expired, auto-refresh required",
                                extra={"path": current_path, "method": request.method, "auth_method": auth_method}
                            )
                            return JSONResponse(status_code=401, content={"detail": "Token expired, auto-refresh required"})
                    except (jwt.ExpiredSignatureError, JWTError):
                        pass

            logger.warning(
                "[auth-middleware] Session expired",
                extra={
                    "path": current_path,
                    "method": request.method,
                    "auth_method": auth_method,
                    "client_ip": request.client.host if request.client else "unknown",
                    "origin": request.headers.get("origin", "unknown"),
                }
            )
            return JSONResponse(status_code=401, content={"detail": "Session expired, Please Login again"})

        except JWTError as e:
            logger.warning(
                "[auth-middleware] Invalid token",
                extra={
                    "path": current_path,
                    "method": request.method,
                    "auth_method": auth_method,
                    "client_ip": request.client.host if request.client else "unknown",
                    "origin": request.headers.get("origin", "unknown"),
                    "error": str(e),
                }
            )
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

        return await call_next(request)






 
