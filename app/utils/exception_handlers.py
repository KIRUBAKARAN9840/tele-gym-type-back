from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from datetime import datetime, timezone
from .logging_setup import jlog
from .logging_utils import FittbotHTTPException

def install_exception_handlers(app):
    @app.exception_handler(FittbotHTTPException)
    async def _handle_fittbot_exc(request: Request, exc: FittbotHTTPException):
        # already logged inside the exception
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error_code": exc.error_code,
                "detail": exc.detail,
                "timestamp": exc.timestamp,
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def _handle_http_exc(request: Request, exc: StarletteHTTPException):
        # unify shape + log once
        ts = datetime.now(timezone.utc).isoformat()
        jlog(
            "warning" if exc.status_code < 500 else "error",
            {
                "type": "error",
                "error_code": f"HTTP_{exc.status_code}",
                "detail": str(exc.detail),
                "status_code": exc.status_code,
                "timestamp": ts,
            },
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "error_code": f"HTTP_{exc.status_code}",
                "detail": exc.detail,
                "timestamp": ts,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def _handle_validation_exc(request: Request, exc: RequestValidationError):
        ts = datetime.now(timezone.utc).isoformat()
        jlog(
            "warning",
            {
                "type": "validation_error",
                "error_code": "VALIDATION_ERROR",
                "detail": str(exc),
                "errors": exc.errors(),
                "status_code": 422,
                "timestamp": ts,
            },
        )
        return JSONResponse(
            status_code=422,
            content={
                "status": "error",
                "error_code": "VALIDATION_ERROR",
                "message": "Request validation failed",
                "errors": exc.errors(),
                "timestamp": ts,
            },
        )