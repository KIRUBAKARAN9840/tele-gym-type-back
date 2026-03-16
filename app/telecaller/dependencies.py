from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.async_database import get_async_db
from app.models.telecaller_models import Manager, Telecaller
from app.utils.security import SECRET_KEY, ALGORITHM
from starlette.requests import Request

security = HTTPBearer()

async def get_current_manager(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get current manager - reads JWT from cookie (set by auth middleware) - async"""

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
                detail="Not authenticated"
            )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        mobile_number: str = payload.get("sub")
        role: str = payload.get("role")
        manager_id: int = payload.get("id")
        user_type: str = payload.get("type")

        if mobile_number is None or role != "manager" or user_type != "telecaller":
            raise credentials_exception
    except jwt.ExpiredSignatureError:
        # Token expired - return 401 with specific detail so frontend can trigger refresh
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except JWTError:
        raise credentials_exception

    stmt = select(Manager).where(Manager.id == manager_id)
    result = await db.execute(stmt)
    manager = result.scalar_one_or_none()

    if manager is None:
        raise credentials_exception

    if not manager.verified or manager.status != "active":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is not verified or inactive"
        )

    return manager

async def get_current_telecaller(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
):
    """Get current telecaller - reads JWT from cookie (set by auth middleware) - async"""

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
                detail="Not authenticated"
            )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        mobile_number: str = payload.get("sub")
        role: str = payload.get("role")
        telecaller_id: int = payload.get("id")
        manager_id: int = payload.get("manager_id")
        user_type: str = payload.get("type")

        if mobile_number is None or role != "telecaller" or user_type != "telecaller":
            raise credentials_exception
    except jwt.ExpiredSignatureError:
        # Token expired - return 401 with specific detail so frontend can trigger refresh
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except JWTError:
        raise credentials_exception

    stmt = select(Telecaller).where(
        Telecaller.id == telecaller_id,
        Telecaller.manager_id == manager_id
    )
    result = await db.execute(stmt)
    telecaller = result.scalar_one_or_none()

    if telecaller is None:
        raise credentials_exception

    if not telecaller.verified or telecaller.status != "active":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is not verified or inactive"
        )

    return telecaller
