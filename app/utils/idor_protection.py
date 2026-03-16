

from fastapi import Request, HTTPException, Depends
from typing import Optional, List, Union
from functools import wraps
import logging

logger = logging.getLogger("idor_protection")


class IDORViolationError(HTTPException):
    """Raised when an IDOR attempt is detected"""
    def __init__(self, user_id: str, attempted_resource: str):
        logger.warning(
            f"IDOR_VIOLATION: user={user_id} attempted_access={attempted_resource}"
        )
        super().__init__(
            status_code=403,
            detail="Access denied: You can only access your own data"
        )


async def get_verified_client_id(request: Request) -> int:

    user_id = getattr(request.state, "user", None)

    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        return int(user_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=401, detail="Invalid user identity")


async def get_verified_user_role(request: Request) -> str:

    role = getattr(request.state, "role", "client")
    return role


async def get_verified_gym_id(request: Request) -> Optional[int]:

    gym_id = getattr(request.state, "gym_id", None)
    if gym_id:
        try:
            return int(gym_id)
        except (ValueError, TypeError):
            return None
    return None


def verify_ownership(
    requested_id: Union[int, str],
    authenticated_id: Union[int, str],
    user_role: str,
    bypass_roles: List[str] = None
) -> bool:

    if bypass_roles is None:
        bypass_roles = ["admin", "owner", "trainer", "manager"]

    if user_role in bypass_roles:
        return True


    if str(requested_id) != str(authenticated_id):
        raise IDORViolationError(
            user_id=str(authenticated_id),
            attempted_resource=f"client_id={requested_id}"
        )

    return True


async def verify_client_id_matches(
    request: Request,
    client_id: int
) -> int:
  
    user_id = getattr(request.state, "user", None)
    user_role = getattr(request.state, "role", "client")

    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    verify_ownership(client_id, user_id, user_role)

    return client_id


class IDORProtectedRoute:


    def __init__(self, request: Request):
        self.request = request
        self._user_id = getattr(request.state, "user", None)
        self._role = getattr(request.state, "role", "client")
        self._gym_id = getattr(request.state, "gym_id", None)

        if not self._user_id:
            raise HTTPException(status_code=401, detail="Not authenticated")

    def get_client_id(self) -> int:
        """Get the verified client_id (from JWT, not request)"""
        try:
            return int(self._user_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=401, detail="Invalid user identity")

    def get_role(self) -> str:
        """Get the verified role"""
        return self._role

    def get_gym_id(self) -> Optional[int]:
        """Get the verified gym_id (if available)"""
        if self._gym_id:
            try:
                return int(self._gym_id)
            except (ValueError, TypeError):
                return None
        return None

    def is_privileged(self) -> bool:
        """Check if user has privileged role"""
        return self._role in ("admin", "owner", "trainer", "manager")

    def verify_access(self, requested_client_id: int) -> bool:
        """
        Verify the user can access the requested client_id.

        Returns True if:
        - User is accessing their own data, OR
        - User has privileged role (admin, owner, trainer)

        Raises HTTPException 403 if access denied.
        """
        return verify_ownership(
            requested_id=requested_client_id,
            authenticated_id=self._user_id,
            user_role=self._role
        )


# Decorator for protecting entire route functions
def require_self_or_admin(func):
    """
    Decorator that ensures the client_id parameter matches authenticated user.

    Usage:
        @router.get("/client/{client_id}/profile")
        @require_self_or_admin
        async def get_profile(request: Request, client_id: int):
            # Will only reach here if client_id matches user or user is admin
            pass
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request = kwargs.get("request") or next(
            (arg for arg in args if isinstance(arg, Request)), None
        )

        if not request:
            raise HTTPException(status_code=500, detail="Request object required")

        client_id = kwargs.get("client_id")
        if client_id is not None:
            user_id = getattr(request.state, "user", None)
            user_role = getattr(request.state, "role", "client")
            verify_ownership(client_id, user_id, user_role)

        return await func(*args, **kwargs)

    return wrapper
