from typing import Any, Optional, Tuple

from fastapi import HTTPException, Request
from jose import JWTError, jwt

from app.utils.security import ALGORITHM, SECRET_KEY


def get_token_from_request(request: Request) -> Optional[str]:
    """Extract bearer token from cookies or Authorization header."""
    token = request.cookies.get("access_token")
    if token:
        return token

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header format")

    return parts[1]


def decode_access_token(token: str) -> Tuple[str, dict]:
    """Decode the JWT access token and return user id + payload."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired authentication token")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=400, detail="Authentication token missing subject")

    return str(user_id), payload


def resolve_authenticated_user_id(
    request: Request,
    provided_user_id: Optional[Any] = None,
    *,
    expected_role: Optional[str] = "client",
) -> str:

    token = get_token_from_request(request)
    token_user_id: Optional[str] = None

    if token:
        token_user_id, payload = decode_access_token(token)
        role = (payload.get("role") or "").lower()
        if expected_role and role and role != expected_role.lower():
            raise HTTPException(status_code=403, detail="User role not permitted for this operation")

        if provided_user_id is not None and str(provided_user_id) != token_user_id:
            raise HTTPException(status_code=400, detail="user_id mismatch with authenticated token")

        return token_user_id

    if provided_user_id is None:
        raise HTTPException(status_code=401, detail="Authentication token required")

    return str(provided_user_id)


def authenticate_identity(
    request: Request,
    *,
    declared_role: Optional[str] = None,
    provided_user_id: Optional[Any] = None,
) -> Tuple[int, str]:
    """
    Ensure the caller's access token matches the expected role and identifier.

    Args:
        request: Incoming FastAPI request.
        declared_role: Role supplied by the client (e.g. "owner", "client"). If provided,
            it must match the role encoded in the token.
        provided_user_id: Optional user identifier supplied alongside the request payload.
            When present, it must match the subject embedded in the token.

    Returns:
        A tuple of (user_id, role) derived from the access token.
    """
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Authentication token required")

    user_id_str, payload = decode_access_token(token)
    role = (payload.get("role") or "").lower()
    if not role:
        raise HTTPException(status_code=403, detail="Authenticated role missing in token")

    if declared_role and role != declared_role.lower():
        raise HTTPException(status_code=403, detail="Role mismatch with authentication token")

    try:
        user_id = int(user_id_str)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid user identifier in token") from exc

    if provided_user_id is not None:
        try:
            expected_id = int(provided_user_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid user identifier supplied") from exc
        if expected_id != user_id:
            raise HTTPException(status_code=403, detail="User identifier mismatch with authentication token")

    return user_id, role
