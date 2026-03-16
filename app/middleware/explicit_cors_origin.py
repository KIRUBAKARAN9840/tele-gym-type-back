from __future__ import annotations

import re
from typing import Iterable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import logging

log = logging.getLogger("cors-debug")


class ExplicitCorsOriginMiddleware(BaseHTTPMiddleware):
    """
    Ensures the response never sends Access-Control-Allow-Origin="*"
    for credentialed requests. Starlette's CORSMiddleware already follows
    the spec, but this guards against misconfigured wildcards by swapping
    the wildcard with the actual request origin when it is from a trusted
    source.
    """

    def __init__(
        self,
        app,
        allowed_origins: Iterable[str],
        origin_regex: Optional[str] = None,
    ):
        super().__init__(app)
        self.allowed_origins = {origin.strip() for origin in allowed_origins if origin.strip()}
        self._origin_regex = re.compile(origin_regex) if origin_regex else None


    def _is_allowed(self, origin: str) -> bool:
        if origin in self.allowed_origins:
            return True
        if self._origin_regex and self._origin_regex.fullmatch(origin):
            return True
        return False

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)

        origin = request.headers.get("origin")
        allow_origin = response.headers.get("access-control-allow-origin")
        has_credentials = "cookie" in request.headers

        if origin:
            log.info(
                "[cors-guard] response path=%s origin=%s allow_header=%s has_cookie=%s",
                request.url.path,
                origin,
                allow_origin,
                has_credentials,
            )

        if origin and allow_origin == "*" and self._is_allowed(origin):
            log.warning(
                "[cors-guard] overriding wildcard response origin=%s path=%s",
                origin,
                request.url.path,
            )
            response.headers["access-control-allow-origin"] = origin
            response.headers.add_vary_header("Origin")
        elif origin and allow_origin == "*":
            log.error(
                "[cors-guard] wildcard not replaced because origin not allowed",
                extra={
                    "origin": origin,
                    "path": request.url.path,
                },
            )
        return response
