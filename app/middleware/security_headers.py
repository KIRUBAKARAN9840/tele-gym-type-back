
from typing import Dict, Optional, Callable, Any
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

@dataclass
class SecurityHeadersConfig:
    """Configuration for security headers"""
    
    # HSTS (HTTP Strict Transport Security)
    enable_hsts: bool = True
    hsts_max_age: int = 31536000  # 1 year
    hsts_include_subdomains: bool = True
    hsts_preload: bool = False
    
    # Frame protection
    x_frame_options: str = "DENY"  # DENY, SAMEORIGIN, or ALLOW-FROM
    
    # Content type protection
    x_content_type_options: str = "nosniff"
    
    # Referrer policy
    referrer_policy: str = "strict-origin-when-cross-origin"
    
    # Permissions policy (formerly Feature Policy)
    permissions_policy: Optional[str] = "geolocation=(), microphone=(), camera=(), payment=(), usb=()"
    
    # Cross-Origin policies
    cross_origin_opener_policy: Optional[str] = "same-origin"
    cross_origin_embedder_policy: Optional[str] = None  # require-corp
    cross_origin_resource_policy: Optional[str] = "same-origin"
    
    # X-Permitted-Cross-Domain-Policies (Adobe Flash/PDF)
    x_permitted_cross_domain_policies: str = "none"
    
    # Content Security Policy
    csp: Optional[str] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self'; connect-src 'self'; frame-ancestors 'none';"
    
    # Remove server identification
    remove_server_header: bool = True
    
    # Custom headers
    custom_headers: Dict[str, str] = field(default_factory=dict)

def swagger_csp() -> str:
    """Generate CSP policy that allows Swagger UI to work"""
    return (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://unpkg.com; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
        "img-src 'self' data: https: blob:; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self' https:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )

def api_csp() -> str:
    """Generate strict CSP for API endpoints"""
    return (
        "default-src 'none'; "
        "frame-ancestors 'none'; "
        "base-uri 'none'; "
        "form-action 'none';"
    )

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add security headers to all responses
    Supports path-specific overrides for different security requirements
    """
    
    def __init__(
        self,
        app: ASGIApp,
        config: SecurityHeadersConfig,
        path_overrides: Optional[Dict[str, Dict[str, Any]]] = None
    ):
        super().__init__(app)
        self.config = config
        self.path_overrides = path_overrides or {}
        
    def _get_config_for_path(self, path: str) -> SecurityHeadersConfig:
        """Get configuration for specific path, applying overrides"""
        # Check for exact path matches first
        if path in self.path_overrides:
            return self._apply_overrides(self.config, self.path_overrides[path])
            
        # Check for prefix matches
        for override_path, overrides in self.path_overrides.items():
            if path.startswith(override_path.rstrip("*")):
                return self._apply_overrides(self.config, overrides)
                
        return self.config
    
    def _apply_overrides(self, base_config: SecurityHeadersConfig, overrides: Dict[str, Any]) -> SecurityHeadersConfig:
        """Apply path-specific overrides to base configuration"""
        import copy
        config = copy.deepcopy(base_config)
        
        for key, value in overrides.items():
            if hasattr(config, key):
                setattr(config, key, value)
            else:
                logger.warning(f"Unknown security header config key: {key}")
                
        return config
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get path-specific configuration
        path = request.url.path
        config = self._get_config_for_path(path)
        
        # Process the request
        response = await call_next(request)
        
        # Add security headers
        self._add_security_headers(response, config, request)
        
        return response
    
    def _add_security_headers(self, response: Response, config: SecurityHeadersConfig, request: Request):
        """Add all configured security headers to the response"""
        
        # HSTS - only for HTTPS
        if config.enable_hsts and (request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https"):
            hsts_value = f"max-age={config.hsts_max_age}"
            if config.hsts_include_subdomains:
                hsts_value += "; includeSubDomains"
            if config.hsts_preload:
                hsts_value += "; preload"
            response.headers["Strict-Transport-Security"] = hsts_value
        
        # X-Frame-Options
        if config.x_frame_options:
            response.headers["X-Frame-Options"] = config.x_frame_options
        
        # X-Content-Type-Options
        if config.x_content_type_options:
            response.headers["X-Content-Type-Options"] = config.x_content_type_options
        
        # Referrer-Policy
        if config.referrer_policy:
            response.headers["Referrer-Policy"] = config.referrer_policy
        
        # Permissions-Policy
        if config.permissions_policy:
            response.headers["Permissions-Policy"] = config.permissions_policy
        
        # Cross-Origin-Opener-Policy
        if config.cross_origin_opener_policy:
            response.headers["Cross-Origin-Opener-Policy"] = config.cross_origin_opener_policy
        
        # Cross-Origin-Embedder-Policy
        if config.cross_origin_embedder_policy:
            response.headers["Cross-Origin-Embedder-Policy"] = config.cross_origin_embedder_policy
        
        # Cross-Origin-Resource-Policy
        if config.cross_origin_resource_policy:
            response.headers["Cross-Origin-Resource-Policy"] = config.cross_origin_resource_policy
        
        # X-Permitted-Cross-Domain-Policies
        if config.x_permitted_cross_domain_policies:
            response.headers["X-Permitted-Cross-Domain-Policies"] = config.x_permitted_cross_domain_policies
        
        # Content-Security-Policy
        if config.csp:
            response.headers["Content-Security-Policy"] = config.csp
        
        # Remove server header if requested
        if config.remove_server_header and "server" in response.headers:
            del response.headers["server"]
        
        # Add custom headers
        for header_name, header_value in config.custom_headers.items():
            response.headers[header_name] = header_value
        
        # Add security headers for API responses
        if request.url.path.startswith("/api/") or response.headers.get("content-type", "").startswith("application/json"):
            response.headers["X-API-Version"] = "v1"
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

# Preset configurations for different environments
def production_config() -> SecurityHeadersConfig:
    """Production-ready security headers configuration"""
    return SecurityHeadersConfig(
        enable_hsts=True,
        hsts_max_age=31536000,  # 1 year
        hsts_include_subdomains=True,
        hsts_preload=True,
        x_frame_options="DENY",
        referrer_policy="strict-origin-when-cross-origin",
        x_content_type_options="nosniff",
        permissions_policy="geolocation=(), microphone=(), camera=(), payment=(), usb=(), accelerometer=(), gyroscope=(), magnetometer=(), clipboard-read=(), clipboard-write=()",
        cross_origin_opener_policy="same-origin",
        cross_origin_resource_policy="same-origin",
        x_permitted_cross_domain_policies="none",
        csp=api_csp(),
        remove_server_header=True,
        custom_headers={
            "X-DNS-Prefetch-Control": "off",
            "X-Download-Options": "noopen",
        }
    )

def development_config() -> SecurityHeadersConfig:
    """Development-friendly security headers configuration"""
    return SecurityHeadersConfig(
        enable_hsts=False,  # Don't enforce HTTPS in dev
        x_frame_options="SAMEORIGIN",
        referrer_policy="strict-origin-when-cross-origin",
        x_content_type_options="nosniff",
        permissions_policy="geolocation=(), microphone=(), camera=()",
        cross_origin_opener_policy="unsafe-none",  # More permissive for dev
        cross_origin_resource_policy="cross-origin",
        x_permitted_cross_domain_policies="none",
        csp="default-src 'self' 'unsafe-inline' 'unsafe-eval'; img-src 'self' data: blob: https:; font-src 'self' data:;",
        remove_server_header=False,
    )

# Helper function to detect environment and return appropriate config
def auto_config() -> SecurityHeadersConfig:
    """Automatically detect environment and return appropriate configuration"""
    import os
    env = os.getenv("ENVIRONMENT", "development").lower()
    
    if env in ["production", "prod"]:
        return production_config()
    else:
        return development_config()