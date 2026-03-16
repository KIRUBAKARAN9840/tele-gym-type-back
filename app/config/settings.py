
import json
from typing import List, Optional
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator

from app.utils.secrets_manager import load_secrets_into_env


BASE_DIR = Path(__file__).resolve().parent.parent.parent

load_secrets_into_env()

class Settings(BaseSettings):

    app_name: str = "NattyBoss API"
    environment: str = Field(default="production", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    db_username: str = Field(env="DB_USERNAME")
    db_password: Optional[str] = Field(default=None, env="DB_PASSWORD")
    db_host: str = Field(env="DB_HOST")
    db_name: str = Field(env="DB_NAME")
    dailypass_db_url: Optional[str] = Field(default=None, env="DAILYPASS_DB_URL")
    dailypass_db_username: Optional[str] = Field(default=None, env="DAILYPASS_DB_USERNAME")
    dailypass_db_password: Optional[str] = Field(default=None, env="DAILYPASS_DB_PASSWORD")
    dailypass_db_host: Optional[str] = Field(default=None, env="DAILYPASS_DB_HOST")
    dailypass_db_name: Optional[str] = Field(default=None, env="DAILYPASS_DB_NAME")

 
    db_pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=5, env="DB_MAX_OVERFLOW")
    db_pool_pre_ping: bool = Field(default=True, env="DB_POOL_PRE_PING")
    db_pool_recycle: int = Field(default=1800, env="DB_POOL_RECYCLE")  # 30 mins
    db_pool_timeout: int = Field(default=30, env="DB_POOL_TIMEOUT")
    db_pool_echo: bool = Field(default=False, env="DB_POOL_ECHO")

    # Celery Worker Pool Settings (1 task at a time per process, needs fewer connections)
    # 1 Fargate × 4 concurrency × (2+2) = 16 connections max
    celery_db_pool_size: int = Field(default=2, env="CELERY_DB_POOL_SIZE")
    celery_db_max_overflow: int = Field(default=2, env="CELERY_DB_MAX_OVERFLOW")
    dailypass_db_pool_size: Optional[int] = Field(default=None, env="DAILYPASS_DB_POOL_SIZE")
    dailypass_db_max_overflow: Optional[int] = Field(default=None, env="DAILYPASS_DB_MAX_OVERFLOW")
    dailypass_db_pool_pre_ping: Optional[bool] = Field(default=None, env="DAILYPASS_DB_POOL_PRE_PING")
    dailypass_db_pool_recycle: Optional[int] = Field(default=None, env="DAILYPASS_DB_POOL_RECYCLE")
    dailypass_db_pool_timeout: Optional[int] = Field(default=None, env="DAILYPASS_DB_POOL_TIMEOUT")
    dailypass_db_pool_echo: Optional[bool] = Field(default=None, env="DAILYPASS_DB_POOL_ECHO")

    # Redis (loaded from fittbot/secrets in production)
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")

    # API Keys (loaded from fittbot/secrets in production)
    openai_api_key: str = Field(env="OPENAI_API_KEY")
    groq_api_key: str = Field(env="GROQ_API_KEY")
    other_api_key: str = Field(env="OTHER_API_KEY")

    # BhashSMS Fallback SMS Provider
    bhashsms_user: Optional[str] = Field(default=None, env="BHASHSMS_USER")
    bhashsms_pass: Optional[str] = Field(default=None, env="BHASHSMS_PASS")

    # WhatsApp / Infinito (loaded from fittbot/secrets in production)
    whatsapp_base_url: str = Field(default="https://103.229.250.150", env="WHATSAPP_BASE_URL")
    whatsapp_bearer_token: Optional[str] = Field(default=None, env="WHATSAPP_BEARER_TOKEN")
    whatsapp_from_number: str = Field(default="919742271245", env="WHATSAPP_FROM_NUMBER")
    whatsapp_dlr_url: str = Field(default="", env="WHATSAPP_DLR_URL")
    whatsapp_client_id: Optional[str] = Field(default=None, env="WHATSAPP_CLIENT_ID")
    whatsapp_client_password: Optional[str] = Field(default=None, env="WHATSAPP_CLIENT_PASSWORD")
    whatsapp_auth_mode: Optional[str] = Field(default=None, env="WHATSAPP_AUTH_MODE")
    whatsapp_authorization: Optional[str] = Field(default=None, env="WHATSAPP_AUTHORIZATION")

    # AES Encryption (loaded from fittbot/secrets in production)
    aes_secret_key: str = Field(env="AES_SECRET_KEY")

    # AWS (loaded from fittbot/secrets in production)
    aws_access_key_id: str = Field(env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(default="ap-south-2", env="AWS_REGION")

    # Payment (loaded from fittbot/secrets in production)
    razorpay_key_id: str = Field(env="RAZORPAY_KEY_ID")
    razorpay_key_secret: str = Field(env="RAZORPAY_KEY_SECRET")
    razorpay_webhook_secret: str = Field(default="naveenkumarmartinrajufromtamilnadu", env="RAZORPAY_WEBHOOK_SECRET")
    webhook_secret: str = Field(default="naveenkumarmartinrajufromtamilnadu", env="WEBHOOK_SECRET")

    # RevenueCat (loaded from fittbot/secrets in production)
    revenuecat_api_key: Optional[str] = Field(default=None, env="REVENUECAT_API_KEY")  # Secret API Key (starts with sk_)
    
    # Email (loaded from fittbot/secrets in production)
    smtp_server: str = Field(env="SMTP_SERVER")
    smtp_port: int = Field(env="SMTP_PORT")
    smtp_email: str = Field(env="SMTP_EMAIL")
    smtp_password: str = Field(env="SMTP_PASSWORD")
    
    # Rate Limiting - IP Based
    rate_limit_requests_per_minute: int = Field(default=60, env="RATE_LIMIT_REQUESTS_PER_MINUTE")
    rate_limit_requests_per_hour: int = Field(default=1000, env="RATE_LIMIT_REQUESTS_PER_HOUR")
    rate_limit_requests_per_day: int = Field(default=10000, env="RATE_LIMIT_REQUESTS_PER_DAY")
    rate_limit_burst_limit: int = Field(default=60, env="RATE_LIMIT_BURST_LIMIT")
    rate_limit_burst_window: int = Field(default=10, env="RATE_LIMIT_BURST_WINDOW")
    rate_limit_whitelist_ips: str = Field(default="127.0.0.1,::1", env="RATE_LIMIT_WHITELIST_IPS")
    
    # Rate Limiting - Admin IP Based Overrides
    admin_rate_limit_requests_per_minute: int = Field(default=600, env="ADMIN_RATE_LIMIT_REQUESTS_PER_MINUTE")
    admin_rate_limit_requests_per_hour: int = Field(default=10000, env="ADMIN_RATE_LIMIT_REQUESTS_PER_HOUR")
    admin_rate_limit_requests_per_day: int = Field(default=100000, env="ADMIN_RATE_LIMIT_REQUESTS_PER_DAY")
    admin_rate_limit_burst_limit: int = Field(default=200, env="ADMIN_RATE_LIMIT_BURST_LIMIT")
    admin_rate_limit_burst_window: int = Field(default=10, env="ADMIN_RATE_LIMIT_BURST_WINDOW")
    
    # Rate Limiting - User Based
    user_limit_requests_per_minute: int = Field(default=120, env="USER_LIMIT_REQUESTS_PER_MINUTE")
    user_limit_requests_per_hour: int = Field(default=2000, env="USER_LIMIT_REQUESTS_PER_HOUR")
    user_limit_requests_per_day: int = Field(default=20000, env="USER_LIMIT_REQUESTS_PER_DAY")
    user_limit_burst_limit: int = Field(default=40, env="USER_LIMIT_BURST_LIMIT")
    user_limit_burst_window: int = Field(default=10, env="USER_LIMIT_BURST_WINDOW")
    
    # Rate Limiting - Admin User Overrides
    admin_user_limit_requests_per_minute: int = Field(default=600, env="ADMIN_USER_LIMIT_REQUESTS_PER_MINUTE")
    admin_user_limit_requests_per_hour: int = Field(default=10000, env="ADMIN_USER_LIMIT_REQUESTS_PER_HOUR")
    admin_user_limit_requests_per_day: int = Field(default=100000, env="ADMIN_USER_LIMIT_REQUESTS_PER_DAY")
    admin_user_limit_burst_limit: int = Field(default=200, env="ADMIN_USER_LIMIT_BURST_LIMIT")
    admin_user_limit_burst_window: int = Field(default=10, env="ADMIN_USER_LIMIT_BURST_WINDOW")
    
    # Security
    allowed_hosts: List[str] = Field(
        default=["*"],
        env="ALLOWED_HOSTS"
    )
    app_api_key: Optional[str] = Field(default=None, env="APP_API_KEY")
    cors_origins: List[str] = Field(default_factory=list, env="CORS_ORIGINS")
    cors_origin_regex: Optional[str] = Field(default=None, env="CORS_ORIGIN_REGEX")

    # Additional AI API Keys (loaded from fittbot/secrets in production)
    gemini_api_key: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    deepseek_api_key: Optional[str] = Field(default=None, env="DEEPSEEK_API_KEY")

    # Rich Notification API Key (for Postman / external triggers)
    notification_api_key: Optional[str] = Field(default=None, env="NOTIFICATION_API_KEY")


    # Platform Pricing Markup (single source of truth for all pricing)
    # Applies to: dailypass, sessions, gym memberships
    # Change this ONE value to update markup across the entire platform
    platform_markup_percent: int = Field(default=10, env="PLATFORM_MARKUP_PERCENT")

    esign_s3_bucket: str = Field(default="fittbot-esign-documents", env="ESIGN_S3_BUCKET")



    pdf_s3_bucket: str = Field(default="fittbot-uploads", env="PDF_S3_BUCKET")
    pdf_template_s3_key: str = Field(default="templates/gym_agreement/v1/Gym Agreement Form.pdf", env="PDF_TEMPLATE_S3_KEY")
    pdf_template_version: str = Field(default="v1", env="PDF_TEMPLATE_VERSION")
    pdf_agreements_prefix: str = Field(default="agreements", env="PDF_AGREEMENTS_PREFIX")
    pdf_presign_expires_seconds: int = Field(default=900, env="PDF_PRESIGN_EXPIRES_SECONDS")
    
    # Cookie configuration
    cookie_secure: bool = Field(default=True, env="COOKIE_SECURE")
    cookie_samesite: str = Field(default="none", env="COOKIE_SAMESITE")
    cookie_domain: str = Field(default=".fittbot.com", env="COOKIE_DOMAIN")

    @field_validator("allowed_hosts", mode="before")
    @classmethod
    def parse_allowed_hosts(cls, value):
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return ["fittbot.com", "*.fittbot.com", "fymble.app", "*.fymble.app", "localhost"]
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                return [h.strip() for h in value.split(",") if h.strip()]
        return value

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return []
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @property
    def cors_origins_resolved(self) -> List[str]:
        """Return CORS origins, falling back to local defaults when unset."""
        origins: List[str] = list(dict.fromkeys(self.cors_origins))
        if not origins:
            origins.extend(["http://10.37.105.156:3000","http://localhost:3000", "http://127.0.0.1:3000","http://192.168.1.21:3000","http://192.168.1.6:3000","https://unadulatory-indomitably-digna.ngrok-free.dev"])
        elif self.environment.lower() != "production":
            for origin in ("http://10.37.105.156:3000","http://localhost:3000", "http://127.0.0.1:3000","http://192.168.1.21:3000","http://192.168.1.6:3000","https://unadulatory-indomitably-digna.ngrok-free.dev"):
                if origin not in origins:
                    origins.append(origin)

        trusted_domains = [
            "https://payments.fymble.app","https://admin.fittbot.com","https://telecaller.fittbot.com"
        ]
        for domain in trusted_domains:
            if domain not in origins:
                origins.append(domain)
        return origins
    
    @property
    def cookie_samesite_value(self) -> str:
        """Return a normalized SameSite value accepted by Starlette."""
        value = (self.cookie_samesite or "").strip().lower()
        if value not in {"lax", "strict", "none"}:
            return "lax"
        return value

    @property
    def cookie_domain_value(self) -> Optional[str]:
        domain = (self.cookie_domain or "").strip()
        return domain or None

    @property
    def database_url(self) -> str:
        """Construct database URL from components"""
        if self.db_password:
            return f"mysql+pymysql://{self.db_username}:{self.db_password}@{self.db_host}/{self.db_name}"
        return f"mysql+pymysql://{self.db_username}@{self.db_host}/{self.db_name}"

    @property
    def dailypass_database_url(self) -> Optional[str]:
        """Construct DailyPass DB URL when separate credentials are provided"""
        if self.dailypass_db_url:
            return self.dailypass_db_url

        username = self.dailypass_db_username or self.db_username
        password = self.dailypass_db_password if self.dailypass_db_password is not None else self.db_password
        host = self.dailypass_db_host or self.db_host
        database = self.dailypass_db_name or "dailypass"

        if not username or not host:
            return None

        if password:
            return f"mysql+pymysql://{username}:{password}@{host}/{database}"
        return f"mysql+pymysql://{username}@{host}/{database}"

    @property
    def whitelist_ips_list(self) -> List[str]:
        """Convert comma-separated whitelist IPs to list"""
        return [ip.strip() for ip in self.rate_limit_whitelist_ips.split(",") if ip.strip()]

    class Config:
        # Load .env file for local development
        # In production, environment variables come from ECS task definition + AWS Secrets Manager
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env that aren't defined in Settings

settings = Settings()
