"""
Configuration settings for the standalone ephemeral task runner.

This mirrors the structure of the main application's settings module so the
task can be deployed independently while keeping environment-driven behavior.
"""
from pathlib import Path
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings

from secrets_manager import load_secrets_into_env


BASE_DIR = Path(__file__).resolve().parent

# Load secrets (if configured) before settings initialization
load_secrets_into_env()


class Settings(BaseSettings):

    app_name: str = Field(default="Ephemeral Task Runner", env="APP_NAME")
    environment: str = Field(default="production", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")

    # Database
    db_username: str = Field(env="DB_USERNAME")
    db_password: Optional[str] = Field(default=None, env="DB_PASSWORD")
    db_host: str = Field(env="DB_HOST")
    db_name: str = Field(env="DB_NAME")

    # Connection pool tuning
    db_pool_size: int = Field(default=5, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=10, env="DB_MAX_OVERFLOW")
    db_pool_pre_ping: bool = Field(default=True, env="DB_POOL_PRE_PING")
    db_pool_recycle: int = Field(default=3600, env="DB_POOL_RECYCLE")
    db_pool_timeout: int = Field(default=30, env="DB_POOL_TIMEOUT")
    db_pool_echo: bool = Field(default=False, env="DB_POOL_ECHO")

    # Redis / other services can be added here if the task needs them.
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")
    rate_limit_whitelist_ips: str = Field(default="127.0.0.1,::1", env="RATE_LIMIT_WHITELIST_IPS")

    @property
    def database_url(self) -> str:
        """Construct SQLAlchemy database URL from components."""
        if self.db_password:
            return f"mysql+pymysql://{self.db_username}:{self.db_password}@{self.db_host}/{self.db_name}"
        return f"mysql+pymysql://{self.db_username}@{self.db_host}/{self.db_name}"

    @property
    def whitelist_ips_list(self) -> List[str]:
        """Placeholder helper mirroring the main service behavior."""
        return [ip.strip() for ip in self.rate_limit_whitelist_ips.split(",") if ip.strip()]

    class Config:
        # Allow loading from a local .env for local testing
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


settings = Settings()

print(
    "[ephemeral-settings] "
    f"environment={settings.environment} "
    f"db_host={settings.db_host} "
    f"db_name={settings.db_name}"
)


