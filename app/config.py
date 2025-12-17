"""
Application configuration.
Uses environment variables with sensible defaults for local development.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = "postgresql://user:pass@localhost:5432/kingsfoil"

    # Application
    app_name: str = "Kingsfoil Data Pipeline"
    debug: bool = True

    # File upload
    max_upload_size_mb: int = 100
    max_header_scan_rows: int = 15

    # Supported file extensions
    allowed_extensions: list[str] = ["csv", "xlsx", "xls", "txt"]

    # Temp file storage for uploads
    temp_upload_dir: Path = Path("/tmp/kingsfoil_uploads")
    temp_file_max_age_hours: int = 24

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
