"""Configuration management for One Piece of Data pipeline."""

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Chapter and Volume Settings
    last_chapter: int = Field(default=1153, alias="OP_LAST_CHAPTER")
    last_volume: int = Field(default=112, alias="OP_LAST_VOLUME")
    
    # Data Directories
    data_dir: Path = Field(default=Path("./data"), alias="OP_DATA_DIR")
    database_path: Path = Field(default=Path("./data/onepiece.duckdb"), alias="OP_DATABASE_PATH")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", alias="OP_LOG_LEVEL")
    log_file: Optional[Path] = Field(default=Path("./logs/onepieceofdata.log"), alias="OP_LOG_FILE")
    
    # Scraping Configuration
    scraping_delay: float = Field(default=1.0, alias="OP_SCRAPING_DELAY")
    max_retries: int = Field(default=3, alias="OP_MAX_RETRIES")
    request_timeout: int = Field(default=30, alias="OP_REQUEST_TIMEOUT")
    
    # Parallel Processing Configuration
    enable_parallel: bool = Field(default=False, alias="OP_ENABLE_PARALLEL")
    max_workers: int = Field(default=4, alias="OP_MAX_WORKERS")
    parallel_chunk_size: int = Field(default=10, alias="OP_PARALLEL_CHUNK_SIZE")

    # PostgreSQL Export Configuration (works for Supabase or local PostgreSQL)
    postgres_url: Optional[str] = Field(default=None, alias="POSTGRES_URL")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="onepiece", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: Optional[str] = Field(default=None, alias="POSTGRES_PASSWORD")
    postgres_ssl_mode: str = Field(default="prefer", alias="POSTGRES_SSL_MODE")

    # Export Configuration
    export_batch_size: int = Field(default=1000, alias="OP_EXPORT_BATCH_SIZE")
    export_enable_sync_tracking: bool = Field(default=True, alias="OP_EXPORT_ENABLE_SYNC_TRACKING")

    # Website URLs
    base_chapter_url: str = Field(default="https://onepiece.fandom.com/wiki/Chapter_", alias="OP_BASE_CHAPTER_URL")
    base_character_url: str = Field(default="https://onepiece.fandom.com/wiki/", alias="OP_BASE_CHARACTER_URL")
    base_volume_url: str = Field(default="https://onepiece.fandom.com/wiki/Volume_", alias="OP_BASE_VOLUME_URL")
    
    @property
    def chapters_json_path(self) -> Path:
        """Path to chapters JSON file."""
        return self.data_dir / "chapters.json"
    
    @property
    def volumes_json_path(self) -> Path:
        """Path to volumes JSON file."""
        return self.data_dir / "volumes.json"
    
    @property
    def characters_json_path(self) -> Path:
        """Path to characters JSON file."""
        return self.data_dir / "characters.json"
    
    @property
    def characters_csv_path(self) -> Path:
        """Path to characters CSV file."""
        return self.data_dir / "characters.csv"
    
    @property
    def chapters_csv_path(self) -> Path:
        """Path to chapters CSV file."""
        return self.data_dir / "chapters.csv"
    
    @property
    def coc_csv_path(self) -> Path:
        """Path to characters of chapters CSV file."""
        return self.data_dir / "coc.csv"

    @property
    def postgres_connection_url(self) -> str:
        """Build PostgreSQL connection URL.

        If POSTGRES_URL is set, use it directly. Otherwise, build from components.
        Works for both Supabase and local PostgreSQL instances.
        """
        if self.postgres_url:
            return self.postgres_url

        if not self.postgres_password:
            raise ValueError("POSTGRES_PASSWORD is required for PostgreSQL export")

        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            f"?sslmode={self.postgres_ssl_mode}"
        )

    @property
    def sync_metadata_path(self) -> Path:
        """Path to sync metadata file for tracking exports."""
        return self.data_dir / "sync_metadata.json"

    def ensure_directories(self) -> None:
        """Ensure all required directories exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.log_file:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings
