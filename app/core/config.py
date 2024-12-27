from dataclasses import dataclass
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from functools import lru_cache
import pytz

ENV = os.getenv("ENV", "dev")  # Default
load_dotenv(f".env.{ENV}")

# Time Zone
korea_tz = pytz.timezone("Asia/Seoul")
utc_tz = pytz.timezone("UTC")


class Settings(BaseSettings):
    # App settings
    ENV: str = ENV
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    PROJECT_NAME: str = "Alphafinder API 1.0"
    API_V1_STR: str = "/api/v1"
    API_V2_STR: str = "/api/v2"
    DATA_DIR: str = os.getenv("DATA_DIR", "./data")

    # RDS settings
    RDS_HOST: str = os.getenv("RDS_HOST", "")
    RDS_USER: str = os.getenv("RDS_USER", "")
    RDS_PASSWORD: str = os.getenv("RDS_PASSWORD", "")
    RDS_DB: str = os.getenv("RDS_DB", "")
    RDS_PORT: int = os.getenv("RDS_PORT", 3306)

    if ENV == "prod":
        CELERY_LOGLEVEL: str = "ERROR"
    else:
        CELERY_LOGLEVEL: str = "INFO"

    @property
    def DATABASE_URL(self) -> str:
        return f"mysql+pymysql://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DB}"

    class Config:
        env_file = f".env.{ENV}"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

print(f"Current environment: {settings.ENV}")


@dataclass
class DatabaseConfig:
    DB_URL: str
    DB_POOL_RECYCLE: int = 3600
    DB_ECHO: bool = True


class DevConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql+pymysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


class ProdConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql+pymysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=False,
        )


class TestConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql+pymysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


def get_database_config():
    """Get database configuration based on environment"""

    config = dict(prod=ProdConfig, dev=DevConfig, test=TestConfig)
    return config[settings.ENV]()
