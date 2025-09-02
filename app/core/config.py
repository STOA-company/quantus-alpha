import os
from dataclasses import dataclass
from functools import lru_cache

import pytz
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

ENV = os.getenv("ENV", "")  # Default
ENV = "local"
load_dotenv(f".env.{ENV}")

# Time Zone
korea_tz = pytz.timezone("Asia/Seoul")
utc_tz = pytz.timezone("UTC")
us_eastern_tz = pytz.timezone("America/New_York")


class Settings(BaseSettings):
    # App settings
    ENV: str = ENV
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    PROJECT_NAME: str = f"Alphafinder API 1.0 - {ENV}"
    API_V1_STR: str = "/api/v1"
    API_V2_STR: str = "/api/v2"
    DATA_DIR: str = os.getenv("DATA_DIR", "./data")

    # RDS settings
    RDS_HOST: str = os.getenv("RDS_HOST", "")
    RDS_USER: str = os.getenv("RDS_USER", "")
    RDS_PASSWORD: str = os.getenv("RDS_PASSWORD", "")
    RDS_DB: str = os.getenv("RDS_DB", "")
    RDS_SERVICE_DB: str = os.getenv("RDS_SERVICE_DB", "")
    RDS_PORT: int = os.getenv("RDS_PORT", 3306)

    # User DB settings
    USER_DB_HOST: str = os.getenv("USER_DB_HOST", "")
    USER_DB_USER: str = os.getenv("USER_DB_USER", "")
    USER_DB_PASSWORD: str = os.getenv("USER_DB_PASSWORD", "")
    USER_DB_NAME: str = os.getenv("USER_DB_NAME", "")
    USER_DB_PORT: int = os.getenv("USER_DB_PORT", 3306)

    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "admin")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "admin")
    RABBITMQ_PORT: int = os.getenv("RABBITMQ_PORT", 5672)

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = os.getenv("REDIS_PORT", 6379)
    REDIS_DB: int = os.getenv("REDIS_DB", 0)
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    CELERY_CONCURRENCY: int = os.getenv("CELERY_CONCURRENCY", 7)

    KIS_APP_KEY: str = os.getenv("KIS_APP_KEY", "")
    KIS_SECRET: str = os.getenv("KIS_SECRET", "")
    KIS_ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "")

    GOOGLE_CLIENT_ID: str = os.getenv("GOOGLE_CLIENT_ID", "")
    GOOGLE_CLIENT_SECRET: str = os.getenv("GOOGLE_CLIENT_SECRET", "")
    GOOGLE_REDIRECT_URI: str = os.getenv("GOOGLE_REDIRECT_URI", "")
    TEST_GOOGLE_REDIRECT_URI: str = os.getenv("TEST_GOOGLE_REDIRECT_URI", "")
    GOOGLE_TOKEN_ENDPOINT: str = os.getenv("GOOGLE_TOKEN_ENDPOINT", "")
    GOOGLE_AUTH_URL: str = os.getenv("GOOGLE_AUTH_URL", "")

    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "")

    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "")
    TEST_FRONTEND_URL: str = os.getenv("TEST_FRONTEND_URL", "")

    REFINITIV_SERVER: str = os.getenv("REFINITIV_SERVER", "")
    REFINITIV_DATABASE: str = os.getenv("REFINITIV_DATABASE", "")
    REFINITIV_USERNAME: str = os.getenv("REFINITIV_USERNAME", "")
    REFINITIV_PASSWORD: str = os.getenv("REFINITIV_PASSWORD", "")

    TOSS_SECRET_KEY: str = os.getenv("TOSS_SECRET_KEY", "")

    NOTION_SECRET_KEY: str = os.getenv("NOTION_SECRET_KEY", "")

    if ENV == "prod":
        CELERY_LOGLEVEL: str = "ERROR"
    else:
        CELERY_LOGLEVEL: str = "INFO"

    @property
    def DATABASE_URL(self) -> str:
        return f"mysql+pymysql://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DB}"

    @property
    def DATABASE_SERVICE_URL(self) -> str:
        return (
            f"mysql+pymysql://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_SERVICE_DB}"
        )

    @property
    def DATABASE_USER_URL(self) -> str:
        return f"mysql+pymysql://{self.USER_DB_USER}:{self.USER_DB_PASSWORD}@{self.USER_DB_HOST}:{self.USER_DB_PORT}/{self.USER_DB_NAME}"

    class Config:
        env_file = f".env.{ENV}"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

print(f"Current environment: {settings.ENV}")


@dataclass
class DatabaseConfig:
    DB_URL: str
    DB_SERVICE_URL: str
    DB_USER_URL: str
    DB_POOL_RECYCLE: int = 3600
    DB_ECHO: bool = True


class DevConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_SERVICE_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_SERVICE_DB}",
            DB_USER_URL=f"mysql://{settings.USER_DB_USER}:{settings.USER_DB_PASSWORD}@{settings.USER_DB_HOST}:{settings.USER_DB_PORT}/{settings.USER_DB_NAME}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


class StageConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_SERVICE_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_SERVICE_DB}",
            DB_USER_URL=f"mysql://{settings.USER_DB_USER}:{settings.USER_DB_PASSWORD}@{settings.USER_DB_HOST}:{settings.USER_DB_PORT}/{settings.USER_DB_NAME}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


class ProdConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_SERVICE_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_SERVICE_DB}",
            DB_USER_URL=f"mysql://{settings.USER_DB_USER}:{settings.USER_DB_PASSWORD}@{settings.USER_DB_HOST}:{settings.USER_DB_PORT}/{settings.USER_DB_NAME}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=False,
        )


class TestConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_SERVICE_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_SERVICE_DB}",
            DB_USER_URL=f"mysql://{settings.USER_DB_USER}:{settings.USER_DB_PASSWORD}@{settings.USER_DB_HOST}:{settings.USER_DB_PORT}/{settings.USER_DB_NAME}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


class BatchConfig(DatabaseConfig):
    def __init__(self):
        super().__init__(
            DB_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_DB}",
            DB_SERVICE_URL=f"mysql://{settings.RDS_USER}:{settings.RDS_PASSWORD}@{settings.RDS_HOST}:{settings.RDS_PORT}/{settings.RDS_SERVICE_DB}",
            DB_USER_URL=f"mysql://{settings.USER_DB_USER}:{settings.USER_DB_PASSWORD}@{settings.USER_DB_HOST}:{settings.USER_DB_PORT}/{settings.USER_DB_NAME}",
            DB_POOL_RECYCLE=3600,
            DB_ECHO=True,
        )


def get_database_config():
    """Get database configuration based on environment"""

    config = dict(prod=ProdConfig, dev=DevConfig, test=TestConfig, stage=StageConfig, batch=BatchConfig)
    return config[settings.ENV]()
