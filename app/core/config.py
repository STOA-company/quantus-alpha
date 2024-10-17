import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from functools import lru_cache

ENV = os.getenv("ENV", "dev")  # Default
load_dotenv(f".env.{ENV}")

class Settings(BaseSettings):
    # App settings
    ENV: str = ENV
    DEBUG: bool = os.getenv('DEBUG', 'False').lower() == 'true'
    PROJECT_NAME: str = "Alphafinder API"
    API_V1_STR: str = "/api/v1"
    DATA_DIR: str = os.getenv('DATA_DIR', './data')

    # S3 settings
    USE_S3: bool = os.getenv('USE_S3', 'False').lower() == 'true'
    AWS_ACCESS_KEY_ID: str = os.getenv('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_ACCESS_KEY: str = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    AWS_REGION: str = os.getenv('AWS_REGION', '')

    class Config:
        env_file = f".env.{ENV}"
        env_file_encoding = 'utf-8'

@lru_cache()
def get_settings() -> Settings:
    return Settings()

settings = get_settings()

print(f"Current environment: {settings.ENV}")