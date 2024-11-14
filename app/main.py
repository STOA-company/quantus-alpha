from typing import Dict
from fastapi import Depends, FastAPI, HTTPException
from requests import Session
from app.core.config import get_database_config, settings
from app.api import routers
from app.database.conn import db
from app.database.crud import database

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Alphafinder API Documentation",
    version="1.0.0",
)
app.include_router(routers.router)

db_config = get_database_config()
db.init_app(app, **db_config.__dict__)



@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

# 응답 모델 정의
class HealthCheckDetails(BaseModel):
    tables_loaded: int
    connection_test: str

class HealthCheckResponse(BaseModel):
    status: str
    database: str
    details: HealthCheckDetails

@app.get("/health-check", response_model=HealthCheckResponse)
async def health_check():
    try:
        # 데이터베이스 연결 확인
        if not database.check_connection():
            raise Exception("Database connection test failed")
        
        # 메타데이터 확인
        tables = database.meta_data.tables.keys()
        
        return HealthCheckResponse(
            status="healthy",
            database="connected",
            details=HealthCheckDetails(
                tables_loaded=len(list(tables)),
                connection_test="successful"
            )
        )
    except Exception as e:
        error_message = f"Database connection error: {str(e)}"
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "disconnected",
                "error": error_message
            }
        )

