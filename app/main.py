from app.middlewares.slack_error import add_slack_middleware
from fastapi import FastAPI, HTTPException, Security
from app.core.config import get_database_config, settings
from app.api import routers
from app.core.exception import handler
from app.database.conn import db
from app.database.crud import database
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from app.core.logging.config import configure_logging
from app.middlewares.trusted_hosts import get_current_username
import logging

logger = logging.getLogger(__name__)

configure_logging()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description=f"Alphafinder API Documentation - {settings.ENV}",
    version="1.0.0",
    swagger_ui_parameters={
        "persistAuthorization": True,  # 인증 정보 유지
        "defaultModelsExpandDepth": -1,  # 모델 확장 깊이 설정 / -1은 축소
        "docExpansion": "none",
    },
    docs_url=None,
    redoc_url=None,
)
handler.initialize(app)

app.include_router(routers.router)

db_config = get_database_config()
db.init_app(app, **db_config.__dict__)


@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}


origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://alpha-dev.quantus.kr",
    "https://develop.alphafinder.dev",
    "https://alphafinder-stage.vercel.app",
    "https://stage.alphafinder.dev",
    "https://live.alphafinder.dev",
]

add_slack_middleware(
    app=app,
    webhook_url="https://hooks.slack.com/services/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",
    mention_usernames=["고경민", "김광윤"],  # 알림을 받을 사용자 이름 (SlackNotifier.SLACK_USER_IDS에 정의된)
    include_traceback=True,
    include_request_body=False,  
    error_status_codes=[500, 503],  # 이 상태 코드들에 대해 알림 발송
    environment=settings.ENV,
    notify_environments=["stage"],  
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "Authorization", "Authorization_Swagger"],
)


class HealthCheckDetails(BaseModel):
    tables_loaded: int
    connection_test: str


class HealthCheckResponse(BaseModel):
    status_code: int
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
            status_code=200,
            database="connected",
            details=HealthCheckDetails(tables_loaded=len(list(tables)), connection_test="successful"),
        )
    except Exception as e:
        error_message = f"Database connection error: {str(e)}"
        raise HTTPException(status_code=503, detail={"status": "503", "database": "disconnected", "error": error_message})


@app.get("/docs", include_in_schema=False)
async def get_swagger_documentation(username: str = Security(get_current_username)):
    from fastapi.openapi.docs import get_swagger_ui_html

    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        swagger_ui_parameters=app.swagger_ui_parameters,
    )


@app.get("/redoc", include_in_schema=False)
async def get_redoc_documentation(username: str = Security(get_current_username)):
    from fastapi.openapi.docs import get_redoc_html

    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
    )
