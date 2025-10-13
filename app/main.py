from fastapi import FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.api import routers
from app.core.config import get_database_config, settings
from app.core.exception import handler
from app.core.logger import configure, get_logger
from app.database.conn import db
from app.database.crud import database
from app.middlewares.rate_limiter_admin import router as rate_limiter_admin_router
from app.middlewares.slack_error import add_slack_middleware
from app.middlewares.trusted_hosts import get_current_username
from app.middlewares.worker_timeout_tracker import add_worker_timeout_tracker
from app.monitoring.endpoints import router as metrics_router
from app.monitoring.middleware import PrometheusMiddleware
from app.elasticsearch.elasticsearch import cleanup_elasticsearch

# zipkin 설정
from app.monitoring.tracing import setup_zipkin_tracing

# 여기로 로거 설정 이동
stage_webhook_url = "https://hooks.slack.com/services/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"
dev_webhook_url = "https://hooks.slack.com/services/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"

slack_webhook_url = stage_webhook_url if settings.ENV == "stage" else dev_webhook_url

# 전역 로거 모듈 설정 - 앱 초기화 전에 구성해야 함
configure(
    environment=settings.ENV,
    app_name=settings.PROJECT_NAME,
    log_level="INFO",
    log_dir="logs",
    separate_error_logs=True,
    console_output=True,
    exception_handlers=["file", "console"],
    send_error_to_slack=True,
    slack_webhook_url=slack_webhook_url,
    slack_webhook_urls={"default": slack_webhook_url},
    default_slack_channel="default",
    notify_in_development=True,
)

# 로거 설정
logger = get_logger(__name__)

# 로그 테스트
logger.info("Application starting...")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description=f"Alphafinder API Documentation - {settings.ENV}",
    version="1.0.0",
    swagger_ui_parameters={
        "persistAuthorization": True,  # 인증 정보 유지
        "defaultModelsExpandDepth": -1,  # 모델 확장 깊이 설정 / -1은 축소
        "docExpansion": "none",
        "filter": True,  # 태그 검색 기능 활성화
    },
    docs_url=None,
    redoc_url=None,
)
handler.initialize(app)

app.include_router(routers.router)
# Include rate limiter admin router
app.include_router(rate_limiter_admin_router)
app.include_router(metrics_router)  # Add metrics endpoints

db_config = get_database_config()
db.init_app(app, **db_config.__dict__)

# Zipkin 트레이싱 설정 활성화
setup_zipkin_tracing(app)


@app.get("/")
def root():
    return {"message": "Welcome to the Financial Data API !!"}


origins = [
    "https://alpha-dev.quantus.kr",
    "https://insight-dev.quantus.kr",
    "https://develop.alphafinder.dev",
    "https://develop.alphafinder.dev/ko",
    "https://develop.alphafinder.dev/en",
    "https://alphafinder-stage.vercel.app",
    "https://stage.alphafinder.dev",
    "https://live.alphafinder.dev",
    "https://www.alphafinder.dev",
    "https://alphafinder-l2xhjep9g-quantus-68c7517d.vercel.app",
    "https://supper-app-dev.quantus.kr",
    "https://superapp-live.quantus.kr",
    "https://superapp-dev.quantus.kr",
    "https://supper-app-dev.vercel.app",
    "https://quantus.kr/",
]

if settings.ENV == "dev":
    # 개발 환경에서는 로컬 개발을 위한 접근 허용
    origins.extend(
        [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:8000",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:3001",
            "http://127.0.0.1:8000",
        ]
    )
elif settings.ENV == "stage":
    # 스테이징 환경에서는 제한된 접근만 허용
    origins.extend(
        [
            "http://localhost:3000",  # 프론트엔드 개발 서버
            "http://127.0.0.1:3000",  # 프론트엔드 개발 서버
            "http://localhost:8000",  # 백엔드 개발 서버
            "http://127.0.0.1:8000",  # 백엔드 개발 서버
        ]
    )

if settings.ENV == "stage":
    webhook_url = stage_webhook_url
else:
    webhook_url = dev_webhook_url

# Add Prometheus middleware first to monitor all requests
app.add_middleware(PrometheusMiddleware)

# Worker timeout 추적 미들웨어 추가
add_worker_timeout_tracker(
    app=app,
    timeout_threshold=80.0,  # 80초 이상 걸리는 요청 추적
    webhook_url=webhook_url,
    environment=settings.ENV,
    notify_environments=["stage", "dev", "prod"],
)

# Slack 오류 알림 미들웨어 설정
add_slack_middleware(
    app=app,
    webhook_url=webhook_url,
    include_traceback=True,
    include_request_body=True,
    error_status_codes=[500, 503, 504],  # 이 상태 코드들에 대해 알림 발송
    environment=settings.ENV,
    notify_environments=["stage", "dev"],
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if settings.ENV == "stage" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "Authorization", "Authorization_Swagger", "Sns-Type", "Client-Type"],
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
        # if not database.check_connection():
        #     raise Exception("Database connection test failed")

        if not await database.check_connection_async():
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


@app.get("/error_test")
def query_test(num: int):
    return num / 0


@app.get("/error_test/{num}")
def parameter_test(num: int):
    if num != 0:
        return num / 0
    else:
        return num / 1


class TestRequest(BaseModel):
    num: int


@app.post("/error_test")
def request_test(request: TestRequest):
    if request.num != 0:
        return request.num / 0
    else:
        return request.num / 1


# 앱 시작/종료 이벤트 핸들러
@app.on_event("startup")
async def startup_event():
    # 채팅 서비스 초기화
    logger.info("Application started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down")
    await cleanup_elasticsearch()
