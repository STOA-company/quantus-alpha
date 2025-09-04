from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
from opentelemetry.sdk.resources import Resource
from fastapi import FastAPI
import logging
import requests

from app.core.config import settings
import dotenv
import os
dotenv.load_dotenv()

logger = logging.getLogger(__name__)


def _check_zipkin_health(endpoint: str) -> bool:
    """Zipkin 엔드포인트 상태 확인"""
    try:
        health_url = endpoint.replace('/api/v2/spans', '/health')
        response = requests.get(health_url, timeout=3)
        return response.status_code == 200
    except Exception as e:
        logger.warning(f"Zipkin health check failed: {e}")
        return False


def setup_zipkin_tracing(app: FastAPI) -> None:
    """
    FastAPI 애플리케이션에 Zipkin 트레이싱을 설정합니다.
    DB 커넥션과 API 요청을 자동으로 추적합니다.
    Zipkin 연결 실패 시 graceful degradation 적용.
    """
    zipkin_endpoint = os.getenv("ZIPKIN_ENDPOINT", "http://localhost:9411/api/v2/spans")
    
    # Zipkin 서버 상태 확인
    if not _check_zipkin_health(zipkin_endpoint):
        logger.warning("Zipkin server unavailable - tracing disabled")
        return
    
    try:
        # Zipkin 설정 (타임아웃 및 배치 크기 제한)
        zipkin_exporter = ZipkinExporter(
            endpoint=zipkin_endpoint,
            timeout=5  # 5초 타임아웃
        )
        processor = BatchSpanProcessor(
            zipkin_exporter,
            max_queue_size=512,      # 큐 크기 제한
            export_timeout_millis=5000,  # 5초 타임아웃
            schedule_delay_millis=5000   # 5초마다 배치 전송
        )

        # 서비스 이름 설정
        resource = Resource.create({
            "service.name": f"alphafinder-{settings.ENV}",
            "service.version": "1.0.0",
            "deployment.environment": settings.ENV,
            "host.name": "alphafinder-api"
        })
        provider = TracerProvider(resource=resource)

        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        # FastAPI instrumentor 활성화 - API 요청 자동 추적
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=provider
        )

        # SQLAlchemy instrumentor 활성화 - DB 쿼리 자동 추적
        SQLAlchemyInstrumentor().instrument(
            enable_commenter=True,      # SQL 주석 추가
            commenter_options={},        # 쿼리 정보 포함
            trace_parent_span=True,     # 부모 스팬과 연결
            span_details=True           # 상세 정보 포함
        )

        # PyMySQL instrumentor 활성화 - MySQL 커넥션 자동 추적
        PyMySQLInstrumentor().instrument()
        
        logger.info("Zipkin tracing configured successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup Zipkin tracing: {e}")
        logger.warning("Application will continue without tracing")