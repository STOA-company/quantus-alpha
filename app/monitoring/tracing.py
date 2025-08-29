from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.exporter.zipkin.json import ZipkinExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
from opentelemetry.sdk.resources import Resource
from fastapi import FastAPI

from app.core.config import settings
import dotenv
import os
dotenv.load_dotenv()


def setup_zipkin_tracing(app: FastAPI) -> None:
    """
    FastAPI 애플리케이션에 Zipkin 트레이싱을 설정합니다.
    DB 커넥션과 API 요청을 자동으로 추적합니다.
    """
    # Zipkin 설정
    zipkin_endpoint = os.getenv("ZIPKIN_ENDPOINT", "http://localhost:9411/api/v2/spans")
    zipkin_exporter = ZipkinExporter(endpoint=zipkin_endpoint)
    processor = BatchSpanProcessor(zipkin_exporter)

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