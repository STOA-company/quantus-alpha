import time
from typing import Callable, List

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .metrics import IP_REQUEST_COUNT, REQUEST_COUNT, REQUEST_LATENCY, update_error_rates, update_system_metrics


class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        # 메트릭 수집에서 제외할 경로 목록
        self.exclude_paths: List[str] = [
            "/metrics",  # 메트릭 엔드포인트 자체
            "/health-check",  # 헬스체크 API
            "/docs",  # Swagger 문서
            "/redoc",  # ReDoc 문서
            "/openapi.json",  # OpenAPI 스키마
        ]
        # 마지막 오류율 업데이트 시간
        self.last_error_rate_update = 0
        # 오류율 업데이트 간격 (초)
        self.error_rate_update_interval = 5

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        path = request.url.path

        # 제외 경로 목록에 있으면 메트릭 수집 없이 바로 처리
        if any(path.startswith(excluded) for excluded in self.exclude_paths):
            return await call_next(request)

        method = request.method
        client_ip = request.client.host if request.client else "unknown"

        try:
            response = await call_next(request)
            status_code = response.status_code

        except Exception as e:
            status_code = 500
            raise e
        finally:
            # Record request duration
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(method=method, endpoint=path).observe(duration)

            # Record request count
            REQUEST_COUNT.labels(method=method, endpoint=path, status_code=status_code).inc()

            # Record IP request count
            IP_REQUEST_COUNT.labels(client_ip=client_ip, endpoint=path, method=method).inc()

            # Update system metrics
            update_system_metrics()

            # 주기적으로 오류율 업데이트 (모든 요청마다 실행하지 않고 일정 간격으로)
            current_time = time.time()
            if current_time - self.last_error_rate_update >= self.error_rate_update_interval:
                update_error_rates()
                self.last_error_rate_update = current_time

        return response
