import time
from typing import Callable, List, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .web_metrics import (
    # 기존 메트릭
    CLIENT_REQUEST_COUNT,
    REQUEST_COUNT,
    REQUEST_LATENCY,
    # 스트리밍 관련 메트릭
    STREAMING_CONNECTIONS,
    STREAMING_ERRORS,
    STREAMING_MESSAGES_COUNT,
    update_error_rates,
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        app_name: str = "starlette",
        exclude_paths: Optional[List[str]] = None,
        error_rate_update_interval: int = 5,
        system_metrics_update_interval: int = 5,
    ):
        super().__init__(app)
        self.app_name = app_name

        # 메트릭 수집에서 제외할 경로 목록
        self.exclude_paths: List[str] = exclude_paths or [
            "/health-check",  # 헬스체크 API
            "/metrics",  # 메트릭 엔드포인트 자체
            "/docs",  # Swagger 문서
            "/redoc",  # ReDoc 문서
            "/openapi.json",  # OpenAPI 스키마
        ]

        # 업데이트 타이밍 관련 변수
        self.last_error_rate_update = 0
        self.last_system_metrics_update = 0
        self.error_rate_update_interval = error_rate_update_interval
        self.system_metrics_update_interval = system_metrics_update_interval

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        path = request.url.path
        method = request.method
        client_ip = request.client.host if request.client else "unknown"

        # 제외 경로 목록에 있으면 메트릭 수집 없이 바로 처리
        if any(path.startswith(excluded) for excluded in self.exclude_paths):
            print(f"[DEBUG] 메트릭 제외 경로: {path}")
            return await call_next(request)

        print(f"[DEBUG] 메트릭 수집 경로: {path}, 메서드: {method}, IP: {client_ip}")

        # 스트리밍 연결 처리
        is_streaming = self._is_streaming_request(request)
        if is_streaming:
            STREAMING_CONNECTIONS.inc()

        # 응답 처리
        try:
            response = await call_next(request)
            status_code = response.status_code

            # 스트리밍 메트릭 처리 (성공 시)
            if is_streaming and status_code < 400:
                conversation_id = self._get_conversation_id(request)
                if conversation_id:
                    STREAMING_MESSAGES_COUNT.labels(conversation_id=conversation_id).inc()

        except Exception as e:
            status_code = 500

            # 스트리밍 에러 메트릭 처리
            if is_streaming:
                conversation_id = self._get_conversation_id(request)
                error_type = type(e).__name__
                STREAMING_ERRORS.labels(error_type=error_type, conversation_id=conversation_id or "unknown").inc()

            raise e

        finally:
            # 스트리밍 연결 종료 시 카운터 감소
            if is_streaming:
                STREAMING_CONNECTIONS.dec()

            # 요청 소요 시간 기록
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(
                app_name=self.app_name, method=method, path=path, status_code=str(status_code)
            ).observe(duration)

            # 일반 요청 메트릭 기록 부분 수정
            REQUEST_COUNT.labels(app_name=self.app_name, method=method, path=path, status_code=str(status_code)).inc()
            CLIENT_REQUEST_COUNT.labels(client_ip=client_ip, endpoint=path, method=method).inc()

            print(
                f"[DEBUG] 메트릭 카운터 증가: app_name={self.app_name}, method={method}, path={path}, status_code={status_code}"
            )

            # 시스템 메트릭 업데이트 (주기적으로)
            current_time = time.time()

            # 주기적으로 오류율 업데이트
            if current_time - self.last_error_rate_update >= self.error_rate_update_interval:
                update_error_rates()
                self.last_error_rate_update = current_time

        return response

    def _is_streaming_request(self, request: Request) -> bool:
        """스트리밍 요청인지 확인"""
        path = request.url.path
        # 스트리밍 API 경로 패턴 확인 (실제 앱에 맞게 수정 필요)
        is_stream_path = path.startswith("/api/v1/chat/stream") or "stream" in path
        # 헤더에서 스트리밍 관련 정보 확인
        accept_header = request.headers.get("accept", "")
        is_stream_header = "text/event-stream" in accept_header

        return is_stream_path or is_stream_header

    def _is_llm_request(self, request: Request) -> bool:
        """LLM API 요청인지 확인"""
        path = request.url.path
        # LLM API 호출 경로 패턴 확인 (실제 앱에 맞게 수정 필요)
        return path.startswith("/api/v1/chat/")

    def _get_conversation_id(self, request: Request) -> Optional[str]:
        """대화 ID 추출"""
        path = request.url.path

        # URL 경로에서 대화 ID 추출 시도 (예: /api/v1/chat/conversation/abc123)
        if "conversation" in path:
            parts = path.split("/")
            for i, part in enumerate(parts):
                if part == "conversation" and i + 1 < len(parts):
                    return parts[i + 1]

        # 쿼리 파라미터에서 추출 시도
        try:
            conversation_id = request.query_params.get("conversation_id")
            if conversation_id:
                return conversation_id
        except Exception:
            pass

        return None
