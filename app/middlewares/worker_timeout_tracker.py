import os
import time
import requests
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.core.logger import setup_logger

logger = setup_logger(__name__)

# 현재 워커의 활성 요청을 추적하는 전역 변수
active_requests = {}


class WorkerTimeoutTracker(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        timeout_threshold: float = 80.0,  # 80초 이상 걸리는 요청들을 추적 (gunicorn timeout 90초보다 적게)
        webhook_url: str = None,
        environment: str = "production",
        notify_environments: list = None,
    ):
        super().__init__(app)
        self.timeout_threshold = timeout_threshold
        self.webhook_url = "https://hooks.slack.com/services/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"
        self.environment = environment
        self.notify_environments = notify_environments or ["stage", "dev", "prod", "production"]
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        
        # 요청 시작 로그
        method = request.method
        path = request.url.path
        client_ip = request.client.host if request.client else "Unknown"
        
        # 헬스 체크 요청은 로그에서 제외
        if path not in ["/health-check", "/health", "/metrics"]:
            logger.info(f"Request started: {method} {path} from {client_ip}")
        
        try:
            response = await call_next(request)
            end_time = time.time()
            duration = end_time - start_time
            
            # 긴 요청 추적
            if duration >= self.timeout_threshold:
                logger.info(f"Request Long Timeout: {method} {path} - {response.status_code} - {duration:.2f}s")
                await self._notify_long_request(request, duration, "COMPLETED")
            
            # 헬스 체크 요청은 로그에서 제외
            if path not in ["/health-check", "/health", "/metrics"]:
                logger.info(f"Request completed: {method} {path} - {response.status_code} - {duration:.2f}s")
            return response
            
        except Exception as exc:
            end_time = time.time()
            duration = end_time - start_time
            
            # 에러가 발생한 긴 요청도 추적
            if duration >= self.timeout_threshold:
                logger.info(f"Request Failed Long Timeout: {method} {path} - {response.status_code} - {duration:.2f}s")
                await self._notify_long_request(request, duration, "ERROR", str(exc))
            
            logger.error(f"Request failed: {method} {path} - {duration:.2f}s - Error: {str(exc)}")
            raise
    
    async def _notify_long_request(
        self, 
        request: Request, 
        duration: float, 
        status: str, 
        error: str = None
    ):
        """긴 요청에 대한 Slack 알림을 전송합니다."""
        if self.environment not in self.notify_environments:
            return
            
        try:
            method = request.method
            path = request.url.path
            full_url = str(request.url)
            client_ip = request.client.host if request.client else "Unknown"
            
            # 경고 이모지 선택
            emoji = "⚠️" if status == "COMPLETED" else "❌"
            
            message_parts = [
                f"{emoji} **Worker Timeout 위험 감지**",
                "",
                f"*환경*: {self.environment}",
                f"*상태*: {status}",
                f"*실행 시간*: {duration:.2f}초 (임계값: {self.timeout_threshold}초)",
                f"*메서드*: {method}",
                f"*경로*: {path}",
                f"*전체 URL*: {full_url}",
                f"*클라이언트 IP*: {client_ip}",
                ""
            ]
            
            if error:
                message_parts.append(f"*에러*: {error}")
            
            if duration >= 90:
                message_parts.append("🚨 **Worker Timeout 임박** - Gunicorn worker가 곧 종료될 수 있습니다!")
            
            message = "\n".join(message_parts)
            
            # Slack 알림 전송
            self._send_slack_message(message)
            
        except Exception as e:
            logger.exception(f"Error in WorkerTimeoutTracker notification: {e}")
    
    def _send_slack_message(self, message: str):
        """Slack에 직접 POST 요청을 보냅니다."""
        if not self.webhook_url:
            logger.warning("Slack webhook URL이 설정되지 않았습니다.")
            return
            
        try:
            payload = {
                "text": message,
                "username": "Worker Timeout Tracker",
                "icon_emoji": ":warning:"
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            logger.info("Slack 알림이 성공적으로 전송되었습니다.")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Slack 알림 전송 실패: {e}")
        except Exception as e:
            logger.error(f"Slack 알림 전송 중 예상치 못한 오류: {e}")


def add_worker_timeout_tracker(
    app,
    timeout_threshold: float = 80.0,
    webhook_url: str = None,
    environment: str = "production",
    notify_environments: list = None,
):
    """
    Worker timeout 추적 미들웨어를 FastAPI 애플리케이션에 추가합니다.
    
    Args:
        app: FastAPI 애플리케이션 인스턴스
        timeout_threshold: 알림을 보낼 임계 시간(초)
        webhook_url: Slack webhook URL
        environment: 현재 환경
        notify_environments: 알림을 보낼 환경 목록
    """
    app.add_middleware(
        WorkerTimeoutTracker,
        timeout_threshold=timeout_threshold,
        webhook_url=webhook_url,
        environment=environment,
        notify_environments=notify_environments,
    )