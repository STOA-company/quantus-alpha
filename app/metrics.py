from prometheus_client import Counter, Histogram, Gauge, REGISTRY, CONTENT_TYPE_LATEST, generate_latest
from typing import Optional
from starlette.responses import Response
import time

# API 요청 메트릭
REQUEST_COUNT = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status"])

# API 응답 시간 메트릭
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0],
)

# 현재 활성 사용자 수
ACTIVE_USERS = Gauge("active_users_total", "Number of currently active users")

# API 별 에러율
ERROR_COUNT = Counter("http_errors_total", "Total number of HTTP errors", ["method", "endpoint", "error_type"])


class MetricsMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]
        if path == "/metrics":
            response = Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
            await response(scope, receive, send)
            return

        method = scope["method"]

        start_time = time.time()
        ACTIVE_USERS.inc()

        async def wrapped_send(message):
            if message["type"] == "http.response.start":
                status_code = message["status"]
                REQUEST_COUNT.labels(method=method, endpoint=path, status=status_code).inc()

                if 400 <= status_code < 600:
                    ERROR_COUNT.labels(method=method, endpoint=path, error_type=str(status_code)).inc()

            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        except Exception as e:
            ERROR_COUNT.labels(method=method, endpoint=path, error_type=type(e).__name__).inc()
            raise
        finally:
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(method=method, endpoint=path).observe(duration)
            ACTIVE_USERS.dec()


def track_active_user(user_id: Optional[str] = None, increment: bool = True):
    """사용자 활성화/비활성화 추적"""
    if increment:
        ACTIVE_USERS.inc()
    else:
        ACTIVE_USERS.dec()
