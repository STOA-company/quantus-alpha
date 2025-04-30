import logging

from prometheus_client import Counter, Gauge, Histogram

# 로거 설정
logger = logging.getLogger(__name__)

# 기본 요청 측정 메트릭
REQUEST_COUNT = Counter("starlette_requests_total", "Total HTTP requests", ["app_name", "method", "path", "status_code"])

CLIENT_REQUEST_COUNT = Counter(
    "starlette_client_requests_total", "Total HTTP requests by client IP", ["client_ip", "endpoint", "method"]
)

REQUEST_LATENCY = Histogram(
    "starlette_request_duration_seconds",
    "HTTP request duration, in seconds",
    ["app_name", "method", "path", "status_code"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0),
)

# 요청 진행 상태 메트릭
REQUESTS_IN_PROGRESS = Gauge(
    "starlette_requests_in_progress", "Total HTTP requests currently in progress", ["app_name", "method"]
)

# 에러율 메트릭
ERROR_RATE = Gauge("starlette_error_rate", "Current error rate (percentage of all requests)", ["app_name", "path"])


# 스트리밍 관련 메트릭
STREAMING_CONNECTIONS = Gauge(
    "starlette_streaming_connections",
    "Current number of active streaming connections",
)

STREAMING_MESSAGES_COUNT = Counter(
    "starlette_streaming_messages_total", "Total number of streaming messages sent", ["conversation_id"]
)

STREAMING_ERRORS = Counter(
    "starlette_streaming_errors_total", "Total number of streaming errors", ["error_type", "conversation_id"]
)

# 전역 카운터 변수 (에러율 계산용)
_total_requests = 0
_error_requests = {}  # path별 에러 카운트


def update_error_rates():
    """
    각 경로별 에러율을 계산하여 메트릭 업데이트
    """
    global _total_requests, _error_requests

    # 현재 응답 코드별 총 요청 수 가져오기
    for path, error_count in _error_requests.items():
        if _total_requests > 0:
            error_rate = (error_count / _total_requests) * 100
            ERROR_RATE.labels(app_name="starlette", path=path).set(error_rate)
            logger.debug(f"Updated error rate for path {path}: {error_rate:.2f}% ({error_count}/{_total_requests})")


def track_request_status(path, status_code):
    """
    요청 결과를 추적하여 에러율 계산을 위한 카운터 업데이트

    Args:
        path (str): 요청 경로
        status_code (int): 응답 상태 코드
    """
    global _total_requests, _error_requests

    _total_requests += 1

    # 에러 응답(4xx, 5xx)인 경우 에러 카운트 증가
    if status_code >= 400:
        if path not in _error_requests:
            _error_requests[path] = 0
        _error_requests[path] += 1
        logger.debug(f"Tracked error for path {path}, status code {status_code}")

    # 디버그 로깅
    if _total_requests % 100 == 0:
        logger.info(f"Total requests tracked: {_total_requests}, Total errors: {sum(_error_requests.values())}")
