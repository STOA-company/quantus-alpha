import time
from typing import Dict

import psutil
from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase

# HTTP request metrics
REQUEST_COUNT = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status_code"])

# IP 주소와 엔드포인트별 요청 수를 추적하는 새 메트릭
IP_REQUEST_COUNT = Counter(
    "http_ip_requests_total", "Total number of HTTP requests by IP", ["client_ip", "endpoint", "method"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
)

# System metrics
CPU_USAGE = Gauge("process_cpu_percent", "Current CPU usage percentage")

MEMORY_USAGE = Gauge("process_memory_rss", "Current memory usage in bytes")

# 시스템 전체 메모리 관련 메트릭 추가
TOTAL_SYSTEM_MEMORY = Gauge("system_memory_total_bytes", "Total system memory in bytes")
AVAILABLE_SYSTEM_MEMORY = Gauge("system_memory_available_bytes", "Available system memory in bytes")
MEMORY_USAGE_PERCENT = Gauge("system_memory_usage_percent", "System memory usage percentage")

# Error rate gauge - 직접 계산된 오류율 메트릭 (0-100%)
ERROR_RATE = Gauge("endpoint_error_rate_percent", "Error rate percentage by endpoint", ["endpoint"])

# 마지막 CPU 측정 시간
last_cpu_measure_time = 0
# 초기 CPU 사용량 측정값
initial_cpu_percent = psutil.Process().cpu_percent()


# Update system metrics
def update_system_metrics():
    global last_cpu_measure_time, initial_cpu_percent
    process = psutil.Process()

    # CPU 사용량 측정 - 최소 1초 간격으로 측정하여 정확성 향상
    current_time = time.time()
    if current_time - last_cpu_measure_time >= 1.0:
        cpu_percent = process.cpu_percent(interval=0)
        if cpu_percent > 0:  # 유효한 값만 설정
            CPU_USAGE.set(cpu_percent)
            last_cpu_measure_time = current_time

    # 메모리 사용량 측정
    MEMORY_USAGE.set(process.memory_info().rss)

    # 시스템 전체 메모리 정보 수집
    mem = psutil.virtual_memory()
    TOTAL_SYSTEM_MEMORY.set(mem.total)
    AVAILABLE_SYSTEM_MEMORY.set(mem.available)
    MEMORY_USAGE_PERCENT.set(mem.percent)


# 엔드포인트별 오류율 업데이트 함수
def update_error_rates():
    # 모든 요청과 오류 요청에 대한 정보를 수집
    from prometheus_client.core import REGISTRY

    # 엔드포인트별 총 요청 수
    endpoints_total = {}
    # 엔드포인트별 오류 요청 수
    endpoints_errors = {}

    # 모든 요청 카운터 데이터 수집
    for metric in REGISTRY.collect():
        if metric.name == "http_requests_total":
            for sample in metric.samples:
                if "endpoint" in sample.labels and "status_code" in sample.labels:
                    endpoint = sample.labels["endpoint"]
                    status_code = sample.labels["status_code"]

                    # 해당 엔드포인트의 총 요청 수 업데이트
                    if endpoint not in endpoints_total:
                        endpoints_total[endpoint] = 0
                    endpoints_total[endpoint] += sample.value

                    # 오류 요청(4xx, 5xx)인 경우 오류 카운트 업데이트
                    if status_code.startswith("4") or status_code.startswith("5"):
                        if endpoint not in endpoints_errors:
                            endpoints_errors[endpoint] = 0
                        endpoints_errors[endpoint] += sample.value

    # 각 엔드포인트별 오류율 계산 및 게이지 업데이트
    for endpoint in endpoints_total:
        if endpoints_total[endpoint] > 0:
            error_count = endpoints_errors.get(endpoint, 0)
            error_rate = (error_count / endpoints_total[endpoint]) * 100
            ERROR_RATE.labels(endpoint=endpoint).set(error_rate)


# Application metrics
ACTIVE_USERS = Gauge("app_active_users", "Number of currently active users")

DB_CONNECTION_POOL = Gauge("app_db_connection_pool_size", "Database connection pool size")

# Redis metrics
REDIS_OPERATIONS = Counter("app_redis_operations_total", "Total number of Redis operations", ["operation", "status"])

REDIS_OPERATION_LATENCY = Histogram(
    "app_redis_operation_duration_seconds",
    "Redis operation duration in seconds",
    ["operation"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1],
)

# All metrics collection for easy access
METRICS: Dict[str, MetricWrapperBase] = {
    "request_count": REQUEST_COUNT,
    "request_latency": REQUEST_LATENCY,
    "ip_request_count": IP_REQUEST_COUNT,
    "cpu_usage": CPU_USAGE,
    "memory_usage": MEMORY_USAGE,
    "error_rate": ERROR_RATE,
    "active_users": ACTIVE_USERS,
    "db_connection_pool": DB_CONNECTION_POOL,
    "redis_operations": REDIS_OPERATIONS,
    "redis_operation_latency": REDIS_OPERATION_LATENCY,
}
