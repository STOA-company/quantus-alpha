from typing import Dict

import psutil
from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase

# HTTP request metrics
REQUEST_COUNT = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status_code"])

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
)

# System metrics
CPU_USAGE = Gauge("process_cpu_percent", "Current CPU usage percentage")

MEMORY_USAGE = Gauge("process_memory_rss", "Current memory usage in bytes")


# Update system metrics
def update_system_metrics():
    process = psutil.Process()
    CPU_USAGE.set(process.cpu_percent())
    MEMORY_USAGE.set(process.memory_info().rss)


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
    "cpu_usage": CPU_USAGE,
    "memory_usage": MEMORY_USAGE,
    "active_users": ACTIVE_USERS,
    "db_connection_pool": DB_CONNECTION_POOL,
    "redis_operations": REDIS_OPERATIONS,
    "redis_operation_latency": REDIS_OPERATION_LATENCY,
}
