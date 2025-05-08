# batch_metric.py
import logging
import time
from functools import wraps

import psutil
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

SYSTEM_MEMORY_USAGE = Gauge("batch_system_memory_usage_bytes", "Memory usage in bytes")

SYSTEM_MEMORY_PERCENT = Gauge("batch_system_memory_usage_percent", "Memory usage percentage")

SYSTEM_CPU_PERCENT = Gauge("batch_system_cpu_usage_percent", "CPU usage percentage")

TASK_EXECUTION_TIME = Histogram(
    "batch_task_execution_time_seconds",
    "Task execution time in seconds",
    ["task_name"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0),
)

TASK_COUNT = Counter(
    "batch_tasks_total",
    "Total number of tasks",
    ["task_name", "status"],  # status: started, success, failed
)

TASKS_IN_PROGRESS = Gauge("batch_tasks_in_progress", "Number of tasks currently in progress", ["task_name"])


def monitor_task_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__

        # 태스크 시작 측정
        TASK_COUNT.labels(task_name=task_name, status="started").inc()
        TASKS_IN_PROGRESS.labels(task_name=task_name).inc()

        start_time = time.time()

        try:
            logger.info(f"Starting task: {task_name}")
            result = func(*args, **kwargs)

            execution_time = time.time() - start_time
            TASK_EXECUTION_TIME.labels(task_name=task_name).observe(execution_time)
            TASK_COUNT.labels(task_name=task_name, status="success").inc()

            logger.info(f"Successfully completed task: {task_name} in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            TASK_COUNT.labels(task_name=task_name, status="failed").inc()
            logger.error(f"Error in {task_name}: {str(e)}", exc_info=True)
            raise
        finally:
            TASKS_IN_PROGRESS.labels(task_name=task_name).dec()

    return wrapper


def collect_system_metrics():
    try:
        memory = psutil.virtual_memory()
        SYSTEM_MEMORY_USAGE.set(memory.used)
        SYSTEM_MEMORY_PERCENT.set(memory.percent)

        cpu_percent = psutil.cpu_percent(interval=1)
        SYSTEM_CPU_PERCENT.set(cpu_percent)

        logger.debug(f"System metrics - Memory: {memory.percent}%, CPU: {cpu_percent}%")
    except Exception as e:
        logger.error(f"Error collecting system metrics: {str(e)}")


def start_metrics_server(port=8000):
    try:
        start_http_server(port)
        logger.info(f"Started metrics server on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {str(e)}")
