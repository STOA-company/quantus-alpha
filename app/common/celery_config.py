from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

if settings.ENV == "prod":
    CELERY_BROKER_URL = "pyamqp://admin:admin@rabbitmq//"
    CELERY_RESULT_BACKEND = "redis://redis"
else:
    CELERY_BROKER_URL = "pyamqp://admin:admin@localhost//"
    CELERY_RESULT_BACKEND = "redis://localhost"

CELERY_APP = Celery("worker", broker=CELERY_BROKER_URL, result_backend=CELERY_RESULT_BACKEND)

# 배치 작업 목록
TASKS = [
    "us_stock_indices_batch",
]

CELERY_APP.conf.beat_schedule = {
    f"{task}-schedule": {
        "task": task,
        "schedule": crontab(minute="*/15"),
    }
    for task in TASKS
}

CELERY_APP.conf.timezone = "UTC"
