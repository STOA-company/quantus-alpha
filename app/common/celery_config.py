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

CELERY_APP.conf.beat_schedule = {
    "hello-task": {
        "task": "hello_task",
        "schedule": crontab(minute="*/1"),
    },
}
CELERY_APP.conf.timezone = "UTC"
