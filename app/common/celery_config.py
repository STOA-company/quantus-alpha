import os
from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

IN_DOCKER = os.getenv("IN_DOCKER", "false").lower() == "true"

if IN_DOCKER:
    CELERY_BROKER_URL = f"pyamqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}@rabbitmq//"
    CELERY_RESULT_BACKEND = "redis://redis"
else:
    CELERY_BROKER_URL = f"pyamqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}@localhost//"
    CELERY_RESULT_BACKEND = "redis://localhost"

CELERY_APP = Celery("worker", broker=CELERY_BROKER_URL, result_backend=CELERY_RESULT_BACKEND)

# Celery 설정
CELERY_APP.conf.timezone = "Asia/Seoul"
CELERY_APP.conf.task_serializer = "json"
CELERY_APP.conf.result_serializer = "json"
CELERY_APP.conf.accept_content = ["json"]
CELERY_APP.conf.task_track_started = True
CELERY_APP.conf.task_time_limit = 1800  # 30분
CELERY_APP.conf.worker_prefetch_multiplier = 1  # 작업 분배 최적화

# Celery Beat Schedule
CELERY_APP.conf.beat_schedule = {
    # 미국 주식 스케줄
    "us-daily-stock-trend": {
        "task": "stock_trend_1d_us",
        "schedule": crontab(hour="6", minute="0"),  # KST 06:00 (EST 16:00) - 미국장 마감 후
    },
    "us-realtime-stock-trend": {
        "task": "stock_trend_realtime_us",
        "schedule": crontab(minute="*/10"),  # 10분마다 실행 (장중)
    },
    # # 한국 주식 스케줄
    "kr-daily-stock-trend": {
        "task": "stock_trend_1d_kr",
        "schedule": crontab(hour="16", minute="0"),  # KST 16:00 - 한국장 마감 후
    },
    # "kr-realtime-stock-trend": {
    #     "task": "stock_trend_realtime_kr",
    #     "schedule": crontab(minute="*/10"),  # 10분마다 실행 (장중)
    # },
    "us-stock-indices": {
        "task": "us_stock_indices_batch",
        "schedule": crontab(minute="*/15"),  # 15분마다 실행
    },
    # 공통 스케줄
    "ticker-update": {
        "task": "stock_trend_tickers",
        "schedule": crontab(hour="5", minute="30"),  # KST 05:30 - 일일 티커 정보 업데이트
    },
    # # 테스트용 태스크
    # "hello-task": {
    #     "task": "hello_task",
    #     "schedule": crontab(minute="*"),  # 1분마다 실행
    # }
    # 뉴스 한국 배치
    "kr-news-batch": {
        "task": "kr_news_batch",
        "schedule": crontab(hour="11", minute="50"),  # 매일 오전 11시 50분
    },
    # 뉴스 미국 배치
    "us-news-batch": {
        "task": "us_news_batch",
        "schedule": crontab(hour="11", minute="50"),  # 매일 오전 11시 50분
    },
    # 공시 한국 배치
    "kr-disclosure-batch": {
        "task": "kr_disclosure_batch",
        "schedule": crontab(minute="6,16,26,36,46,56"),  # 매 10분마다 실행
    },
    # 공시 미국 배치
    "us-disclosure-batch": {
        "task": "us_disclosure_batch",
        "schedule": crontab(minute="6,16,26,36,46,56"),  # 매 10분마다 실행
    },
    # 뉴스 업데이트 renewal
    "kr-news-renewal": {
        "task": "kr_news_renewal",
        "schedule": crontab(minute="10,40"),
    },
    "us-news-renewal": {
        "task": "us_news_renewal",
        "schedule": crontab(minute="10,40"),
    },
}
