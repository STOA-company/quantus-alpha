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
    # 메모리 상태 체크
    "memory-status": {
        "task": "memory-status",
        "schedule": crontab(minute="1,11,21,31,41,51"),
    },
    # 한국 주식 분봉 스케줄
    "kr-stock-minute-batch": {
        "task": "kr_stock_minute_batch",
        "schedule": crontab(minute="1,16,31,46"),  # 15분마다 실행 (장중)
    },
    "kr-stock-minute-batch-last": {
        "task": "kr_stock_minute_batch_last",
        "schedule": crontab(hour="15", minute="30"),
    },
    # 미국 주식 스케줄
    "us-daily-stock-trend": {
        "task": "stock_trend_1d_us",
        "schedule": crontab(hour="11", minute="3"),  # KST 11:03 미국장 마감 후
    },
    "us-realtime-stock-trend": {
        "task": "stock_trend_realtime_us",
        "schedule": crontab(minute="12,27,42,57"),  # 15분마다 실행 (장중)
    },
    "us-daily-stock-trend-reset": {
        "task": "stock_trend_reset_us",
        "schedule": crontab(hour="22", minute="20"),  # KST 22:20
    },
    # # 한국 주식 스케줄
    "kr-daily-stock-trend": {
        "task": "stock_trend_1d_kr",
        "schedule": crontab(hour="16", minute="10"),  # KST 16:10
    },
    "kr-daily-stock-trend-reset": {
        "task": "stock_trend_reset_kr",
        "schedule": crontab(hour="8", minute="50"),  # KST 08:50
    },
    "kr-realtime-stock-trend": {
        "task": "stock_trend_realtime_kr",
        "schedule": crontab(minute="12,27,42,57"),  # 15분마다 실행 (장중)
    },
    # 주가 지수 스케줄
    "us-stock-indices": {
        "task": "us_stock_indices_batch",
        "schedule": crontab(minute="*/15"),  # 15분마다 실행
    },
    "kr-stock-indices": {
        "task": "kr_stock_indices_batch",
        "schedule": crontab(minute="*/15"),  # 15분마다 실행
    },
    "kr-stock-indices-collect": {
        "task": "kr_stock_indices_collect",
        "schedule": crontab(minute="*/2"),  # 2분마다 실행
    },
    "us-stock-indices-collect": {
        "task": "us_stock_indices_collect",
        "schedule": crontab(minute="*/2"),  # 2분마다 실행
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
        "schedule": crontab(minute="10,25,40,55"),
    },
    "us-news-renewal": {
        "task": "us_news_renewal",
        "schedule": crontab(minute="10,25,40,55"),
    },
    "process_outliers_us": {
        "task": "process_outliers_us",
        "schedule": crontab(hour="23", minute="35"),
    },
    "process_outliers_kr": {
        "task": "process_outliers_kr",
        "schedule": crontab(hour="09", minute="05"),
    },
    "check_warned_stock_kr": {
        "task": "check_warned_stock_kr",
        "schedule": crontab(hour="08", minute="00"),
    },
    "check_warned_stock_us": {
        "task": "check_warned_stock_us",
        "schedule": crontab(hour="22", minute="00"),
    },
    "reset-daily-leaderboard": {
        "task": "reset_daily_leaderboard",
        "schedule": crontab(hour="0", minute="0"),
    },
    "update_us_top_gainers": {
        "task": "update_us_top_gainers",
        "schedule": crontab(hour="22, 23", minute="50"),
    },
    "update_us_top_losers": {
        "task": "update_us_top_losers",
        "schedule": crontab(hour="22, 23", minute="50"),
    },
    "update_kr_top_gainers": {
        "task": "update_kr_top_gainers",
        "schedule": crontab(hour="9", minute="1"),
    },
    "update_kr_top_losers": {
        "task": "update_kr_top_losers",
        "schedule": crontab(hour="9", minute="1"),
    },
    "update_us_etf_parquet": {  # 미국 ETF 팩터 파일 업데이트
        "task": "update_us_etf_parquet",
        "schedule": crontab(hour="11", minute="30"),
    },
    "update_kr_etf_parquet": {  # 한국 ETF 팩터 파일 업데이트
        "task": "update_kr_etf_parquet",
        "schedule": crontab(hour="18", minute="30"),
    },
    "update_us_stock_dividend_parquet": {  # 미국 주식 배당 파일 업데이트
        "task": "update_us_stock_dividend_parquet",
        "schedule": crontab(hour="11", minute="37"),
    },
    "update_kr_stock_dividend_parquet": {  # 한국 주식 배당 파일 업데이트
        "task": "update_kr_stock_dividend_parquet",
        "schedule": crontab(hour="18", minute="37"),
    },
    "update_us_dividend_rds": {  # 미국 배당 데이터베이스 업데이트
        "task": "update_us_dividend_rds",
        "schedule": crontab(hour="11", minute="45"),
    },
    "update_kr_dividend_rds": {  # 한국 배당 데이터베이스 업데이트
        "task": "update_kr_dividend_rds",
        "schedule": crontab(hour="18", minute="45"),
    },
    "update_us_etf_price": {
        "task": "update_us_etf_price",
        "schedule": crontab(hour="11", minute="50"),
    },
    "update_kr_etf_price": {
        "task": "update_kr_etf_price",
        "schedule": crontab(hour="18", minute="50"),
    },
    "kr_update_etf_status": {
        "task": "kr_update_etf_status",
        "schedule": crontab(hour="18", minute="25"),
    },
    "us_update_etf_status": {
        "task": "us_update_etf_status",
        "schedule": crontab(hour="11", minute="25"),
    },
    "kr_update_etf_holdings": {
        "task": "kr_update_etf_holdings",
        "schedule": crontab(hour="16", minute="30", day_of_week="5"),
    },
    "us_update_etf_holdings": {
        "task": "us_update_etf_holdings",
        "schedule": crontab(hour="11", minute="30", day_of_week="6"),
    },
    "update_krx_etf_data": {
        "task": "update_krx_etf_data",
        "schedule": crontab(hour="17", minute="00"),
    },
}
