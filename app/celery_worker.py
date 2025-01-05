import logging
from celery.schedules import crontab
from app.common.celery_config import CELERY_APP
from app.core.config import settings
from app.batches.run_stock_trend import (
    run_stock_trend_by_1d_batch,
)
from app.utils.date_utils import get_session_checker, now_kr
from functools import wraps


def check_market_open(func):
    """미국 장 운영 중일 때만 실행하는 데코레이터"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        session_checker = get_session_checker("US", now_kr())
        if session_checker.is_session(now_kr()):
            return func(*args, **kwargs)
        else:
            logging.info("Market is not open. Skipping realtime task.")
            return None

    return wrapper


def check_market_closed(func):
    """미국 장 종료 후에만 실행하는 데코레이터"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        session_checker = get_session_checker("US", now_kr())
        if not session_checker.is_session(now_kr()):
            return func(*args, **kwargs)
        else:
            logging.info("Market is still open. Skipping daily task.")
            return None

    return wrapper


# Stock trend batch tasks
@CELERY_APP.task(name="stock_trend_1d")
@check_market_closed
def stock_trend_1d_task():
    """일별 주가 트렌드 배치 태스크 (장 마감 후 실행)"""
    try:
        run_stock_trend_by_1d_batch()
    except Exception as e:
        logging.error(f"Error in stock_trend_1d_task: {str(e)}")


# @CELERY_APP.task(name="stock_trend_tickers")
# @check_market_closed
# def stock_trend_tickers_task():
#     """티커 정보 업데이트 배치 태스크 (장 마감 후, 자정 전)"""
#     try:
#         run_stock_trend_tickers_batch()
#     except Exception as e:
#         logging.error(f"Error in stock_trend_tickers_task: {str(e)}")


# Test task
@CELERY_APP.task(name="hello_task", ignore_result=True)
def hello_task():
    print("Hello, World!")


# Existing tasks
@CELERY_APP.task(name="us_stock_indices_batch", ignore_result=True)
def us_run_stock_indices_batch():
    try:
        us_run_stock_indices_batch()
    except Exception as e:
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


# Celery Beat Schedule
CELERY_APP.conf.beat_schedule = {
    # Stock trend schedules
    "daily-stock-trend": {
        "task": "stock_trend_1d",
        "schedule": crontab(hour="6", minute="00"),  # 매일 06:00 KST (미국장 마감 1시간 후)
    },
    # 'ticker-update': {
    #     'task': 'stock_trend_tickers',
    #     'schedule': crontab(hour='5', minute='30'),  # 매일 05:30 KST (미국장 마감 30분 후)
    # },
}

# Celery configuration
CELERY_APP.conf.timezone = "Asia/Seoul"

if __name__ == "__main__":
    CONCURRENCY = 1
    CELERY_APP.worker_main(
        argv=[
            "worker",
            "--beat",
            f"--loglevel={settings.CELERY_LOGLEVEL}",
            "-n",
            "node1@%h",
            f"--concurrency={CONCURRENCY}",
        ]
    )
