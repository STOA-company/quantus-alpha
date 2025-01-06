import logging
from celery.schedules import crontab
from functools import wraps
from typing import Literal

from app.common.celery_config import CELERY_APP
from app.core.config import settings
from app.modules.common.enum import TrendingCountry
from app.batches.run_stock_trend import (
    run_stock_trend_by_1d_batch,
    run_stock_trend_tickers_batch,
    run_stock_trend_by_realtime_batch,
)
from app.batches.run_stock_indices import us_run_stock_indices_batch
from app.utils.date_utils import get_session_checker, now_kr


def check_market_status(country: Literal["US", "KR"], require_open: bool = True, skip_on_failure: bool = True):
    """시장 상태를 체크하는 데코레이터

    Args:
        country: 확인할 국가 (US 또는 KR)
        require_open: True면 장 운영 중일 때만 실행, False면 장 종료 후에만 실행
        skip_on_failure: True면 조건 불만족시 건너뛰기, False면 에러 발생
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = now_kr()
            session_checker = get_session_checker(country, current_time)
            is_market_open = session_checker.is_session(current_time)

            if is_market_open == require_open:
                return func(*args, **kwargs)

            msg = f"Market is {'not ' if require_open else ''}open for {country}"
            if skip_on_failure:
                logging.info(f"{msg}. Skipping task.")
                return None
            raise RuntimeError(msg)

        return wrapper

    return decorator


def log_task_execution(func):
    """태스크 실행 로깅 데코레이터"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            logging.info(f"Starting task: {task_name}")
            result = func(*args, **kwargs)
            logging.info(f"Successfully completed task: {task_name}")
            return result
        except Exception as e:
            logging.error(f"Error in {task_name}: {str(e)}", exc_info=True)
            raise

    return wrapper


# Test task
@CELERY_APP.task(name="hello_task", ignore_result=True)
def hello_task():
    """Test task that prints Hello World"""
    print("Hello, World!")


# US Stock Indices task
@CELERY_APP.task(name="us_stock_indices_batch", ignore_result=True)
def us_stock_indices_batch():
    """미국 주가지수 데이터 업데이트"""
    try:
        us_run_stock_indices_batch()
    except Exception as e:
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


# Stock trend tasks
@CELERY_APP.task(name="stock_trend_1d_us")
@log_task_execution
@check_market_status("US", require_open=False)
def stock_trend_1d_us_task():
    """미국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)


@CELERY_APP.task(name="stock_trend_1d_kr")
@log_task_execution
@check_market_status("KR", require_open=False)
def stock_trend_1d_kr_task():
    """한국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    run_stock_trend_by_1d_batch(ctry=TrendingCountry.KR)


@CELERY_APP.task(name="stock_trend_realtime_us")
@log_task_execution
@check_market_status("US", require_open=True)
def stock_trend_realtime_us_task():
    """미국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
    run_stock_trend_by_realtime_batch(ctry=TrendingCountry.US)


# @CELERY_APP.task(name="stock_trend_realtime_kr")
# @log_task_execution
# @check_market_status("KR", require_open=True)
# def stock_trend_realtime_kr_task():
#     """한국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
#     run_stock_trend_by_realtime_batch(ctry=TrendingCountry.KR)


@CELERY_APP.task(name="stock_trend_tickers")
@log_task_execution
def stock_trend_tickers_task():
    """티커 정보 업데이트 (매일)"""
    run_stock_trend_tickers_batch()


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
    # 한국 주식 스케줄
    "kr-daily-stock-trend": {
        "task": "stock_trend_1d_kr",
        "schedule": crontab(hour="16", minute="0"),  # KST 16:00 - 한국장 마감 후
    },
    # "kr-realtime-stock-trend": {
    #     "task": "stock_trend_realtime_kr",
    #     "schedule": crontab(minute="*/10"),  # 10분마다 실행 (장중)
    # },
    # 공통 스케줄
    "ticker-update": {
        "task": "stock_trend_tickers",
        "schedule": crontab(hour="5", minute="30"),  # KST 05:30 - 일일 티커 정보 업데이트
    },
}

# Celery 설정
CELERY_APP.conf.timezone = "Asia/Seoul"
CELERY_APP.conf.task_serializer = "json"
CELERY_APP.conf.result_serializer = "json"
CELERY_APP.conf.accept_content = ["json"]
CELERY_APP.conf.task_track_started = True
CELERY_APP.conf.task_time_limit = 1800  # 30분
CELERY_APP.conf.worker_prefetch_multiplier = 1  # 작업 분배 최적화

# Worker 시작점
if __name__ == "__main__":
    CONCURRENCY = getattr(settings, "CELERY_CONCURRENCY", 1)
    CELERY_APP.worker_main(
        argv=[
            "worker",
            "--beat",  # beat 서버 통합 실행
            f"--loglevel={settings.CELERY_LOGLEVEL}",
            "-n",
            "worker@%h",
            f"--concurrency={CONCURRENCY}",
            "--max-tasks-per-child=1000",  # 메모리 누수 방지
        ]
    )
