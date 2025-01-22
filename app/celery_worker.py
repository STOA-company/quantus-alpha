import logging
from functools import wraps
from typing import Literal

from app.batches.run_news import (
    kr_run_news_batch,
    temp_kr_run_news_is_top_story,
    temp_us_run_news_is_top_story,
    us_run_news_batch,
    renewal_kr_run_news_batch,
    renewal_us_run_news_batch,
    renewal_kr_run_news_is_top_story,
    renewal_us_run_news_is_top_story,
)
from app.common.celery_config import CELERY_APP
from app.core.config import settings
from app.core.extra.SlackNotifier import SlackNotifier
from app.modules.common.enum import TrendingCountry
from app.batches.run_stock_trend import (
    run_stock_trend_by_1d_batch,
    run_stock_trend_tickers_batch,
    run_stock_trend_by_realtime_batch,
)
from app.batches.run_stock_indices import us_run_stock_indices_batch
from app.utils.date_utils import get_session_checker, get_time_checker, now_kr
from app.batches.run_disclosure import (
    renewal_kr_run_disclosure_batch,
    temp_kr_run_disclosure_is_top_story,
    renewal_us_run_disclosure_batch,
    temp_us_run_disclosure_is_top_story,
)

notifier = SlackNotifier()


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
            check_date = now_kr(is_date=True)  # 영업일 체크용

            # 1. 영업일 체크
            session_checker = get_session_checker(country, check_date)
            is_business_day = session_checker.is_session(check_date)

            # 2. 운영 시간 체크
            is_trading_hours = get_time_checker(country)

            # 3. 둘 다 만족해야 장이 열린 것으로 판단
            is_market_open = is_business_day and is_trading_hours

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


@CELERY_APP.task(name="hello_task", ignore_result=True)
def hello_task():
    """Test task that prints Hello World"""
    current_time = now_kr().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello, World! Current time: {current_time}"
    print(message)
    logging.info(message)


@CELERY_APP.task(name="kr_news_batch", ignore_result=True)
def kr_news_batch():
    """한국 뉴스 배치"""
    notifier.notify_info("KR_news_batch process started")
    try:
        records_count = kr_run_news_batch()
        notifier.notify_success(f"KR_news_batch process completed processed: {records_count}")
    except Exception as e:
        notifier.notify_error(f"KR_news_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_news_is_top_story", ignore_result=True)
def kr_news_is_top_story():
    """한국 뉴스 상위 스토리 업데이트"""
    notifier.notify_info("KR_news_is_top_story process started")
    try:
        temp_kr_run_news_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("KR_news_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"KR_news_is_top_story process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_news_batch", ignore_result=True)
def us_news_batch():
    """미국 뉴스 배치"""
    notifier.notify_info("US_news_batch process started")
    try:
        us_run_news_batch()
        notifier.notify_success("US_news_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_news_is_top_story", ignore_result=True)
def us_news_is_top_story():
    """미국 뉴스 상위 스토리 업데이트"""
    notifier.notify_info("US_news_is_top_story process started")
    try:
        temp_us_run_news_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("US_news_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_is_top_story process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_disclosure_batch", ignore_result=True)
def kr_disclosure_batch():
    """한국 공시 배치"""
    notifier.notify_info("KR_disclosure_batch process started")
    try:
        renewal_kr_run_disclosure_batch()
        notifier.notify_success("KR_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_disclosure_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_disclosure_is_top_story", ignore_result=True)
def kr_disclosure_is_top_story():
    """한국 공시 상위 스토리 업데이트"""
    notifier.notify_info("KR_disclosure_is_top_story process started")
    try:
        temp_kr_run_disclosure_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("KR_disclosure_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"KR_disclosure_is_top_story process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_disclosure_batch", ignore_result=True)
def us_disclosure_batch():
    """미국 공시 배치"""
    notifier.notify_info("US_disclosure_batch process started")
    try:
        renewal_us_run_disclosure_batch()
        notifier.notify_success("US_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_disclosure_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_disclosure_is_top_story", ignore_result=True)
def us_disclosure_is_top_story():
    """미국 공시 상위 스토리 업데이트"""
    notifier.notify_info("US_disclosure_is_top_story process started")
    try:
        temp_us_run_disclosure_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("US_disclosure_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"US_disclosure_is_top_story process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_news_renewal", ignore_result=True)
def kr_news_renewal():
    """한국 뉴스 업데이트"""
    notifier.notify_info("KR_news_renewal process started")
    try:
        renewal_kr_run_news_batch()
        notifier.notify_success("KR_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"KR_news_renewal process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_news_renewal", ignore_result=True)
def us_news_renewal():
    """미국 뉴스 업데이트"""
    notifier.notify_info("US_news_renewal process started")
    try:
        renewal_us_run_news_batch()
        notifier.notify_success("US_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_renewal process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_news_is_top_story_renewal", ignore_result=True)
def kr_news_is_top_story_renewal():
    """한국 뉴스 상위 스토리 업데이트"""
    notifier.notify_info("KR_news_is_top_story_renewal process started")
    try:
        renewal_kr_run_news_is_top_story()
        notifier.notify_success("KR_news_is_top_story_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"KR_news_is_top_story_renewal process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_news_is_top_story_renewal", ignore_result=True)
def us_news_is_top_story_renewal():
    """미국 뉴스 상위 스토리 업데이트"""
    notifier.notify_info("US_news_is_top_story_renewal process started")
    try:
        renewal_us_run_news_is_top_story()
        notifier.notify_success("US_news_is_top_story_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_is_top_story_renewal process failed: {str(e)}")
        raise


# Worker 시작점
if __name__ == "__main__":
    CONCURRENCY = getattr(settings, "CELERY_CONCURRENCY", 1)
    CELERY_APP.worker_main(
        argv=[
            "worker",
            "--beat",
            f"--loglevel={settings.CELERY_LOGLEVEL}",
            "-n",
            "worker@%h",
            f"--concurrency={CONCURRENCY}",
            "--max-tasks-per-child=1000",
        ]
    )
