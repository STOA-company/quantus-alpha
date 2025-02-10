import logging
from functools import wraps

from app.batches.run_news import (
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
from app.batches.run_stock_indices import us_run_stock_indices_batch, kr_run_stock_indices_batch, get_stock_indices_data
from app.utils.date_utils import now_kr
from app.batches.run_disclosure import (
    renewal_kr_run_disclosure_batch,
    kr_run_disclosure_is_top_story,
    renewal_us_run_disclosure_batch,
    us_run_disclosure_is_top_story,
)
from app.batches.run_kr_stock_minute import collect_kr_stock_minute_data
from app.batches.check_split import check_kr_stock_splits, check_us_stock_splits
from app.batches.check_outliers import check_and_recollect_outliers_kr, check_and_recollect_outliers_us

from app.utils.date_utils import check_market_status

notifier = SlackNotifier()


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
    notifier.notify_info("US_stock_indices_batch process started")
    try:
        if check_market_status("US"):
            us_run_stock_indices_batch()
        else:
            notifier.notify_info("US market is not open. US_stock_indices_batch process skipped.")
            logging.info("US market is not open. US_stock_indices_batch process skipped.")
            return

        notifier.notify_success("US_stock_indices_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_indices_batch process failed: {str(e)}")
        logging.error(f"Error in us_run_stock_indices_batch: {str(e)}")


# KR Stock Indices task
@CELERY_APP.task(name="kr_stock_indices_batch", ignore_result=True)
def kr_stock_indices_batch():
    """한국 주가지수 데이터 업데이트"""
    notifier.notify_info("KR_stock_indices_batch process started")
    try:
        if check_market_status("KR"):
            kr_run_stock_indices_batch()
        else:
            notifier.notify_info("KR market is not open. KR_stock_indices_batch process skipped.")
            logging.info("KR market is not open. KR_stock_indices_batch process skipped.")
            return

        notifier.notify_success("KR_stock_indices_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_indices_batch process failed: {str(e)}")
        logging.error(f"Error in kr_run_stock_indices_batch: {str(e)}")


# Stock trend tasks
@CELERY_APP.task(name="stock_trend_1d_us")
@log_task_execution
def stock_trend_1d_us_task():
    """미국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    if check_market_status("US"):
        logging.info("US market is open. US_stock_trend_1d_batch process skipped.")
        return
    notifier.notify_info("US_stock_trend_1d_batch process started")
    try:
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_trend_1d_batch process failed: {str(e)}")
        logging.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_1d_kr")
@log_task_execution
def stock_trend_1d_kr_task():
    """한국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    if check_market_status("KR"):
        logging.info("KR market is open. KR_stock_trend_1d_batch process skipped.")
        return
    notifier.notify_info("KR_stock_trend_1d_batch process started")
    try:
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("KR_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_trend_1d_batch process failed: {str(e)}")
        logging.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_realtime_us")
@log_task_execution
def stock_trend_realtime_us_task():
    """미국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
    if not check_market_status("US"):
        logging.info("US market is not open. US_stock_trend_realtime_batch process skipped.")
        return
    notifier.notify_info("US_stock_trend_realtime_batch process started")
    try:
        run_stock_trend_by_realtime_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_stock_trend_realtime_batch process completed")
    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_realtime_kr")
@log_task_execution
def stock_trend_realtime_kr_task():
    """한국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
    if not check_market_status("KR"):
        logging.info("KR market is open. KR_stock_trend_realtime_batch process skipped.")
        return
    notifier.notify_info("KR_stock_trend_realtime_batch process started")
    try:
        run_stock_trend_by_realtime_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("KR_stock_trend_realtime_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_trend_realtime_batch process failed: {str(e)}")
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")


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


@CELERY_APP.task(name="kr_disclosure_batch", ignore_result=True)
def kr_disclosure_batch():
    """한국 공시 배치"""
    notifier.notify_info("KR_disclosure_batch process started")
    try:
        renewal_kr_run_disclosure_batch()
        notifier.notify_success("KR_disclosure_batch process completed")
        kr_run_disclosure_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("KR_disclosure_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"KR_disclosure_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_disclosure_batch", ignore_result=True)
def us_disclosure_batch():
    """미국 공시 배치"""
    notifier.notify_info("US_disclosure_batch process started")
    try:
        renewal_us_run_disclosure_batch()
        notifier.notify_success("US_disclosure_batch process completed")
        us_run_disclosure_is_top_story()  # stock_trend_1d 테이블 완성 시 temp 제거한 로직 사용
        notifier.notify_success("US_disclosure_is_top_story process completed")
    except Exception as e:
        notifier.notify_error(f"US_disclosure_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_news_renewal", ignore_result=True)
def kr_news_renewal():
    """한국 뉴스 업데이트"""
    notifier.notify_info("KR_news_renewal process started")
    try:
        renewal_kr_run_news_batch()
        notifier.notify_success("KR_news_renewal process completed")
        renewal_kr_run_news_is_top_story()
        notifier.notify_success("KR_news_is_top_story_renewal process completed")
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
        renewal_us_run_news_is_top_story()
        notifier.notify_success("US_news_is_top_story_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_renewal process failed: {str(e)}")
        raise


@CELERY_APP.task(name="memory-status", ignore_result=True)
def memory_status():
    """메모리 상태 확인"""
    notifier.notify_memory_status()


@CELERY_APP.task(name="kr_stock_minute_batch", ignore_result=True)
def kr_stock_minute_batch():
    """한국 주식 분봉 데이터 업데이트"""
    notifier.notify_info("KR_stock_minute_batch process started")
    if check_market_status("KR"):
        collect_kr_stock_minute_data()
        notifier.notify_success("KR_stock_minute_batch process completed")
    else:
        notifier.notify_info("KR market is not open. KR_stock_minute_batch process skipped.")
        return


@CELERY_APP.task(name="process_outliers_kr", ignore_result=True)
def process_outliers_kr():
    """한국 주식 이상치 처리"""
    notifier.notify_info("KR_process_outliers process started")
    try:
        check_kr_stock_splits()
        check_and_recollect_outliers_kr()
        stock_trend_1d_kr_task()
        notifier.notify_success("KR_process_outliers process completed")
    except Exception as e:
        notifier.notify_error(f"KR_process_outliers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="process_outliers_us", ignore_result=True)
def process_outliers_us():
    """미국 주식 이상치 처리"""
    notifier.notify_info("US_process_outliers process started")
    try:
        check_us_stock_splits()
        check_and_recollect_outliers_us()
        stock_trend_1d_us_task()
        notifier.notify_success("US_process_outliers process completed")
    except Exception as e:
        notifier.notify_error(f"US_process_outliers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_stock_indices_collect", ignore_result=True)
def kr_stock_indices_collect():
    """한국 주가지수 데이터 수집"""
    if not check_market_status("KR"):
        notifier.notify_info("KR market is not open. KR_stock_indices_collect process skipped.")
        return
    try:
        notifier.notify_info("KR_stock_indices_collect process started")
        get_stock_indices_data("KOSPI")
        get_stock_indices_data("KOSDAQ")
        notifier.notify_success("KR_stock_indices_collect process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_indices_collect process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_stock_indices_collect", ignore_result=True)
def us_stock_indices_collect():
    """미국 주가지수 데이터 수집"""
    if not check_market_status("US"):
        logging.info("US market is not open. US_stock_indices_collect process skipped.")
        return
    try:
        notifier.notify_info("US_stock_indices_collect process started")
        get_stock_indices_data("NASDAQ")
        get_stock_indices_data("SNP500")
        notifier.notify_success("US_stock_indices_collect process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_indices_collect process failed: {str(e)}")
        raise


# Worker 시작점
if __name__ == "__main__":
    CONCURRENCY = getattr(settings, "CELERY_CONCURRENCY", 7)
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
