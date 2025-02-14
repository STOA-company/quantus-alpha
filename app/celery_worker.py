import logging
from functools import wraps

from app.batches.run_community import update_post_statistics, update_stock_statistics
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
    run_stock_trend_reset_batch,
)
from app.batches.run_stock_indices import us_run_stock_indices_batch, kr_run_stock_indices_batch, get_stock_indices_data
from app.utils.date_utils import get_session_checker, now_kr, now_us
from app.batches.run_disclosure import (
    renewal_kr_run_disclosure_batch,
    kr_run_disclosure_is_top_story,
    renewal_us_run_disclosure_batch,
    us_run_disclosure_is_top_story,
)
from app.batches.run_kr_stock_minute import collect_kr_stock_minute_data
from app.batches.check_split import check_kr_stock_splits, check_us_stock_splits
from app.batches.check_outliers import check_and_recollect_outliers
from app.batches.check_stock_status import check_warned_stock_us_batch

from app.utils.date_utils import check_market_status

notifier = SlackNotifier()
logger = logging.getLogger(__name__)


def log_task_execution(func):
    """태스크 실행 로깅 데코레이터"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            logger.info(f"Starting task: {task_name}")
            result = func(*args, **kwargs)
            logger.info(f"Successfully completed task: {task_name}")
            return result
        except Exception as e:
            logger.error(f"Error in {task_name}: {str(e)}", exc_info=True)
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
            logger.info("US market is not open. US_stock_indices_batch process skipped.")
            return

        notifier.notify_success("US_stock_indices_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_indices_batch process failed: {str(e)}")
        logger.error(f"Error in us_run_stock_indices_batch: {str(e)}")


# KR Stock Indices task
@CELERY_APP.task(name="kr_stock_indices_batch", ignore_result=True)
def kr_stock_indices_batch():
    """한국 주가지수 데이터 업데이트"""

    try:
        if check_market_status("KR"):
            notifier.notify_info("KR_stock_indices_batch process started")
            kr_run_stock_indices_batch()

        else:
            notifier.notify_info("KR market is not open. KR_stock_indices_batch process skipped.")
            logger.info("KR market is not open. KR_stock_indices_batch process skipped.")
            return

        notifier.notify_success("KR_stock_indices_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_indices_batch process failed: {str(e)}")
        logger.error(f"Error in kr_run_stock_indices_batch: {str(e)}")


# Stock trend tasks
@CELERY_APP.task(name="stock_trend_1d_us")
@log_task_execution
def stock_trend_1d_us_task():
    """미국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    if check_market_status("US"):
        logger.info("US market is open. US_stock_trend_1d_batch process skipped.")
        return
    notifier.notify_info("US_stock_trend_1d_batch process started")
    try:
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_trend_1d_batch process failed: {str(e)}")
        logger.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_1d_kr")
@log_task_execution
def stock_trend_1d_kr_task():
    """한국 주식 일별 트렌드 업데이트 (장 마감 후)"""
    if check_market_status("KR"):
        logger.info("KR market is open. KR_stock_trend_1d_batch process skipped.")
        return
    notifier.notify_info("KR_stock_trend_1d_batch process started")
    try:
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("KR_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_trend_1d_batch process failed: {str(e)}")
        logger.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_reset_kr")
@log_task_execution
def stock_trend_reset_kr():
    """한국 주식 일별 트렌드 업데이트"""
    notifier.notify_info("KR_stock_trend_1d_batch process started")
    try:
        run_stock_trend_reset_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("KR_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_trend_1d_batch process failed: {str(e)}")
        logger.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_reset_us")
@log_task_execution
def stock_trend_reset_us():
    """미국 주식 일별 트렌드 업데이트"""
    notifier.notify_info("US_stock_trend_1d_batch process started")
    try:
        run_stock_trend_reset_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_stock_trend_1d_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_stock_trend_1d_batch process failed: {str(e)}")
        logger.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_realtime_us")
@log_task_execution
def stock_trend_realtime_us_task():
    """미국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
    if not check_market_status("US"):
        logger.info("US market is not open. US_stock_trend_realtime_batch process skipped.")
        return
    notifier.notify_info("US_stock_trend_realtime_batch process started")
    try:
        run_stock_trend_by_realtime_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_stock_trend_realtime_batch process completed")
    except Exception as e:
        logger.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")


@CELERY_APP.task(name="stock_trend_realtime_kr")
@log_task_execution
def stock_trend_realtime_kr_task():
    """한국 주식 실시간 트렌드 업데이트 (장 운영 중)"""
    if not check_market_status("KR"):
        logger.info("KR market is open. KR_stock_trend_realtime_batch process skipped.")
        return
    notifier.notify_info("KR_stock_trend_realtime_batch process started")
    try:
        run_stock_trend_by_realtime_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("KR_stock_trend_realtime_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_trend_realtime_batch process failed: {str(e)}")
        logger.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")


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
    logger.info(message)


@CELERY_APP.task(name="kr_disclosure_batch", ignore_result=True)
def kr_disclosure_batch():
    """한국 공시 배치"""
    notifier.notify_info("KR_disclosure_batch process started")
    try:
        renewal_kr_run_disclosure_batch()
        notifier.notify_success("KR_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_disclosure_batch process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="us_disclosure_batch", ignore_result=True)
def us_disclosure_batch():
    """미국 공시 배치"""
    notifier.notify_info("US_disclosure_batch process started")
    try:
        renewal_us_run_disclosure_batch()
        notifier.notify_success("US_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_disclosure_batch process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="kr_news_renewal", ignore_result=True)
def kr_news_renewal():
    """한국 뉴스 업데이트"""
    notifier.notify_info("KR_news_renewal process started")
    try:
        renewal_kr_run_news_batch()
        notifier.notify_success("KR_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"KR_news_renewal process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="us_news_renewal", ignore_result=True)
def us_news_renewal():
    """미국 뉴스 업데이트"""
    notifier.notify_info("US_news_renewal process started")
    try:
        renewal_us_run_news_batch()
        notifier.notify_success("US_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_renewal process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="kr_top_stories", ignore_result=True)
def kr_top_stories():
    """한국 주요 소식 선정"""
    notifier.notify_info("KR_top_stories process started")

    has_error = False

    try:
        kr_run_disclosure_is_top_story()
    except Exception as e:
        has_error = True
        error_msg = f"KR disclosure top story failed: {str(e)}"
        notifier.notify_error(error_msg, "고경민")
        logging.error(error_msg)

    try:
        renewal_kr_run_news_is_top_story()
    except Exception as e:
        has_error = True
        error_msg = f"KR news top story failed: {str(e)}"
        notifier.notify_error(error_msg, "고경민")
        logging.error(error_msg)

    if not has_error:
        notifier.notify_success("KR_top_stories process completed")


@CELERY_APP.task(name="us_top_stories", ignore_result=True)
def us_top_stories():
    """미국 주요 소식 선정"""
    notifier.notify_info("US_top_stories process started")
    has_error = False

    try:
        us_run_disclosure_is_top_story()
    except Exception as e:
        has_error = True
        error_msg = f"US disclosure top story failed: {str(e)}"
        notifier.notify_error(error_msg, "고경민")
        logging.error(error_msg)

    try:
        renewal_us_run_news_is_top_story()
    except Exception as e:
        has_error = True
        error_msg = f"US news top story failed: {str(e)}"
        notifier.notify_error(error_msg, "고경민")
        logging.error(error_msg)

    if not has_error:
        notifier.notify_success("US_top_stories process completed")


@CELERY_APP.task(name="memory-status", ignore_result=True)
def memory_status():
    """메모리 상태 확인"""
    notifier.notify_memory_status()


@CELERY_APP.task(name="kr_stock_minute_batch", ignore_result=True)
def kr_stock_minute_batch():
    """한국 주식 분봉 데이터 업데이트"""
    notifier.notify_info("KR_stock_minute_batch process started")
    if check_market_status("KR"):
        failed_count = collect_kr_stock_minute_data()
        if failed_count > 0:
            notifier.notify_error(f"KR_stock_minute_batch process failed. Failed count: {failed_count}")
        else:
            notifier.notify_success("KR_stock_minute_batch process completed")
    else:
        notifier.notify_info("KR market is not open. KR_stock_minute_batch process skipped.")
        return


@CELERY_APP.task(name="kr_stock_minute_batch_last", ignore_result=True)
def kr_stock_minute_batch_last():
    """한국 주식 분봉 데이터 업데이트 (장 마감 전)"""
    notifier.notify_info("KR_stock_minute_batch_last process started")
    try:
        collect_kr_stock_minute_data(last=True)
        notifier.notify_success("KR_stock_minute_batch_last process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_minute_batch_last process failed: {str(e)}")
        raise


@CELERY_APP.task(name="process_outliers_kr", ignore_result=True)
def process_outliers_kr():
    """한국 주식 이상치 처리"""
    notifier.notify_info("KR_process_outliers process started")
    try:
        check_kr_stock_splits()
        check_and_recollect_outliers(nation="KR")
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.KR)
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
        check_and_recollect_outliers(nation="US")
        run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)
        notifier.notify_success("US_process_outliers process completed")
    except Exception as e:
        notifier.notify_error(f"US_process_outliers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_stock_indices_collect", ignore_result=True)
def kr_stock_indices_collect():
    """한국 주가지수 데이터 수집"""
    now_kr_datetime = now_kr()
    now_kr_date = now_kr_datetime.strftime("%Y-%m-%d")
    now_kr_time = now_kr_datetime.strftime("%H:%M:%S")
    if (
        get_session_checker(country="KR", start_date=now_kr_date).is_session(now_kr_date)
        and "09:00:00" <= now_kr_time <= "15:40:00"
    ):
        try:
            notifier.notify_info("KR_stock_indices_collect process started")
            get_stock_indices_data("KOSPI")
            get_stock_indices_data("KOSDAQ")
            notifier.notify_success("KR_stock_indices_collect process completed")
        except Exception as e:
            notifier.notify_error(f"KR_stock_indices_collect process failed: {str(e)}", "고경민")
            raise
    else:
        logger.info("KR market is not open. KR_stock_indices_collect process skipped.")


@CELERY_APP.task(name="us_stock_indices_collect", ignore_result=True)
def us_stock_indices_collect():
    """미국 주가지수 데이터 수집"""
    now_us_datetime = now_us()
    now_us_date = now_us_datetime.strftime("%Y-%m-%d")
    now_us_time = now_us_datetime.strftime("%H:%M:%S")
    if (
        get_session_checker(country="US", start_date=now_us_date).is_session(now_us_date)
        and "09:30:00" <= now_us_time <= "16:30:00"
    ):
        try:
            notifier.notify_info("US_stock_indices_collect process started")
            get_stock_indices_data("NASDAQ")
            get_stock_indices_data("SP500")
            notifier.notify_success("US_stock_indices_collect process completed")
        except Exception as e:
            notifier.notify_error(f"US_stock_indices_collect process failed: {str(e)}", "고경민")
            raise
    else:
        logger.info("US market is not open. US_stock_indices_collect process skipped.")
        return


@CELERY_APP.task(name="iscd_stat_cls_code_batch", ignore_result=True)
def iscd_stat_cls_code_batch():
    """한국 주식 상태 코드 업데이트"""
    try:
        notifier.notify_info("iscd_stat_cls_code_batch process started")
        iscd_stat_cls_code_batch()
        notifier.notify_success("iscd_stat_cls_code_batch process completed")
    except Exception as e:
        notifier.notify_error(f"iscd_stat_cls_code_batch process failed: {str(e)}")
        raise


@CELERY_APP.task(name="check_warned_stock_us", ignore_result=True)
def check_warned_stock_us():
    """미국 주식 경고 처리"""
    try:
        notifier.notify_info("check_warned_stock_us process started")
        check_warned_stock_us_batch()
        notifier.notify_success("check_warned_stock_us process completed")
    except Exception as e:
        notifier.notify_error(f"check_warned_stock_us process failed: {str(e)}")
        raise


@CELERY_APP.task(name="community_trending_stock_update", ignore_result=True)
def community_trending_stock_update():
    """커뮤니티 인기 종목 업데이트"""
    notifier.notify_info("Community_trending_stock_update process started")
    try:
        update_stock_statistics()
        notifier.notify_success("Community_trending_stock_update process completed")
    except Exception as e:
        notifier.notify_error(f"Community_trending_stock_update process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="community_trending_post_update", ignore_result=True)
def community_trending_post_update():
    """커뮤니티 인기 게시글 업데이트"""
    notifier.notify_info("Community_trending_post_update process started")
    try:
        update_post_statistics()
        notifier.notify_success("Community_trending_post_update process completed")
    except Exception as e:
        notifier.notify_error(f"Community_trending_post_update process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="reset_daily_leaderboard", ignore_result=True)
def reset_daily_leaderboard():
    """일일 리더보드 초기화"""
    notifier.notify_info("Reset_daily_leaderboard process started")
    try:
        from app.core.redis import redis_client

        redis_client.delete("daily_search_leaderboard")
        notifier.notify_success("Reset_daily_leaderboard process completed")
    except Exception as e:
        notifier.notify_error(f"Reset_daily_leaderboard process failed: {str(e)}")
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
