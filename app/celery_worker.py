import logging
from functools import wraps

from app.batches.check_stock_status import check_warned_stock_us_batch, iscd_stat_cls_code_batch
from app.batches.run_disclosure import run_disclosure_batch
from app.batches.run_dividend import insert_dividend
from app.batches.run_etf_price import run_etf_price, update_etf_status
from app.batches.run_etf_screener import run_etf_screener_data
from app.batches.run_kr_etf_holdings import update_kr_etf_holdings
from app.batches.run_kr_stock_minute import collect_kr_stock_minute_data
from app.batches.run_news import run_news_batch
from app.batches.run_stock_indices import get_stock_indices_data, kr_run_stock_indices_batch, us_run_stock_indices_batch
from app.batches.run_stock_trend import (
    run_stock_trend_by_1d_batch,
    run_stock_trend_by_realtime_batch,
    run_stock_trend_reset_batch,
    run_stock_trend_tickers_batch,
)
from app.batches.run_us_etf_holdings import update_etf_top_holdings
from app.common.celery_config import CELERY_APP
from app.core.config import settings
from app.core.extra.SlackNotifier import SlackNotifier
from app.modules.common.enum import TrendingCountry
from app.modules.screener.utils import screener_utils
from app.monitoring.batch_metrics import collect_system_metrics, monitor_task_execution
from app.utils.date_utils import (
    check_market_status,
    get_session_checker,
    is_business_day,
    is_us_market_open_or_recently_closed,
    now_kr,
)
from app.utils.krx import create_etf_integrated_info
from app.utils.stock_utils import kr_stock_utils, us_stock_utils

notifier = SlackNotifier()
notifier_1d = SlackNotifier(
    webhook_url="https://hooks.slack.com/services/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"
)
logger = logging.getLogger(__name__)


def log_task_execution(func):
    """태스크 실행 로깅 및 메트릭 수집 데코레이터"""

    @monitor_task_execution
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# US Stock Indices task
@CELERY_APP.task(name="us_stock_indices_batch", ignore_result=True)
@log_task_execution
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
@log_task_execution
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
    if not is_business_day("KR"):
        logger.info("KR market is not a business day. stock_trend_reset_kr process skipped.")
        return
    notifier.notify_info("stock_trend_reset_kr process started")
    try:
        run_stock_trend_reset_batch(ctry=TrendingCountry.KR)
        notifier.notify_success("stock_trend_reset_kr process completed")
    except Exception as e:
        notifier.notify_error(f"stock_trend_reset_kr process failed: {str(e)}")
        logger.error(f"Error in stock_trend_reset_kr: {str(e)}")


@CELERY_APP.task(name="stock_trend_reset_us")
@log_task_execution
def stock_trend_reset_us():
    """미국 주식 일별 트렌드 업데이트"""
    if not is_business_day("US"):
        logger.info("US market is not a business day. stock_trend_reset_us process skipped.")
        return
    notifier.notify_info("stock_trend_reset_us process started")
    try:
        run_stock_trend_reset_batch(ctry=TrendingCountry.US)
        notifier.notify_success("stock_trend_reset_us process completed")
    except Exception as e:
        notifier.notify_error(f"stock_trend_reset_us process failed: {str(e)}")
        logger.error(f"Error in stock_trend_reset_us: {str(e)}")


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
@log_task_execution
def hello_task():
    """Test task that prints Hello World"""
    current_time = now_kr().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Hello, World! Current time: {current_time}"
    print(message)
    logger.info(message)


@CELERY_APP.task(name="kr_disclosure_batch", ignore_result=True)
@log_task_execution
def kr_disclosure_batch():
    """한국 공시 배치"""
    notifier.notify_info("KR_disclosure_batch process started")
    try:
        run_disclosure_batch(ctry="KR")
        notifier.notify_success("KR_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"KR_disclosure_batch process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="us_disclosure_batch", ignore_result=True)
@log_task_execution
def us_disclosure_batch():
    """미국 공시 배치"""
    notifier.notify_info("US_disclosure_batch process started")
    try:
        run_disclosure_batch(ctry="US")
        notifier.notify_success("US_disclosure_batch process completed")
    except Exception as e:
        notifier.notify_error(f"US_disclosure_batch process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="kr_news_renewal", ignore_result=True)
@log_task_execution
def kr_news_renewal():
    """한국 뉴스 업데이트"""
    notifier.notify_info("KR_news_renewal process started")
    try:
        run_news_batch(ctry="KR")
        notifier.notify_success("KR_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"KR_news_renewal process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="us_news_renewal", ignore_result=True)
@log_task_execution
def us_news_renewal():
    """미국 뉴스 업데이트"""
    notifier.notify_info("US_news_renewal process started")
    try:
        run_news_batch(ctry="US")
        notifier.notify_success("US_news_renewal process completed")
    except Exception as e:
        notifier.notify_error(f"US_news_renewal process failed: {str(e)}", "고경민")
        raise


@CELERY_APP.task(name="memory-status", ignore_result=True)
@log_task_execution
def memory_status():
    """메모리 상태 확인"""
    notifier.notify_memory_status()


@CELERY_APP.task(name="kr_stock_minute_batch", ignore_result=True)
@log_task_execution
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
@log_task_execution
def kr_stock_minute_batch_last():
    """한국 주식 분봉 데이터 업데이트 (장 마감 전)"""
    notifier.notify_info("KR_stock_minute_batch_last process started")
    try:
        collect_kr_stock_minute_data(last=True)
        notifier.notify_success("KR_stock_minute_batch_last process completed")
    except Exception as e:
        notifier.notify_error(f"KR_stock_minute_batch_last process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_stock_indices_collect", ignore_result=True)
@log_task_execution
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
@log_task_execution
def us_stock_indices_collect():
    """미국 주가지수 데이터 수집"""
    if is_us_market_open_or_recently_closed(extra_hours=1):
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


@CELERY_APP.task(name="check_warned_stock_kr", ignore_result=True)
@log_task_execution
def check_warned_stock_kr():
    """한국 주식 경고 처리"""
    try:
        notifier.notify_info("check_warned_stock_kr process started")
        iscd_stat_cls_code_batch()
        notifier.notify_success("check_warned_stock_kr process completed")
    except Exception as e:
        notifier.notify_error(f"check_warned_stock_kr process failed: {str(e)}")
        raise


@CELERY_APP.task(name="check_warned_stock_us", ignore_result=True)
@log_task_execution
def check_warned_stock_us():
    """미국 주식 경고 처리"""
    try:
        notifier.notify_info("check_warned_stock_us process started")
        check_warned_stock_us_batch()
        notifier.notify_success("check_warned_stock_us process completed")
    except Exception as e:
        notifier.notify_error(f"check_warned_stock_us process failed: {str(e)}")
        raise


@CELERY_APP.task(name="reset_daily_leaderboard", ignore_result=True)
@log_task_execution
def reset_daily_leaderboard():
    """일일 리더보드 초기화"""
    notifier.notify_info("reset_daily_leaderboard process started")
    try:
        from app.core.redis import redis_client

        redis_client().delete("daily_search_leaderboard")
        notifier.notify_success("reset_daily_leaderboard process completed")
    except Exception as e:
        notifier.notify_error(f"reset_daily_leaderboard process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_us_top_gainers", ignore_result=True)
@log_task_execution
def update_us_top_gainers():
    """미국 상승 종목 업데이트"""
    notifier.notify_info("update_us_top_gainers process started")
    try:
        us_stock_utils.update_top_gainers()
        notifier.notify_success("update_us_top_gainers process completed")
    except Exception as e:
        notifier.notify_error(f"update_us_top_gainers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_us_top_losers", ignore_result=True)
@log_task_execution
def update_us_top_losers():
    """미국 하락 종목 업데이트"""
    notifier.notify_info("update_us_top_losers process started")
    try:
        us_stock_utils.update_top_losers()
        notifier.notify_success("update_us_top_losers process completed")
    except Exception as e:
        notifier.notify_error(f"update_us_top_losers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_kr_top_gainers", ignore_result=True)
@log_task_execution
def update_kr_top_gainers():
    """한국 상승 종목 업데이트"""
    notifier.notify_info("update_kr_top_gainers process started")
    try:
        kr_stock_utils.update_top_gainers()
        notifier.notify_success("update_kr_top_gainers process completed")
    except Exception as e:
        notifier.notify_error(f"update_kr_top_gainers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_kr_top_losers", ignore_result=True)
@log_task_execution
def update_kr_top_losers():
    """한국 하락 종목 업데이트"""
    notifier.notify_info("update_kr_top_losers process started")
    try:
        kr_stock_utils.update_top_losers()
        notifier.notify_success("update_kr_top_losers process completed")
    except Exception as e:
        notifier.notify_error(f"update_kr_top_losers process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_us_stock_parquet", ignore_result=True)
@log_task_execution
def update_us_stock_parquet():
    """미국 주식 파일 업데이트"""
    notifier_1d.notify_info("update_us_stock_parquet process started")
    try:
        screener_utils.process_us_factor_data()
        notifier_1d.notify_success("update_us_stock_parquet process completed")
    except Exception as e:
        notifier_1d.notify_error(f"update_us_stock_parquet process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_kr_stock_parquet", ignore_result=True)
@log_task_execution
def update_kr_stock_parquet():
    """한국 주식 파일 업데이트"""
    notifier_1d.notify_info("update_kr_stock_parquet process started")
    try:
        screener_utils.process_kr_factor_data()
        notifier_1d.notify_success("update_kr_stock_parquet process completed")
    except Exception as e:
        notifier_1d.notify_error(f"update_kr_stock_parquet process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_kr_etf_parquet", ignore_result=True)
@log_task_execution
def update_kr_etf_parquet():
    """한국 ETF 팩터 파일 업데이트"""
    notifier_1d.notify_info("update_kr_etf_parquet process started")
    if is_business_day("KR"):
        try:
            run_etf_screener_data("KR")
            notifier_1d.notify_success("update_kr_etf_parquet process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_kr_etf_parquet process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("KR market is not open. update_kr_etf_parquet process skipped.")


@CELERY_APP.task(name="update_us_etf_parquet", ignore_result=True)
@log_task_execution
def update_us_etf_parquet():
    """미국 ETF 팩터 파일 업데이트"""
    notifier_1d.notify_info("update_us_etf_parquet process started")
    if is_business_day("US"):
        try:
            run_etf_screener_data("US")
            notifier_1d.notify_success("update_us_etf_parquet process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_us_etf_parquet process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("US market is not open. update_us_etf_parquet process skipped.")


@CELERY_APP.task(name="update_us_stock_dividend_parquet", ignore_result=True)
@log_task_execution
def update_us_stock_dividend_parquet():
    """미국 주식 배당금 파일 업데이트"""
    notifier_1d.notify_info("update_us_stock_dividend_parquet process started")
    if is_business_day("US"):
        try:
            from app.batches.run_dividend import StockDividendDataDownloader

            downloader = StockDividendDataDownloader()
            downloader.download_stock_dividend(ctry="US", download=True)
            notifier_1d.notify_success("update_us_stock_dividend_parquet process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_us_stock_dividend_parquet process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("US market is not open. update_us_stock_dividend_parquet process skipped.")


@CELERY_APP.task(name="update_kr_stock_dividend_parquet", ignore_result=True)
@log_task_execution
def update_kr_stock_dividend_parquet():
    """한국 주식 배당금 파일 업데이트"""
    notifier_1d.notify_info("update_kr_stock_dividend_parquet process started")
    if is_business_day("KR"):
        try:
            from app.batches.run_dividend import StockDividendDataDownloader

            downloader = StockDividendDataDownloader()
            downloader.download_stock_dividend(ctry="KR", download=True)
            notifier_1d.notify_success("update_kr_stock_dividend_parquet process completed")
        except Exception as e:
            notifier.notify_error(
                f"update_kr_stock_dividend_parquet process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("KR market is not open. update_kr_stock_dividend_parquet process skipped.")


@CELERY_APP.task(name="update_us_dividend_rds", ignore_result=True)
@log_task_execution
def update_us_dividend_rds():
    """미국 주식/ETF 배당금 데이터베이스 업데이트"""
    notifier_1d.notify_info("update_us_dividend_rds process started")
    if is_business_day("US"):
        try:
            insert_dividend(ctry="US", type="stock")
            notifier_1d.notify_success("update_us_stock_dividend_rds process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_us_stock_dividend_rds process failed: {str(e)}",
            )
        try:
            insert_dividend(ctry="US", type="etf")
            notifier_1d.notify_success("update_us_etf_dividend_rds process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_us_etf_dividend_rds process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("US market is not open. update_us_dividend_rds process skipped.")


@CELERY_APP.task(name="update_kr_dividend_rds", ignore_result=True)
@log_task_execution
def update_kr_dividend_rds():
    """한국 주식/ETF 배당금 데이터베이스 업데이트"""
    notifier_1d.notify_info("update_kr_dividend_rds process started")
    if is_business_day("KR"):
        try:
            insert_dividend(ctry="KR", type="stock")
            notifier_1d.notify_success("update_kr_stock_dividend_rds process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_kr_stock_dividend_rds process failed: {str(e)}",
            )
        try:
            insert_dividend(ctry="KR", type="etf")
            notifier_1d.notify_success("update_kr_etf_dividend_rds process completed")
        except Exception as e:
            notifier_1d.notify_error(
                f"update_kr_etf_dividend_rds process failed: {str(e)}",
            )
    else:
        notifier_1d.notify_info("KR market is not open. update_kr_dividend_rds process skipped.")


@CELERY_APP.task(name="update_us_etf_price", ignore_result=True)
@log_task_execution
def update_us_etf_price():
    """미국 ETF 시세 업데이트"""
    notifier_1d.notify_info("update_us_etf_price process started")
    try:
        run_etf_price("US")
        notifier_1d.notify_success("update_us_etf_price process completed")
    except Exception as e:
        notifier_1d.notify_error(f"update_us_etf_price process failed: {str(e)}")
        raise


@CELERY_APP.task(name="update_kr_etf_price", ignore_result=True)
@log_task_execution
def update_kr_etf_price():
    """한국 ETF 시세 업데이트"""
    notifier_1d.notify_info("update_kr_etf_price process started")
    try:
        run_etf_price("KR")
        notifier_1d.notify_success("update_kr_etf_price process completed")
    except Exception as e:
        notifier_1d.notify_error(f"update_kr_etf_price process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_update_etf_status", ignore_result=True)
@log_task_execution
def kr_update_etf_status():
    """한국 ETF 상태 업데이트"""
    notifier_1d.notify_info("kr_update_etf_status process started")
    try:
        update_etf_status("KR")
        notifier_1d.notify_success("kr_update_etf_status process completed")
    except Exception as e:
        notifier_1d.notify_error(f"kr_update_etf_status process failed: {str(e)}")
        raise


@CELERY_APP.task(name="us_update_etf_status", ignore_result=True)
@log_task_execution
def us_update_etf_status():
    """미국 ETF 상태 업데이트"""
    notifier_1d.notify_info("us_update_etf_status process started")
    try:
        update_etf_status("US")
        notifier_1d.notify_success("us_update_etf_status process completed")
    except Exception as e:
        notifier_1d.notify_error(f"us_update_etf_status process failed: {str(e)}")
        raise


@CELERY_APP.task(name="kr_update_etf_holdings", ignore_result=True)
@log_task_execution
def kr_update_etf_holdings():
    """한국 ETF 구성종목 업데이트"""
    if is_business_day("KR"):
        notifier_1d.notify_info("kr_update_etf_holdings process started")
        try:
            update_kr_etf_holdings()
            notifier_1d.notify_success("kr_update_etf_holdings process completed")
        except Exception as e:
            notifier_1d.notify_error(f"kr_update_etf_holdings process failed: {str(e)}")
            raise
    else:
        notifier_1d.notify_info("KR market is not open. kr_update_etf_holdings process skipped.")


@CELERY_APP.task(name="us_update_etf_holdings", ignore_result=True)
@log_task_execution
def us_update_etf_holdings():
    """미국 ETF 구성종목 업데이트"""
    if is_business_day("US"):
        notifier_1d.notify_info("us_update_etf_holdings process started")
        try:
            update_etf_top_holdings(ctry="US")
            notifier_1d.notify_success("us_update_etf_holdings process completed")
        except Exception as e:
            notifier_1d.notify_error(f"us_update_etf_holdings process failed: {str(e)}")
            raise
    else:
        notifier_1d.notify_info("US market is not open. us_update_etf_holdings process skipped.")


@CELERY_APP.task(name="update_krx_etf_data")
@log_task_execution
def update_krx_etf_data():
    """KRX의 ETF 통합 정보를 업데이트하는 태스크"""
    notifier_1d.notify_info("update_krx_etf_data process started")
    try:
        logger.info("KRX ETF 통합 정보 업데이트 시작")
        create_etf_integrated_info()
        logger.info("KRX ETF 통합 정보 업데이트 완료")

        notifier_1d.notify_success("update_krx_etf_data process completed")
        return {"status": "success", "message": "ETF 통합 데이터 업데이트가 완료되었습니다."}
    except Exception as e:
        error_msg = f"ETF 통합 데이터 업데이트 실패: {str(e)}"
        logger.error(error_msg)
        notifier_1d.notify_error(error_msg)
        return {"status": "error", "message": error_msg}


@CELERY_APP.task(name="collect_system_metrics", ignore_result=True)
@log_task_execution
def collect_system_metrics_task():
    notifier.notify_info("collect_system_metrics_task started")
    logger.info("메트릭 수집 시작")
    collect_system_metrics()
    notifier.notify_success("collect_system_metrics_task completed")
    logger.info("메트릭 수집 완료")


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
