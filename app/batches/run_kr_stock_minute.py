import logging
from datetime import datetime
from typing import List, Dict
from app.kispy.manager import KISAPIManager
from app.database.crud import database
import pytz
import time


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def save_minute_data(ticker: str, data: List[Dict]) -> bool:
    """
    분봉 데이터 저장
    Returns:
        bool: 저장 성공 여부
    """
    try:
        records = []

        for record in data:
            records.append(
                {
                    "Ticker": "A" + ticker,
                    "Date": record["stck_cntg_hour"],
                    "Open": float(record["stck_oprc"]),
                    "High": float(record["stck_hgpr"]),
                    "Low": float(record["stck_lwpr"]),
                    "Close": float(record["stck_prpr"]),
                    "Volume": float(record["cntg_vol"]),
                }
            )

        if records:
            database._insert(table="stock_kr_1m", sets=records)
            logger.info(f"Successfully saved {len(records)} records for {ticker}")
            return True
        else:
            logger.warning(f"No records to save for ticker {ticker}")
            return False

    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {e}")
        logger.error(f"Error details: {str(e)}")
        return False


def process_single_ticker(api, ticker: str, max_retries: int = 3) -> bool:
    """
    단일 티커 처리 함수
    Returns:
        bool: 처리 성공 여부
    """
    ticker = ticker[1:] if ticker.startswith("A") else ticker
    retry_count = 0

    while retry_count < max_retries:
        try:
            kr_tz = pytz.timezone("Asia/Seoul")
            now = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(kr_tz)
            logger.info(f"Processing ticker {ticker} at KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            current_time = now.strftime("%H%M%S")

            while True:
                try:
                    data = api.get_stock_price_history_by_minute(symbol=ticker, time=current_time, limit=16, desc=True)

                    if not data:
                        logger.info(f"No more data for ticker {ticker}")
                        break

                    logger.info(f"Retrieved {len(data)} records for {ticker} from {current_time}")
                    if not save_minute_data(ticker, data):
                        return False

                    if len(data) < 100:
                        logger.info(f"Less than 100 records received, finishing {ticker}")
                        break

                    last_time = data[-1]["stck_cntg_hour"].strftime("%H%M%S")
                    current_time = str(int(last_time) - 1).zfill(6)

                except Exception as e:
                    if "EGW00133" in str(e):
                        wait_time = min(60 * (retry_count + 1), 300)  # 점진적으로 대기 시간 증가, 최대 5분
                        logger.warning(
                            f"Rate limit reached (attempt {retry_count + 1}/{max_retries}). Waiting {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.error(f"Max retries reached for ticker {ticker}")
                            return False
                        break
                    else:
                        raise

            if retry_count < max_retries:
                return True

        except Exception as e:
            retry_count += 1
            logger.error(f"Error processing ticker {ticker} (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(5)
            else:
                return False

    return False


def collect_kr_stock_minute_data():
    """국내 주식 분봉 데이터 수집"""
    try:
        api = KISAPIManager().get_api()
        failed_tickers = []

        tickers = database._select(table="stock_information", columns=["ticker"], ctry="kr")

        if not tickers:
            logger.warning("No tickers found")
            return

        # 첫 번째 수집 시도
        for ticker_row in tickers:
            ticker = ticker_row[0]
            if not process_single_ticker(api, ticker):
                failed_tickers.append(ticker)

        # 실패한 티커 재처리
        if failed_tickers:
            logger.info(f"Retrying {len(failed_tickers)} failed tickers...")
            retry_failed_tickers = failed_tickers.copy()
            failed_tickers.clear()

            for ticker in retry_failed_tickers:
                if not process_single_ticker(api, ticker):
                    failed_tickers.append(ticker)

        if failed_tickers:
            logger.error(f"Failed to process the following tickers after retries: {failed_tickers}")
        else:
            logger.info("Successfully processed all tickers")

    except Exception as e:
        logger.error(f"Error in data collection process: {e}")
        raise


if __name__ == "__main__":
    collect_kr_stock_minute_data()
