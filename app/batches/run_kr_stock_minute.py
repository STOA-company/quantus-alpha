import logging
from datetime import datetime
from typing import List, Dict
from app.kispy.sdk import auth
from kispy.domestic_stock import QuoteAPI
from app.database.crud import database

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def save_minute_data(ticker: str, data: List[Dict]):
    """분봉 데이터 저장"""
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
        else:
            logger.warning(f"No records to save for ticker {ticker}")

    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {e}")
        logger.error(f"Error details: {str(e)}")  # 상세 에러 메시지
        raise


def collect_kr_stock_minute_data():
    """국내 주식 분봉 데이터 수집"""
    try:
        api = QuoteAPI(auth=auth)

        tickers = database._select(table="stock_information", columns=["ticker"], ctry="kr")

        if not tickers:
            logger.warning("No tickers found")
            return

        for ticker_row in tickers:
            ticker = ticker_row[0]
            ticker = ticker_row[0][1:] if ticker_row[0].startswith("A") else ticker_row[0]

            try:
                current_time = datetime.now("Asia/Seoul").strftime("%H%M%S")
                while True:
                    data = api.get_stock_price_history_by_minute(symbol=ticker, time=current_time, limit=16, desc=True)

                    if not data:
                        logger.info(f"No more data for ticker {ticker}")
                        break

                    logger.info(f"Retrieved {len(data)} records for {ticker} from {current_time}")
                    save_minute_data(ticker, data)

                    if len(data) < 100:
                        logger.info(f"Less than 100 records received, finishing {ticker}")
                        break

                    last_time = data[-1]["stck_cntg_hour"].strftime("%H%M%S")
                    current_time = str(int(last_time) - 1).zfill(6)

            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}")
                continue

        logger.info(f"Completed processing {len(tickers)} tickers")

    except Exception as e:
        logger.error(f"Error in data collection process: {e}")
        raise


if __name__ == "__main__":
    collect_kr_stock_minute_data()
