import logging
from datetime import datetime
from typing import Dict, List

import pytz

from app.database.crud import database
from app.kispy.manager import KISAPIManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


FETCH_COUNT = 30


def save_minute_data(ticker: str, data: List[Dict]):
    """분봉 데이터 저장"""
    try:
        records = []
        ticker_symbol = "A" + ticker

        # 저장하려는 데이터의 날짜들
        dates = [record["stck_cntg_hour"] for record in data]

        existing_records = database._select(
            table="stock_kr_1m",
            columns=["Date"],
            Ticker=ticker_symbol,
            Date__in=dates,
        )

        existing_dates = {record[0] for record in existing_records} if existing_records else set()

        for record in data:
            date = record["stck_cntg_hour"]
            if date not in existing_dates:
                records.append(
                    {
                        "Ticker": ticker_symbol,
                        "Date": date,
                        "Open": float(record["stck_oprc"]),
                        "High": float(record["stck_hgpr"]),
                        "Low": float(record["stck_lwpr"]),
                        "Close": float(record["stck_prpr"]),
                        "Volume": float(record["cntg_vol"]),
                    }
                )

        if records:
            database._insert(table="stock_kr_1m", sets=records)
            logger.info(f"Successfully saved {len(records)} new records for {ticker}")
            logger.info(f"Skipped {len(data) - len(records)} existing records for {ticker}")
        else:
            logger.info(f"All {len(data)} records already exist for {ticker}")

    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {e}")
        logger.error(f"Error details: {str(e)}")


def collect_kr_stock_minute_data(last: bool = False):
    """국내 주식 분봉 데이터 수집"""
    try:
        api = KISAPIManager().get_api()

        tickers = database._select(table="stock_information", columns=["ticker"], ctry="kr")

        if not tickers:
            logger.warning("No tickers found")
            return

        for ticker_row in tickers:
            ticker = ticker_row[0]
            ticker = ticker_row[0][1:] if ticker_row[0].startswith("A") else ticker_row[0]

            try:
                kr_tz = pytz.timezone("Asia/Seoul")
                now = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(kr_tz)
                logger.info(f"Starting data collection at KST: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                current_time = now.strftime("%H%M%S")

                if last:
                    current_time = "153000"

                while True:
                    logger.info(f"Current time: {current_time}")
                    data = api.get_stock_price_history_by_minute(
                        symbol=ticker, time=current_time, limit=FETCH_COUNT, desc=True
                    )

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
                raise

        logger.info(f"Completed processing {len(tickers)} tickers")

    except Exception as e:
        logger.error(f"Error in data collection process: {e}")
        raise


if __name__ == "__main__":
    collect_kr_stock_minute_data(last=True)
