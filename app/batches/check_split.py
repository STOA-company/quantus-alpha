import logging
from datetime import datetime
import yfinance as yf
import pytz
from app.database.crud import database

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def check_stock_splits():
    try:
        # 활성화된 종목 정보 조회
        stock_info = database._select(table="stock_information", columns=["ticker", "market"], is_activate=1)
        logger.info(f"Checking splits for {len(stock_info)} active tickers")

        # yfinance용 티커 변환 및 조회 준비
        ticker_mapping = {}
        yf_tickers = []

        for ticker, market in stock_info:
            # yfinance 티커 포맷으로 변환
            if market == "KOSPI":
                yf_ticker = f"{ticker[1:]}.KS"
            elif market == "KOSDAQ":
                yf_ticker = f"{ticker[1:]}.KQ"
            else:
                yf_ticker = ticker

            ticker_mapping[yf_ticker] = ticker
            yf_tickers.append(yf_ticker)

        # 티커 객체 생성
        tickers_obj = yf.Tickers(" ".join(yf_tickers))
        deactivated_count = 0

        # 각 종목별로 분할/병합 확인
        for yf_ticker, original_ticker in ticker_mapping.items():
            try:
                ticker_obj = tickers_obj.tickers[yf_ticker]
                recent_splits = ticker_obj.splits[
                    ticker_obj.splits.index >= datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
                ]

                # 분할/병합이 있는 경우 is_activate를 0으로 변경
                if not recent_splits.empty and any(recent_splits != 0):
                    logger.info(f"Found split for {original_ticker}, deactivating...")

                    # stock_trend 테이블 비활성화
                    database._update(table="stock_trend", sets={"is_activate": 0}, ticker=original_ticker)

                    # stock_information 테이블 비활성화
                    database._update(table="stock_information", sets={"is_activate": 0}, ticker=original_ticker)

                    deactivated_count += 1

            except Exception as e:
                logger.warning(f"Failed to process ticker {original_ticker}: {str(e)}")
                continue

        logger.info(f"Split check completed. Deactivated {deactivated_count} tickers")

    except Exception as e:
        logger.error(f"Error in check_stock_splits: {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting stock split check process...")
    check_stock_splits()
    logger.info("Completed stock split check process")
