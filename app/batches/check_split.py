import logging
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pytz
from sqlalchemy import text
from app.kispy.auth import auth
from kispy import KisClientV2, KisClient

client = KisClient(auth)


from app.database.crud import database

logger = logging.getLogger(__name__)


def _fetch_stock_data(symbol: str, market: str):
    try:
        # KisClientV2 초기화
        client = KisClientV2(auth=auth, nation="KR" if market in ["KOSPI", "KOSDAQ"] else "US")

        # 전체 기간 일봉 데이터 조회
        # period="d": 일봉
        # is_adjust=True: 수정주가 반영
        ohlcv = client.fetch_ohlcv(symbol=symbol, period="d", is_adjust=True)

        return pd.DataFrame(
            [
                {
                    "Date": item.date,
                    "Open": item.open,
                    "High": item.high,
                    "Low": item.low,
                    "Close": item.close,
                    "Volume": item.volume,
                }
                for item in ohlcv
            ]
        )

    except Exception as e:
        logger.error(f"Error fetching price data from KIS API for {symbol}: {str(e)}")
        raise


def check_and_update_splits():
    try:
        stock_info = database._select(table="stock_information", columns=["ticker", "market"], can_use=1)
        logger.info(f"Checking splits for {len(stock_info)} active tickers")

        success_count = 0
        error_count = 0
        split_found_count = 0

        # yfinance는 split 확인용으로만 사용
        ticker_mapping = {}
        yf_tickers = []
        for info in stock_info:
            ticker = info[0]
            market = info[1]

            if market == "KOSPI":
                yf_ticker = f"{ticker[1:]}.KS"
            elif market == "KOSDAQ":
                yf_ticker = f"{ticker[1:]}.KQ"
            else:
                yf_ticker = ticker

            ticker_mapping[yf_ticker] = (ticker, market)
            yf_tickers.append(yf_ticker)

        tickers_obj = yf.Tickers(" ".join(yf_tickers))

        for yf_ticker, (original_ticker, market) in ticker_mapping.items():
            try:
                country = "kr" if market in ["KOSPI", "KOSDAQ"] else "us"
                ticker_obj = tickers_obj.tickers[yf_ticker]
                try:
                    splits = ticker_obj.splits
                    if not splits.empty:
                        splits.index = splits.index.tz_convert("UTC")
                        check_date = datetime.now(pytz.UTC) - timedelta(days=1)
                        recent_splits = splits[splits.index > check_date]

                        if not recent_splits.empty and any(recent_splits != 0):
                            split_found_count += 1
                            logger.info(f"Found split for {original_ticker}")
                            logger.info(f"Splits data: {recent_splits}")

                            daily_data = _fetch_stock_data(
                                symbol=original_ticker,
                                market=market,
                            )

                            with database.get_connection() as conn:
                                try:
                                    if not daily_data.empty:
                                        _update_price_data(original_ticker, daily_data, f"stock_{country}_1d", conn)
                                        logger.info(f"Updated daily data for {original_ticker}")

                                    conn.commit()
                                    success_count += 1
                                except Exception as e:
                                    conn.rollback()
                                    logger.error(f"Transaction failed for {original_ticker}: {str(e)}")
                                    error_count += 1
                                    continue

                except Exception as e:
                    logger.warning(f"Failed to check splits for {original_ticker}: {str(e)}")
                    error_count += 1
                    continue

            except Exception as e:
                logger.error(f"Error processing ticker {original_ticker}: {str(e)}")
                error_count += 1
                continue

        logger.info(f"Split check completed: {len(stock_info)} tickers processed")
        logger.info(f"Splits found: {split_found_count}")
        logger.info(f"Successful updates: {success_count}")
        logger.info(f"Errors: {error_count}")

    except Exception as e:
        logger.error(f"Error in check_and_update_splits: {str(e)}")
        raise


def _update_price_data(ticker: str, df: pd.DataFrame, table: str, conn=None):
    try:
        logger.info(f"Updating price data for {ticker} in {table}")

        existing_data = database._select(
            table=table, columns=["Name", "Isin", "Market", "Category"], Ticker=ticker, limit=1
        )

        if not existing_data:
            logger.warning(f"No existing data found for {ticker} in {table}")
            return

        df = df.reset_index()
        df = df.rename(
            columns={
                "Date": "Date",
                "Datetime": "Date",
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume",
            }
        )

        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        # 기존 데이터의 정보 사용
        df["Ticker"] = ticker
        df["Name"] = existing_data[0][0]  # Name
        df["Isin"] = existing_data[0][1]  # Isin
        df["Market"] = existing_data[0][2]  # Market
        df["Category"] = existing_data[0][3]  # Category

        # 기존 데이터 삭제
        delete_query = text(f"DELETE FROM {table} WHERE Ticker = :ticker")
        if conn:
            conn.execute(delete_query, {"ticker": ticker})
        else:
            database._execute(delete_query, {"ticker": ticker})

        # 청크 단위로 데이터 삽입
        chunk_size = 1000
        for i in range(0, len(df), chunk_size):
            chunk = df[i : i + chunk_size]
            records = chunk.to_dict("records")
            for record in records:
                # transaction 내에서는 직접 insert 쿼리 실행
                if conn:
                    insert_query = text(f"""
                        INSERT INTO {table} (Date, Ticker, Name, Isin, Market, Category,
                                          Open, High, Low, Close, Volume)
                        VALUES (:Date, :Ticker, :Name, :Isin, :Market, :Category,
                               :Open, :High, :Low, :Close, :Volume)
                    """)
                    conn.execute(insert_query, record)
                else:
                    database._insert(table=table, sets=record)

        logger.info(f"Updated total {len(df)} records for {ticker} in {table}")

    except Exception as e:
        logger.error(f"Error updating price data for {ticker} in {table}: {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting stock split check and update process...")
    check_and_update_splits()
    logger.info("Completed stock split check and update process")
