import logging
import pandas as pd

import yfinance as yf
from app.database.crud import database
from app.kispy.sdk import fetch_stock_data


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CHECK_PERIOD = "1d"


def _update_price_data(ticker: str, df: pd.DataFrame, nation: str):
    try:
        logger.info(f"Updating price data for {ticker}")

        table = "stock_kr_1d" if nation == "KR" else "stock_us_1d"

        existing_data = database._select(table=table, columns=["Category", "Market"], Ticker=ticker, limit=1)
        if not existing_data:
            logger.warning(f"No existing data found for {ticker} in {table}")
            return False

        category = existing_data[0][0] if existing_data else ""
        market = existing_data[0][1] if existing_data else ""

        df = df.reset_index() if "Date" not in df.columns else df

        update_data = []
        for _, row in df.iterrows():
            data = {
                "Ticker": ticker,
                "Date": row["Date"],
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Volume": row["Volume"],
                "Market": market,
                "Category": category,
            }
            update_data.append(data)

        if not update_data:
            logger.warning(f"No data to update for {ticker}")
            return False

        database._delete(table=table, Ticker=ticker)
        for data in update_data:
            database._insert(table=table, sets=data)

        logger.info(f"Successfully updated {len(update_data)} records for {ticker}")
        return True

    except Exception as e:
        logger.error(f"Error updating price data for {ticker}: {str(e)}")
        return False


def check_kr_stock_splits():
    """한국 시장 주식 분할 체크 및 비활성화"""
    try:
        market_list = ["KOSPI", "KOSDAQ"]

        stock_info = database._select(
            table="stock_information", columns=["ticker", "market"], is_activate=1, market__in=market_list
        )
        logger.info(f"Checking splits for {len(stock_info)} active KR tickers")

        tickers = []
        split_detected = []
        deactivated_count = 0

        for ticker, market in stock_info:
            ticker = f"{ticker[1:]}.KS" if market == "KOSPI" else f"{ticker[1:]}.KQ"
            tickers.append(ticker)

        if not tickers:
            logger.warning("No active KR tickers found")
            return

        tickers_obj = yf.Tickers(" ".join(tickers))

        for ticker in tickers:
            try:
                stock = tickers_obj.tickers[ticker]
                history = stock.history(period=CHECK_PERIOD)
                if history.empty:
                    logger.warning(f"No history data for {ticker}")
                    continue

                recent_splits = history.iloc[-1]["Stock Splits"]

                if recent_splits != 0:
                    logger.info(f"Split detected for {ticker}")

                    df = fetch_stock_data(ticker, "KR")
                    if df is not None and _update_price_data(ticker, df, "KR"):
                        logger.warning(f"Updated {ticker}")

            except Exception as e:
                logger.error(f"Failed to process KR ticker {ticker}: {str(e)}")
                continue

        logger.info(f"KR split check completed. Deactivated {deactivated_count} tickers")
        if split_detected:
            logger.info(f"Splits detected for: {split_detected}")

    except Exception as e:
        logger.error(f"Error in check_kr_stock_splits: {str(e)}")
        raise


def check_us_stock_splits():
    """미국 시장 주식 분할 체크 및 데이터 업데이트"""
    try:
        market_list = ["NAS", "NYS", "AMS"]

        stock_info = database._select(
            table="stock_information", columns=["ticker", "market"], is_activate=1, market__in=market_list
        )
        logger.info(f"Checking splits for {len(stock_info)} active US tickers")

        tickers = []
        split_detected = []
        updated_count = 0

        for ticker, market in stock_info:
            tickers.append(ticker)

        if not tickers:
            logger.warning("No active US tickers found")
            return

        logger.warning(f"Tickers: {tickers}")

        tickers_obj = yf.Tickers(" ".join(tickers))

        for ticker in tickers:
            try:
                stock = tickers_obj.tickers[ticker]
                history = stock.history(period=CHECK_PERIOD)
                if history.empty:
                    logger.warning(f"No history data for {ticker}")
                    continue

                recent_splits = history.iloc[-1]["Stock Splits"]

                if recent_splits != 0:
                    logger.warning(f"Split detected for {ticker}")
                    split_detected.append(ticker)

                    df = fetch_stock_data(ticker, "US")
                    if df is not None and _update_price_data(ticker, df, "US"):
                        updated_count += 1
                        logger.warning(f"Updated {ticker}")

            except Exception as e:
                logger.error(f"Failed to process US ticker {ticker}: {str(e)}")
                continue

        logger.info(f"US split check completed. Updated {updated_count} tickers")
        if split_detected:
            logger.info(f"Splits detected for: {split_detected}")

    except Exception as e:
        logger.error(f"Error in check_us_stock_splits: {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting stock split check process...")
    check_kr_stock_splits()
    check_us_stock_splits()
    logger.info("Completed stock split check process")
