import numpy as np
import pandas as pd
from scipy import stats
from app.database.crud import database
import logging
from app.kispy.sdk import fetch_stock_data
from app.utils.activate_utils import activate_stock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import time

logger = logging.getLogger(__name__)

ZSCORE_THRESHOLD = 3
MAX_WORKERS = 10


def detect_stock_trend_outliers(nation: str) -> List[str]:
    """
    stock_trend change_rt 이상치 탐지
    """
    market = {"US": ["NAS", "NYS", "AMS"], "KR": ["KOSPI", "KOSDAQ"]}.get(nation)

    if not market:
        raise ValueError(f"Invalid nation: {nation}")

    df = pd.DataFrame(
        database._select(
            table="stock_trend",
            columns=[
                "ticker",
                "market",
                "change_rt",
                "change_1d",
                "change_1w",
                "change_1m",
                "change_6m",
                "change_1y",
            ],
            is_trading_stopped=0,
            is_delisted=0,
            market__in=market,
        )
    )

    change_columns = ["change_rt", "change_1d", "change_1w", "change_1m", "change_6m", "change_1y"]
    outlier_tickers = set()

    for column in change_columns:
        valid_data = df.dropna(subset=[column])

        if len(valid_data) > 0:
            z_scores = np.abs(stats.zscore(valid_data[column]))
            outliers = valid_data[z_scores > ZSCORE_THRESHOLD]

            if not outliers.empty:
                logger.info(f"\nOutliers in {column}: {len(outliers)} found")
                outlier_tickers.update(outliers["ticker"].tolist())

    return list(outlier_tickers)


def process_ticker_batch(tickers: List[str], nation: str) -> List[Dict[str, Any]]:
    """
    배치로 티커 데이터 처리
    """
    all_update_data = []

    for ticker in tickers:
        try:
            ticker_ = ticker[1:] if nation == "KR" else ticker
            new_data = fetch_stock_data(symbol=ticker_, nation=nation)

            if new_data is None:
                logger.error(f"Failed to fetch new data for {ticker}")
                continue

            table = "stock_kr_1d" if nation == "KR" else "stock_us_1d"

            # Get existing data
            existing_data = database._select(table=table, columns=["Category", "Market"], Ticker=ticker, limit=1)
            category = existing_data[0][0] if existing_data else ""
            market = existing_data[0][1] if existing_data else ""

            # For KR stocks, get additional info
            if nation == "KR":
                stock_info = database._select(
                    table="stock_information", columns=["kr_name", "market"], ticker=ticker, limit=1
                )

                if not stock_info:
                    logger.warning(f"No stock information found for {ticker}")
                    continue

                kr_name = stock_info[0][0]
                market = stock_info[0][1]

            # Prepare data for update
            df = new_data.reset_index() if "Date" not in new_data.columns else new_data

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

                if nation == "KR":
                    data.update({"Name": kr_name, "Isin": ""})

                all_update_data.append(data)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {str(e)}")

    return all_update_data


def check_and_recollect_outliers(nation: str):
    """
    이상치 감지 및 재수집
    """
    start_time = time.time()
    outlier_tickers = detect_stock_trend_outliers(nation=nation)

    if not outlier_tickers:
        logger.info("No outliers detected")
        return

    logger.info(f"Found {len(outlier_tickers)} outliers. Starting batch processing...")

    database._update(
        table="stock_trend",
        sets={"is_activate": 0},
        ticker__in=outlier_tickers,
    )

    batch_size = 20
    ticker_batches = [outlier_tickers[i : i + batch_size] for i in range(0, len(outlier_tickers), batch_size)]

    all_update_data = []
    table = "stock_kr_1d" if nation == "KR" else "stock_us_1d"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(process_ticker_batch, batch, nation): batch for batch in ticker_batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_data = future.result()
                all_update_data.extend(batch_data)
            except Exception as e:
                logger.error(f"Batch processing failed: {str(e)}")

    if all_update_data:
        try:
            database._delete(table=table, Ticker__in=outlier_tickers)

            batch_size = 1000
            for i in range(0, len(all_update_data), batch_size):
                batch = all_update_data[i : i + batch_size]
                database._bulk_insert(table=table, data_list=batch)

            for ticker in outlier_tickers:
                activate_stock(ticker)

        except Exception as e:
            logger.error(f"Bulk database update failed: {str(e)}")

    end_time = time.time()
    logger.info(f"Processing completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Processed {len(outlier_tickers)} tickers with {len(all_update_data)} total records")


if __name__ == "__main__":
    check_and_recollect_outliers(nation="KR")
