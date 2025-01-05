import logging
from app.database.crud import database
from app.modules.common.enum import TrendingCountry


def run_stock_trend_tickers_batch():
    """티커 정보 배치 처리"""
    try:
        us_tickers = database._select(table="stock_us_1d", columns=["ticker"], distinct=True)
        kr_tickers = database._select(table="stock_kr_1d", columns=["ticker"], distinct=True)
        info_tickers = database._select(
            table="stock_information", columns=["ticker", "kr_name", "en_name", "market", "ctry"], distinct=True
        )
        existing_tickers = database._select(table="stock_trend", columns=["ticker"], distinct=True)

        # Convert query results to sets for intersection
        us_ticker_set = {row[0] for row in us_tickers}
        kr_ticker_set = {row[0] for row in kr_tickers}
        info_set = {row[0] for row in info_tickers}
        existing_set = {row[0] for row in existing_tickers}

        # Find common tickers that don't exist in stock_trend
        us_common_tickers = (us_ticker_set & info_set) - existing_set
        kr_common_tickers = (kr_ticker_set & info_set) - existing_set

        # Prepare data for insertion using list comprehension
        us_insert_data = [
            {"ticker": row[0], "kr_name": row[1], "en_name": row[2], "market": row[3], "ctry": row[4]}
            for row in info_tickers
            if row[0] in us_common_tickers
        ]

        kr_insert_data = [
            {"ticker": row[0], "kr_name": row[1], "en_name": row[2], "market": row[3], "ctry": row[4]}
            for row in info_tickers
            if row[0] in kr_common_tickers
        ]

        # Insert data into stock_trend table
        if us_insert_data:
            database._insert(table="stock_trend", sets=us_insert_data)
            logging.info(f"Inserted {len(us_insert_data)} new US stocks into stock_trend table")

        if kr_insert_data:
            database._insert(table="stock_trend", sets=kr_insert_data)
            logging.info(f"Inserted {len(kr_insert_data)} new KR stocks into stock_trend table")

        logging.info("Stock trend tickers batch completed successfully")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_tickers_batch: {str(e)}")
        raise e


def run_stock_trend_by_1d_batch(ctry: TrendingCountry):
    pass
