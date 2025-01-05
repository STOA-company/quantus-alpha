import logging
from app.database.crud import database
from app.modules.common.enum import TrendingCountry


def run_stock_trend_tickers_batch():
    """티커 정보 배치 처리"""
    try:
        us_im_tickers = database._select(table="stock_us_1m", columns=["ticker"], distinct=True)
        us_1d_tickers = database._select(table="stock_us_1d", columns=["ticker"], distinct=True)
        kr_1d_tickers = database._select(table="stock_kr_1d", columns=["ticker"], distinct=True)
        info_tickers = database._select(
            table="stock_information", columns=["ticker", "kr_name", "en_name", "market", "ctry"], distinct=True
        )
        existing_tickers = database._select(table="stock_trend", columns=["ticker"], distinct=True)

        # Convert query results to sets for intersection
        us_1m_set = {row[0] for row in us_im_tickers}
        us_1d_set = {row[0] for row in us_1d_tickers}
        kr_1d_set = {row[0] for row in kr_1d_tickers}
        info_set = {row[0] for row in info_tickers}
        existing_set = {row[0] for row in existing_tickers}

        # Find common tickers that don't exist in stock_trend
        us_common_tickers = (us_1m_set & us_1d_set & info_set) - existing_set
        kr_common_tickers = (kr_1d_set & info_set) - existing_set

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


def run_stock_trend_by_1d_batch():
    pass


def run_stock_trend_by_realtime_batch(ctry: TrendingCountry):
    """
    TODO : CTE 추가 후 성능 개선
    """
    try:
        stock_trends = database._select(
            table="stock_trend", columns=["ticker", "current_price"], distinct=True, ctry=ctry.value
        )
        stock_trend_dict = {row[0]: row[1] for row in stock_trends}

        latest_date_tickers = database._select(
            table=f"stock_{ctry.value}_1m",
            columns=["ticker", "close", "volume"],
            group_by=["ticker"],
            aggregates={"max_date": ("date", "max")},
        )

        latest_date_tickers = [row for row in latest_date_tickers if row[0] in stock_trend_dict]
        update_data = []
        for ticker, max_date, close, volume in latest_date_tickers:
            prev_data = stock_trend_dict[ticker]

            last_updated = max_date
            current_price = close
            volume_rt = volume
            prev_close = prev_data if prev_data else 0

            change_rt = round(((current_price - prev_close) / prev_close * 100), 4) if prev_close != 0 else 0

            if current_price > prev_close:
                change_sign = 1
            elif current_price < prev_close:
                change_sign = -1
            else:
                change_sign = 0

            volume_change_rt = current_price * volume_rt

            update_data.append(
                {
                    "ticker": ticker,
                    "last_updated": last_updated,
                    "prev_close": prev_close,
                    "current_price": current_price,
                    "change_sign": change_sign,
                    "change_rt": change_rt,
                    "volume_rt": volume_rt,
                    "volume_change_rt": volume_change_rt,
                }
            )

        database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")
        logging.info(f"Successfully updated {len(update_data)} records in stock_trend table")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")
        raise e
