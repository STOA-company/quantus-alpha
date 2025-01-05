import logging
from app.database.crud import database
from app.modules.common.enum import TrendingCountry
import pandas as pd


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


def run_stock_trend_by_1d_batch(ctry: TrendingCountry, chunk_size: int = 100000):
    try:
        # 1. stock_trend와 1일 데이터 테이블의 공통 티커 조회
        stock_trend_tickers = database._select(table="stock_trend", columns=["ticker"], distinct=True, ctry=ctry.value)
        stock_trend_set = set(row[0] for row in stock_trend_tickers)

        latest_date_tickers = database._select(
            table=f"stock_{ctry.value}_1d",
            columns=["ticker"],
            group_by=["ticker"],
            aggregates={"max_date": ("date", "max")},
        )

        latest_tickers = [row for row in latest_date_tickers if row[0] in stock_trend_set]

        for i in range(0, len(latest_tickers), chunk_size):
            chunk_tickers = latest_tickers[i : i + chunk_size]
            daily_data = []

            for ticker, max_date in chunk_tickers:
                one_year_ago = (pd.Timestamp(max_date) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")

                ticker_data = database._select(
                    table=f"stock_{ctry.value}_1d",
                    columns=["ticker", "date", "close", "volume"],
                    ticker=ticker,
                    date__gte=one_year_ago,
                    order="date",
                    ascending=False,
                )
                daily_data.extend(ticker_data)

            df = pd.DataFrame(daily_data, columns=["ticker", "date", "close", "volume"])
            current_data = df.groupby("ticker").first().reset_index()
            prev_data = df.groupby("ticker").nth(1).reset_index()

            results = pd.DataFrame()
            results["ticker"] = current_data["ticker"]
            results["last_updated"] = current_data["date"]
            results["current_price"] = current_data["close"]
            results["prev_close"] = prev_data["close"]
            results["change_1d"] = (current_data["close"] - prev_data["close"]) / prev_data["close"] * 100
            results["volume_1d"] = current_data["volume"]
            results["volume_change_1d"] = current_data["volume"] * current_data["close"]

            periods = {"1w": 7, "1m": 30, "6m": 180, "1y": 365}

            for period, days in periods.items():
                cutoff_dates = current_data.set_index("ticker").apply(
                    lambda x: x["date"] - pd.Timedelta(days=days), axis=1
                )

                df_period = df.copy()
                df_period["cutoff_date"] = df_period["ticker"].map(cutoff_dates)
                period_data = df_period[df_period["date"] >= df_period["cutoff_date"]]

                period_start_prices = period_data.groupby("ticker").last()[["close"]].reset_index()

                period_volumes = period_data.groupby("ticker").agg({"volume": "sum"}).reset_index()

                results = results.merge(period_start_prices, on="ticker", suffixes=("", f"_start_{period}"))
                results[f"change_{period}"] = (results["current_price"] - results["close"]) / results["close"] * 100
                results = results.drop(columns=["close"])

                results = results.merge(period_volumes, on="ticker", suffixes=("", f"_{period}"))
                results[f"volume_{period}"] = results["volume"]
                results[f"volume_change_{period}"] = results["volume"] * results["current_price"]
                results = results.drop(columns=["volume"])

        update_data = []
        for _, row in results.iterrows():
            update_dict = {
                "ticker": row["ticker"],
                "last_updated": row["last_updated"],
                "current_price": row["current_price"],
                "prev_close": row["prev_close"],
                "change_1d": row["change_1d"],
                "change_1w": row["change_1w"],
                "change_1m": row["change_1m"],
                "change_6m": row["change_6m"],
                "change_1y": row["change_1y"],
                "volume_1d": row["volume_1d"],
                "volume_1w": row["volume_1w"],
                "volume_1m": row["volume_1m"],
                "volume_6m": row["volume_6m"],
                "volume_1y": row["volume_1y"],
                "volume_change_1d": row["volume_change_1d"],
                "volume_change_1w": row["volume_change_1w"],
                "volume_change_1m": row["volume_change_1m"],
                "volume_change_6m": row["volume_change_6m"],
                "volume_change_1y": row["volume_change_1y"],
            }
            update_data.append(update_dict)

        # 벌크 업데이트 실행
        database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")
        logging.info(f"Successfully updated {len(update_data)} records in stock_trend table")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_1d_batch: {str(e)}")
        raise e


def run_stock_trend_by_realtime_batch(ctry: TrendingCountry):
    """
    TODO : CTE 추가 후 성능 개선
    """
    try:
        stock_trends = database._select(
            table="stock_trend",
            columns=["ticker", "current_price"],
            distinct=True,
            ctry=ctry.value,
        )
        stock_trend_dict = {row[0]: row[1] for row in stock_trends}

        latest_date_tickers = database._select(
            table=f"stock_{ctry.value}_1m",
            columns=["ticker", "close", "volume"],
            group_by=["ticker"],
            aggregates={"max_date": ("date", "max")},
        )

        latest_tickers = [row for row in latest_date_tickers if row[0] in stock_trend_dict]
        update_data = []
        for ticker, max_date, close, volume in latest_tickers:
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
