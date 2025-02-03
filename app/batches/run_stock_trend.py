import logging

from sqlalchemy import text
from app.database.crud import database
from app.modules.common.enum import TrendingCountry
import pandas as pd


def run_stock_trend_tickers_batch():
    """티커 정보 배치 처리"""
    try:
        us_1m_tickers = database._select(table="stock_us_1m", columns=["Ticker"], distinct=True)
        us_1d_tickers = database._select(table="stock_us_1d", columns=["Ticker"], distinct=True)
        kr_1d_tickers = database._select(table="stock_kr_1d", columns=["Ticker"], distinct=True)
        info_tickers = database._select(
            table="stock_information", columns=["ticker", "kr_name", "en_name", "market", "ctry"], distinct=True
        )
        existing_tickers = database._select(table="stock_trend", columns=["ticker"], distinct=True)

        # Convert query results to sets for intersection
        us_1m_set = {row[0] for row in us_1m_tickers}
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
            columns=["Ticker"],
            group_by=["Ticker"],
            aggregates={"max_date": ("Date", "max")},
        )

        latest_tickers = [row for row in latest_date_tickers if row[0] in stock_trend_set]

        for i in range(0, len(latest_tickers), chunk_size):
            chunk_tickers = latest_tickers[i : i + chunk_size]
            daily_data = []

            for ticker, max_date in chunk_tickers:
                one_year_ago = (pd.Timestamp(max_date) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")

                ticker_data = database._select(
                    table=f"stock_{ctry.value}_1d",
                    columns=["Ticker", "Date", "Close", "Volume"],
                    Ticker=ticker,
                    Date__gte=one_year_ago,
                    order="Date",
                    ascending=False,
                )
                daily_data.extend(ticker_data)

            df = pd.DataFrame(daily_data, columns=["Ticker", "Date", "Close", "Volume"])
            df = df.sort_values(by=["Ticker", "Date"], ascending=[True, False])

            current_data = df.groupby("Ticker").first().reset_index()
            prev_data = df.groupby("Ticker").nth(1).reset_index()

            results = pd.DataFrame()
            results["ticker"] = current_data["Ticker"]
            results["last_updated"] = current_data["Date"]
            results["current_price"] = current_data["Close"]
            results["prev_close"] = prev_data["Close"]
            results["change_1d"] = (current_data["Close"] - prev_data["Close"]) / prev_data["Close"] * 100
            results["volume_1d"] = current_data["Volume"]
            results["volume_change_1d"] = current_data["Volume"] * current_data["Close"]

            periods = {"1w": 5, "1m": 20, "6m": 120, "1y": None}

            for period, n_records in periods.items():
                if n_records is None:
                    period_data = df.copy()
                else:
                    period_data = df.groupby("Ticker").head(n_records)

                period_start_prices = period_data.groupby("Ticker").last()[["Close"]].reset_index()

                period_volumes = period_data.groupby("Ticker").agg({"Volume": "sum"}).reset_index()

                results = results.merge(
                    period_start_prices, left_on="ticker", right_on="Ticker", suffixes=("", f"_start_{period}")
                )
                results[f"change_{period}"] = (results["current_price"] - results["Close"]) / results["Close"] * 100
                results = results.drop(columns=["Close"])

                results = results.merge(period_volumes, left_on="ticker", right_on="Ticker", suffixes=("", f"_{period}"))
                results[f"volume_{period}"] = results["Volume"]
                results[f"volume_change_{period}"] = results["Volume"] * results["current_price"]
                results = results.drop(columns=["Volume"])

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
    try:
        stock_trends = database._select(
            table="stock_trend",
            columns=["ticker", "prev_close"],
            distinct=True,
            ctry=ctry.value,
        )
        stock_trend_dict = {row[0]: row[1] for row in stock_trends}

        if ctry.value == "us":
            table_name = "stock_us_1m"
        elif ctry.value == "kr":
            table_name = "stock_kr_1d"
        else:
            raise ValueError(f"Invalid country: {ctry.value}")

        query = text(f"""
            SELECT t1.Ticker, t1.Close, t1.Volume, t1.Date
            FROM {table_name} t1
            INNER JOIN (
            SELECT Ticker, MAX(Date) as max_date
            FROM {table_name}
            GROUP BY Ticker
            ) t2
            ON t1.Ticker = t2.Ticker AND t1.Date = t2.max_date
            ORDER BY t1.Ticker;
        """)

        latest_date_tickers = database._execute(query)

        latest_tickers = [row for row in latest_date_tickers if row[0] in stock_trend_dict]
        update_data = []
        for ticker, close, volume, max_date in latest_tickers:
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

            volume_change_rt = round(current_price * volume_rt, 4)

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

        # df_u = pd.DataFrame(update_data)
        # df_u.to_csv(f"stock_trend_{ctry.value}.csv", index=False)
        # return 0

        database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")
        logging.info(f"Successfully updated {len(update_data)} records in stock_trend table")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")
        raise e


if __name__ == "__main__":
    run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)
