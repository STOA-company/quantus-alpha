from datetime import datetime, timedelta
import logging
import numpy as np
import pandas as pd

from app.common.constants import KR_EXCLUDE_DATES, US_EXCLUDE_DATES
from app.database.crud import database
from app.modules.common.enum import TrendingCountry
from app.utils.date_utils import get_business_days
from app.common.mapping import timezone_map, market_close_times_map


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
                    columns=["Ticker", "Date", "Close", "Volume", "Open", "High", "Low"],
                    Ticker=ticker,
                    Date__gte=one_year_ago,
                    order="Date",
                    ascending=False,
                )
                daily_data.extend(ticker_data)

            df = pd.DataFrame(daily_data, columns=["Ticker", "Date", "Close", "Volume", "Open", "High", "Low"])
            df = df.sort_values(by=["Ticker", "Date"], ascending=[True, False])

            df["volume_change"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4 * df["Volume"]

            current_data = df.groupby("Ticker").first().reset_index()
            prev_data = df.groupby("Ticker").nth(1).reset_index()

            close_time = market_close_times_map[ctry.value.upper()]
            current_data["Date"] = pd.to_datetime(current_data["Date"]).apply(
                lambda x: x.replace(hour=close_time["hour"], minute=close_time["minute"], second=close_time["second"])
            )

            results = pd.DataFrame()
            results["ticker"] = current_data["Ticker"]
            results["last_updated"] = current_data["Date"]
            results["current_price"] = current_data["Close"].round(4)
            results["prev_close"] = prev_data["Close"].round(4)
            results["change_1d"] = ((current_data["Close"] - prev_data["Close"]) / prev_data["Close"] * 100).round(4)
            results["volume_1d"] = current_data["Volume"].round(4)
            results["volume_change_1d"] = current_data["volume_change"].round(4)
            results["change_sign"] = np.where(
                current_data["Close"] > prev_data["Close"], 1, np.where(current_data["Close"] < prev_data["Close"], -1, 0)
            )

            periods = {"1w": 5, "1m": 20, "6m": 120, "1y": None}

            for period, n_records in periods.items():
                if n_records is None:
                    period_data = df.copy()
                else:
                    period_data = df.groupby("Ticker").head(n_records)

                period_start_prices = period_data.groupby("Ticker").last()[["Close"]].reset_index()

                period_volumes = (
                    period_data.groupby("Ticker").agg({"Volume": "sum", "volume_change": "sum"}).reset_index()
                )

                results = results.merge(
                    period_start_prices, left_on="ticker", right_on="Ticker", suffixes=("", f"_start_{period}")
                )
                results[f"change_{period}"] = (
                    (results["current_price"] - results["Close"]) / results["Close"] * 100
                ).round(4)
                results = results.drop(columns=["Close"])

                results = results.merge(period_volumes, left_on="ticker", right_on="Ticker", suffixes=("", f"_{period}"))
                results[f"volume_{period}"] = results["Volume"].round(4)
                results[f"volume_change_{period}"] = results["volume_change"].round(4)
                results = results.drop(columns=["Volume", "volume_change"])

        update_data = []
        for _, row in results.iterrows():
            update_dict = {
                "ticker": row["ticker"],
                "last_updated": row["last_updated"],
                "current_price": row["current_price"],
                "prev_close": row["prev_close"],
                "change_sign": row["change_sign"],
                "change_rt": row["change_1d"],
                "change_1d": row["change_1d"],
                "change_1w": row["change_1w"],
                "change_1m": row["change_1m"],
                "change_6m": row["change_6m"],
                "change_1y": row["change_1y"],
                "volume_rt": row["volume_1d"],
                "volume_1d": row["volume_1d"],
                "volume_1w": row["volume_1w"],
                "volume_1m": row["volume_1m"],
                "volume_6m": row["volume_6m"],
                "volume_1y": row["volume_1y"],
                "volume_change_rt": row["volume_change_1d"],
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
        current_time = datetime.now(timezone_map[ctry.value.upper()])
        end_date = current_time.date()
        start_date = end_date - timedelta(days=14)

        business_days = get_business_days(ctry.value.upper(), start_date, end_date)
        exclude_dates = KR_EXCLUDE_DATES if ctry == TrendingCountry.KR else US_EXCLUDE_DATES
        business_days = [bd for bd in business_days if bd.strftime("%Y-%m-%d") not in exclude_dates]
        business_days.sort()

        latest_business_day = business_days[-1]
        if ctry == TrendingCountry.KR:
            if pd.Timestamp(current_time.date()) in business_days:
                print(f"current_time: {current_time}###")
                latest_business_day = business_days[-2]
            else:
                latest_business_day = business_days[-1]

        stock_trends = database._select(
            table="stock_trend",
            columns=["ticker", "prev_close"],
            distinct=True,
            ctry=ctry.value,
        )
        stock_trend_dict = {row[0]: row[1] for row in stock_trends}
        unique_tickers = set(row[0] for row in stock_trends)

        if ctry == TrendingCountry.US:
            table_name = "stock_us_1m"
        elif ctry == TrendingCountry.KR:
            table_name = "stock_kr_1m"
        else:
            raise ValueError(f"Invalid country: {ctry.value}")

        start_datetime = latest_business_day.replace(hour=0, minute=0, second=0)
        end_datetime = latest_business_day.replace(hour=23, minute=59, second=59)

        df = pd.DataFrame(
            database._select(
                table=table_name,
                columns=["Ticker", "Open", "High", "Low", "Close", "Volume", "Date"],
                Date__gte=start_datetime,
                Date__lte=end_datetime,
                Ticker__in=unique_tickers,
            )
        )

        if df.empty:
            error_msg = f"""
            `최근 영업일 데이터 누락: {table_name} 테이블 데이터 체크 필요합니다.`
            * latest_business_day: {latest_business_day}
            """
            raise ValueError(error_msg)

        # 정렬 및 평균 가격 계산
        df = df.sort_values(by=["Ticker", "Date"], ascending=[True, False])
        df["volume_change"] = np.multiply(np.mean([df["Open"], df["High"], df["Low"], df["Close"]], axis=0), df["Volume"])

        # 티커별 집계
        grouped = (
            df.groupby("Ticker")
            .agg({"Close": "first", "Date": "first", "Volume": "sum", "volume_change": "sum"})
            .reset_index()
        )

        # 결과 데이터 생성
        update_data = []
        for _, row in grouped.iterrows():
            ticker = row["Ticker"]
            if ticker not in stock_trend_dict:
                continue

            current_price = row["Close"]
            prev_close = stock_trend_dict[ticker]

            if prev_close == 0:
                change_rt = 0
            else:
                change_rt = round(((current_price - prev_close) / prev_close * 100), 4)

            change_sign = 1 if current_price > prev_close else -1 if current_price < prev_close else 0

            update_data.append(
                {
                    "ticker": ticker,
                    "last_updated": row["Date"],
                    "current_price": current_price,
                    "change_sign": change_sign,
                    "change_rt": change_rt,
                    "volume_rt": row["Volume"],
                    "volume_change_rt": round(row["volume_change"], 4),
                }
            )

        if update_data:
            database._bulk_update(table="stock_trend", data=update_data, key_column="ticker")
            logging.info(f"Successfully updated {len(update_data)} records in stock_trend table")
        else:
            logging.info(f"No records to update for {ctry.value}")

    except Exception as e:
        logging.error(f"Error in run_stock_trend_by_realtime_batch: {str(e)}")
        raise e


if __name__ == "__main__":
    run_stock_trend_by_1d_batch(ctry=TrendingCountry.US)
    # run_stock_trend_by_realtime_batch(ctry=TrendingCountry.US)
