import os
import time

import numpy as np
import pandas as pd

from app.batches.run_update_kr_ticker import update_kr_ticker
from app.common.constants import ETF_DATA_DIR
from app.core.extra.SlackNotifier import SlackNotifier
from app.core.logger.logger import setup_logger
from app.database.crud import database
from app.modules.screener.etf.utils import ETFDataDownloader

logger = setup_logger(__name__)
slack_noti = SlackNotifier()


def update_etf_information(ctry: str, df: pd.DataFrame):
    if ctry not in ["US", "KR"]:
        raise ValueError("ctry must be US or KR")
    logger.info(f"update_etf_information: {ctry}")

    # KR 시장의 경우 ticker 변환을 먼저 수행
    if ctry == "KR":
        df["Ticker"] = df["Ticker"].str.replace("^K", "A", regex=True)

    # Ticker 기준으로 중복 제거
    df = df.drop_duplicates(subset=["Ticker"])
    logger.info(f"drop_duplicates df: {df}")

    # Get existing tickers from database
    stock_information_ticker = database._select(table="stock_information", columns=["ticker"], type="etf", ctry=ctry)
    # 튜플 리스트에서 ticker 값만 추출
    existing_tickers = [ticker[0] for ticker in stock_information_ticker]
    logger.info(f"existing_tickers: {existing_tickers}")

    # stock_information에 없는 ticker만 필터링
    df = df[~df["Ticker"].isin(existing_tickers)]
    logger.info(f"df: {df}")
    if df.empty:
        logger.info("df is empty")
        return
    logger.info(f"new_tickers: {df['Ticker'].unique()}")

    # nan 값을 None으로 변환
    df = df.replace({np.nan: None})

    insert_data = []
    for _, row in df.iterrows():
        insert_data.append(
            {
                "ticker": row["Ticker"],
                "en_name": row.get("DsQtName", None),
                "ctry": ctry.lower(),
                "market": row.get("Market", None),
                "is_activate": False,
                "is_pub": False,
                "type": "etf",
            }
        )
        if len(insert_data) >= 1000:
            database._insert(table="stock_information", sets=insert_data)
            insert_data = []
    # Bulk insert remaining data
    if insert_data:
        database._insert(table="stock_information", sets=insert_data)
    logger.info(f"update_etf_information: {ctry} end")


def run_etf_price(ctry: str):
    try:
        if ctry == "US":
            df = pd.read_parquet(os.path.join(ETF_DATA_DIR, "us_etf_price.parquet"))
            country = "us"
        elif ctry == "KR":
            df = pd.read_parquet(os.path.join(ETF_DATA_DIR, "kr_etf_price.parquet"))
            country = "kr"
        else:
            raise ValueError("ctry must be US or KR")
        if df.columns[0] == "Unnamed: 0":
            df = df.drop(columns=["Unnamed: 0"])
        df = df[
            [
                "Ticker",
                "MarketDate",
                "DsQtName",
                "DsQtName",
                "Open_",
                "High",
                "Low",
                "Close_",
                "Volume",
                "ExchIntCode",
                "Bid",
                "Ask",
                "MktCap",
                "NumShrs",
            ]
        ]

        df = df.rename(
            columns={
                "MarketDate": "Date",
                "Open_": "Open",
                "Close_": "Close",
                "ExchIntCode": "Market",
                "MktCap": "MarketCap",
            }
        )

        if ctry == "US":
            df["Market"] = np.where(df["Market"] == 244, "NYS", df["Market"])
            df["Market"] = np.where(df["Market"] == 135, "NAS", df["Market"])
            df["Market"] = np.where(df["Market"] == 278, "BATS", df["Market"])
            df["Market"] = np.where(df["Market"] == 147, "OTC", df["Market"])
            df["Market"] = np.where(df["Market"] == 145, "NYS", df["Market"])
        elif ctry == "KR":
            df["Market"] = np.where(df["Market"] == 177, "KRX", df["Market"])
            df["Ticker"] = df["Ticker"].str.replace("^K", "A", regex=True)

        df["Date"] = pd.to_datetime(df["Date"])

        # stock_information에 없는 종목 업데이트
        if ctry == "US":
            update_etf_information(ctry=ctry, df=df)
        if ctry == "KR":
            update_kr_ticker()
        df = df.drop(columns=["DsQtName"])

        information_tickers = database._select(table="stock_information", columns=["ticker"], type="etf", ctry=country)
        list_information_tickers = [ticker[0] for ticker in information_tickers]
        df = df[df["Ticker"].isin(list_information_tickers)]

        df = df.replace({np.nan: None})

        # 최근 1달 데이터만 추출
        df = df[df["Date"] >= (pd.Timestamp.now() - pd.DateOffset(months=1))]

        price_data = []
        for ticker in df["Ticker"].unique():
            df_price_ticker = df[df["Ticker"] == ticker]
            df_price_ticker = df_price_ticker.sort_values(by="Date", ascending=True)

            existing_records = database._select(
                table=f"etf_{country}_1d",
                columns=["Ticker", "Date"],
                Ticker=ticker,
                Date__in=df_price_ticker["Date"].dt.strftime("%Y-%m-%d").tolist(),
            )

            existing_set = {(ticker, date.strftime("%Y-%m-%d")) for _, date in existing_records}

            df_price_ticker = df_price_ticker[
                ~df_price_ticker.apply(
                    lambda row: (row["Ticker"], row["Date"].strftime("%Y-%m-%d")) in existing_set,
                    axis=1,
                )
            ]

            if df_price_ticker.empty:
                continue

            price_data.extend(
                df_price_ticker.assign(Date=df_price_ticker["Date"].dt.strftime("%Y-%m-%d")).to_dict(orient="records")
            )

            if len(price_data) >= 1000:
                # pd.DataFrame(price_data).to_csv(f"/Users/kyungmin/git_repo/alpha-finder/check_data/etf/us_etf_price1_test_{ticker}.csv", index=False)
                database._insert(table=f"etf_{country}_1d", sets=price_data)
                price_data = []
                time.sleep(1)

        if price_data:
            # pd.DataFrame(price_data).to_csv(f"/Users/kyungmin/git_repo/alpha-finder/check_data/etf/us_etf_price1_test_{ticker}.csv", index=False)
            database._insert(table=f"etf_{country}_1d", sets=price_data)

    except Exception as e:
        print(e)
        slack_noti.notify_error(e, "고경민")
        raise e


def update_etf_status(ctry: str):
    if ctry not in ["US", "KR"]:
        raise ValueError("ctry must be US or KR")
    etf_downloader = ETFDataDownloader()
    etf_status = etf_downloader.get_suspended_etf(ctry=ctry)

    # 티커 변환
    if ctry == "KR":
        etf_status["DsLocalCode"] = etf_status["DsLocalCode"].apply(lambda x: "A" + x[1:] if x.startswith("K") else x)

    etf_status = etf_status.rename(columns={"DsLocalCode": "Ticker"})
    # 종목 분류
    active_tickers = etf_status[etf_status["StatusCode"] == "A"]["Ticker"].tolist()
    delisted_tickers = etf_status[etf_status["StatusCode"] == "D"]["Ticker"].tolist()
    suspended_tickers = etf_status[etf_status["StatusCode"] == "S"]["Ticker"].tolist()

    logger.info(f"active_tickers: {active_tickers}")
    logger.info(f"delisted_tickers: {delisted_tickers}")
    logger.info(f"suspended_tickers: {suspended_tickers}")

    # stock_information테이블 업데이트
    database._update(
        table="stock_information", sets={"is_delisted": False, "is_trading_stopped": False}, ticker__in=active_tickers
    )
    database._update(table="stock_information", sets={"is_delisted": True}, ticker__in=delisted_tickers)
    database._update(table="stock_information", sets={"is_trading_stopped": True}, ticker__in=suspended_tickers)


if __name__ == "__main__":
    run_etf_price(ctry="US")
