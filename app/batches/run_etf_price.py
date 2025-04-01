import os
import time
from app.common.constants import ETF_DATA_DIR
from app.database.crud import database

import numpy as np
import pandas as pd
from app.core.extra.SlackNotifier import SlackNotifier

slack_noti = SlackNotifier()


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

        print(f"{ctry} 데이터 전처리 완료###1")
        information_tickers = database._select(table="stock_information", columns=["ticker"], type="etf", ctry=country)
        print(f"{ctry} 데이터 전처리 완료###2")
        list_information_tickers = [ticker[0] for ticker in information_tickers]
        df = df[df["Ticker"].isin(list_information_tickers)]

        df = df.replace({np.nan: None})
        print(f"{ctry} 데이터 전처리 완료###3")
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


if __name__ == "__main__":
    run_etf_price(ctry="KR")
