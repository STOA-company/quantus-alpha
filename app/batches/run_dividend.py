import time
import pandas as pd
from app.database.crud import database


def insert_dividend(ctry: str):
    if ctry == "US":
        contry = "us"
    else:
        raise ValueError("ctry must be US")

    # parquet 파일 읽기
    df_dividend = pd.read_parquet("static/dividend1.parquet")
    df_dividend = df_dividend.rename(
        columns={
            "Ticker": "ticker",
            "배당금": "per_share",
            "배당지급일": "payment_date",
            "배당락일": "ex_date",
            "Per Share": "per_share",
        }
    )
    df_dividend["ex_date"] = pd.to_datetime(df_dividend["ex_date"])
    df_dividend["payment_date"] = pd.to_datetime(df_dividend["payment_date"])

    # 유효한 ticker 필터링
    information_tickers = database._select(
        table="stock_information",
        columns=["ticker"],
    )
    list_information_tickers = [ticker[0] for ticker in information_tickers]
    df_dividend = df_dividend[df_dividend["ticker"].isin(list_information_tickers)]
    df_dividend = df_dividend.drop_duplicates()

    # Special 배당금 처리
    if "Desc_" in df_dividend.columns:
        df_dividend = (
            df_dividend.groupby(["ticker", "payment_date", "ex_date"])
            .agg(
                {
                    "per_share": "sum",
                }
            )
            .reset_index()
        )

    # 티커별 처리
    dividend_data = []
    for ticker in df_dividend["ticker"].unique():
        df_dividend_ticker = df_dividend[df_dividend["ticker"] == ticker]
        df_dividend_ticker = df_dividend_ticker.sort_values(by="ex_date", ascending=True)

        # 현재 ticker의 데이터에 대해서만 DB 조회
        existing_records = database._select(
            table="dividend_information",
            columns=["ticker", "ex_date", "payment_date"],
            ticker=ticker,
            ex_date__in=df_dividend_ticker["ex_date"].dt.strftime("%Y-%m-%d").tolist(),
            payment_date__in=df_dividend_ticker["payment_date"].dt.strftime("%Y-%m-%d").tolist(),
        )

        # 중복 체크를 위한 set 생성
        existing_set = {
            (ticker, ex_date.strftime("%Y-%m-%d"), payment_date.strftime("%Y-%m-%d"))
            for _, ex_date, payment_date in existing_records
        }

        # 중복 제거
        df_dividend_ticker = df_dividend_ticker[
            ~df_dividend_ticker.apply(
                lambda row: (row["ticker"], row["ex_date"].strftime("%Y-%m-%d"), row["payment_date"].strftime("%Y-%m-%d"))
                in existing_set,
                axis=1,
            )
        ]

        if df_dividend_ticker.empty:
            continue

        # ex_date 리스트 생성
        list_ex_date = df_dividend_ticker["ex_date"].dt.strftime("%Y-%m-%d").tolist()

        # 가격 데이터 조회 및 수익률 계산
        df_data_price = pd.DataFrame(
            database._select(
                table=f"stock_{contry}_1d",
                columns=["Date", "Close"],
                Ticker=ticker,
                Date__in=list_ex_date,
            )
        )
        if df_data_price.empty:
            continue

        df_data_price = df_data_price.rename(
            columns={
                "Date": "ex_date",
                "Close": "price",
            }
        )
        ticker_data = pd.merge(df_dividend_ticker, df_data_price, on="ex_date", how="left")
        ticker_data["yield_rate"] = round((ticker_data["per_share"] / ticker_data["price"]) * 100, 2)

        # bulk insert를 위한 데이터 준비
        dividend_data.extend(
            [
                {
                    "ticker": row["ticker"],
                    "payment_date": row["payment_date"],
                    "ex_date": row["ex_date"],
                    "per_share": row["per_share"],
                    "yield_rate": row["yield_rate"],
                }
                for _, row in ticker_data.iterrows()
                if pd.notna(row["yield_rate"])  # null 값 제외
            ]
        )
        # 1000건씩 bulk insert
        if len(dividend_data) >= 1000:
            database._insert(table="dividend_information", sets=dividend_data)
            dividend_data = []
            time.sleep(1)

    # 남은 데이터 처리
    if dividend_data:
        database._insert(table="dividend_information", sets=dividend_data)


if __name__ == "__main__":
    insert_dividend("US")
