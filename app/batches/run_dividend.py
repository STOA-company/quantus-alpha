import os
import time
import pandas as pd
from app.common.constants import ETF_DATA_DIR
from app.database.crud import database
from app.utils.etf_utils import ETFDataDownloader


def insert_dividend(ctry: str, type: str):
    if ctry == "KR":
        contry = "kr"
    elif ctry == "US":
        contry = "us"
    else:
        raise ValueError("ctry must be US or KR")

    if type not in ["stock", "etf"]:
        raise ValueError("type must be stock or etf")
    # parquet 파일 읽기
    df_dividend = pd.read_parquet(os.path.join(ETF_DATA_DIR, f"{contry}_{type}_dividend.parquet"))

    # 과거 배당금 데이터 전처리
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
        type=type,
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


class StockDividendDataDownloader(ETFDataDownloader):
    def __init__(self):
        super().__init__()

    def download_stock_dividend(self, ctry: str, download: bool = False):
        """
        주식 배당 데이터 다운로드

        Args:
            ctry (str): 국가코드 (US, KR)

        Returns:
            pd.DataFrame: 데이터프레임
        """
        if ctry not in ["US", "KR"]:
            raise ValueError("ctry must be 'US' or 'KR'")
        if ctry == "US":
            country = "us"
            query = """
            WITH DSINFO AS (
                SELECT infocode
                FROM DS2CtryQtInfo
                WHERE Region = 'us'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'EQ'  -- ET :ETF, EQ: 주식
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
                WHERE D.EFFECTIVEDATE BETWEEN '2020-01-01' AND GETDATE()
                GROUP BY D.INFOCODE, D.EFFECTIVEDATE, D.PayDate
            ),
            DSSUMADJ AS (
                SELECT A.INFOCODE,
                    A.ADJDATE,
                    A.CUMADJFACTOR,
                    C.EFFECTIVEDATE,
                    C.DSSUM AS UNADJ_DIV,
                    C.PayDate,
                    ROW_NUMBER() OVER(PARTITION BY A.INFOCODE ORDER BY A.ADJDATE) AS RN
                FROM DS2ADJ A WITH (INDEX(DS2Adj_1))
                INNER JOIN DSSUM C ON C.INFOCODE = A.INFOCODE
                                AND C.EFFECTIVEDATE BETWEEN A.ADJDATE AND ISNULL(A.ENDADJDATE, GETDATE())
                WHERE A.ADJDATE BETWEEN '1993-01-01' AND GETDATE()
                AND A.ADJTYPE = '2'
            ),
            LatestTicker AS (
                SELECT
                    InfoCode,
                    Ticker,
                    ROW_NUMBER() OVER (PARTITION BY InfoCode ORDER BY EndDate DESC) AS rn
                FROM Ds2MnemChg
            )
            SELECT
                T.Ticker as 'ticker',
                A.EFFECTIVEDATE as 'ex_date',
                A.UNADJ_DIV,
                A.PayDate as 'payment_date',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR
                    ELSE B.CUMADJFACTOR
                END as 'adj_factor',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR * A.UNADJ_DIV
                    ELSE A.UNADJ_DIV * B.CUMADJFACTOR
                END AS 'per_share'
            FROM DSSUMADJ AS A
            LEFT OUTER JOIN DSSUMADJ AS B ON A.INFOCODE = B.INFOCODE AND A.RN - 1 = B.RN
            LEFT OUTER JOIN LatestTicker T ON T.InfoCode = A.INFOCODE AND T.rn = 1
            WHERE A.EFFECTIVEDATE IS NOT NULL
            ORDER BY T.Ticker, A.EFFECTIVEDATE;
            """

        if ctry == "KR":
            country = "kr"
            query = """
            WITH DSINFO AS (
                SELECT infocode, DsLocalCode
                FROM DS2CtryQtInfo
                WHERE Region = 'kr'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'EQ'
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
                WHERE D.EFFECTIVEDATE BETWEEN '2020-01-01' AND GETDATE()
                GROUP BY D.INFOCODE, D.EFFECTIVEDATE, D.PayDate
            ),
            DSSUMADJ AS (
                SELECT A.INFOCODE,
                    A.ADJDATE,
                    A.CUMADJFACTOR,
                    C.EFFECTIVEDATE,
                    C.DSSUM AS UNADJ_DIV,
                    C.PayDate,
                    ROW_NUMBER() OVER(PARTITION BY A.INFOCODE ORDER BY A.ADJDATE) AS RN
                FROM DS2ADJ A WITH (INDEX(DS2Adj_1))
                INNER JOIN DSSUM C ON C.INFOCODE = A.INFOCODE
                                AND C.EFFECTIVEDATE BETWEEN A.ADJDATE AND ISNULL(A.ENDADJDATE, GETDATE())
                WHERE A.ADJDATE BETWEEN '1993-01-01' AND GETDATE()
                AND A.ADJTYPE = '2'
            ),
            LatestTicker AS (
                SELECT
                    InfoCode,
                    Ticker,
                    ROW_NUMBER() OVER (PARTITION BY InfoCode ORDER BY EndDate DESC) AS rn
                FROM Ds2MnemChg
            )
            SELECT
                C.DsLocalCode as 'ticker',
                A.EFFECTIVEDATE as 'ex_date',
                A.UNADJ_DIV,
                A.PayDate as 'payment_date',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR
                    ELSE B.CUMADJFACTOR
                END as 'adj_factor',
                CASE
                    WHEN A.EFFECTIVEDATE <> A.ADJDATE THEN A.CUMADJFACTOR * A.UNADJ_DIV
                    ELSE A.UNADJ_DIV * B.CUMADJFACTOR
                END AS 'per_share'
            FROM DSSUMADJ AS A
            LEFT OUTER JOIN DSSUMADJ AS B ON A.INFOCODE = B.INFOCODE AND A.RN - 1 = B.RN
            LEFT OUTER JOIN DSINFO AS C ON A.INFOCODE = C.INFOCODE
            LEFT OUTER JOIN LatestTicker T ON T.InfoCode = A.INFOCODE AND T.rn = 1
            WHERE A.EFFECTIVEDATE IS NOT NULL
            ORDER BY T.Ticker, A.EFFECTIVEDATE;

            """
        df = self._get_refinitiv_data(query)

        list_db_tickers = self._get_db_tickers_list(ctry=country, type="stock")

        if ctry == "KR":
            list_db_tickers = [self.kr_pattern.sub("K", ticker) for ticker in list_db_tickers]
        df = df[df["ticker"].isin(list_db_tickers)]

        if download:
            if ctry == "KR":
                df.to_parquet(os.path.join(self.DATA_DIR, "kr_stock_dividend.parquet"), index=False)
            elif ctry == "US":
                df.to_parquet(os.path.join(self.DATA_DIR, "us_stock_dividend.parquet"), index=False)
        return df


if __name__ == "__main__":
    downloader = StockDividendDataDownloader()
    downloader.download_stock_dividend("KR", download=True)
