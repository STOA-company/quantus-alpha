# Standard library imports
import csv
import datetime
import glob
import os
import random
import re
import time

# Third party imports
import numpy as np
import pandas as pd
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Local imports
from app.common.constants import ETF_DATA_DIR, KRX_DIR, MORNINGSTAR_DIR, PARQUET_DIR
from app.core.config import settings
from app.database.crud import database
from app.common.mapping import (
    base_asset_classification_map,
    etf_column_mapping,
    etf_risk_map,
    multiplier_map,
    replication_map,
)
from app.modules.screener.etf.enum import ETFMarketEnum


class ETFFactorExtractor:
    def __init__(self, price_path=None):
        self.db = database
        self.loader = ETFDataLoader()

    def extract_factor(self, ctry):
        # 가격 데이터 로드
        df_price = self.loader.load_etf_price(ctry)

        if "Unnamed: 0" in df_price.columns:
            df_price = df_price.drop(columns=["Unnamed: 0"])

        # 데이터 정렬
        df_sort = df_price.sort_values(["Ticker", "MarketDate"])

        # 날짜 형식 변환
        df_sort["MarketDate"] = pd.to_datetime(df_sort["MarketDate"])

        # 기본 수익률 계산
        df_sort["수정주가수익률"] = df_sort.groupby(["Ticker"])["Close_"].transform(lambda x: x.pct_change())

        # 수정주가 열 추가
        df_sort["수정주가"] = df_sort["Close_"]

        # 거래대금 계산 (VWAP 열이 없는 경우 Close_로 대체)
        if "VWAP" in df_sort.columns:
            df_sort["거래대금"] = df_sort["Volume"] * df_sort["VWAP"]
        else:
            df_sort["거래대금"] = (
                df_sort["Volume"] * (df_sort["Open"] + df_sort["High"] + df_sort["Low"] + df_sort["Close_"]) / 4
            )

        # 시가총액 정규화 (단위 맞추기)
        if "MktCap" in df_sort.columns:
            # 국가별 단위 조정
            if ctry == "KR":
                df_sort["marketCap"] = df_sort["MktCap"] / 100_000_000  # 단위 조정 / 원 -> 억원
                df_sort["거래대금"] = df_sort["거래대금"] / 100_000_000  # 단위 조정 / 원 -> 억원
            else:
                df_sort["marketCap"] = df_sort["MktCap"] / 1_000  # 단위 조정 / 달러 -> 천달러
                df_sort["거래대금"] = df_sort["거래대금"] / 1_000  # 단위 조정 / 달러 -> 천달러
        else:
            # MktCap이 없는 경우 대체 계산 (NumShrs가 있는 경우)
            if "NumShrs" in df_sort.columns:
                df_sort["marketCap"] = df_sort["NumShrs"] * df_sort["Close_"]
                if ctry == "KR":
                    df_sort["marketCap"] = df_sort["marketCap"] / 100_000_000
                else:
                    df_sort["marketCap"] = df_sort["marketCap"] / 1_000
            else:
                # 둘 다 없는 경우 임시값
                df_sort["marketCap"] = 0

        # 모멘텀 지표 계산
        df_sort = self._calculate_momentum(df_sort)

        # Bid-Ask 스프레드 계산
        df_sort = self._calculate_bid_ask_spread(df_sort)

        # 이격도 계산
        df_sort = self._calculate_disparity(df_sort)

        # 변동성 및 베타 계산
        df_sort = self._calculate_volatility_and_beta(df_sort)

        # RSI 계산
        df_sort = self._calculate_rsi(df_sort)

        # Sharpe, Sortino 계산
        df_sort = self._calculate_sharpe_sortino(df_sort)

        # 기간별 수익률 계산
        df_sort = self._calculate_returns(df_sort)

        # 52주 최고가, 최저가 계산
        df_sort = self._calculate_52week_high_low(df_sort)

        # 추가 팩터 계산
        df_sort = self._calculate_additional_factors(df_sort)

        # NaN 값 제거
        df_result = df_sort.dropna(subset=["수정주가수익률"])

        return df_result

    def _calculate_momentum(self, df):
        """모멘텀 지표 계산"""
        # 누적 수익 계산
        df["cum_return"] = df.groupby(["Ticker"])["수정주가수익률"].transform(lambda x: (1 + x).cumprod())

        # 다양한 기간의 모멘텀 계산
        df["momentum_1"] = df.groupby(["Ticker"])["cum_return"].transform(
            lambda x: x.pct_change(20)  # 약 1개월
        )

        df["momentum_3"] = df.groupby(["Ticker"])["cum_return"].transform(
            lambda x: x.pct_change(60)  # 약 3개월
        )

        df["momentum_6"] = df.groupby(["Ticker"])["cum_return"].transform(
            lambda x: x.pct_change(120)  # 약 6개월
        )

        df["momentum_12"] = df.groupby(["Ticker"])["cum_return"].transform(
            lambda x: x.pct_change(252)  # 약 1년
        )

        # 필요 없는 중간 계산 컬럼 제거
        df.drop(columns=["cum_return"], inplace=True)

        return df

    def _calculate_bid_ask_spread(self, df):
        """Bid-Ask 스프레드 계산"""
        # Bid와 Ask 컬럼이 존재하는 경우에만 계산
        if "Bid" in df.columns and "Ask" in df.columns:
            # 절대 스프레드 계산
            df["ba_absolute_spread"] = df["Ask"] - df["Bid"]

            # 상대적 스프레드 계산 (%)
            # 중간가격 기준 스프레드
            df["ba_mid_price"] = (df["Bid"] + df["Ask"]) / 2
            df["ba_relative_spread"] = (df["Ask"] - df["Bid"]) / df["ba_mid_price"] * 100

            # 20일 평균 스프레드 계산
            df["ba_spread_20d_avg"] = df.groupby(["Ticker"])["ba_relative_spread"].transform(
                lambda x: x.rolling(20).mean()
            )

            # 스프레드 변동성 (20일 표준편차)
            df["ba_spread_20d_std"] = df.groupby(["Ticker"])["ba_relative_spread"].transform(
                lambda x: x.rolling(20).std()
            )

            # 1일 스프레드 변화율 계산 (%)
            df["ba_spread_1d_change"] = (
                df.groupby(["Ticker"])["ba_relative_spread"].transform(lambda x: x.pct_change(1)) * 100
            )  # 퍼센트로 표시

            # 20일 스프레드 변화율 계산 (%)
            df["ba_spread_20d_change"] = (
                df.groupby(["Ticker"])["ba_relative_spread"].transform(lambda x: x.pct_change(20)) * 100
            )

            # 중간 계산 컬럼 제거
            df.drop(columns=["ba_mid_price"], inplace=True)
        else:
            # Bid, Ask 데이터가 없는 경우 NaN으로 채움
            spread_columns = [
                "ba_absolute_spread",
                "ba_relative_spread",
                "ba_spread_20d_avg",
                "ba_spread_20d_std",
                "ba_spread_1d_change",
                "ba_spread_20d_change",
            ]
            for col in spread_columns:
                df[col] = np.nan

            print("Bid 또는 Ask 데이터가 없어 스프레드를 계산할 수 없습니다.")

        return df

    def _calculate_disparity(self, df):
        """이격도 계산"""
        # 이동평균 계산
        for ma in [5, 10, 20, 50, 100, 200]:
            df[f"ma_{ma}"] = df.groupby(["Ticker"])["수정주가"].transform(lambda x: x.rolling(ma).mean())
            # 이격도 계산
            df[f"disparity_{ma}"] = df["수정주가"] / df[f"ma_{ma}"] * 100
            # 이동평균 컬럼 제거
            df.drop(columns=[f"ma_{ma}"], inplace=True)

        return df

    def _calculate_volatility_and_beta(self, df):
        """변동성 및 베타 계산"""
        # 변동성 (60일, 1년)
        df["vol_60"] = df.groupby(["Ticker"])["수정주가수익률"].transform(lambda x: x.rolling(60).std() * np.sqrt(252))

        df["vol"] = df.groupby(["Ticker"])["수정주가수익률"].transform(lambda x: x.rolling(252).std() * np.sqrt(252))

        # # 시장 수익률 데이터 가져오기
        # try:
        #     marketDf = self._get_market_returns(ctry, 2)
        #     marketDf.index = pd.to_datetime(marketDf.index).tz_localize(None)
        #     market_series = marketDf.squeeze()
        #     market_series = market_series.sort_index(ascending=False)

        #     # 전체 데이터를 날짜로 피봇하여 한 번에 처리
        #     pivot_data = df.sort_values('MarketDate').pivot(
        #         index='MarketDate',
        #         columns='Ticker',
        #         values='수정주가수익률'
        #     )

        #     # 인덱스를 datetime으로 변환하고 정렬
        #     pivot_data.index = pd.to_datetime(pivot_data.index)
        #     pivot_data = pivot_data.sort_index(ascending=False)

        #     # 중복된 인덱스 처리 (모든 종목에 대해 한 번에)
        #     pivot_data = pivot_data[~pivot_data.index.duplicated(keep='last')]

        #     # 기간별 데이터 준비
        #     one_year_data = pivot_data[:252]  # 약 1년치 데이터
        #     days_60_data = pivot_data[:60]    # 약 3개월치 데이터

        #     # market_series 정렬 및 기간 맞추기
        #     market_aligned = market_series.reindex(pivot_data.index)
        #     market_1y = market_aligned[:252]
        #     market_60d = market_aligned[:60]

        #     # 벡터화된 베타 계산
        #     def vectorized_beta(returns, market, min_periods):
        #         # 모든 종목에 대해 한 번에 공분산 계산
        #         covariance = returns.apply(
        #             lambda x: x.cov(market) if len(x.dropna()) >= min_periods else np.nan
        #         )
        #         market_variance = market.var()
        #         return covariance / market_variance if market_variance != 0 else pd.Series(np.nan, index=returns.columns)

        #     # 베타 계산
        #     if len(one_year_data) > 0 and len(market_1y) > 0:
        #         betas = vectorized_beta(one_year_data, market_1y, 200)

        #         # 결과를 데이터프레임으로 변환
        #         beta_df = pd.DataFrame({
        #             'Ticker': betas.index,
        #             'beta': betas.values
        #         })

        #         # df와 병합
        #         df = df.merge(beta_df, on='Ticker', how='left')
        #     else:
        #         df['beta'] = 1.0  # 기본값

        #     # 60일 베타 계산
        #     if len(days_60_data) > 0 and len(market_60d) > 0:
        #         betas_60 = vectorized_beta(days_60_data, market_60d, 40)

        #         # 결과를 데이터프레임으로 변환
        #         beta_60_df = pd.DataFrame({
        #             'Ticker': betas_60.index,
        #             'beta_60': betas_60.values
        #         })

        #         # df와 병합
        #         df = df.merge(beta_60_df, on='Ticker', how='left')
        #     else:
        #         df['beta_60'] = 1.0  # 기본값

        #     # 절대 베타 계산
        #     df['abs_beta'] = df['beta'].abs()
        #     df['abs_beta_60'] = df['beta_60'].abs()

        # except Exception as e:
        #     print(f"베타 계산 중 오류 발생: {e}")
        #     # 오류 발생 시 기본값 설정
        #     df['beta'] = 1.0
        #     df['beta_60'] = 1.0
        #     df['abs_beta'] = 1.0
        #     df['abs_beta_60'] = 1.0

        return df

    def _get_market_returns(self, ctry, years=2):
        """시장 수익률 데이터 가져오기"""
        pass

    def _calculate_rsi(self, df):
        """RSI 계산"""

        def cal_rsi(series, periods):
            delta = series.diff()
            gains = delta.where(delta > 0, 0)
            losses = -delta.where(delta < 0, 0)

            avg_gain = gains.rolling(window=periods).mean()
            avg_loss = losses.rolling(window=periods).mean()

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            return rsi

        # transform 사용하여 인덱스 일관성 유지
        for period in [9, 14, 25]:
            df[f"rsi_{period}"] = df.groupby(["Ticker"])["수정주가"].transform(lambda x: cal_rsi(x, period))

        return df

    def cal_downside(self, returns):
        """하방 표준편차 계산 함수"""
        return np.sqrt((returns[returns < 0] ** 2).sum() / len(returns))

    def _calculate_sharpe_sortino(self, df):
        """Sharpe 및 Sortino 비율 계산"""
        # 수익률 통계 계산
        df["profit_mean"] = df.groupby(["Ticker"])["수정주가수익률"].transform(
            lambda x: x.rolling(252, min_periods=1).mean()
        )
        df["profit_std"] = df.groupby(["Ticker"])["수정주가수익률"].transform(
            lambda x: x.rolling(252, min_periods=1).std()
        )
        df["profit_downside"] = df.groupby(["Ticker"])["수정주가수익률"].transform(
            lambda x: x.rolling(252, min_periods=1).apply(self.cal_downside)
        )

        df["sharpe"] = df["profit_mean"] / df["profit_std"]
        df["sortino"] = df["profit_mean"] / df["profit_downside"]

        # 무한값 처리
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        return df

    def _calculate_52week_high_low(self, df):
        # 52주 최고가, 최저가 대비 현재가
        df["week_52_high"] = df.groupby(["Ticker"])["High"].transform(lambda x: x.rolling(252).max())
        df["week_52_low"] = df.groupby(["Ticker"])["Low"].transform(lambda x: x.rolling(252).min())

        return df

    def _calculate_returns(self, df):
        """기간별 수익률 계산 (1달, 3달, 6달, 1년)"""
        # 각 기간별 이전 가격 구하기
        df["price_1m_ago"] = df.groupby(["Ticker"])["Close_"].transform(
            lambda x: x.shift(21)  # 약 1달 (21 영업일)
        )
        df["price_3m_ago"] = df.groupby(["Ticker"])["Close_"].transform(
            lambda x: x.shift(63)  # 약 3달 (63 영업일)
        )
        df["price_6m_ago"] = df.groupby(["Ticker"])["Close_"].transform(
            lambda x: x.shift(126)  # 약 6달 (126 영업일)
        )
        df["price_1y_ago"] = df.groupby(["Ticker"])["Close_"].transform(
            lambda x: x.shift(252)  # 약 1년 (252 영업일)
        )

        # 기간별 수익률 계산 (%)
        df["return_1m"] = ((df["Close_"] / df["price_1m_ago"]) - 1) * 100
        df["return_3m"] = ((df["Close_"] / df["price_3m_ago"]) - 1) * 100
        df["return_6m"] = ((df["Close_"] / df["price_6m_ago"]) - 1) * 100
        df["return_1y"] = ((df["Close_"] / df["price_1y_ago"]) - 1) * 100

        # 필요 없는 중간 계산 컬럼 제거
        df.drop(columns=["price_1m_ago", "price_3m_ago", "price_6m_ago", "price_1y_ago"], inplace=True)

        return df

    def calculate_all_factors(self, ctry):
        """모든 팩터 계산 및 결과 반환"""
        df_factors = self.extract_factor(ctry)

        # 각 티커별 최신 데이터만 필터링
        df_latest = df_factors.sort_values(["Ticker", "MarketDate"]).groupby("Ticker").tail(1)

        return df_latest

    def _calculate_additional_factors(self, df):
        """추가 팩터 계산"""
        # MDD (Maximum Drawdown) 계산 - 주석 처리
        # df['rolling_max'] = df.groupby(['Code'])['close'].transform(
        #     lambda x: x.cummax()
        # )
        # df['drawdown'] = (df['close'] / df['rolling_max'] - 1) * 100
        # df['MDD'] = df.groupby(['Code'])['drawdown'].transform(
        #     lambda x: x.rolling(252).min()
        # )

        # 1년 drawdown 계산
        df["rolling_max_1y"] = df.groupby(["Ticker"])["Close_"].transform(lambda x: x.rolling(252, min_periods=1).max())
        df["drawdown_1y"] = (df["Close_"] / df["rolling_max_1y"] - 1) * 100

        # 평균 거래대금을 1개월(약 21 영업일) 중앙값으로 변경
        # 현재 날짜에서 1개월 전 날짜를 계산
        today = df["MarketDate"].max()
        one_m_ago = today - pd.DateOffset(months=1)

        # 지난 1개월 데이터만 필터링
        monthly_data = df[(df["MarketDate"] >= one_m_ago) & (df["MarketDate"] <= today)]

        # 종목별 거래대금 중앙값 계산
        median_trade = monthly_data.groupby(["Ticker"])["거래대금"].median().reset_index()
        median_trade.rename(columns={"거래대금": "median_trade"}, inplace=True)

        # 계산된 중앙값을 원본 데이터프레임에 병합
        df = pd.merge(df, median_trade, how="left", on=["Ticker"])

        # 필요 없는 중간 계산 컬럼 제거
        df.drop(columns=["rolling_max_1y"], inplace=True)

        return df


class ETFDataDownloader:
    def __init__(self):
        self.refinitiv_server = settings.REFINITIV_SERVER
        self.refinitiv_database = settings.REFINITIV_DATABASE
        self.refinitiv_username = settings.REFINITIV_USERNAME
        self.refinitiv_password = settings.REFINITIV_PASSWORD
        self.DATA_DIR = ETF_DATA_DIR
        self.db = database
        self.kr_pattern = re.compile(r"^A")

    def _get_refinitiv_data(self, query):
        """
        Refinitiv 데이터 가져오기

        Args:
            query (str): 쿼리 문자열

        Returns:
            pd.DataFrame: 데이터프레임
        """
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.refinitiv_server};DATABASE={self.refinitiv_database};UID={self.refinitiv_username};PWD={self.refinitiv_password};TrustServerCertificate=Yes"
        conn = pyodbc.connect(conn_str)

        # cursor = conn.cursor()
        # cursor.execute(query)

        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def download_etf_dividend(self, ctry="KR", download=False):
        """
        배당 데이터 다운로드

        Args:
            ctry (str): 국가코드 (US, KR)

        Returns:
            pd.DataFrame: 데이터프레임
        """
        if ctry not in ["US", "KR"]:
            raise ValueError("ctry must be 'US' or 'KR'")
        if ctry == "US":
            query = """
            WITH DSINFO AS (
                SELECT infocode
                FROM DS2CtryQtInfo
                WHERE Region = 'us'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'ET'  -- ET :ETF, EQ: 주
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
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
            query = """
            WITH DSINFO AS (
                SELECT infocode, DsLocalCode
                FROM DS2CtryQtInfo
                WHERE Region = 'kr'
                AND StatusCode IN ('A', 'S')
                AND TypeCode = 'ET'
            ),
            DSSUM AS (
                SELECT D.INFOCODE,
                    D.EFFECTIVEDATE,
                    D.PayDate,
                    SUM(D.DIVRATE) AS DSSUM
                FROM DS2DIV D WITH (INDEX(DS2Div_1))
                INNER JOIN DSINFO I ON D.INFOCODE = I.INFOCODE
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

        list_db_tickers = self._get_db_tickers_list(ctry)

        if ctry == "KR":
            list_db_tickers = [self.kr_pattern.sub("K", ticker) for ticker in list_db_tickers]
        df = df[df["ticker"].isin(list_db_tickers)]

        if download:
            if ctry == "KR":
                df.to_parquet(os.path.join(self.DATA_DIR, "kr_etf_dividend.parquet"), index=False)
            elif ctry == "US":
                df.to_parquet(os.path.join(self.DATA_DIR, "us_etf_dividend.parquet"), index=False)
        return df

    def dwonload_etf_price(self, ctry: str, download: bool = False):
        """
        가격 데이터

        Args:
            ctry (str): 국가코드 (US, KR)

        Returns:
            pd.DataFrame: 데이터프레임
        """
        if ctry not in ["US", "KR"]:
            raise ValueError("ctry must be 'US' or 'KR'")

        if ctry == "US":
            query = """
            SELECT
                c.Ticker,
                b.MarketDate, b.Open_, b.High, b.Low, b.Close_, b.Volume, b.ExchIntCode, b.Bid, b.Ask,
                d.MktCap, d.NumShrs,
                e.VWAP
            FROM
                Ds2CtryQtInfo a
            JOIN
                vw_Ds2Pricing b
                ON b.InfoCode = a.InfoCode
            JOIN
                Ds2MnemChg c
                ON c.InfoCode  = a.InfoCode
                AND c.EndDate = (
                    SELECT MAX(EndDate)
                    FROM Ds2MnemChg
                    WHERE InfoCode = a.InfoCode
                )
            JOIN
                vw_Ds2MktCap d
                ON a.InfoCode = d.InfoCode
                AND b.MarketDate = d.MarketDate
                AND b.ExchIntCode=d.PrimExchIntCode
            LEFT JOIN
                DS2PrimQtPrc e
                ON a.InfoCode = e.InfoCode
                AND b.MarketDate = e.MarketDate
                AND b.ExchIntCode = e.ExchIntCode
            where a.Region = 'US'
                and a.TypeCode ='ET' -- 주식 : EQ / ETF : ET
                and a.StatusCode != 'D'
                and b.MarketDate >='2024-01-01'
                and b.AdjType = 2
                and b.Currency = 'usd'
                and c.EndDate >= '2025-01-01'
            ;
            """

        if ctry == "KR":
            query = """
            SELECT
                a.DsLocalCode as 'Ticker',
                b.MarketDate, b.Open_, b.High, b.Low, b.Close_, b.Volume, b.ExchIntCode, b.Bid, b.Ask,
                d.MktCap, d.NumShrs,
                e.VWAP
            FROM
                Ds2CtryQtInfo a
            LEFT JOIN
                vw_Ds2Pricing b
                ON b.InfoCode = a.InfoCode
            LEFT JOIN
                vw_Ds2MktCap d
                ON a.InfoCode = d.InfoCode
                AND b.MarketDate = d.MarketDate
                AND b.ExchIntCode=d.PrimExchIntCode
            LEFT JOIN
                DS2PrimQtPrc e
                ON a.InfoCode = e.InfoCode
                AND b.MarketDate = e.MarketDate
                AND b.ExchIntCode = e.ExchIntCode
            where a.Region = 'KR'
                and a.TypeCode ='ET' -- 주식 : EQ / ETF : ET
                and a.StatusCode != 'D'
                and b.MarketDate >='2024-01-01'
                and b.AdjType = 2
                and b.Currency = 'krw'
            ;
            """

        df = self._get_refinitiv_data(query)
        list_db_tickers = self._get_db_tickers_list(ctry)
        if ctry == "KR":
            list_db_tickers = [self.kr_pattern.sub("K", ticker) for ticker in list_db_tickers]
        df = df[df["Ticker"].isin(list_db_tickers)]
        if download:
            if ctry == "KR":
                df.to_parquet(os.path.join(self.DATA_DIR, "kr_etf_price.parquet"), index=False)
            elif ctry == "US":
                df.to_parquet(os.path.join(self.DATA_DIR, "us_etf_price.parquet"), index=False)
        return df

    def _get_db_tickers_list(self, ctry: str, type: str = "etf"):
        tickers = self.db._select(table="stock_information", columns=["ticker"], ctry=ctry, type=type)
        tickers = [ticker[0] for ticker in tickers]
        return tickers


class KRXDownloader:
    """
    KRX 웹사이트에서 데이터를 다운로드하는 클래스
    """

    def __init__(self, download_path=None, use_headless=True):
        """
        KRXDownloader 클래스 초기화

        Args:
            download_path (str, optional): 다운로드 폴더 경로. 기본값은 사용자의 Downloads 폴더.
            use_headless (bool, optional): headless 모드 사용 여부. 기본값은 True.
        """
        # 다운로드 폴더 설정
        self.download_path = download_path or os.path.join(os.path.expanduser("~"), "Downloads")
        self.use_headless = use_headless
        self.chrome_options = self._configure_chrome_options()

    def _configure_chrome_options(self):
        """
        Chrome 브라우저 옵션 설정

        Returns:
            Options: 설정된 Chrome 옵션
        """
        chrome_options = Options()
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": self.download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True,
            },
        )

        if self.use_headless:
            # headless 모드에서도 작동하도록 추가 설정
            chrome_options.add_argument("--headless=new")  # 새로운 headless 모드 사용
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            # 사용자 에이전트 설정 (headless 감지 방지)
            chrome_options.add_argument(
                "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
            )

        return chrome_options

    def rename_latest_download(self, new_filename):
        """
        가장 최근에 다운로드된 CSV 파일의 이름을 변경

        Args:
            new_filename (str): 새로운 파일 이름

        Returns:
            str: 변경된 파일의 경로 또는 파일을 찾지 못한 경우 None
        """
        # 다운로드 폴더의 파일 목록 가져오기 (수정 시간 기준으로 정렬)
        files = glob.glob(os.path.join(self.download_path, "*.csv"))
        if not files:
            return None

        # 가장 최근에 다운로드된 CSV 파일 찾기
        latest_file = max(files, key=os.path.getmtime)

        # 새 파일 경로 생성
        if not new_filename.endswith(".csv"):
            new_filename += ".csv"
        new_file_path = os.path.join(self.download_path, new_filename)

        # 이미 같은 이름의 파일이 있다면 덮어쓰기 전에 백업
        if os.path.exists(new_file_path):
            backup_name = f"{new_filename.split('.')[0]}_{int(time.time())}.csv"
            backup_path = os.path.join(self.download_path, backup_name)
            os.rename(new_file_path, backup_path)
            print(f"기존 파일을 백업했습니다: {backup_name}")

        # 파일 이름 변경
        os.rename(latest_file, new_file_path)
        print(f"파일 이름을 변경했습니다: {os.path.basename(latest_file)} -> {new_filename}")

        return new_file_path

    def download_data(self, is_detail=False, custom_filename=None):
        """
        KRX 웹사이트에서 데이터 다운로드

        Args:
            is_detail (bool, optional): 상세 데이터 다운로드 여부. 기본값은 False.
            custom_filename (str, optional): 사용자 지정 파일 이름. 기본값은 None.

        Returns:
            str: 다운로드된 파일의 경로
        """
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.chrome_options)

        try:
            # 웹페이지 접속
            url_detail = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020103010901"
            url_base = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030104"
            url = url_detail if is_detail else url_base

            driver.get(url)

            # 현재 날짜 가져오기 (파일명에 사용)
            today = datetime.now().strftime("%Y%m%d")

            # 페이지 로딩 대기
            print("페이지 로딩 중...")
            time.sleep(10)

            # JavaScript 실행을 통해 다운로드 버튼 클릭
            print("다운로드 버튼 찾는 중...")
            driver.execute_script("document.querySelector('.CI-MDI-UNIT-DOWNLOAD').click();")
            time.sleep(3)  # 팝업이 나타날 때까지 대기

            # JavaScript를 사용하여 CSV 다운로드 버튼 클릭
            print("CSV 버튼 클릭 시도...")
            driver.execute_script("document.querySelector('div[data-type=\"csv\"] a').click();")

            # 다운로드 완료 대기
            print("다운로드 대기 중...")
            time.sleep(10)
            print(f"다운로드가 완료되었습니다. 파일 위치: {self.download_path}")

            # 파일 이름 변경
            try:
                if custom_filename:
                    new_filename = custom_filename
                else:
                    page_type = "krx_detail" if is_detail else "krx_base"
                    new_filename = f"{page_type}_{today}"

                renamed_file = self.rename_latest_download(new_filename)

                if renamed_file:
                    print(f"최종 파일 경로: {renamed_file}")
                    return renamed_file
                else:
                    print("다운로드된 파일을 찾을 수 없습니다.")
                    return None
            except Exception as e:
                print(f"파일 이름 변경 중 오류 발생: {e}")
                return None

        except Exception as e:
            print(f"다운로드 중 오류 발생: {e}")
            # 디버깅을 위해 페이지의 HTML 콘텐츠 출력
            print("현재 페이지 HTML 일부:")
            print(driver.page_source[:1000])  # 처음 1000자만 출력
            return None

        finally:
            # 브라우저 종료
            driver.quit()


class ETFDataLoader:
    """ETF 데이터를 로드하는 클래스"""

    def __init__(self):
        self.db = database
        self.base_dir = ETF_DATA_DIR
        self.krx_dir = KRX_DIR
        self.parquet_dir = PARQUET_DIR
        self.morningstar_dir = MORNINGSTAR_DIR

    def load_factor(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_factor.parquet"
        df = pd.read_parquet(os.path.join(self.base_dir, file_name))
        return df

    def load_etf_info(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        if ctry == "KR":
            select_colums = ["ticker", "ctry", "market"]
        else:
            select_colums = ["ticker", "ctry", "market", "en_name"]
        etf_info = pd.DataFrame(
            self.db._select(table="stock_information", columns=select_colums, ctry=country, type="etf")
        )
        return etf_info

    def load_etf_price(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_price.parquet"
        df = pd.read_parquet(os.path.join(self.base_dir, file_name))
        return df

    def load_etf_dividend(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_dividend.parquet"
        df = pd.read_parquet(os.path.join(self.base_dir, file_name))
        return df

    def load_etf_dividend_factor(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_dividend_factor.parquet"
        df = pd.read_parquet(os.path.join(self.base_dir, file_name))
        return df

    def load_krx(self, base=False, detail=False):
        if not base and not detail:
            raise ValueError("base or detail must be True")

        if base:
            df_base = pd.read_parquet(os.path.join(self.krx_dir, "data_base.parquet"))
        if detail:
            df_detail = pd.read_parquet(os.path.join(self.krx_dir, "data_detail.parquet"))

        if base and detail:
            df_krx = pd.merge(df_base, df_detail, left_on="단축코드", right_on="종목코드", how="left")
        elif base:
            df_krx = df_base
        else:
            df_krx = df_detail

        return df_krx

    def load_etf_factors(self, market_filter: ETFMarketEnum):
        df = pd.DataFrame()
        if market_filter in [ETFMarketEnum.US, ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
            df = pd.read_parquet(os.path.join(self.parquet_dir, "us_etf_factors.parquet"))
            df["country"] = "us"
        elif market_filter == ETFMarketEnum.KR:
            df = pd.read_parquet(os.path.join(self.parquet_dir, "kr_etf_factors.parquet"))
            df["country"] = "kr"
        elif market_filter == ETFMarketEnum.ALL:
            df = pd.read_parquet(os.path.join(self.parquet_dir, "global_etf_factors.parquet"))
        else:
            raise ValueError(f"Invalid market: {market_filter}")

        if "volatility" in df.columns:
            df.rename(columns={"volatility": "risk_rating"}, inplace=True)

        return df

    def load_morningstar(self, is_expense: bool = True, is_rating: bool = True):
        df = pd.DataFrame()
        if is_expense:
            df_expense = pd.read_parquet(os.path.join(self.morningstar_dir, "us_etf_morningstar_expense.parquet"))
            df = df_expense if df.empty else pd.merge(df, df_expense, on="ticker", how="left")
        if is_rating:
            df_rating = pd.read_parquet(os.path.join(self.morningstar_dir, "us_etf_morningstar_rating.parquet"))
            df = df_rating if df.empty else pd.merge(df, df_rating, on="ticker", how="left")
        return df


class ETFDividendFactorExtractor:
    """
    한국 ETF 배당 팩터 추출기
    - 배당 주기 (dividend_frequency)
    - 주당 배당금 (last_dividend_per_share)
    - 배당 수익률(최근) (recent_dividend_yield)
    - 배당 성장률 (dividend_growth_rate)
    """

    def __init__(self):
        """
        한국 ETF 배당 팩터 추출기 초기화
        """
        self.loader = ETFDataLoader()

    def extract_dividend_factors(self, output_path=None, ctry="KR"):
        """
        배당 관련 팩터 추출 메인 함수

        Args:
            output_path (str): 결과를 저장할 CSV 파일 경로

        Returns:
            pd.DataFrame: 배당 팩터가 추출된 데이터프레임
        """
        print("ETF 배당 팩터 추출 시작...")
        start_time = datetime.datetime.now()

        # 1. 데이터 로드
        dividend_data = self.loader.load_etf_dividend(ctry)
        etf_price_data = self.loader.load_etf_price(ctry)

        print(f"배당 데이터 로드 완료: {len(dividend_data)}개 레코드")
        print(f"ETF 가격 데이터 로드 완료: {len(etf_price_data)}개 레코드")

        # 2. 날짜 형식 변환 - CSV에서 문자열로 로드된 날짜를 datetime 객체로 변환
        if "payment_date" in dividend_data.columns:
            dividend_data["payment_date"] = pd.to_datetime(dividend_data["payment_date"], errors="coerce")
        if "ex_date" in dividend_data.columns:
            dividend_data["ex_date"] = pd.to_datetime(dividend_data["ex_date"], errors="coerce")
        if "MarketDate" in etf_price_data.columns:
            etf_price_data["MarketDate"] = pd.to_datetime(etf_price_data["MarketDate"], errors="coerce")

        # 날짜 변환 후 NaT 값 제거 또는 처리
        if "payment_date" in dividend_data.columns:
            dividend_data = dividend_data.dropna(subset=["payment_date"])

        # 3. ETF별 최신 가격 정보 추출
        latest_prices = {}
        if not etf_price_data.empty:
            try:
                if "MarketDate" in etf_price_data.columns and "Ticker" in etf_price_data.columns:
                    etf_price_data = etf_price_data.sort_values("MarketDate")
                    latest_idx = etf_price_data.groupby("Ticker")["MarketDate"].idxmax()
                    latest_prices_df = etf_price_data.loc[latest_idx]

                    latest_prices = {row["Ticker"]: row for _, row in latest_prices_df.iterrows()}
            except Exception as e:
                print(f"최신 가격 정보 추출 중 오류 발생: {e}")

        # 4. 결과 데이터프레임을 위한 리스트
        results = []

        # 5. 각 티커별로 계산 진행
        if "ticker" not in dividend_data.columns:
            print("Error: 'ticker' 컬럼이 배당 데이터에 없습니다.")
            return pd.DataFrame()

        ticker_groups = dict(list(dividend_data.groupby("ticker")))
        total_tickers = len(ticker_groups)
        processed = 0

        for ticker, dividend_group in ticker_groups.items():
            try:
                # 진행 상황 표시
                processed += 1
                if processed % 50 == 0 or processed == total_tickers:
                    print(f"진행 중: {processed}/{total_tickers} ETFs 처리 완료 ({processed/total_tickers*100:.1f}%)")

                # 가격 정보 가져오기
                latest_price_info = latest_prices.get(ticker)
                current_price = None

                # 가격 정보 추출
                if latest_price_info is not None and "Close_" in latest_price_info:
                    current_price = latest_price_info["Close_"]

                # 최신 가격 정보가 없는 경우 배당 데이터의 가격 사용
                if current_price is None and "price" in dividend_group.columns:
                    current_price = dividend_group.sort_values("payment_date", ascending=False).iloc[0]["price"]

                # 배당 관련 팩터 계산
                dividend_count = self._calculate_dividend_count(dividend_group)
                recent_dividend_yield = self._calculate_recent_dividend_yield(dividend_group, current_price)
                ttm_dividend_yield = self._calculate_ttm_dividend_yield(dividend_group, current_price)
                dividend_growth_rate_3y = self._calculate_dividend_growth_rate(dividend_group, 3)
                dividend_growth_rate_5y = self._calculate_dividend_growth_rate(dividend_group, 5)
                dividend_frequency = self._calculate_dividend_frequency(dividend_group)

                # 최신 배당 정보
                if len(dividend_group) > 0:
                    latest_dividend = dividend_group.sort_values("payment_date", ascending=False).iloc[0]

                    results.append(
                        {
                            "ticker": ticker,
                            "dividend_count": dividend_count,
                            "dividend_frequency": dividend_frequency,
                            "last_dividend_date": latest_dividend["payment_date"],
                            "last_dividend_per_share": latest_dividend["per_share"],
                            "recent_dividend_yield": recent_dividend_yield,
                            "ttm_dividend_yield": ttm_dividend_yield,
                            "dividend_growth_rate_3y": dividend_growth_rate_3y,
                            "dividend_growth_rate_5y": dividend_growth_rate_5y,
                        }
                    )
            except Exception as e:
                print(f"Error processing ticker {ticker}: {e}")

        # 6. 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)

        # 배당 수익률 기준으로 정렬
        if not result_df.empty and "recent_dividend_yield" in result_df.columns:
            result_df = result_df.sort_values("recent_dividend_yield", ascending=False)

        print(f"ETF 배당 팩터 추출 완료: {len(result_df)}개 ETF")
        print(f"처리 시간: {datetime.datetime.now() - start_time}")

        # 결과 저장
        if output_path:  # TODO :: parquet 파일로 변경
            result_df.to_csv(output_path, index=False)
            print(f"결과가 {output_path}에 저장되었습니다.")

        return result_df

    def _calculate_dividend_count(self, ticker_dividends):
        """
        배당 주기 계산 함수 - 5년 동안의 연평균 배당 지급 횟수 기준

        Args:
            ticker_dividends (DataFrame): 특정 ETF의 배당 데이터

        Returns:
            float: 연간 평균 배당 횟수 (정보 없는 경우 0 반환)
        """
        if len(ticker_dividends) == 0:
            return 0

        # 최근 5년치 데이터 고려 (올해 제외)
        current_year = datetime.datetime.now().year
        five_years_ago = current_year - 5

        # payment_date가 datetime 타입인지 확인
        if pd.api.types.is_datetime64_any_dtype(ticker_dividends["payment_date"]):
            # 올해를 제외한 최근 5년 데이터만 필터링
            historical_dividends = ticker_dividends[
                (ticker_dividends["payment_date"].dt.year < current_year)
                & (ticker_dividends["payment_date"].dt.year >= five_years_ago)
            ]

            if len(historical_dividends) == 0:
                return 0

            # 연도별 배당 횟수 계산
            yearly_counts = historical_dividends["payment_date"].dt.year.value_counts().sort_index()

            # 데이터가 있는 총 연도 수
            data_years = len(yearly_counts)

            if data_years == 0:
                return 0

            # 평균 배당 횟수 계산
            total_payments = sum(yearly_counts)
            avg_yearly_payments = total_payments / data_years

            return avg_yearly_payments
        else:
            # payment_date가 datetime 타입이 아닌 경우
            return 0

    def _calculate_recent_dividend_yield(self, ticker_dividends, current_price):
        """
        최근 배당 수익률 계산 함수 (작년 배당 데이터와 현재 가격 사용)

        Args:
            ticker_dividends (DataFrame): 특정 ETF의 배당 데이터
            current_price (float): 현재 가격

        Returns:
            float: 최근 배당 수익률 (%)
        """
        # 필요한 데이터가 없거나 current_price가 0 또는 None인 경우
        if ticker_dividends.empty or current_price is None or current_price <= 0:
            return None

        # payment_date가 datetime 타입인지 확인
        if not pd.api.types.is_datetime64_any_dtype(ticker_dividends["payment_date"]):
            return None

        current_year = datetime.datetime.now().year
        last_year = current_year - 1

        # 작년 배당 데이터 필터링
        year_dividends = ticker_dividends[ticker_dividends["payment_date"].dt.year == last_year]

        # 작년 데이터가 없는 경우, 가장 최근 연도의 데이터 사용
        if len(year_dividends) == 0:
            # 모든 연도 가져오기
            available_years = sorted(ticker_dividends["payment_date"].dt.year.unique())

            # 가장 최근 연도 선택
            if available_years:
                most_recent_year = max(available_years)
                year_dividends = ticker_dividends[ticker_dividends["payment_date"].dt.year == most_recent_year]
            else:
                return None

        # 총 배당금 합계
        total_dividend = year_dividends["per_share"].sum()

        # 배당 수익률 계산 (%)
        dividend_yield = (total_dividend / current_price) * 100

        return dividend_yield

    def _calculate_ttm_dividend_yield(self, ticker_dividends, current_price):
        """
        TTM(Trailing Twelve Months) 배당 수익률 계산 함수
        최근 12개월간의 배당 데이터와 현재 가격을 사용하여 배당 수익률 계산

        Args:
            ticker_dividends (DataFrame): 특정 ETF의 배당 데이터
            current_price (float): 현재 가격

        Returns:
            float: TTM 배당 수익률 (%)
        """
        # 필요한 데이터가 없거나 current_price가 0 또는 None인 경우
        if ticker_dividends.empty or current_price is None or current_price <= 0:
            return None

        # payment_date가 datetime 타입인지 확인
        if not pd.api.types.is_datetime64_any_dtype(ticker_dividends["payment_date"]):
            return None

        # 현재 날짜
        current_date = datetime.datetime.now()

        # 12개월 전 날짜 계산
        one_year_ago = current_date - datetime.timedelta(days=365)

        # 최근 12개월 배당 데이터 필터링
        ttm_dividends = ticker_dividends[ticker_dividends["payment_date"] >= one_year_ago]

        # 최근 12개월간 데이터가 없는 경우 최근 데이터 사용
        if len(ttm_dividends) == 0:
            # 모든 배당 날짜가 정렬된 리스트
            sorted_dates = sorted(ticker_dividends["payment_date"].unique())

            # 가장 최근 12개월에 해당하는 데이터가 없으면 가장 최근 12개월 기간의 데이터 사용
            if len(sorted_dates) > 0:
                latest_date = sorted_dates[-1]
                oldest_date_within_year = latest_date - datetime.timedelta(days=365)
                ttm_dividends = ticker_dividends[
                    (ticker_dividends["payment_date"] <= latest_date)
                    & (ticker_dividends["payment_date"] >= oldest_date_within_year)
                ]

                # 최소 1개 이상의 데이터가 있어야 함
                if len(ttm_dividends) == 0:
                    # 1년치 데이터가 없으면 가장 최근의 배당 데이터만 사용
                    newest_dividends = ticker_dividends[ticker_dividends["payment_date"] == latest_date]
                    if len(newest_dividends) > 0:
                        ttm_dividends = newest_dividends
                    else:
                        return None
            else:
                return None

        # 총 배당금 합계
        total_dividend = ttm_dividends["per_share"].sum()

        # 배당 수익률 계산 (%)
        ttm_dividend_yield = (total_dividend / current_price) * 100

        return ttm_dividend_yield

    def _calculate_dividend_growth_rate(self, ticker_dividends, period=5):
        """
        배당 성장률 계산 함수

        Args:
            ticker_dividends (DataFrame): 특정 ETF의 배당 데이터

        Returns:
            float: 배당 성장률 (%) 또는 None (완전한 데이터가 없는 경우)
        """
        if ticker_dividends.empty:
            return None

        # payment_date가 datetime 타입인지 확인
        if not pd.api.types.is_datetime64_any_dtype(ticker_dividends["payment_date"]):
            return None

        # 현재 연도에서 1을 빼서 최근 완료된 연도 구하기
        current_year = datetime.datetime.now().year
        latest_year = current_year - 1

        # 5년 전 연도 계산
        period_years_ago = latest_year - (period - 1)

        # 연도별 배당금 합계 계산
        yearly_dividends = ticker_dividends.groupby(ticker_dividends["payment_date"].dt.year)["per_share"].sum()

        # 최근 완료된 연도의 배당금이 있는지 확인
        if latest_year not in yearly_dividends.index:
            # 최근 연도 데이터가 없으면 가장 최근의 완료된 연도 찾기
            available_years = [year for year in yearly_dividends.index if year < current_year]
            if not available_years:
                return None
            latest_year = max(available_years)
            period_years_ago = latest_year - (period - 1)  # 5년 범위 재조정

        # 5년 전 연도 데이터가 있는지 확인
        if period_years_ago not in yearly_dividends.index:
            # 5년 전 정확한 데이터가 없는 경우, 가장 가까운 이전 연도 사용
            earlier_years = [year for year in yearly_dividends.index if year <= period_years_ago]
            if not earlier_years:
                return None
            period_years_ago = max(earlier_years)

        # 실제 연도 차이 계산
        years_diff = latest_year - period_years_ago

        # 연도 차이가 1년 미만이면 성장률 계산 불가
        if years_diff < 1:
            return None

        # 시작 연도와 종료 연도의 배당금
        start_dividend = yearly_dividends[period_years_ago]
        end_dividend = yearly_dividends[latest_year]

        # 초기 배당금이 0이면 성장률 계산 불가
        if start_dividend <= 0:
            return None

        # 연평균 성장률 계산 (CAGR)
        growth_rate = (np.power(end_dividend / start_dividend, 1 / years_diff) - 1) * 100

        return growth_rate

    def _calculate_dividend_frequency(self, ticker_dividends):
        """
        배당 주기 계산 함수 - dividend_count 값을 기반으로 배당 주기 문자열 반환

        Args:
            ticker_dividends (DataFrame): 특정 ETF의 배당 데이터

        Returns:
            str: 배당 주기 문자열 (yearly, half, quarter, month, week 또는 unknown)
        """
        # dividend_count 계산
        dividend_count = self._calculate_dividend_count(ticker_dividends)

        # 배당 주기 결정
        if dividend_count == 0:
            return None  # 배당 데이터 없음
        elif dividend_count <= 1.5:
            return "yearly"  # 연 1회 배당 (연간)
        elif dividend_count <= 2.5:
            return "half"  # 연 2회 배당 (반기)
        elif dividend_count <= 4.5:
            return "quarter"  # 연 4회 배당 (분기)
        elif dividend_count <= 13:
            return "month"  # 연 12회 배당 (월간)
        else:
            return "week"  # 연 52회 배당 (주간)


# 데이터 전처리
class ETFDataPreprocessor:
    def __init__(self):
        self.loader = ETFDataLoader()

    def info_data_preprocess(self, df: pd.DataFrame):
        """
        정보 데이터 전처리

        Args:
            df (pd.DataFrame): 정보 데이터

        Returns:
            pd.DataFrame: 전처리된 정보 데이터
        """
        # all_columns = ["ticker", "ctry", "market"]
        # select_columns = [col for col in all_columns if col in df.columns]
        # df_select = df[select_columns]
        pass

    def krx_data_preprocess(self, df: pd.DataFrame):
        select_columns_base = [
            "단축코드",
            "한글종목약명",
            # "영문종목명",
            "상장일_y",
            "기초지수명",
            "추적배수",
            "복제방법_y",
            "기초자산분류",
            "운용사_y",
            "총보수_y",
            "과세유형_y",
        ]
        select_columns_detail = ["추적오차", "괴리율", "변동성"]
        df_select = df[select_columns_base + select_columns_detail]

        # 컬럼명 변경
        df_select.rename(
            columns={
                "단축코드": "ticker",
                "한글종목약명": "kr_name",
                # "영문종목명": "en_name",
                "상장일_y": "listing_date",
                "기초지수명": "base_index_name",
                "추적배수": "tracking_multiplier",
                "복제방법_y": "replication_method",
                "기초자산분류": "base_asset_classification",
                "운용사_y": "manager",
                "총보수_y": "total_fee",
                "과세유형_y": "tax_type",
                "추적오차": "tracking_error",
                "괴리율": "disparity",
                "변동성": "risk_rating",
            },
            inplace=True,
        )

        # 티커 변경
        df_select["ticker"] = "A" + df_select["ticker"]

        # YYYY/MM/DD 에서 YYYY-MM-DD 로 변경
        df_select["listing_date"] = pd.to_datetime(df_select["listing_date"])

        # 추적 배수 변환
        df_select["tracking_multiplier"] = df_select["tracking_multiplier"].map(multiplier_map)

        # 복제 방법 변환
        df_select["replication_method"] = df_select["replication_method"].map(replication_map)

        # 기초 자산 분류 변환
        df_select["base_asset_classification"] = df_select["base_asset_classification"].map(base_asset_classification_map)

        # 변동성 변환
        df_select["risk_rating"] = df_select["risk_rating"].map(etf_risk_map)

        # 환헤지 여부
        df_select["is_hedge"] = df_select["kr_name"].str.contains(r"\(H\)$", regex=True)

        return df_select

    def morningstar_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        모닝스타 데이터 전처리
        """
        all_columns = ["ticker", "expense_ratio", "star_rating", "company_name"]
        select_columns = [col for col in all_columns if col in df.columns]
        if "ticker" not in df.columns:
            select_columns.append("ticker_x")
        if "expense_ratio" not in df.columns:
            select_columns.append("expense_ratio_x")
        if "star_rating" not in df.columns:
            select_columns.append("star_rating_x")
        if "company_name" not in df.columns:
            select_columns.append("company_name_x")
        df_select = df[select_columns]

        df_select = df_select.rename(
            columns={
                "ticker_x": "ticker",
                "expense_ratio_x": "expense_ratio",
                "star_rating_x": "star_rating",
                "company_name_x": "company_name",
            }
        )
        df_select = df_select.rename(
            columns={
                "expense_ratio": "total_fee",
                "star_rating": "risk_rating",
                "company_name": "manager",
            }
        )

        return df_select

    def factor_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        팩터 데이터 전처리

        Args:
            df (pd.DataFrame): 계산된 팩터 데이터
            ctry (str): 국가코드 (KR, US)

        Returns:
            pd.DataFrame: 전처리된 팩터 데이터
        """
        all_columns = [
            "Ticker",
            "Close_",
            "Bid",
            "Ask",
            "NumShrs",
            "거래대금",
            "marketCap",
            "momentum_1",
            "momentum_3",
            "momentum_6",
            "momentum_12",
            "ba_absolute_spread",
            "ba_relative_spread",
            "ba_spread_20d_avg",
            "ba_spread_20d_std",
            "ba_spread_1d_change",
            "ba_spread_20d_change",
            "disparity_5",
            "disparity_10",
            "disparity_20",
            "disparity_50",
            "disparity_100",
            "disparity_200",
            "vol_60",
            "vol",
            "rsi_9",
            "rsi_14",
            "rsi_25",
            "sharpe",
            "sortino",
            "return_1m",
            "return_3m",
            "return_6m",
            "return_1y",
            "week_52_high",
            "week_52_low",
            "drawdown_1y",
            "median_trade",
        ]

        # 실제 존재하는 컬럼만 선택
        select_columns = [col for col in all_columns if col in df.columns]
        df_select = df[select_columns]

        # 존재하는 컬럼만 이름 변경
        for old_col, new_col in etf_column_mapping.items():
            if old_col in df_select.columns:
                df_select.rename(columns={old_col: new_col}, inplace=True)

        # 티커 변경
        if ctry == "KR":
            df_select["ticker"] = df_select["ticker"].str.replace("^K", "A", regex=True)

        # 모든 데이터 소수점 2자리로 변경
        numeric_columns = [
            "trade_amount",
            "market_cap",
            "momentum_1",
            "momentum_3",
            "momentum_6",
            "momentum_12",
            "ba_relative_spread",
            "ba_spread_20d_avg",
            "ba_spread_20d_std",
            "ba_spread_1d_change",
            "ba_spread_20d_change",
            "disparity_5",
            "disparity_10",
            "disparity_20",
            "disparity_50",
            "disparity_100",
            "disparity_200",
            "vol_60",
            "vol",
            "rsi_9",
            "rsi_14",
            "rsi_25",
            "sharpe",
            "sortino",
            "return_1m",
            "return_3m",
            "return_6m",
            "return_1y",
            "drawdown_1y",
            "median_trade",
        ]

        for col in numeric_columns:
            if col in df_select.columns and df_select[col] is not None:
                df_select[col] = pd.to_numeric(df_select[col], errors="coerce")

        return df_select

    def dividend_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        배당 데이터 전처리

        Args:
            df (pd.DataFrame): 배당 데이터
        """
        pass

    def dividend_factor_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        배당 팩터 데이터 전처리
        """
        all_columns = [
            "ticker",
            "dividend_count",
            "dividend_frequency",
            "last_dividend_date",
            "last_dividend_per_share",
            "recent_dividend_yield",
            "ttm_dividend_yield",
            "dividend_growth_rate_3y",
            "dividend_growth_rate_5y",
        ]
        select_columns = [col for col in all_columns if col in df.columns]
        df_select = df[select_columns]

        if ctry == "KR":
            df_select["ticker"] = df_select["ticker"].str.replace("^K", "A", regex=True)

        df_select["last_dividend_date"] = pd.to_datetime(df_select["last_dividend_date"])

        # 모든 데이터 소수점 2자리로 변경
        numeric_columns = [
            "last_dividend_per_share",
            "recent_dividend_yield",
            "ttm_dividend_yield",
            "dividend_growth_rate_3y",
            "dividend_growth_rate_5y",
        ]
        for col in numeric_columns:
            if col in df_select.columns and df_select[col] is not None:
                df_select[col] = pd.to_numeric(df_select[col], errors="coerce")

        return df_select

    def etf_info_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        정보 데이터 전처리
        """
        if ctry == "KR":
            return df
        elif ctry == "US":
            country = "us"  # noqa
        else:
            raise ValueError(f"Invalid country: {ctry}")

        all_columns = ["ticker", "ctry", "market", "en_name"]
        select_columns = [col for col in all_columns if col in df.columns]
        df_select = df[select_columns]

        df_select["is_hedge"] = df_select["en_name"].str.contains(" H$", regex=True)
        return df_select


# def get_etf_price_from_kis():
#     """
#     Collect 'etf_cnfg_issu_cnt' data for ETF tickers from KIS API and save to CSV.

#     Retrieves ETF tickers from stock_information table, fetches data for each ticker
#     using the KIS API, and creates a CSV file with 'ticker' and 'etf_cnfg_issu_cnt' columns.
#     """
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
#     logger = logging.getLogger(__name__)

#     # Create ETFDataDownloader instance
#     # downloader = ETFDataDownloader()

#     # Get all ETF tickers from the database
#     logger.info("Fetching ETF tickers from database...")
#     etf_tickers = database._select(table="stock_information", columns=["ticker"], ctry="KR", type="etf")

#     if not etf_tickers:
#         logger.warning("No ETF tickers found in the database.")
#         return

#     tickers = [ticker.ticker for ticker in etf_tickers]
#     logger.info(f"Found {len(tickers)} ETF tickers.")

#     result_data = []
#     failed_tickers = []

#     for i, ticker in enumerate(tickers):
#         try:
#             logger.info(f"Processing ticker {i+1}/{len(tickers)}: {ticker}")

#             price_data = downloader.get_etf_price_from_kis(ticker)

#             if price_data and "etf_cnfg_issu_cnt" in price_data:
#                 etf_cnfg_issu_cnt = price_data["etf_cnfg_issu_cnt"]
#                 result_data.append({"ticker": ticker, "etf_cnfg_issu_cnt": etf_cnfg_issu_cnt})
#                 logger.info(f"Successfully collected data for {ticker}: etf_cnfg_issu_cnt = {etf_cnfg_issu_cnt}")
#             else:
#                 failed_tickers.append(ticker)
#                 logger.warning(f"Failed to retrieve etf_cnfg_issu_cnt for {ticker}")

#             time.sleep(0.5)

#         except Exception as e:
#             failed_tickers.append(ticker)
#             logger.error(f"Error processing ticker {ticker}: {str(e)}")

#     # Save the collected data to CSV
#     if result_data:
#         output_file = "etf_cnfg_issu_cnt_data.csv"

#         try:
#             with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
#                 fieldnames = ["ticker", "etf_cnfg_issu_cnt"]
#                 writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

#                 writer.writeheader()
#                 for row in result_data:
#                     writer.writerow(row)

#             logger.info(f"Successfully saved data to {output_file}")
#             logger.info(f"Collected data for {len(result_data)} tickers")

#             if failed_tickers:
#                 logger.warning(f"Failed to collect data for {len(failed_tickers)} tickers: {', '.join(failed_tickers)}")

#         except Exception as e:
#             logger.error(f"Error saving CSV file: {str(e)}")
#     else:
#         logger.warning("No data collected. CSV file not created.")

#     return output_file if result_data else None


class MorningstarETFCrawler:
    """
    모닝스타 웹사이트에서 ETF 정보를 크롤링하는 클래스
    """

    # 거래소 코드 매핑
    EXCHANGE_MAPPING = {
        "nys": ["arcx", "xnys"],  # NYSE는 arcx(Arca) 또는 xnys일 수 있음
        "nas": ["xnas"],  # NASDAQ
        "bats": ["bats"],  # BATS
    }

    def __init__(self, chrome_driver_path=None, headless=True, db_connector=None):
        """
        크롤러 초기화

        Args:
            chrome_driver_path (str, optional): 크롬 드라이버 경로. None이면 자동 설치.
            headless (bool): 브라우저 창 숨김 여부
            db_connector (obj, optional): 데이터베이스 연결 객체. None이면 CSV만 사용.
        """
        self.db = database
        self.driver = self._setup_driver(chrome_driver_path, headless)

    def _setup_driver(self, chrome_driver_path, headless):
        """셀레니움 웹드라이버 설정"""
        chrome_options = Options()

        if headless:
            chrome_options.add_argument("--headless")

        # 브라우저 성능 및 안정성 설정
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")

        # 봇 탐지 방지 설정
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        # 현대적인 사용자 에이전트 설정
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        if chrome_driver_path:
            service = Service(executable_path=chrome_driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)

        # 웹드라이버 탐지 방지 스크립트
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def get_etf_ticker(self, ctry):
        """
        데이터베이스에서 ETF 티커 목록을 가져옵니다.

        Args:
            ctry (str): 국가 코드 ('US' 또는 'KR')

        Returns:
            list: (티커, 거래소) 튜플의 리스트
        """
        # 데이터베이스 연결이 없는 경우 샘플 데이터 반환
        if not self.db:
            if ctry == "US":
                return [("SPY", "nys"), ("QQQ", "nas"), ("VTI", "nys"), ("IWM", "nys"), ("EEM", "nys")]
            else:
                return [("069500", "KRX"), ("229200", "KRX"), ("114800", "KRX")]

        # 데이터베이스에서 데이터 가져오기
        if ctry == "KR":
            country = "kr"
        elif ctry == "US":
            country = "us"
        else:
            raise ValueError(f"지원하지 않는 국가 코드: {ctry}")

        tickers = self.db._select(table="stock_information", columns=["ticker", "market"], ctry=country, type="etf")
        tickers = [(ticker.ticker, ticker.market) for ticker in tickers]
        return tickers

    def extract_star_rating(self):
        """
        현재 페이지에서 별점 정보를 추출합니다.

        Returns:
            str: 별점 정보 (숫자만) 또는 'N/A'
        """
        try:
            # 별점 요소가 로드될 때까지 대기 (최대 10초)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.mdc-security-header__star-rating"))
            )

            # 방법 1: title 속성으로 별점 확인
            star_span = self.driver.find_element(By.CSS_SELECTOR, "span.mdc-security-header__star-rating")
            rating_title = star_span.get_attribute("title")

            if rating_title and rating_title != "undefined":
                # "5 Stars"에서 숫자만 추출
                return rating_title.split()[0]  # "5 Stars" -> "5"

            # 방법 2: 별 SVG 아이콘 개수 세기
            star_svgs = self.driver.find_elements(
                By.CSS_SELECTOR, "span.mdc-security-header__star-rating svg.mdc-security-header__star"
            )
            if star_svgs:
                return str(len(star_svgs))

        except (TimeoutException, NoSuchElementException) as e:
            print(f"별점 추출 실패: {e}")

        return "N/A"

    def extract_fund_company(self):
        """
        현재 페이지에서 펀드 운용사 정보를 추출합니다.

        Returns:
            str: 운용사 이름 또는 'N/A'
        """
        try:
            # 페이지가 완전히 로드될 때까지 대기 (최대 20초)
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sal-dp-name")))

            # 'Firm Name' 레이블을 가진 요소 찾기
            firm_labels = self.driver.find_elements(By.CSS_SELECTOR, "div.sal-dp-name")
            for label in firm_labels:
                if "Firm Name" in label.text:
                    # 찾은 레이블의 부모 요소에서 값(sal-dp-value) 찾기
                    parent_div = label.find_element(By.XPATH, "./..")
                    value_div = parent_div.find_element(By.CSS_SELECTOR, "div.sal-dp-value")
                    company_name = value_div.text.strip()
                    print(f"운용사 정보 찾음: {company_name}")
                    return company_name

            # 위 방법으로 찾지 못한 경우 다른 방법 시도
            print("'Firm Name' 레이블을 찾지 못함, 대체 방법 시도...")

            # 모든 sal-dp-value 요소 중 첫 번째 것 시도
            value_divs = self.driver.find_elements(By.CSS_SELECTOR, "div.sal-dp-value")
            if value_divs:
                company_name = value_divs[0].text.strip()
                print(f"첫 번째 값 요소에서 운용사 정보 찾음: {company_name}")
                return company_name

            # 페이지 제목에서 추출 시도
            title = self.driver.title
            pattern = r"– Parent – (.*?) \| Morningstar"
            match = re.search(pattern, title)
            if match:
                company_name = match.group(1).strip()
                print(f"페이지 제목에서 운용사 정보 찾음: {company_name}")
                return company_name

        except (TimeoutException, NoSuchElementException) as e:
            print(f"운용사 정보 추출 실패: {e}")

        return "N/A"

    def extract_expense_ratio(self):
        """
        현재 페이지에서 비용 비율(Expense Ratio)을 추출합니다.

        Returns:
            str: 비용 비율 (% 기호 제외) 또는 'N/A'
        """
        try:
            # 페이지가 완전히 로드될 때까지 대기 (최대 20초)
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sal-dp-name")))

            # 'Expense Ratio' 레이블을 가진 요소 찾기
            expense_labels = self.driver.find_elements(By.CSS_SELECTOR, "div.sal-dp-name")
            for label in expense_labels:
                if "Expense Ratio" in label.text:
                    # 찾은 레이블의 부모 요소에서 값(sal-dp-value) 찾기
                    parent_div = label.find_element(By.XPATH, "./..")
                    value_div = parent_div.find_element(By.CSS_SELECTOR, "div.sal-dp-value")
                    expense_ratio_text = value_div.text.strip()

                    # % 기호 제거하고 숫자만 추출
                    expense_ratio = expense_ratio_text.replace("%", "")
                    print(f"비용 비율 정보 찾음: {expense_ratio} (원본: {expense_ratio_text})")
                    return expense_ratio

            print("'Expense Ratio' 레이블을 찾지 못함")

        except (TimeoutException, NoSuchElementException) as e:
            print(f"비용 비율 추출 실패: {e}")

        return "N/A"

    def get_etf_info(self, ctry, max_tickers=None, include_company=True):
        """
        ETF 별점 및 운용사 정보를 수집합니다.

        Args:
            ctry (str): 국가 코드 ('US' 또는 'KR')
            max_tickers (int, optional): 처리할 최대 티커 수. None이면 전체 수집.
            include_company (bool): 운용사 정보 수집 여부

        Returns:
            tuple: (결과 리스트, 저장된 파일명)
        """
        tickers = self.get_etf_ticker(ctry)
        if max_tickers:
            tickers = tickers[:max_tickers]

        results = []
        processed_count = 0

        # 파일 준비
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fieldnames = ["ticker", "exchange", "star_rating", "url", "country"]
        if include_company:
            fieldnames.insert(3, "company_name")

        filename = f"etf_ratings_{ctry}_{current_time}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        print(f"결과를 {filename}에 저장합니다.")

        for idx, (ticker, market) in enumerate(tickers):
            ticker = ticker.lower()
            if market.lower() not in self.EXCHANGE_MAPPING:
                continue

            market_exchanges = self.EXCHANGE_MAPPING[market.lower()]
            ticker_success = False

            print(f"처리 중... ({idx+1}/{len(tickers)}): {ticker.upper()} (거래소: {market.upper()})")

            for exchange in market_exchanges:
                if ticker_success:
                    break

                url = f"https://www.morningstar.com/etfs/{exchange}/{ticker}/parent"
                print(f"접속 중: {url}")

                try:
                    # 페이지 로드
                    self.driver.get(url)

                    # 페이지 로드 확인
                    try:
                        # 타이틀 확인
                        WebDriverWait(self.driver, 20).until(lambda driver: "Morningstar" in driver.title)
                        print(f"페이지 제목: {self.driver.title}")

                        # 콘텐츠 로드 확인
                        WebDriverWait(self.driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.sal-dp-value"))
                        )
                        print("페이지 컨텐츠가 완전히 로드되었습니다.")

                        # 페이지 오류 확인
                        if "Page Not Found" in self.driver.title or "Error" in self.driver.title:
                            print(f"페이지를 찾을 수 없음: {url}")
                            continue

                        # 별점 정보 추출
                        star_rating = self.extract_star_rating()
                        print(f"별점 정보: {star_rating}")

                        # 결과 저장 준비
                        result = {
                            "ticker": ticker.upper(),
                            "exchange": exchange,
                            "star_rating": star_rating,
                            "url": url,
                            "country": ctry,
                        }

                        # 필요하면 운용사 정보도 추출
                        if include_company:
                            company_name = self.extract_fund_company()
                            result["company_name"] = company_name

                        # 파일에 저장
                        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writerow(result)

                        results.append(result)
                        ticker_success = True
                        print(f"성공: {ticker.upper()} - {exchange}")

                    except TimeoutException:
                        print(f"페이지 로드 타임아웃: {url}")
                        continue

                except WebDriverException as e:
                    print(f"브라우저 오류 발생: {str(e)}")
                    continue

            # 모든 거래소 시도 후 실패 기록
            if not ticker_success:
                print(f"모든 거래소 시도 실패: {ticker.upper()}")

                # 실패 정보 저장
                with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    fail_record = {
                        "ticker": ticker.upper(),
                        "exchange": "N/A",
                        "star_rating": "N/A",
                        "url": "N/A",
                        "country": ctry,
                    }

                    if include_company:
                        fail_record["company_name"] = "N/A"

                    writer.writerow(fail_record)

            # 대기 시간 설정
            processed_count += 1
            wait_time = random.uniform(1, 3)
            print(f"{wait_time:.1f}초 대기 중...")
            time.sleep(wait_time)

            # 일정 간격마다 추가 대기
            if processed_count % 10 == 0:
                extra_wait = random.uniform(5, 8)
                print(f"{processed_count}개 요청 완료. 추가 {extra_wait:.1f}초 대기 중...")
                time.sleep(extra_wait)

                # 브라우저 메모리 관리
                self.driver.delete_all_cookies()

        print(f"총 {len(results)}/{len(tickers)} 티커의 정보를 수집했습니다.")
        return results, filename

    def get_etf_expense_ratios(self, ctry, max_tickers=None):
        """
        ETF 비용 비율 정보를 수집합니다.

        Args:
            ctry (str): 국가 코드 ('US' 또는 'KR')
            max_tickers (int, optional): 처리할 최대 티커 수. None이면 전체 수집.

        Returns:
            tuple: (결과 리스트, 저장된 파일명)
        """
        tickers = self.get_etf_ticker(ctry)
        if max_tickers:
            tickers = tickers[:max_tickers]

        results = []
        processed_count = 0

        # 파일 준비
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fieldnames = ["ticker", "exchange", "expense_ratio", "url", "country"]

        filename = f"etf_expense_ratios_{ctry}_{current_time}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        print(f"결과를 {filename}에 저장합니다.")

        for idx, (ticker, market) in enumerate(tickers):
            ticker = ticker.lower()
            if market.lower() not in self.EXCHANGE_MAPPING:
                continue

            market_exchanges = self.EXCHANGE_MAPPING[market.lower()]
            ticker_success = False

            print(f"처리 중... ({idx+1}/{len(tickers)}): {ticker.upper()} (거래소: {market.upper()})")

            for exchange in market_exchanges:
                if ticker_success:
                    break

                # Quote 페이지 접속
                url = f"https://www.morningstar.com/etfs/{exchange}/{ticker}/quote"
                print(f"접속 중: {url}")

                try:
                    # 페이지 로드
                    self.driver.get(url)

                    # 페이지 로드 확인
                    try:
                        # 타이틀 확인
                        WebDriverWait(self.driver, 20).until(lambda driver: "Morningstar" in driver.title)
                        print(f"페이지 제목: {self.driver.title}")

                        # 콘텐츠 로드 확인
                        WebDriverWait(self.driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.sal-dp-value"))
                        )
                        print("페이지 컨텐츠가 완전히 로드되었습니다.")

                        # 페이지 오류 확인
                        if "Page Not Found" in self.driver.title or "Error" in self.driver.title:
                            print(f"페이지를 찾을 수 없음: {url}")
                            continue

                        # 비용 비율 추출
                        expense_ratio = self.extract_expense_ratio()
                        print(f"비용 비율: {expense_ratio}")

                        # 결과 저장
                        result = {
                            "ticker": ticker.upper(),
                            "exchange": exchange,
                            "expense_ratio": expense_ratio,
                            "url": url,
                            "country": ctry,
                        }

                        # 파일에 저장
                        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writerow(result)

                        results.append(result)
                        ticker_success = True
                        print(f"성공: {ticker.upper()} - {exchange}")

                    except TimeoutException:
                        print(f"페이지 로드 타임아웃: {url}")
                        continue

                except WebDriverException as e:
                    print(f"브라우저 오류 발생: {str(e)}")
                    continue

            # 모든 거래소 시도 후 실패 기록
            if not ticker_success:
                print(f"모든 거래소 시도 실패: {ticker.upper()}")

                # 실패 정보 저장
                with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerow(
                        {
                            "ticker": ticker.upper(),
                            "exchange": "N/A",
                            "expense_ratio": "N/A",
                            "url": "N/A",
                            "country": ctry,
                        }
                    )

            # 대기 시간 설정
            processed_count += 1
            wait_time = random.uniform(1, 3)
            print(f"{wait_time:.1f}초 대기 중...")
            time.sleep(wait_time)

            # 일정 간격마다 추가 대기
            if processed_count % 10 == 0:
                extra_wait = random.uniform(5, 8)
                print(f"{processed_count}개 요청 완료. 추가 {extra_wait:.1f}초 대기 중...")
                time.sleep(extra_wait)

                # 브라우저 메모리 관리
                self.driver.delete_all_cookies()

        print(f"총 {len(results)}/{len(tickers)} 티커의 비용 비율 정보를 수집했습니다.")
        return results, filename

    def close(self):
        """브라우저 종료"""
        if self.driver:
            self.driver.quit()


class ETFDataMerger:
    """
    데이터 합치는 클래스
    """

    def __init__(self):
        self.loader = ETFDataLoader()
        self.preprocessor = ETFDataPreprocessor()

    def merge_data(
        self, ctry, factor=False, price=False, dividend_factor=False, info=False, krx=False, morningstar=False
    ):
        """
        데이터 합치기
        Args:
            ctry (str): 국가코드 (KR, US)
            factor (bool): 팩터 데이터 합치기 여부
        """
        df_merged = None
        if factor:
            # 데이터 가져오기
            df_factors = self.loader.load_factor(ctry)
            # 데이터 전처리
            df_factors = self.preprocessor.factor_data_preprocess(df_factors, ctry)
            # 데이터 합치기
            df_merged = df_factors if df_merged is None else pd.merge(df_merged, df_factors, on="ticker", how="left")
        if price:
            # 데이터 가져오기
            df_price = self.loader.load_etf_price(ctry)
            # 데이터 전처리
            #

            # 데이터 합치기
            df_merged = df_price if df_merged is None else pd.merge(df_merged, df_price, on="ticker", how="left")
        if dividend_factor:
            # 데이터 가져오기
            df_dividend_factor = self.loader.load_etf_dividend_factor(ctry)
            # 데이터 전처리
            df_dividend_factor = self.preprocessor.dividend_factor_data_preprocess(df_dividend_factor, ctry)
            # 데이터 합치기
            df_merged = (
                df_dividend_factor
                if df_merged is None
                else pd.merge(df_merged, df_dividend_factor, on="ticker", how="left")
            )
        if info:
            # 데이터 가져오기
            df_info = self.loader.load_etf_info(ctry)

            # 데이터 전처리
            df_info = self.preprocessor.etf_info_data_preprocess(df_info, ctry)

            # 데이터 합치기
            if df_info is not None:
                df_merged = df_info if df_merged is None else pd.merge(df_merged, df_info, on="ticker", how="left")
        if krx:
            # 데이터 가져오기
            df_krx = self.loader.load_krx(base=True, detail=True)
            # 데이터 전처리
            df_krx = self.preprocessor.krx_data_preprocess(df_krx)
            # 데이터 합치기
            df_merged = df_krx if df_merged is None else pd.merge(df_merged, df_krx, on="ticker", how="left")
        if morningstar:
            # 데이터 가져오기
            df_morningstar = self.loader.load_morningstar(is_expense=True, is_rating=True)
            # 데이터 전처리
            df_morningstar = self.preprocessor.morningstar_data_preprocess(df_morningstar, ctry)
            # 데이터 합치기

            df_merged = (
                df_morningstar if df_merged is None else pd.merge(df_merged, df_morningstar, on="ticker", how="left")
            )

        df_merged = df_merged.rename(columns={"ticker": "Code", "kr_name": "Name", "en_name": "Name"})
        df_merged["market"] = np.where(df_merged["market"] == "NYS", "NYSE", df_merged["market"])

        # 숫자로 변환 가능한 문자열을 숫자로 변환
        for col in df_merged.columns:
            # Code, Name 등 명시적으로 문자열인 컬럼은 제외
            if col not in ["Code", "Name"]:
                # 데이터 타입이 object인 경우에만 변환 시도
                if df_merged[col].dtype == "object":
                    # pd.to_numeric 함수를 사용하여 변환 가능한 값만 숫자로 변환
                    df_merged[col] = pd.to_numeric(df_merged[col], errors="ignore")

        return df_merged


# 사용 예시
if __name__ == "__main__":
    downloader = ETFDataDownloader()
    downloader.download_etf_dividend(ctry="KR", download=True)
