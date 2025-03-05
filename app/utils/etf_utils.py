import datetime
import os
import numpy as np
import pandas as pd
import pyodbc
import csv
import time
import logging

from app.database.crud import database
from app.common.mapping import (
    multiplier_map,
    replication_map,
    base_asset_classification_map,
    etf_column_mapping,
    etf_risk_map,
)
from app.core.config import settings
from app.modules.screener_etf.enum import ETFMarketEnum


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
            else:
                df_sort["marketCap"] = df_sort["MktCap"] / 1_000  # 단위 조정 / 달러 -> 천달러
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

        # 평균 거래대금
        df["median_trade"] = df.groupby(["Ticker"])["거래대금"].transform(lambda x: x.rolling(20).median())

        # 필요 없는 중간 계산 컬럼 제거
        df.drop(columns=["rolling_max_1y"], inplace=True)

        return df


class ETFDataDownloader:
    def __init__(self):
        self.refinitiv_server = settings.REFINITIV_SERVER
        self.refinitiv_database = settings.REFINITIV_DATABASE
        self.refinitiv_username = settings.REFINITIV_USERNAME
        self.refinitiv_password = settings.REFINITIV_PASSWORD

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

        cursor = conn.cursor()
        cursor.execute(query)

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
            SELECT
                c.Ticker as 'ticker',
                d.Desc_,
                a.DivRate as 'per_share', a.PayDate as 'payment_date', a.EffectiveDate as 'ex_date'
            from DS2Div a
            join DS2CtryQtInfo b
                on b.InfoCode = a.InfoCode
            JOIN
                Ds2MnemChg c
                ON c.InfoCode  = a.InfoCode
                AND c.EndDate = (
                    SELECT MAX(EndDate)
                    FROM Ds2MnemChg
                    WHERE InfoCode = a.InfoCode
                )
            JOIN
                DS2XRef d ON a.DivTypeCode = d.Code AND d.Type_ = 8
            where b.Region = 'us'
                and b.TypeCode ='ET'
                and b.StatusCode != 'D'
                and c.EndDate >= '2025-01-01'
                and (a.LicFlag = 1 or a.LicFlag = 128)
                and a.ISOCurrCode = 'USD'
            order by Ticker,PayDate
            ;
            """
        if ctry == "KR":
            query = """
            SELECT
                d.Desc_,
                a.DivRate as 'per_share', a.PayDate as 'payment_date', a.EffectiveDate as 'ex_date',
                b.DsLocalCode as 'ticker', b.DsQtName, b.Region,
                e.Close_ as 'price',
                f.PrimExchIntCode
            from DS2Div a
            join DS2CtryQtInfo b
                on b.InfoCode = a.InfoCode
            JOIN
                DS2XRef d
                ON a.DivTypeCode = d.Code AND d.Type_ = 8
            JOIN
                vw_Ds2Pricing e
                ON e.InfoCode = b.InfoCode
                AND e.MarketDate = a.EffectiveDate
            JOIN
                vw_Ds2SecInfo f
                ON f.InfoCode = b.InfoCode
            where b.Region = 'KR'
                and b.TypeCode ='ET'
                and b.StatusCode != 'D'
                and a.LicFlag = 8
                and a.ISOCurrCode = 'KRW'
                and e.MarketDate >='2020-01-01'
                and e.AdjType = 2
            order by b.DsLocalCode, a.PayDate
            """
        df = self._get_refinitiv_data(query)
        if download:
            if ctry == "KR":
                df.to_csv("/Users/kyungmin/git_repo/alpha-finder/check_data/etf/kr_etf_dividend.csv", index=False)
            elif ctry == "US":
                df.to_csv("/Users/kyungmin/git_repo/alpha-finder/check_data/etf/us_etf_dividend.csv", index=False)
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
        df = self._get_refinitiv_data(query)
        if download:
            df.to_csv(
                "/Users/kyungmin/git_repo/alpha-finder/check_data/etf/us_etf_price.csv", index=False
            )  # TODO :: parquet 파일로 변경
        return df

    def get_etf_price_from_kis(self, ticker: str):
        return self.kis_api.etf_price(ticker)


class ETFDataLoader:  # TODO :: parquet 파일로 변경
    def __init__(self):
        self.db = database

    def load_factor(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_factor.csv"
        base_dir = "/Users/kyungmin/git_repo/alpha-finder/check_data/etf"
        df = pd.read_csv(os.path.join(base_dir, file_name))
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
        file_name = f"{country}_etf_price.csv"
        base_dir = "/Users/kyungmin/git_repo/alpha-finder/check_data/etf"
        df = pd.read_csv(os.path.join(base_dir, file_name))
        return df

    def load_etf_dividend(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_dividend.csv"
        base_dir = "/Users/kyungmin/git_repo/alpha-finder/check_data/etf"
        df = pd.read_csv(os.path.join(base_dir, file_name))
        return df

    def load_etf_dividend_factor(self, ctry):
        country = "kr" if ctry == "KR" else "us"
        file_name = f"{country}_etf_dividend_factor.csv"
        base_dir = "/Users/kyungmin/git_repo/alpha-finder/check_data/etf"
        df = pd.read_csv(os.path.join(base_dir, file_name))
        return df

    def load_krx(self, base=False, detail=False):
        if not base and not detail:
            raise ValueError("base or detail must be True")
        if base:
            df_base = pd.read_csv(
                "/Users/kyungmin/git_repo/alpha-finder/check_data/etf_krx/data_base.csv", encoding="euc-kr"
            )
        if detail:
            df_detail = pd.read_csv(
                "/Users/kyungmin/git_repo/alpha-finder/check_data/etf_krx/data_detail.csv", encoding="euc-kr"
            )

        if base and detail:
            df_krx = pd.merge(df_base, df_detail, left_on="단축코드", right_on="종목코드", how="left")
        elif base or detail:
            df_krx = df_base if base else df_detail
        df_krx.to_csv("/Users/kyungmin/git_repo/alpha-finder/check_data/etf_krx/data_merged.csv", index=False)
        return df_krx

    def load_etf_factors(self, market_filter: ETFMarketEnum):
        df = pd.DataFrame()
        if market_filter in [ETFMarketEnum.US, ETFMarketEnum.NYSE, ETFMarketEnum.NASDAQ, ETFMarketEnum.BATS]:
            df = pd.read_parquet("static/us_etf_factors.parquet")
        elif market_filter == ETFMarketEnum.KR:
            df = pd.read_parquet("static/kr_etf_factors.parquet")
        else:
            raise ValueError(f"Invalid market: {market_filter}")
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
                dividend_count = self._calculate_dividend_frequency(dividend_group)
                recent_dividend_yield = self._calculate_recent_dividend_yield(dividend_group, current_price)
                dividend_growth_rate_3y = self._calculate_dividend_growth_rate(dividend_group, 3)
                dividend_growth_rate_5y = self._calculate_dividend_growth_rate(dividend_group, 5)

                # 최신 배당 정보
                if len(dividend_group) > 0:
                    latest_dividend = dividend_group.sort_values("payment_date", ascending=False).iloc[0]

                    results.append(
                        {
                            "ticker": ticker,
                            "dividend_count": dividend_count,
                            "last_dividend_date": latest_dividend["payment_date"],
                            "last_dividend_per_share": latest_dividend["per_share"],
                            "recent_dividend_yield": recent_dividend_yield,
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

    def _calculate_dividend_frequency(self, ticker_dividends):
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

            # 소수점 1자리로 반올림하여 반환
            return round(avg_yearly_payments, 1)
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
            "영문종목명",
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
                "영문종목명": "en_name",
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
        df_select["is_hedge"] = df_select["kr_name"].str.contains("\(H\)$", regex=True)

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
                df_select[col] = df_select[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

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
            "last_dividend_date",
            "last_dividend_per_share",
            "recent_dividend_yield",
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
            "dividend_growth_rate_3y",
            "dividend_growth_rate_5y",
        ]
        for col in numeric_columns:
            if col in df_select.columns and df_select[col] is not None:
                df_select[col] = pd.to_numeric(df_select[col], errors="coerce")
                df_select[col] = df_select[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

        return df_select

    def etf_info_data_preprocess(self, df: pd.DataFrame, ctry: str):
        """
        정보 데이터 전처리
        """
        if ctry == "KR":
            return None
        elif ctry == "US":
            country = "us"  # noqa
        else:
            raise ValueError(f"Invalid country: {ctry}")

        all_columns = ["ticker", "ctry", "market", "en_name"]
        select_columns = [col for col in all_columns if col in df.columns]
        df_select = df[select_columns]

        df_select["is_hedge"] = df_select["en_name"].str.contains(" H$", regex=True)
        return df_select


class ETFDataMerger:
    """
    데이터 합치는 클래스
    """

    def __init__(self):
        self.loader = ETFDataLoader()
        self.preprocessor = ETFDataPreprocessor()

    def merge_data(self, ctry, factor=False, price=False, dividend_factor=False, info=False, krx=False):
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
            if ctry == "US":
                df_info = self.preprocessor.etf_info_data_preprocess(df_info, ctry)
            # 데이터 합치기
            df_merged = df_info if df_merged is None else pd.merge(df_merged, df_info, on="ticker", how="left")
        if krx:
            # 데이터 가져오기
            df_krx = self.loader.load_krx(base=True, detail=True)
            # 데이터 전처리
            df_krx = self.preprocessor.krx_data_preprocess(df_krx)
            # 데이터 합치기
            df_merged = df_krx if df_merged is None else pd.merge(df_merged, df_krx, on="ticker", how="left")

        return df_merged


def get_etf_price_from_kis():
    """
    Collect 'etf_cnfg_issu_cnt' data for ETF tickers from KIS API and save to CSV.

    Retrieves ETF tickers from stock_information table, fetches data for each ticker
    using the KIS API, and creates a CSV file with 'ticker' and 'etf_cnfg_issu_cnt' columns.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    # Create ETFDataDownloader instance
    downloader = ETFDataDownloader()

    # Get all ETF tickers from the database
    logger.info("Fetching ETF tickers from database...")
    etf_tickers = database._select(table="stock_information", columns=["ticker"], ctry="KR", type="etf")

    if not etf_tickers:
        logger.warning("No ETF tickers found in the database.")
        return

    tickers = [ticker.ticker for ticker in etf_tickers]
    logger.info(f"Found {len(tickers)} ETF tickers.")

    result_data = []
    failed_tickers = []

    for i, ticker in enumerate(tickers):
        try:
            logger.info(f"Processing ticker {i+1}/{len(tickers)}: {ticker}")

            price_data = downloader.get_etf_price_from_kis(ticker)

            if price_data and "etf_cnfg_issu_cnt" in price_data:
                etf_cnfg_issu_cnt = price_data["etf_cnfg_issu_cnt"]
                result_data.append({"ticker": ticker, "etf_cnfg_issu_cnt": etf_cnfg_issu_cnt})
                logger.info(f"Successfully collected data for {ticker}: etf_cnfg_issu_cnt = {etf_cnfg_issu_cnt}")
            else:
                failed_tickers.append(ticker)
                logger.warning(f"Failed to retrieve etf_cnfg_issu_cnt for {ticker}")

            time.sleep(0.5)

        except Exception as e:
            failed_tickers.append(ticker)
            logger.error(f"Error processing ticker {ticker}: {str(e)}")

    # Save the collected data to CSV
    if result_data:
        output_file = "etf_cnfg_issu_cnt_data.csv"

        try:
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["ticker", "etf_cnfg_issu_cnt"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for row in result_data:
                    writer.writerow(row)

            logger.info(f"Successfully saved data to {output_file}")
            logger.info(f"Collected data for {len(result_data)} tickers")

            if failed_tickers:
                logger.warning(f"Failed to collect data for {len(failed_tickers)} tickers: {', '.join(failed_tickers)}")

        except Exception as e:
            logger.error(f"Error saving CSV file: {str(e)}")
    else:
        logger.warning("No data collected. CSV file not created.")

    return output_file if result_data else None


# if __name__ == "__main__":

