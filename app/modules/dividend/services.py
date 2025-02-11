from app.modules.common.enum import Country, FinancialCountry
from app.modules.dividend.schemas import DividendItem, DividendDetail, DividendYearResponse
from app.database.crud import database
from app.modules.common.utils import contry_mapping
import pandas as pd
import os
import numpy as np


class DividendService:
    def __init__(self):
        self.db = database

    async def get_dividend(self, ctry: FinancialCountry, ticker: str) -> DividendItem:
        """배당 정보 조회"""
        table_name = f"stock_{ctry}_1d"

        # 배당 데이터 가져오기 (parquet 파일 사용)
        file_path = os.path.join("static", "dividend.parquet")
        df1 = pd.read_parquet(file_path)
        df1 = df1[df1["Ticker"] == ticker]

        # ticker가 없는 경우 체크
        if df1.empty:
            raise Exception(f"없는 ticker입니다: {ticker}")

        # 날짜에서 시간 정보 제거 (YYYY-MM-DD 형식으로 변환)
        df1["배당락일"] = df1["배당락일"].str[:10]  # 처음 10자리만 사용 (YYYY-MM-DD)
        df1["배당지급일"] = df1["배당지급일"].str[:10]

        # 주가 데이터 가져오기
        date_list = df1["배당락일"].tolist()
        columns = ["Ticker", "Date", "Close", "Market"]
        order = "Date"
        condition = {"Ticker": ticker, "Date__in": date_list}
        df = pd.DataFrame(self.db._select(table=table_name, columns=columns, order=order, **condition))

        # Date 컬럼도 문자열로 변환
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

        # 데이터프레임 병합
        df1 = df1.merge(df[["Date", "Close"]], left_on="배당락일", right_on="Date", how="left")
        df1 = df1.drop_duplicates(subset=["배당락일", "배당지급일", "배당금"])  # 중복 제거

        # 배당수익률 계산 (소수점 2자리까지)
        df1["dividend_yield"] = (df1["배당금"] / df1["Close"]) * 100
        df1["dividend_yield"] = df1["dividend_yield"].replace([np.inf, -np.inf, np.nan], 0).round(2)

        # DividendDetail 생성 (가장 최근 데이터 사용)
        detail = DividendDetail(
            ex_dividend_date=df1["배당락일"].iloc[-1],
            dividend_payment_date=df1["배당지급일"].iloc[-1],
            dividend_per_share=float(df1["배당금"].iloc[-1]),
            dividend_yield=float(df1["dividend_yield"].iloc[-1]),
        )

        # 배당락일 기준으로 연도 추출
        df1["year"] = pd.to_datetime(df1["배당락일"]).dt.year

        # 현재 연도 구하기
        current_year = pd.Timestamp.now().year

        # 6년 전 연도 계산 (현재 연도 포함 총 7년)
        df2 = df1[df1["year"] >= (current_year - 6)].copy()
        # 3년 전 연도 계산 (현재 연도 포함 총 4년)
        min_year = current_year - 3

        # 최근 4년 데이터만 필터링 (예: 2024~2021)
        df1 = df1[df1["year"] >= min_year]
        yearly_groups = df1.groupby("year")

        # 연도별 상세 정보 생성 (중복 없이)
        yearly_details = []
        for year, group in yearly_groups:
            dividend_details = []
            unique_group = group.sort_values(["배당락일", "배당지급일", "배당금"]).drop_duplicates(
                subset=["배당락일", "배당지급일", "배당금"]
            )
            for _, row in unique_group.iterrows():
                detail = DividendDetail(
                    ex_dividend_date=row["배당락일"],
                    dividend_payment_date=row["배당지급일"],
                    dividend_per_share=round(float(row["배당금"]), 2),
                    dividend_yield=round(float(row["dividend_yield"]), 2),
                )
                dividend_details.append(detail)

            yearly_details.append(DividendYearResponse(year=int(year), dividend_detail=dividend_details))

        # 배당지급일에서 년도와 월 추출 (YYYY-MM-DD 형식)
        df1["payment_year"] = df1["배당지급일"].str[:4].astype(int)  # YYYY
        df1["payment_month"] = df1["배당지급일"].str[5:7]  # MM

        # 현재 연도 구하기
        current_year = pd.Timestamp.now().year
        last_year = current_year - 1

        # 배당지급일 기준으로 작년 데이터 필터링
        last_year_data = df1[df1["payment_year"] == last_year]

        last_dividend_ratio = self.calculate_dividend_ratio(df1, ctry, ticker)
        last_dividend_ratio = round(last_dividend_ratio, 2) if last_dividend_ratio is not None else None
        last_dividend_growth_rate = self.calculate_growth_rate(df2, current_year)

        if last_dividend_growth_rate is None:
            last_dividend_growth_rate = None
        elif np.isnan(last_dividend_growth_rate) or np.isinf(last_dividend_growth_rate):
            last_dividend_growth_rate = None
        else:
            last_dividend_growth_rate = round(last_dividend_growth_rate, 2)

        return DividendItem(
            ticker=ticker,
            name=df["Market"].iloc[0],
            ctry=ctry,
            last_year_dividend_count=len(last_year_data),
            last_year_dividend_date=last_year_data["payment_month"].tolist(),
            last_dividend_per_share=round(float(df1["배당금"].iloc[-1]), 2),
            last_dividend_ratio=last_dividend_ratio,
            last_dividend_growth_rate=last_dividend_growth_rate,
            detail=sorted(yearly_details, key=lambda x: x.year, reverse=True),
        )

    def calculate_dividend_ratio(self, df, ctry: Country, ticker: str):
        """배당성향(Dividend Payout Ratio) 계산"""
        reverse_mapping = {v: k for k, v in contry_mapping.items()}
        ctry_three = reverse_mapping.get(ctry)

        table_name = f"{ctry_three}_stock_factors"

        if ctry_three == "USA":
            ticker = f"{ticker}-US"

        shares_data = self.db._select(table=table_name, columns=["shared_outstanding"], limit=1, **{"ticker": ticker})

        income_data = self.db._select(
            table=f"{ctry_three}_income",
            columns=["net_income"],
            limit=1,
            order="StmtDt",
            ascending=False,
            **{"Code": ticker},
        )

        if not shares_data or not income_data:
            return None

        latest_shares = shares_data[0][0]
        latest_net_income = income_data[0][0] * 1_000_000  # 백만 단위를 실제 금액으로 변환

        # 가장 최근 1주당 배당금
        latest_dividend_per_share = float(df["배당금"].iloc[-1])

        if latest_shares == 0:
            return 0.0

        # 주당순이익(EPS) 계산
        eps = latest_net_income / latest_shares  # 이제 단위가 맞음

        # 배당성향 = (1주당 배당금 / EPS) * 100
        return (latest_dividend_per_share / eps) * 100 if eps != 0 else None

    def calculate_growth_rate(self, df, current_year):
        """배당 성장률 계산"""
        latest_year = current_year - 1
        current_year_div = df[df["year"] == (latest_year)]["배당금"].sum()
        prev_year_div = df[df["year"] == (latest_year - 5)]["배당금"].sum()

        result = ((current_year_div - prev_year_div) ** (1 / 5)) - 1 if prev_year_div != 0 else None

        return result


def get_dividend_service():
    return DividendService()
