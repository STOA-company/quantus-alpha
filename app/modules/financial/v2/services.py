from typing import Optional, Dict, List
from collections import defaultdict
from decimal import Decimal
import statistics

from fastapi import HTTPException

from app.core.logger import setup_logger
from app.database.crud import database, database_service
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.financial.v2.schemas import (
    IncomePerformanceResponse,
    QuarterlyIncome,
    IncomeMetric
    )
from app.modules.common.enum import FinancialCountry
from app.modules.common.utils import contry_mapping
from app.core.exception.custom import AnalysisException, DataNotFoundException, InvalidCountryException

logger = setup_logger(__name__)

class FinancialService:
    def __init__(self):
        self.db = database_service
        self.data_db = database

    async def get_income_performance_data(
        self, 
        ctry: str,
        ticker: str,
        lang: TranslateCountry,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        )-> BaseResponse[IncomePerformanceResponse]:


        try:
            country = FinancialCountry(ctry)
            date_conditions = await self._get_date_conditions_ten(start_date, end_date)

            ######################################################### 주식 기본정보 로드 #########################################################
            stock_name = "kr_name" if lang == TranslateCountry.KO else "en_name"
            sector_name = "sector_ko" if lang == TranslateCountry.KO else "sector_2"

            stock_info = await self.data_db._select_async(table="stock_information",columns=[stock_name, sector_name, "ticker"], **{"ticker": ticker}, limit=1)

            if not stock_info:
                ### 기본정보 로드
                logger.warning(f"Stock not found: {ticker}")
                raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")

            company_name, company_sector, _ = stock_info[0]

            if company_sector:
                sector_tickers = await self.data_db._select_async(table="stock_information", columns=["ticker"], **{sector_name: company_sector})
                sector_tickers = [row.ticker for row in sector_tickers]
            else:
                sector_tickers = [ticker]

            company_name = company_name if company_name else ticker

            if ctry == "USA":
                ticker = f"{ticker}-US"
                tickers_condition = [f"{t}-US" for t in sector_tickers]
            else:
                ticker = ticker
                tickers_condition = sector_tickers
            ##################################################################################################################################

            ######################################################### 발행수식 정보 로드 #########################################################
            country_enum = FinancialCountry(country)
            table_name = f"{country_enum.value}_stock_factors"

            stock_factors = await self.data_db._select_async(table=table_name, columns=["ticker", "shared_outstanding"], **{"ticker__in": tickers_condition}, limit=len(tickers_condition))
            if not stock_factors:
                logger.warning(f"Stock factors not found: {ticker}")
                raise HTTPException(status_code=404, detail=f"Stock factors not found: {ticker}")
            
            shared_outstanding = {}
            for row in stock_factors:
                shared_outstanding[row.ticker] = float(row.shared_outstanding) if row.shared_outstanding else 0.0 
            ##################################################################################################################################

            ######################################################### 실적테이블 생성 ############################################################
            table_name = f"{country.value}_income"
            income_conditions = {
                "Code__in": tickers_condition,
                **date_conditions,
            }
            income_data = await self.data_db._select_async(table=table_name, order="period_q", ascending=False, **income_conditions)

            if not income_data:
                logger.warning(f"Income data not found: {ticker}")
                raise HTTPException(status_code=404, detail=f"Income data not found: {ticker}")
            ##################################################################################################################################


            ######################################################### 계산 로직 생성 ############################################################

            quarterly_statements = await self._process_income_performance_quarterly_result(
                income_data, ticker, ctry, tickers_condition, shared_outstanding
            )
            yearly_statements = await self._process_income_performance_yearly_result(
                income_data, ticker, ctry, tickers_condition, shared_outstanding
            )
            ##################################################################################################################################
            
            ctry = contry_mapping.get(ctry)

            performance_response = IncomePerformanceResponse(
                code=ticker,
                name=company_name,
                ctry=ctry,
                sector=company_sector if company_sector else "",
                quarterly=quarterly_statements,
                yearly=yearly_statements,
            )

            logger.info(f"Successfully retrieved income performance data for {ticker}")
            return BaseResponse[IncomePerformanceResponse](
                status_code=200, message="실적 데이터를 성공적으로 조회했습니다.", data=performance_response
            )
        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_performance_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="실적 조회", detail=str(e))


######################################################## 데이터 계산 헬퍼 메서드############################################################

    async def _get_date_conditions_ten(self, start_date: Optional[str], end_date: Optional[str]) -> Dict:
        from datetime import datetime

        conditions = {}
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # 현재 월에 따른 가장 최근 분기 말월 계산
        latest_quarter_month = ((current_month - 1) // 3) * 3
        if latest_quarter_month == 0:
            latest_quarter_month = 12
            current_year -= 1
        latest_quarter_month = str(latest_quarter_month).zfill(2)  # 한 자리 월을 두 자리로 변환

        if not start_date:
            # 분기별 데이터는 2.5년(10분기)치, 연간 데이터는 10년치 조회를 위해 10년 전부터 데이터 조회
            conditions["period_q__gte"] = f"{current_year - 10}01"  # 10년 전부터
            conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"  # 현재 연도의 마지막 분기
        else:
            conditions["period_q__gte"] = f"{start_date[:4]}01"  # 시작년도의 1월
            if end_date:
                conditions["period_q__lte"] = f"{end_date[:4]}{end_date[4:6]}"
            else:
                conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"

        return conditions


        # 분기 실적
    
    async def _process_income_performance_quarterly_result(
        self, result, ticker, ctry, sector_tickers, all_shares
    ) -> List[QuarterlyIncome]:
        if not result:
            return []

        tickers = sector_tickers
        company_shares = all_shares.get(ticker, 0)

        # 회사 데이터와 섹터 데이터 분리
        company_data = defaultdict(dict)
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            # 국가별 티커 처리
            if ctry == "USA":
                clean_row_ticker = row_ticker.replace("-US", "")
                shares_ticker = row_ticker
                multiplier = 1000
            else:
                clean_row_ticker = row_ticker
                shares_ticker = row_ticker
                multiplier = 100000000

            period = row[2]

            if row_ticker == ticker:
                company_data[period] = {
                    "rev": float(row[4]) if row[4] is not None else 0.0,
                    "operating_income": float(row[9]) if row[9] is not None else 0.0,
                    "net_income_total": float(row[18]) if row[18] is not None else 0.0,
                }

            if clean_row_ticker in tickers:
                shares = all_shares.get(shares_ticker, 0)
                net_income = float(row[18]) if row[18] is not None else 0.0
                # 국가별 단위에 맞춰 EPS 계산
                eps = (net_income * multiplier) / shares if shares > 0 else 0.0

                sector_data[period]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[period]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[period]["net_income_total"].append(net_income)
                sector_data[period]["eps"].append(eps)

        # 섹터 평균 계산
        sector_averages = {}
        for period, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income_total"]:
                sector_averages[period] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income_total": statistics.mean(values["net_income_total"])
                    if values["net_income_total"]
                    else 0.0,
                    "eps": statistics.mean(values["eps"])  # 각 회사 EPS의 평균 계산
                    if values["eps"]
                    else 0.0,
                }

        quarterly_results = []
        for period in sorted(company_data.keys(), reverse=True):
            company_values = company_data[period]

            # 회사의 EPS 계산도 국가별 단위에 맞춤
            multiplier = 100000000 if ctry != "USA" else 1000
            eps_company = (
                (company_values["net_income_total"] * multiplier) / company_shares if company_shares > 0 else 0.0
            )

            quarterly_income = QuarterlyIncome(
                period_q=period,
                rev=IncomeMetric(
                    company=Decimal(str(company_values["rev"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("rev", 0.0))),
                ),
                operating_income=IncomeMetric(
                    company=Decimal(str(company_values["operating_income"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("operating_income", 0.0))),
                ),
                net_income=IncomeMetric(
                    company=Decimal(str(company_values["net_income_total"])),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("net_income_total", 0.0))),
                ),
                eps=IncomeMetric(
                    company=Decimal(str(eps_company)),
                    industry_avg=Decimal(str(sector_averages.get(period, {}).get("eps", 0.0))),
                ),
            )
            quarterly_results.append(quarterly_income)

        # 최근 10분기 데이터만 사용
        quarterly_results = quarterly_results[:10]

        return quarterly_results

    # 연간 실적
    async def _process_income_performance_yearly_result(
        self, result, ticker, ctry, sector_tickers, all_shares
    ) -> List[QuarterlyIncome]:
        if not result:
            return []

        # count 키 추가
        company_data = defaultdict(
            lambda: {
                "rev": 0.0,
                "operating_income": 0.0,
                "net_income_total": 0.0,
                "count": 0,
            }
        )

        for row in result:
            year = row[2][:4]
            if row[0] == ticker:
                company_data[year]["rev"] += float(row[4]) if row[4] is not None else 0.0
                company_data[year]["operating_income"] += float(row[9]) if row[9] is not None else 0.0
                company_data[year]["net_income_total"] += float(row[18]) if row[18] is not None else 0.0
                company_data[year]["count"] += 1

        # shares 조회 결과 확인
        tickers = sector_tickers
        company_shares = all_shares.get(ticker, 0)

        # 회사 데이터와 섹터 데이터 분리
        sector_data = defaultdict(lambda: defaultdict(list))

        for row in result:
            row_ticker = row[0]
            # 국가별 티커 처리
            if ctry == "USA":
                clean_row_ticker = row_ticker.replace("-US", "")
                shares_ticker = row_ticker
                multiplier = 1000  # 백만 달러 -> 천 달러
            else:
                clean_row_ticker = row_ticker
                shares_ticker = row_ticker
                multiplier = 100000000  # 억원 -> 원

            year = row[2][:4]  # 연도만 추출

            # 섹터 데이터 수집 (중복 제거)
            if clean_row_ticker in tickers:
                shares = all_shares.get(shares_ticker, 0)
                net_income = float(row[18]) if row[18] is not None else 0.0
                eps = (net_income * multiplier) / shares if shares > 0 else 0.0

                sector_data[year]["rev"].append(float(row[4]) if row[4] is not None else 0.0)
                sector_data[year]["operating_income"].append(float(row[9]) if row[9] is not None else 0.0)
                sector_data[year]["net_income_total"].append(net_income)
                sector_data[year]["eps"].append(eps)

        # 섹터 평균 계산
        sector_averages = {}
        for year, values in sector_data.items():
            if values["rev"] or values["operating_income"] or values["net_income_total"]:
                sector_averages[year] = {
                    "rev": statistics.mean(values["rev"]) if values["rev"] else 0.0,
                    "operating_income": statistics.mean(values["operating_income"])
                    if values["operating_income"]
                    else 0.0,
                    "net_income_total": statistics.mean(values["net_income_total"])
                    if values["net_income_total"]
                    else 0.0,
                    "eps": statistics.mean(values["eps"]) if values["eps"] else 0.0,
                }

        # 회사 데이터 연간 평균 계산
        for year, data in company_data.items():
            if data["count"] > 0:
                company_data[year]["rev"] /= data["count"]
                company_data[year]["operating_income"] /= data["count"]
                company_data[year]["net_income_total"] /= data["count"]

        yearly_results = []
        for year in sorted(company_data.keys(), reverse=True):
            company_values = company_data[year]

            # 국가별 단위에 맞춰 EPS 계산
            multiplier = 100000000 if ctry != "USA" else 1000
            eps_company = (
                (company_values["net_income_total"] * multiplier) / company_shares if company_shares > 0 else 0.0
            )

            yearly_income = QuarterlyIncome(
                period_q=year,
                rev=IncomeMetric(
                    company=Decimal(str(company_values["rev"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("rev", 0.0))),
                ),
                operating_income=IncomeMetric(
                    company=Decimal(str(company_values["operating_income"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("operating_income", 0.0))),
                ),
                net_income=IncomeMetric(
                    company=Decimal(str(company_values["net_income_total"])),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("net_income_total", 0.0))),
                ),
                eps=IncomeMetric(
                    company=Decimal(str(eps_company)),
                    industry_avg=Decimal(str(sector_averages.get(year, {}).get("eps", 0.0))),
                ),
            )
            yearly_results.append(yearly_income)

        # 최근 10년치 데이터만 사용
        yearly_results = yearly_results[:10]

        return yearly_results


def get_financial_service() -> FinancialService:
    return FinancialService()