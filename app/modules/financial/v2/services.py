from ssl import DER_cert_to_PEM_cert
from typing import Optional, Dict, List, Tuple
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import statistics
import math

from fastapi import HTTPException

from app.core.logger import setup_logger
from app.database.crud import database, database_service
from app.modules.common.enum import TranslateCountry
from app.modules.common.schemas import BaseResponse
from app.modules.financial.v2.schemas import (
    IncomePerformanceResponse,
    QuarterlyIncome,
    IncomeMetric,
    DebtRatioResponse,
    LiquidityRatioResponse,
    InterestCoverageRatioResponse
    )
from app.modules.common.enum import FinancialCountry
from app.modules.common.utils import contry_mapping
from app.core.exception.custom import AnalysisException, DataNotFoundException, InvalidCountryException
from app.models.models_stock import StockInformation


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

            # stock_info = await self.data_db._select_async(table="stock_information",columns=[stock_name, sector_name, "ticker"], **{"ticker": ticker}, limit=1)
            stock_info = await self._get_stock_info_by_ticker(ticker)

            if not stock_info:
                ### 기본정보 로드
                logger.warning(f"Stock not found: {ticker}")
                raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")

            company_name = getattr(stock_info, stock_name)
            company_sector = getattr(stock_info, sector_name)

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

    async def get_financial_ratio(self, ctry: str, ticker: str, stock_info: StockInformation) -> Tuple[DebtRatioResponse, LiquidityRatioResponse, InterestCoverageRatioResponse]:
        country = FinancialCountry(ctry)

        if ctry == "USA":
            ticker = f"{ticker}-US"

        logger.info(f"get_financial_ratio: {ctry}, {ticker}")

        finpos_info = await self.data_db._select_async(
            table=f"{country.value}_finpos", 
            Code=ticker, 
            order="period_q", 
            ascending=False, 
            limit=4
        )

        income_info = await self.data_db._select_async(
            table=f"{country.value}_income", 
            Code=ticker, 
            order="period_q", 
            ascending=False, 
            limit=4
        )

        if not finpos_info:
            raise HTTPException(status_code=404, detail=f"Financial position data not found: {ticker}")
        elif len(finpos_info) < 4:
            raise HTTPException(status_code=404, detail=f"Financial position data not found: {ticker}")
        
        debt_ratio_data = await self._get_debt_ratio_data(country, ticker, finpos_info, stock_info)
        liquidity_ratio_data = await self._get_liquidity_ratio_data(country, ticker, finpos_info, stock_info)
        interest_coverage_ratio_data = await self._get_interest_coverage_ratio_data(country, ticker, income_info, stock_info)
        
        return debt_ratio_data, liquidity_ratio_data, interest_coverage_ratio_data


######################################################## 데이터 계산 헬퍼 메서드############################################################

    async def _get_stock_info_by_ticker(self, ticker: str) -> StockInformation:
        stock_info = await self.data_db._select_async(table="stock_information", ticker=ticker)
        if not stock_info:
            raise HTTPException(status_code=404, detail=f"Stock not found: {ticker}")
        return stock_info[0]

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


    async def _get_debt_ratio_data(self, country: FinancialCountry, ticker: str, finpos_info, stock_info) -> DebtRatioResponse:
        try:
            # 벡터화된 계산
            debt_ratios = [
                float((self._to_decimal(q.total_dept) / self._to_decimal(q.total_asset)) * 100)
                if self._to_decimal(q.total_asset) != 0
                else 0.0
                for q in finpos_info
            ]
            logger.info(f"debt_ratios: {debt_ratios}")
            # 평균 계산
            average_debt_ratio = self._round_and_clean(sum(debt_ratios) / len(debt_ratios))
            
        except TypeError:
            average_debt_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        # 산업 평균 조회
        industry_avg = await self._get_debt_ratio_industry_avg(country, ticker, stock_info)

        debt_ratio_response = DebtRatioResponse(
            ratio=average_debt_ratio, 
            industry_avg=industry_avg
        )

        return debt_ratio_response

    async def _get_liquidity_ratio_data(self, country: FinancialCountry, ticker: str, finpos_info, stock_info) -> LiquidityRatioResponse:
        try:
            # 벡터화된 계산
            liquidity_ratios = [
                float((self._to_decimal(q.current_asset) / self._to_decimal(q.current_dept)) * 100)
                if self._to_decimal(q.current_dept) != 0
                else 0.0
                for q in finpos_info
            ]
            logger.info(f"liquidity_ratios: {liquidity_ratios}")
            
            # 평균 계산
            average_liquidity_ratio = self._round_and_clean(sum(liquidity_ratios) / len(liquidity_ratios))
            
        except TypeError:
            average_liquidity_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        # 산업 평균 조회 (일단 0으로 설정, 나중에 구현)
        industry_avg = await self._get_liquidity_ratio_industry_avg(country, ticker, stock_info)

        liquidity_ratio_response = LiquidityRatioResponse(
            ratio=average_liquidity_ratio, 
            industry_avg=industry_avg
        )

        return liquidity_ratio_response

    async def _get_interest_coverage_ratio_data(self, country: FinancialCountry, ticker: str, income_info, stock_info) -> InterestCoverageRatioResponse:
        try:
            interest_coverage_ratios = [
                float(self._to_decimal(q.operating_income) / self._to_decimal(q.fin_cost))
                if self._to_decimal(q.fin_cost) != 0
                else 0.0
                for q in income_info
            ]
            logger.info(f"interest_coverage_ratios: {interest_coverage_ratios}")
            average_interest_coverage_ratio = self._round_and_clean(sum(interest_coverage_ratios) / len(interest_coverage_ratios))
            
        except TypeError:
            average_interest_coverage_ratio = 0.0  # TODO:: FE에서 None 받을 수 있는지 확인 후 'None' 반환 필요

        industry_avg = await self._get_interest_coverage_ratio_industry_avg(country, ticker, stock_info)
        interest_coverage_ratio_response = InterestCoverageRatioResponse(
            ratio=average_interest_coverage_ratio, 
            industry_avg=industry_avg
        )

        return interest_coverage_ratio_response

######################################################## 숫자 계산 ############################################################

    def _round_and_clean(self, value) -> float:
        """
        소수점 첫째자리에서 반올림하고, 소수점이 0이면 정수로 변환
        Decimal과 float 타입 모두 처리
        """
        # Decimal을 float로 변환
        if isinstance(value, Decimal):
            value = float(value)

        rounded = round(value, 1)
        # float의 is_integer() 메서드 사용
        return int(rounded) if float(rounded).is_integer() else rounded


    async def _get_debt_ratio_industry_avg(self, country: FinancialCountry, ticker: str, stock_info: StockInformation) -> float:
        """업종 평균 부채비율 조회 - 기존 메서드 사용"""
        try:
            clean_ticker = ticker.replace("-US", "") if country == FinancialCountry.USA else ticker
            is_usa = country == FinancialCountry.USA
            table_name = f"{country.value}_finpos"
            
            logger.info(f"Using get_financial_industry_avg_data for {ticker}, sector: {stock_info.sector_2}")
            
            industry_avg = await self.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=clean_ticker,
                is_usa=is_usa,
                ratio_type="debt"
            )
            
            logger.info(f"Industry average debt ratio: {industry_avg}")
            return self._round_and_clean(industry_avg)
            
        except Exception as e:
            logger.error(f"Error calculating industry average debt ratio: {e}")
            return 0.0


    async def _get_liquidity_ratio_industry_avg(self, country: FinancialCountry, ticker: str, stock_info: StockInformation) -> float:
        """업종 평균 부채비율 조회 - 기존 메서드 사용"""
        try:
            clean_ticker = ticker.replace("-US", "") if country == FinancialCountry.USA else ticker
            is_usa = country == FinancialCountry.USA
            table_name = f"{country.value}_finpos"
            
            logger.info(f"Using get_financial_industry_avg_data for {ticker}, sector: {stock_info.sector_2}")
            
            industry_avg = await self.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=clean_ticker,
                is_usa=is_usa,
                ratio_type="liquidity"
            )
            
            logger.info(f"Industry average debt ratio: {industry_avg}")
            return self._round_and_clean(industry_avg)
            
        except Exception as e:
            logger.error(f"Error calculating industry average debt ratio: {e}")
            return 0.0
    
    async def _get_interest_coverage_ratio_industry_avg(self, country: FinancialCountry, ticker: str, stock_info: StockInformation) -> float:
        """업종 평균 부채비율 조회 - 기존 메서드 사용"""
        try:
            clean_ticker = ticker.replace("-US", "") if country == FinancialCountry.USA else ticker
            is_usa = country == FinancialCountry.USA
            table_name = f"{country.value}_income"
            
            logger.info(f"Using get_financial_industry_avg_data for {ticker}, sector: {stock_info.sector_2}")
            
            industry_avg = await self.get_financial_industry_avg_data(
                table_name=table_name,
                base_ticker=clean_ticker,
                is_usa=is_usa,
                ratio_type="interest"
            )
            
            logger.info(f"Industry average debt ratio: {industry_avg}")
            return self._round_and_clean(industry_avg)
            
        except Exception as e:
            logger.error(f"Error calculating industry average debt ratio: {e}")
            return 0.0
    # async def _get_debt_ratio_industry_avg(self, country: FinancialCountry, ticker: str, stock_info: StockInformation) -> float:
    #     """업종 평균 부채비율 조회 - 같은 분기 비교"""
    #     try:
    #         clean_ticker = ticker.replace("-US", "") if country == FinancialCountry.USA else ticker
            
    #         if not stock_info.sector_2:
    #             return 0.0
            
    #         # 대상 주식의 최근 4분기 데이터 조회 (분기 정보 확인용)
    #         target_data = await self.data_db._select_async(
    #             table=f"{country.value}_finpos",
    #             Code=ticker,
    #             order="period_q",
    #             ascending=False,
    #             limit=4
    #         )
            
    #         if len(target_data) < 4:
    #             return 0.0
            
    #         # 대상 주식의 분기들 추출
    #         target_quarters = [q.period_q for q in target_data]
    #         logger.info(f"Target quarters for {ticker}: {target_quarters}")
            
    #         # 동일 섹터의 다른 종목들 조회 (자기 자신 제외)
    #         sector_tickers = await self.data_db._select_async(
    #             table="stock_information", 
    #             columns=["ticker"], 
    #             sector_2=stock_info.sector_2
    #         )
            
    #         if not sector_tickers:
    #             return 0.0
            
    #         # 자기 자신 제외 (v1과 동일)
    #         sector_tickers = [t for t in sector_tickers if t.ticker != clean_ticker]
            
    #         if not sector_tickers:
    #             return 0.0
            
    #         # 국가별 티커 처리
    #         if country == FinancialCountry.USA:
    #             sector_tickers_list = [f"{t.ticker}-US" for t in sector_tickers]
    #         else:
    #             sector_tickers_list = [t.ticker for t in sector_tickers]
            
    #         # 한 번의 쿼리로 모든 섹터 데이터 조회 (같은 분기만)
    #         all_sector_data = await self.data_db._select_async(
    #             table=f"{country.value}_finpos",
    #             Code__in=sector_tickers_list,
    #             period_q__in=target_quarters,  # 같은 분기만 조회
    #             order="period_q",
    #             ascending=False
    #         )
            
    #         if not all_sector_data:
    #             return 0.0
            
    #         # 회사별로 데이터 그룹화
    #         company_data = {}
    #         for row in all_sector_data:
    #             if row.Code not in company_data:
    #                 company_data[row.Code] = []
    #             company_data[row.Code].append(row)
            
    #         # 각 회사의 평균 계산 (같은 분기들만)
    #         company_averages = []
    #         for company_code, data in company_data.items():
    #             if len(data) >= 4:  # 4분기 이상 데이터가 있는 경우만
    #                 # 분기 순서대로 정렬
    #                 sorted_data = sorted(data, key=lambda x: x.period_q, reverse=True)
                    
    #                 company_ratios = [
    #                     float((self._to_decimal(q.total_dept) / self._to_decimal(q.total_asset)) * 100)
    #                     if self._to_decimal(q.total_asset) != 0
    #                     else 0.0
    #                     for q in sorted_data
    #                 ]
                    
    #                 if company_ratios:
    #                     company_avg = sum(company_ratios) / len(company_ratios)
    #                     if company_avg > 0:  # 양수인 경우만 포함
    #                         company_averages.append(company_avg)
    #                         logger.info(f"Company {company_code} avg debt ratio: {company_avg}")
            
    #         if not company_averages:
    #             logger.warning(f"No valid company averages found for sector {stock_info.sector_2}")
    #             return 0.0
            
    #         # 전체 섹터의 평균
    #         industry_avg = sum(company_averages) / len(company_averages)
    #         logger.info(f"Industry average debt ratio: {industry_avg} (from {len(company_averages)} companies)")
    #         return self._round_and_clean(industry_avg)
            
    #     except Exception as e:
    #         logger.error(f"Error calculating industry average debt ratio: {e}")
    #         return 0.0

    async def get_financial_industry_avg_data(
        self, table_name: str, base_ticker: str, is_usa: bool, ratio_type: str
    ) -> float:
        """업종 평균 재무비율 조회"""
        from sqlalchemy import text
        
        ratio_calculations = {
            "debt": """WHEN CAST(f.total_asset AS DECIMAL) != 0
                      THEN (CAST(f.total_dept AS DECIMAL) / CAST(f.total_asset AS DECIMAL)) * 100""",
            "liquidity": """WHEN CAST(f.current_dept AS DECIMAL) != 0
                           THEN (CAST(f.current_asset AS DECIMAL) / CAST(f.current_dept AS DECIMAL)) * 100""",
            "interest": """WHEN CAST(f.fin_cost AS DECIMAL) != 0
                            THEN CAST(f.operating_income AS DECIMAL) / CAST(f.fin_cost AS DECIMAL)""",
        }

        query = text(f"""
            WITH sector AS (
                SELECT sector_2
                FROM stock_information
                WHERE ticker = :base_ticker
            ),
            sector_companies AS (
                SELECT
                    CASE
                        WHEN :is_usa THEN CONCAT(si.ticker, '-US')
                        ELSE si.ticker
                    END AS ticker
                FROM stock_information si
                JOIN sector s ON si.sector_2 = s.sector_2
                WHERE si.ticker != :base_ticker
            ),
            company_ratios AS (
                SELECT
                    sc.ticker,
                    AVG(
                        CASE
                            {ratio_calculations[ratio_type]}
                            ELSE 0
                        END
                    ) as avg_ratio
                FROM sector_companies sc
                JOIN {table_name} f ON sc.ticker = f.Code
                GROUP BY sc.ticker
                HAVING COUNT(*) >= 4
            )
            SELECT ROUND(AVG(avg_ratio), 2) as industry_avg
            FROM company_ratios
            WHERE avg_ratio > 0
        """)

        try:
            logger.info(f"Executing industry avg query for {ratio_type}: base_ticker={base_ticker}, is_usa={is_usa}, table={table_name}")
            result = await self.data_db._execute_async(query, {"base_ticker": base_ticker, "is_usa": is_usa})
            industry_avg = result.scalar_one_or_none() or 0.0
            logger.info(f"Industry avg query result: {industry_avg}")
            return industry_avg
        except Exception as e:
            logger.error(f"업종 평균 {ratio_type} 비율 조회 중 오류 발생: {str(e)}")
            return 0.0



    def _to_decimal(self, value) -> Optional[Decimal]:
        """
        값을 Decimal로 변환하고 JSON 직렬화 가능한 값으로 처리
        소수점 2자리까지 반올림
        None이나 빈 값은 None으로 반환
        """
        try:
            if value is None or (isinstance(value, str) and not value.strip()):
                return None

            if isinstance(value, (float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return None
                if isinstance(value, Decimal) and value.is_nan():
                    return None
                if isinstance(value, float) and math.isinf(value):
                    return None
                if isinstance(value, Decimal) and value.is_infinite():
                    return None

            # 값을 Decimal로 변환하고 소수점 2자리로 반올림
            return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to convert value to Decimal: {value}")
            return Decimal("0.00")

def get_financial_service() -> FinancialService:
    return FinancialService()