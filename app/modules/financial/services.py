import pandas as pd
from app.core.logging.config import get_logger
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, List, Tuple
from fastapi import HTTPException, Depends
import math

from app.database.crud import database
from app.modules.common.enum import FinancialCountry
from app.modules.common.services import CommonService, get_common_service
from app.modules.financial.schemas import (
    CashFlowResponse,
    FinPosDetail,
    FinPosResponse,
    FinancialRatioResponse,
    IncomePerformanceResponse,
    IncomeStatement,
    IncomeStatementDetail,
    CashFlowDetail,
    IncomeStatementResponse,
    InterestCoverageRatioResponse,
    LiquidityRatioResponse,
)
from app.modules.common.schemas import BaseResponse
from app.core.exception.custom import DataNotFoundException, InvalidCountryException, AnalysisException

logger = get_logger(__name__)


class FinancialService:
    def __init__(self, common_service: CommonService):
        self.db = database
        self.common_service = common_service
        self._setup_tables()

    def _setup_tables(self):
        """
        테이블 설정 - 국가 코드를 기반으로 동적으로 테이블 이름 생성
        """

        def create_table_mapping(table_type: str) -> Dict[FinancialCountry, str]:
            return {country: f"{country.value}_{table_type}" for country in FinancialCountry}

        self.income_tables = create_table_mapping("income")
        self.cashflow_tables = create_table_mapping("cashflow")
        self.finpos_tables = create_table_mapping("finpos")

    def _get_date_conditions(self, start_date: Optional[str], end_date: Optional[str]) -> Dict:
        """
        날짜 조건 생성
        start_date (Optional[str]): YYYYMM 형식의 시작일
        end_date (Optional[str]): YYYYMM 형식의 종료일
        기본값은 5년치 데이터를 조회
        """
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
            conditions["period_q__gte"] = f"{current_year - 5}01"  # 5년 전부터
            conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"  # 현재 연도의 마지막 분기
        else:
            conditions["period_q__gte"] = f"{start_date[:4]}01"  # 시작년도의 1월
            if end_date:
                conditions["period_q__lte"] = f"{end_date[:4]}{end_date[4:6]}"
            else:
                conditions["period_q__lte"] = f"{current_year}{latest_quarter_month}"

        return conditions

    def _to_decimal(self, value) -> Decimal:
        """
        값을 Decimal로 변환하고 JSON 직렬화 가능한 값으로 처리
        """
        try:
            if value is None or (isinstance(value, str) and not value.strip()):
                return Decimal("0")
            if isinstance(value, (float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return Decimal("0")
                if isinstance(value, Decimal) and value.is_nan():
                    return Decimal("0")
                if isinstance(value, float) and math.isinf(value):
                    return Decimal("0")
                if isinstance(value, Decimal) and value.is_infinite():
                    return Decimal("0")

            return Decimal(str(value))

        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to convert value to Decimal: {value}")
            return Decimal("0")

    ########################################## Router에서 호출하는 메서드 #########################################
    # 실적 데이터 조회
    async def get_income_performance_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[IncomePerformanceResponse]:
        """
        실적 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying income performance for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income performance data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="실적")

            quarterly_statements, yearly_statements = self._process_income_performance_statement_result(result)

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""

            performance_response = IncomePerformanceResponse(
                code=ticker, name=name, quarterly=quarterly_statements, yearly=yearly_statements
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

    # 손익계산서
    async def get_income_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 시계열 분석
        """
        logger.info(f"Starting income analysis for {ticker}")

        try:
            income_data = await self.get_income_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

            # data를 details로 변경
            if not income_data.data.details:
                logger.warning(f"No data found for ticker: {ticker}")
                return BaseResponse[IncomeStatementResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=IncomeStatementResponse(code=ticker, name="", details=[]),
                )

            logger.info(f"Successfully completed income analysis for {ticker}")
            return income_data

        except Exception as e:
            logger.error(f"Error during income analysis for {ticker}: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 시계열", detail=str(e))

    # 현금흐름표
    async def get_cashflow_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름 시계열 분석
        """
        try:
            cashflow_data = await self.get_cashflow_data(
                ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date
            )
            if not cashflow_data.data.details:
                logger.warning(f"No data found for ticker: {ticker}")
                return BaseResponse[CashFlowResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=CashFlowResponse(code=ticker, name="", ttm=CashFlowDetail(), details=[]),
                )

            logger.info(f"Successfully completed cashflow analysis for {ticker}")
            return cashflow_data

        except Exception as e:
            logger.error(f"Error during cashflow analysis for {ticker}: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="현금흐름표 시계열", detail=str(e))

    # 재무상태표
    async def get_finpos_analysis(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 시계열 분석
        """
        try:
            finpos_data = await self.get_finpos_data(ctry=ctry, ticker=ticker, start_date=start_date, end_date=end_date)

            if not finpos_data.data.details:
                return BaseResponse[FinPosResponse](
                    status_code=404,
                    message="데이터를 찾을 수 없습니다.",
                    data=FinPosResponse(code=ticker, name="", ttm=FinPosDetail(), details=[]),
                )

            logger.info(f"Successfully completed finpos analysis for {ticker}")
            return finpos_data

        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_timeseries_analysis: {str(e)}")
            raise HTTPException(status_code=500, detail="내부 서버 오류")

    # 재무비율
    async def get_financial_ratio(self, ctry: FinancialCountry, ticker: str) -> BaseResponse[FinancialRatioResponse]:
        """
        재무비율 조회
        """
        try:
            # finpos 테이블에서 조회
            financial_ratio_data = await self.get_financial_ratio_data(ctry, ticker)
            return financial_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_financial_ratio: {str(e)}")
            raise AnalysisException(analysis_type="재무비율 조회", detail=str(e))

    # 유동비율
    async def get_liquidity_ratio(self, ctry: FinancialCountry, ticker: str) -> BaseResponse[LiquidityRatioResponse]:
        """
        유동비율 조회
        """
        try:
            # finpos 테이블에서 조회
            liquidity_ratio_data = await self.get_liquidity_ratio_data(ctry, ticker)
            return liquidity_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_liquidity_ratio: {str(e)}")
            raise AnalysisException(analysis_type="유동비율 조회", detail=str(e))

    # 이자보상배율
    async def get_interest_coverage_ratio(
        self, ctry: FinancialCountry, ticker: str
    ) -> BaseResponse[InterestCoverageRatioResponse]:
        """
        이자보상배율 조회
        """
        try:
            # finpos 테이블에서 조회
            interest_coverage_ratio_data = await self.get_interest_coverage_ratio_data(ctry, ticker)
            return interest_coverage_ratio_data
        except Exception as e:
            logger.error(f"Unexpected error in get_interest_coverage_ratio: {str(e)}")
            raise AnalysisException(analysis_type="이자보상배율 조회", detail=str(e))

    ########################################## 데이터 조회 메서드 #########################################
    # 손익계산서
    async def get_income_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[IncomeStatementResponse]:
        """
        손익계산서 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying income data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No income data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="손익계산")

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""  # result[0][1]은 Name 컬럼의 값

            statements = self._process_income_statement_result(result)
            ttm = self._process_income_ttm_result(result)

            # IncomeStatementResponse 객체 생성
            income_statement_response = IncomeStatementResponse(code=ticker, name=name, ttm=ttm, details=statements)

            # BaseResponse 생성
            logger.info(f"Successfully retrieved income data for {ticker}")
            return BaseResponse[IncomeStatementResponse](
                status_code=200, message="손익계산서 데이터를 성공적으로 조회했습니다.", data=income_statement_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_income_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="손익계산서 조회", detail=str(e))

    # 현금흐름표
    async def get_cashflow_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[CashFlowResponse]:
        """
        현금흐름표 데이터 조회
        """
        try:
            table_name = self.cashflow_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying cashflow data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No cashflow data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="현금흐름")

            statements = self._process_cashflow_result(result)
            ttm = self._process_cashflow_ttm_result(result)

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""

            cashflow_response = CashFlowResponse(code=ticker, name=name, ttm=ttm, details=statements)

            logger.info(f"Successfully retrieved cashflow data for {ticker}")
            return BaseResponse[CashFlowResponse](
                status_code=200, message="현금흐름표 데이터를 성공적으로 조회했습니다.", data=cashflow_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_cashflow_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="현금흐름표 조회", detail=str(e))

    # 재무상태표
    async def get_finpos_data(
        self,
        ctry: FinancialCountry,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> BaseResponse[FinPosResponse]:
        """
        재무상태표 데이터 조회
        """
        try:
            table_name = self.finpos_tables.get(ctry)
            if not table_name:
                logger.warning(f"Invalid country code: {ctry}")
                raise InvalidCountryException()

            conditions = {"Code": ticker, **self._get_date_conditions(start_date, end_date)}

            logger.debug(f"Querying finpos data for {ticker} with conditions: {conditions}")
            result = self.db._select(table=table_name, order="period_q", ascending=False, **conditions)

            if not result:
                logger.warning(f"No finpos data found for ticker: {ticker}")
                raise DataNotFoundException(ticker=ticker, data_type="재무상태")

            statements = self._process_finpos_result(result)
            ttm = self._process_finpos_ttm_result(result)

            # DB 결과에서 직접 이름 추출
            name = result[0][1] if result else ""

            finpos_response = FinPosResponse(code=ticker, name=name, ttm=ttm, details=statements)

            logger.info(f"Successfully retrieved finpos data for {ticker}")
            return BaseResponse[FinPosResponse](
                status_code=200, message="재무상태표 데이터를 성공적으로 조회했습니다.", data=finpos_response
            )

        except (InvalidCountryException, DataNotFoundException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_finpos_data: {str(e)}", exc_info=True)
            raise AnalysisException(analysis_type="재무상태표 조회", detail=str(e))

    async def _get_latest_quarter(self, ctry: FinancialCountry, ticker: str) -> str:
        """
        가장 최근 분기 데이터 조회
        """
        try:
            table_name = self.income_tables.get(ctry)
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid country code")

            result = self.db._select(
                table=table_name, columns=["period_q"], order="period_q", ascending=False, limit=1, Code=ticker
            )

            if not result:
                raise HTTPException(status_code=404, detail=f"No data found for {ticker}")

            return result[0][0]

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting latest quarter: {e}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    ########################################## 계산 메서드 #########################################
    # 부채비율 계산
    async def get_financial_ratio_data(
        self, ctry: FinancialCountry, ticker: str
    ) -> Tuple[str, BaseResponse[FinancialRatioResponse]]:
        """
        재무비율 데이터 조회 - 부채비율 (최근 4분기 평균)
        부채비율 = (총부채 / 자기자본) * 100
        회사명도 함께 반환
        """
        table_name = self.finpos_tables.get(ctry)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {ctry}")
            raise InvalidCountryException()

        conditions = {"Code": ticker}
        result = self.db._select(table=table_name, order="period_q", ascending=False, limit=4, **conditions)

        if not result:
            logger.warning(f"재무비율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="재무비율")

        # 회사명 추출
        company_name = result[0].Name

        # 4분기 각각의 부채비율 계산
        debt_ratios = []
        for quarter in result:
            total_debt = self._to_decimal(quarter.total_dept)
            equity = self._to_decimal(quarter.equity)

            if equity != 0:
                quarter_ratio = float((total_debt / equity) * 100)
                debt_ratios.append(quarter_ratio)
            else:
                debt_ratios.append(0.0)

        # 4분기 평균 계산 및 소수점 2자리로 반올림
        average_debt_ratio = round(sum(debt_ratios) / len(debt_ratios), 2)

        # TODO: 업종 평균 Mock 데이터
        financial_ratio_response = FinancialRatioResponse(
            code=ticker, name=company_name, ratio=average_debt_ratio, industry_avg="23.5"
        )

        return company_name, BaseResponse[FinancialRatioResponse](
            status_code=200,
            message="부채비율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=financial_ratio_response,
        )

    # 유동비율 계산
    async def get_liquidity_ratio_data(self, ctry: FinancialCountry, ticker: str) -> BaseResponse[LiquidityRatioResponse]:
        """
        유동비율 데이터 조회 (최근 4분기 평균)
        유동비율 = (유동자산 / 유동부채) * 100
        """
        table_name = self.finpos_tables.get(ctry)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {ctry}")
            raise InvalidCountryException()

        conditions = {"Code": ticker}
        result = self.db._select(table=table_name, order="period_q", ascending=False, limit=4, **conditions)

        if not result:
            logger.warning(f"유동비율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="유동비율")

        if len(result) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="유동비율(4분기)")

        # 4분기 각각의 유동비율 계산
        liquidity_ratios = []
        for quarter in result:
            current_asset = self._to_decimal(quarter.current_asset)
            current_debt = self._to_decimal(quarter.current_dept)

            if current_debt != 0:
                quarter_ratio = float((current_asset / current_debt) * 100)
                liquidity_ratios.append(quarter_ratio)
            else:
                liquidity_ratios.append(0.0)

        # 4분기 평균 계산 및 소수점 2자리로 반올림
        average_liquidity_ratio = round(sum(liquidity_ratios) / len(liquidity_ratios), 2)

        # TODO: 업종 평균 Mock 데이터
        liquidity_ratio_response = LiquidityRatioResponse(
            code=ticker, name=result[0].Name, ratio=average_liquidity_ratio, industry_avg="17.4"
        )

        return BaseResponse[LiquidityRatioResponse](
            status_code=200,
            message="유동비율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=liquidity_ratio_response,
        )

    # 이자보상배율 계산
    async def get_interest_coverage_ratio_data(
        self, ctry: FinancialCountry, ticker: str
    ) -> BaseResponse[InterestCoverageRatioResponse]:
        """
        이자보상배율 데이터 조회 (최근 4분기 평균)
        이자보상배율 = 영업이익 / 금융비용
        """
        table_name = self.income_tables.get(ctry)
        if not table_name:
            logger.warning(f"잘못된 국가 코드: {ctry}")
            raise InvalidCountryException()

        conditions = {"Code": ticker}
        result = self.db._select(table=table_name, order="period_q", ascending=False, limit=4, **conditions)

        if not result:
            logger.warning(f"이자보상배율 데이터를 찾을 수 없습니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="이자보상배율")

        if len(result) < 4:
            logger.warning(f"4분기 데이터가 부족합니다: {ticker}")
            raise DataNotFoundException(ticker=ticker, data_type="이자보상배율(4분기)")

        # 4분기 각각의 이자보상배율 계산
        interest_coverage_ratios = []
        for quarter in result:
            operating_income = self._to_decimal(quarter.operating_income)
            fin_cost = self._to_decimal(quarter.fin_cost)

            if fin_cost != 0:
                quarter_ratio = float(operating_income / fin_cost)
                interest_coverage_ratios.append(quarter_ratio)
            else:
                interest_coverage_ratios.append(0.0)

        # 4분기 평균 계산 및 소수점 2자리로 반올림
        average_interest_coverage_ratio = round(sum(interest_coverage_ratios) / len(interest_coverage_ratios), 2)

        # TODO: 업종 평균 Mock 데이터
        interest_coverage_ratio_response = InterestCoverageRatioResponse(
            code=ticker, name=result[0].Name, ratio=average_interest_coverage_ratio, industry_avg="-12.7"
        )

        return BaseResponse[InterestCoverageRatioResponse](
            status_code=200,
            message="이자보상배율(4분기 평균) 데이터를 성공적으로 조회했습니다.",
            data=interest_coverage_ratio_response,
        )

    ########################################## ttm 메서드 #########################################
    # 손익계산서 ttm
    def _process_income_ttm_result(self, result) -> IncomeStatementDetail:
        """
        손익계산서 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return IncomeStatementDetail()

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번째 row에서 컬럼 추출 (exclude_columns 제외)
        exclude_columns = ["Code", "Name", "StmtDt"]  # period_q는 제외하지 않음
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_income_statement_detail(ttm_dict)

    # 현금흐름표 ttm
    def _process_cashflow_ttm_result(self, result) -> CashFlowDetail:
        """
        현금흐름표 ttm 결과 처리 - 모든 재무 항목에 대해 최근 12개월 합산
        """
        if not result:
            return CashFlowDetail()

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번재 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_cashflow_detail(ttm_dict)

    # 재무상태표 ttm
    def _process_finpos_ttm_result(self, result) -> FinPosDetail:
        """
        재무상태표 ttm 결과 처리 - 각 컬럼별로 최근 12개월 합산
        """
        if not result:
            return FinPosDetail()

        # 최근 12개월 데이터 선택
        recent_12_months = result[-12:]

        # 첫 번재 row에서 컬럼 추출
        exclude_columns = ["Code", "Name", "StmtDt"]
        first_row = recent_12_months[0]

        # TTM 계산을 위한 딕셔너리 초기화
        ttm_dict = {
            col: sum(self._to_decimal(getattr(row, col, 0)) for row in recent_12_months)
            for col, val in zip(first_row._fields, first_row)
            if col not in exclude_columns and col != "period_q"
        }

        # TTM 값에는 'TTM'이라고 표시
        ttm_dict["period_q"] = "TTM"

        return self._create_finpos_detail(ttm_dict)

    ########################################## 결과 처리 메서드 #########################################
    # 실적
    def _process_income_performance_statement_result(
        self, result, exclude_columns=["StmtDt"]
    ) -> Tuple[List[IncomeStatement], List[IncomeStatement]]:
        """
        실적 결과 처리 - 분기별 및 연도별 데이터를 분리하여 처리
        """
        if not result:
            return [], []

        # SQLAlchemy 결과를 DataFrame으로 변환
        df = pd.DataFrame([{col: val for col, val in zip(row._fields, row)} for row in result])

        # eps Mock 데이터
        df["eps"] = 100000000

        # 필요한 컬럼만 선택
        required_columns = [
            "Code",
            "Name",
            "period_q",
            "rev",
            "gross_profit",
            "operating_income",
            "net_income",
            "net_income_not_control",
            "net_income_total",
            "eps",
        ]
        df = df[required_columns]

        # 연도별 데이터 처리
        df["year"] = df["period_q"].astype(str).str[:4]
        agg_columns = [col for col in required_columns if col not in ["Code", "Name", "period_q"]]

        yearly_sum = df.groupby(["Code", "Name", "year"]).agg({col: "sum" for col in agg_columns}).reset_index()
        yearly_sum["period_q"] = yearly_sum["year"]
        yearly_sum = yearly_sum.drop("year", axis=1)

        # 분기별/연도별 statement 생성
        quarterly_statements = [self._create_comprehensive_income_statement(row.to_dict()) for _, row in df.iterrows()]
        yearly_statements = [
            self._create_comprehensive_income_statement(row.to_dict()) for _, row in yearly_sum.iterrows()
        ]

        # 정렬
        quarterly_statements.sort(key=lambda x: x.period_q, reverse=True)
        yearly_statements.sort(key=lambda x: x.period_q, reverse=True)

        return quarterly_statements, yearly_statements

    # 손익계산서
    def _process_income_statement_result(
        self, result, exclude_columns=["Code", "Name", "StmtDt"]
    ) -> List[IncomeStatementDetail]:
        """
        손익계산 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_income_statement_detail(row_dict))

        return statements

    # 현금흐름표
    def _process_cashflow_result(self, result, exclude_columns=["Code", "Name", "StmtDt"]) -> List[CashFlowDetail]:
        """
        현금흐름 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_cashflow_detail(row_dict))

        return statements

    # 재무상태표
    def _process_finpos_result(self, result, exclude_columns=["Code", "Name", "StmtDt"]) -> List[FinPosDetail]:
        """
        재무상태표 결과 처리
        """
        if not result:
            return []

        statements = []
        for row in result:
            # 제외할 컬럼들의 인덱스를 제외한 데이터만 사용
            row_dict = {col: val for col, val in zip(row._fields, row) if col not in exclude_columns}
            statements.append(self._create_finpos_detail(row_dict))

        return statements

    ########################################## 데이터 생성 메서드 #########################################
    # 실적
    def _create_comprehensive_income_statement(self, row_dict: Dict) -> IncomeStatement:
        """
        모든 실적 정보를 포함하는 통합 Statement 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                try:
                    values[field_name] = self._to_decimal(value)
                except Exception as e:
                    logger.warning(f"Error converting {field_name}: {str(e)}")
                    values[field_name] = Decimal("0")

        return IncomeStatement(**values)

    # 손익계산서
    def _create_income_statement_detail(self, row_dict: Dict) -> IncomeStatementDetail:
        """
        손익계산서 상세 정보 생성
        """
        # 컬럼명 매핑
        field_mapping = {"equity method gain": "equity_method_gain"}

        values = {}
        for field_name, value in row_dict.items():
            # period_q는 Decimal에서 str로 변환
            if field_name == "period_q":
                values[field_name] = str(value)
            # 필드명 매핑 적용
            elif field_name in field_mapping:
                values[field_mapping[field_name]] = self._to_decimal(value)
            else:
                values[field_name] = self._to_decimal(value)

        return IncomeStatementDetail(**values)

    # 현금흐름표
    def _create_cashflow_detail(self, row_dict: Dict) -> CashFlowDetail:
        """
        현금흐름 상세 정보 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                values[field_name] = self._to_decimal(value)

        return CashFlowDetail(**values)

    # 재무상태표
    def _create_finpos_detail(self, row_dict: Dict) -> FinPosDetail:
        """
        재무상태표 상세 정보 생성
        """
        values = {}
        for field_name, value in row_dict.items():
            if field_name == "period_q":
                values[field_name] = str(value)
            else:
                values[field_name] = self._to_decimal(value)

        return FinPosDetail(**values)


def get_financial_service(common_service: CommonService = Depends(get_common_service)) -> FinancialService:
    return FinancialService(common_service=common_service)
